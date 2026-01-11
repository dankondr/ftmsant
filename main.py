import asyncio
import itertools
import os
import signal
import sys
import threading
import time
from collections import deque
from typing import Optional

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakBluetoothNotAvailableError
from loguru import logger
from openant.easy.channel import Channel
from openant.easy.node import Node
from pycycling.fitness_machine_service import FitnessMachineService

ANTPLUS_NETWORK = 0
ANTPLUS_KEY = list(bytes.fromhex("B9A521FBBD72C345"))

DEVICE_NAME = "UnixFit SB-700"
DEVICE_NUMBER = 700
DEVICE_TYPE = 0x11
TRANSMISSION_TYPE = 0x05

RF_FREQ = 57
CHANNEL_PERIOD = 8192  # 4 Hz

FTMS_DEVICE_NAME = os.getenv("FTMS_DEVICE_NAME", "SB-700")
FTMS_DEVICE_ADDRESS = os.getenv("FTMS_DEVICE_ADDRESS")
FTMS_SERVICE_UUID = "00001826-0000-1000-8000-00805f9b34fb"
FTMS_SCAN_TIMEOUT_S = 8.0
FTMS_RECONNECT_DELAY_S = 2.0
FTMS_CONTROL_DELAY_S = 0.2

FE_TYPE_TRAINER = 25
FE_STATE_READY = 2
FE_STATE_IN_USE = 3

OUTLIER_WINDOW = 3
OUTLIER_RESET_S = 3.0


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


SPEED_MAX_UP_MPS_S = _env_float("SPEED_MAX_UP_MPS_S", 2.5)
CADENCE_MAX_UP_RPM_S = _env_float("CADENCE_MAX_UP_RPM_S", 40.0)
POWER_MAX_UP_W_S = _env_float("POWER_MAX_UP_W_S", 300.0)


class RollingMedianFilter:
    def __init__(self, window: int) -> None:
        self.window = window
        self.values = deque(maxlen=window)

    def reset(self) -> None:
        self.values.clear()

    def update(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        self.values.append(float(value))
        if len(self.values) < self.window:
            return float(value)
        sorted_values = sorted(self.values)
        return sorted_values[len(sorted_values) // 2]


class SlewRateLimiter:
    def __init__(self, max_up_per_s: float) -> None:
        self.max_up_per_s = max_up_per_s
        self.last_value: Optional[float] = None
        self.last_time: Optional[float] = None

    def reset(self) -> None:
        self.last_value = None
        self.last_time = None

    def update(self, value: Optional[float], now: float) -> Optional[float]:
        if value is None:
            return None
        value = float(value)
        if self.last_value is None or self.last_time is None:
            self.last_value = value
            self.last_time = now
            return value
        dt = max(now - self.last_time, 0.0)
        if dt <= 0.0:
            return self.last_value
        if self.max_up_per_s > 0 and value > self.last_value:
            max_value = self.last_value + self.max_up_per_s * dt
            if value > max_value:
                value = max_value
        self.last_value = value
        self.last_time = now
        return value


class BridgeState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.speed_mps: Optional[float] = None
        self.cadence_rpm: Optional[int] = None
        self.instant_power: Optional[int] = None
        self.distance_m: Optional[int] = None
        self.elapsed_time_s: Optional[int] = None
        self.last_ftms_update: Optional[float] = None
        self.speed_limiter = SlewRateLimiter(SPEED_MAX_UP_MPS_S)
        self.cadence_limiter = SlewRateLimiter(CADENCE_MAX_UP_RPM_S)
        self.power_limiter = SlewRateLimiter(POWER_MAX_UP_W_S)
        self.speed_filter = RollingMedianFilter(OUTLIER_WINDOW)
        self.cadence_filter = RollingMedianFilter(OUTLIER_WINDOW)
        self.power_filter = RollingMedianFilter(OUTLIER_WINDOW)


def clamp_int(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def compute_fe_state(cadence_rpm: Optional[int], power_w: Optional[int]) -> int:
    if (cadence_rpm or 0) > 0 or (power_w or 0) > 0:
        return FE_STATE_IN_USE
    return FE_STATE_READY


def build_page_10(
    elapsed_time: int,
    speed_mps: Optional[float],
    distance_m: Optional[int],
    fe_state: int,
) -> list[int]:
    if speed_mps is None:
        speed = 0xFFFF
    else:
        speed = int(max(0.0, speed_mps) * 1000.0) & 0xFFFF
    if distance_m is None:
        distance = 0xFF
        capabilities = 0x00
    else:
        distance = distance_m & 0xFF
        capabilities = 1 << 2
    fe_state = (fe_state & 0x0F) << 4
    return [
        0x10,
        FE_TYPE_TRAINER & 0xFF,
        elapsed_time & 0xFF,
        distance,
        speed & 0xFF,
        (speed >> 8) & 0xFF,
        0xFF,
        capabilities | fe_state,
    ]


def build_page_19(
    event: int,
    cadence_rpm: Optional[int],
    accumulated_power: int,
    instant_power: Optional[int],
    fe_state: int,
) -> list[int]:
    if cadence_rpm is None:
        cadence = 0xFF
    else:
        cadence = clamp_int(int(round(cadence_rpm)), 0, 254)
    if instant_power is None:
        instant = 0x0FFF
        accumulated = 0xFFFF
    else:
        instant = clamp_int(int(round(instant_power)), 0, 0x0FFE)
        accumulated = accumulated_power & 0xFFFF
    trainer_status = 0x00
    flags = 0x00
    fe_state = (fe_state & 0x0F) << 4
    return [
        0x19,
        event & 0xFF,
        cadence & 0xFF,
        accumulated & 0xFF,
        (accumulated >> 8) & 0xFF,
        instant & 0xFF,
        ((trainer_status & 0x0F) << 4) | ((instant >> 8) & 0x0F),
        (fe_state & 0xF0) | (flags & 0x0F),
    ]


def update_state_from_ftms(state: BridgeState, data) -> None:
    if data is None:
        return
    if not isinstance(data, dict):
        if hasattr(data, "_asdict"):
            data = data._asdict()
        else:
            data = {
                "instant_speed": getattr(data, "instant_speed", None),
                "instant_cadence": getattr(data, "instant_cadence", None),
                "total_distance": getattr(data, "total_distance", None),
                "instant_power": getattr(data, "instant_power", None),
                "elapsed_time": getattr(data, "elapsed_time", None),
            }
    now = time.monotonic()
    with state.lock:
        last_update = state.last_ftms_update
        state.last_ftms_update = now
        if last_update is not None and now - last_update > OUTLIER_RESET_S:
            state.speed_limiter.reset()
            state.cadence_limiter.reset()
            state.power_limiter.reset()
            state.speed_filter.reset()
            state.cadence_filter.reset()
            state.power_filter.reset()
        if data.get("instant_speed") is not None:
            speed_mps = float(data["instant_speed"]) / 3.6
            limited_speed = state.speed_limiter.update(speed_mps, now)
            filtered_speed = state.speed_filter.update(limited_speed)
            if filtered_speed is not None:
                state.speed_mps = filtered_speed
        if data.get("instant_cadence") is not None:
            cadence = float(data["instant_cadence"])
            limited_cadence = state.cadence_limiter.update(cadence, now)
            filtered_cadence = state.cadence_filter.update(limited_cadence)
            if filtered_cadence is not None:
                state.cadence_rpm = int(round(filtered_cadence))
        if data.get("instant_power") is not None:
            power = float(data["instant_power"])
            limited_power = state.power_limiter.update(power, now)
            filtered_power = state.power_filter.update(limited_power)
            if filtered_power is not None:
                state.instant_power = int(round(filtered_power))
        if data.get("total_distance") is not None:
            state.distance_m = int(data["total_distance"])
        if data.get("elapsed_time") is not None:
            state.elapsed_time_s = int(data["elapsed_time"])


def run_ant_bridge(state: BridgeState, stop_event: threading.Event) -> None:
    node = Node()
    node.set_network_key(ANTPLUS_NETWORK, ANTPLUS_KEY)

    ch = node.new_channel(Channel.Type.BIDIRECTIONAL_TRANSMIT)
    ch.set_id(DEVICE_NUMBER, DEVICE_TYPE, TRANSMISSION_TYPE)
    ch.set_period(CHANNEL_PERIOD)
    ch.set_rf_freq(RF_FREQ)
    ch.open()

    logger.info(
        "ANT+ channel open: name={} dev={} type=0x{:02X} tx=0x{:02X}",
        DEVICE_NAME,
        DEVICE_NUMBER,
        DEVICE_TYPE,
        TRANSMISSION_TYPE,
    )

    dispatch_thread = threading.Thread(
        target=node.start, name="ant-dispatch", daemon=True
    )
    dispatch_thread.start()

    pages = itertools.cycle([0x10, 0x19])
    page25_event = 0
    elapsed_time = 0
    accumulated_power = 0
    last_stats_log = 0.0

    try:
        while not stop_event.is_set():
            with state.lock:
                speed_mps = state.speed_mps
                cadence_rpm = state.cadence_rpm
                instant_power = state.instant_power
                distance_m = state.distance_m
                elapsed_time_s = state.elapsed_time_s
                last_ftms_update = state.last_ftms_update

            fe_state = compute_fe_state(cadence_rpm, instant_power)

            page = next(pages)
            if page == 0x19:
                if instant_power is not None:
                    accumulated_power = (
                        accumulated_power + clamp_int(int(round(instant_power)), 0, 0x0FFE)
                    ) & 0xFFFF
                page25_event = (page25_event + 1) & 0xFF

            if elapsed_time_s is not None:
                elapsed_time_value = int(elapsed_time_s * 4) & 0xFF
            else:
                elapsed_time_value = elapsed_time

            if page == 0x10:
                payload = build_page_10(
                    elapsed_time_value, speed_mps, distance_m, fe_state
                )
            else:
                payload = build_page_19(
                    page25_event,
                    cadence_rpm,
                    accumulated_power,
                    instant_power,
                    fe_state,
                )

            ch.send_broadcast_data(payload)

            now = time.monotonic()
            if now - last_stats_log >= 1.0:
                last_stats_log = now
                speed_kmh = None if speed_mps is None else speed_mps * 3.6
                if last_ftms_update is None:
                    ftms_age = "never"
                else:
                    ftms_age = f"{now - last_ftms_update:.1f}s ago"
                logger.info(
                    "Stats: speed={} km/h cadence={} rpm power={} W distance={} m elapsed={} s (ftms={})",
                    "-" if speed_kmh is None else f"{speed_kmh:.1f}",
                    "-" if cadence_rpm is None else cadence_rpm,
                    "-" if instant_power is None else instant_power,
                    "-" if distance_m is None else distance_m,
                    "-" if elapsed_time_s is None else elapsed_time_s,
                    ftms_age,
                )

            if elapsed_time_s is None:
                elapsed_time = (elapsed_time + 1) & 0xFF
            time.sleep(0.25)
    finally:
        ch.close()
        node.stop()
        dispatch_thread.join(timeout=2.0)


async def _find_ftms_device_by_name(target_name: str) -> Optional[BLEDevice]:
    def matches(device, advertisement_data) -> bool:
        name = advertisement_data.local_name or device.name
        return bool(name and target_name in name)

    device = None
    try:
        device = await BleakScanner.find_device_by_filter(
            matches,
            timeout=FTMS_SCAN_TIMEOUT_S,
            service_uuids=[FTMS_SERVICE_UUID],
        )
    except BleakBluetoothNotAvailableError as exc:
        logger.warning("Bluetooth not available: {}", exc)
        return None
    if device is None:
        try:
            device = await BleakScanner.find_device_by_filter(
                matches,
                timeout=FTMS_SCAN_TIMEOUT_S,
            )
        except BleakBluetoothNotAvailableError as exc:
            logger.warning("Bluetooth not available: {}", exc)
            return None
    return device


async def _resolve_ftms_device() -> Optional[BLEDevice]:
    if FTMS_DEVICE_ADDRESS:
        logger.info("BLE scanning for address {}", FTMS_DEVICE_ADDRESS)
        try:
            device = await BleakScanner.find_device_by_address(
                FTMS_DEVICE_ADDRESS,
                timeout=FTMS_SCAN_TIMEOUT_S,
            )
        except BleakBluetoothNotAvailableError as exc:
            logger.warning("Bluetooth not available: {}", exc)
            return None
        return device
    logger.info("BLE scanning for {}", FTMS_DEVICE_NAME)
    return await _find_ftms_device_by_name(FTMS_DEVICE_NAME)


async def run_ftms_bridge(state: BridgeState, stop_event: threading.Event) -> None:
    current_client: Optional[BleakClient] = None
    try:
        while not stop_event.is_set():
            device = await _resolve_ftms_device()
            if device is None:
                await asyncio.sleep(FTMS_RECONNECT_DELAY_S)
                continue

            def handle_disconnect(client: BleakClient) -> None:
                logger.warning("BLE disconnected: {}", client.address)

            data_count = 0

            def handle_bike_data(data) -> None:
                nonlocal data_count
                data_count += 1
                if data_count <= 5:
                    try:
                        payload = data._asdict()
                    except AttributeError:
                        payload = data
                    logger.info("FTMS data packet {} data={}", data_count, payload)
                update_state_from_ftms(state, data)

            def handle_control_response(response) -> None:
                logger.info(
                    "FTMS control response: {} {}",
                    response.request_code_enum.name,
                    response.result_code_enum.name,
                )

            try:
                logger.info(
                    "BLE connecting to {} ({})",
                    device.name or FTMS_DEVICE_NAME,
                    device.address,
                )
                client = BleakClient(device, disconnected_callback=handle_disconnect)
                current_client = client
                await client.connect()
                logger.info("BLE connected: {}", client.address)
                fms = FitnessMachineService(client)
                fms.set_indoor_bike_data_handler(handle_bike_data)
                fms.set_control_point_response_handler(handle_control_response)
                await fms.enable_control_point_indicate()
                await fms.enable_indoor_bike_data_notify()
                await fms.request_control()
                await asyncio.sleep(FTMS_CONTROL_DELAY_S)
                await fms.start_or_resume()
                while client.is_connected and not stop_event.is_set():
                    await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("BLE session error: {}", exc)
            finally:
                if current_client is not None and current_client.is_connected:
                    await current_client.disconnect()
                current_client = None
            if not stop_event.is_set():
                await asyncio.sleep(FTMS_RECONNECT_DELAY_S)
    finally:
        if current_client is not None and current_client.is_connected:
            await current_client.disconnect()


def main() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level} | {message}")

    state = BridgeState()
    stop_event = threading.Event()

    def handle_signal(signum, _frame) -> None:
        logger.info("Signal received: {}", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    ant_thread = threading.Thread(
        target=run_ant_bridge, args=(state, stop_event), daemon=True
    )
    ant_thread.start()

    try:
        asyncio.run(run_ftms_bridge(state, stop_event))
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        ant_thread.join(timeout=2.0)


if __name__ == "__main__":
    main()

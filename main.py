import asyncio
import itertools
import sys
import threading
import time
from typing import Optional

from bleak import BleakClient, BleakScanner
from loguru import logger
from openant.easy.channel import Channel
from openant.easy.node import Node

ANTPLUS_NETWORK = 0
ANTPLUS_KEY = list(bytes.fromhex("B9A521FBBD72C345"))

DEVICE_NAME = "UnixFit SB-700"
DEVICE_NUMBER = 700
DEVICE_TYPE = 0x11
TRANSMISSION_TYPE = 0x05

RF_FREQ = 57
CHANNEL_PERIOD = 8192  # 4 Hz

FTMS_DEVICE_NAME = "SB-700"
FTMS_SERVICE_UUID = "00001826-0000-1000-8000-00805f9b34fb"
FTMS_CHAR_INDOOR_BIKE_DATA = "00002ad2-0000-1000-8000-00805f9b34fb"
FTMS_CHAR_CONTROL_POINT = "00002ad9-0000-1000-8000-00805f9b34fb"

FTMS_OP_REQUEST_CONTROL = 0x00
FTMS_OP_START_OR_RESUME = 0x07

FE_TYPE_TRAINER = 25
FE_STATE_READY = 2
FE_STATE_IN_USE = 3


class BridgeState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.speed_mps: Optional[float] = None
        self.cadence_rpm: Optional[int] = None
        self.instant_power: Optional[int] = None
        self.distance_m: Optional[int] = None
        self.elapsed_time_s: Optional[int] = None
        self.last_ftms_update: Optional[float] = None


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


def parse_indoor_bike_data(message: bytes) -> dict:
    data = {
        "instant_speed": None,
        "instant_cadence": None,
        "total_distance": None,
        "instant_power": None,
        "elapsed_time": None,
    }
    if len(message) < 2:
        return data
    flag_more_data = bool(message[0] & 0b00000001)
    flag_average_speed = bool(message[0] & 0b00000010)
    flag_instantaneous_cadence = bool(message[0] & 0b00000100)
    flag_average_cadence = bool(message[0] & 0b00001000)
    flag_total_distance = bool(message[0] & 0b00010000)
    flag_resistance_level = bool(message[0] & 0b00100000)
    flag_instantaneous_power = bool(message[0] & 0b01000000)
    flag_average_power = bool(message[0] & 0b10000000)
    flag_expended_energy = bool(message[1] & 0b00000001)
    flag_heart_rate = bool(message[1] & 0b00000010)
    flag_metabolic_equivalent = bool(message[1] & 0b00000100)
    flag_elapsed_time = bool(message[1] & 0b00001000)
    flag_remaining_time = bool(message[1] & 0b00010000)

    idx = 2
    if not flag_more_data and len(message) >= idx + 2:
        data["instant_speed"] = int.from_bytes(
            message[idx : idx + 2], "little", signed=False
        ) / 100
        idx += 2
    if flag_average_speed and len(message) >= idx + 2:
        idx += 2
    if flag_instantaneous_cadence and len(message) >= idx + 2:
        data["instant_cadence"] = int.from_bytes(
            message[idx : idx + 2], "little", signed=False
        ) / 2
        idx += 2
    if flag_average_cadence and len(message) >= idx + 2:
        idx += 2
    if flag_total_distance and len(message) >= idx + 3:
        data["total_distance"] = int.from_bytes(
            message[idx : idx + 3], "little", signed=False
        )
        idx += 3
    if flag_resistance_level and len(message) >= idx + 2:
        idx += 2
    if flag_instantaneous_power and len(message) >= idx + 2:
        data["instant_power"] = int.from_bytes(
            message[idx : idx + 2], "little", signed=True
        )
        idx += 2
    if flag_average_power and len(message) >= idx + 2:
        idx += 2
    if flag_expended_energy and len(message) >= idx + 5:
        idx += 5
    if flag_heart_rate and len(message) >= idx + 1:
        idx += 1
    if flag_metabolic_equivalent and len(message) >= idx + 1:
        idx += 1
    if flag_elapsed_time and len(message) >= idx + 2:
        data["elapsed_time"] = int.from_bytes(
            message[idx : idx + 2], "little", signed=False
        )
        idx += 2
    if flag_remaining_time and len(message) >= idx + 2:
        idx += 2
    return data


def update_state_from_ftms(state: BridgeState, data: dict) -> None:
    with state.lock:
        state.last_ftms_update = time.monotonic()
        if data.get("instant_speed") is not None:
            state.speed_mps = float(data["instant_speed"]) / 3.6
        if data.get("instant_cadence") is not None:
            state.cadence_rpm = int(round(data["instant_cadence"]))
        if data.get("instant_power") is not None:
            state.instant_power = int(round(data["instant_power"]))
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
                    page25_event, cadence_rpm, accumulated_power, instant_power, fe_state
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


def build_ftms_control_command(opcode: int) -> bytes:
    return bytes([opcode])


async def run_bridge() -> None:
    logger.info("Scanning for FTMS bike named {}", FTMS_DEVICE_NAME)
    device = await BleakScanner.find_device_by_name(FTMS_DEVICE_NAME)
    if not device:
        logger.error("FTMS device not found: {}", FTMS_DEVICE_NAME)
        return

    logger.info("Found FTMS device: name={} address={}", device.name, device.address)

    state = BridgeState()
    stop_event = threading.Event()
    ant_thread = threading.Thread(
        target=run_ant_bridge, args=(state, stop_event), daemon=True
    )
    ant_thread.start()

    try:
        async with BleakClient(device.address, timeout=30, services=[FTMS_SERVICE_UUID]) as client:
            logger.info("FTMS connected")

            def on_indoor_bike_data(_sender, data):
                parsed = parse_indoor_bike_data(bytes(data))
                update_state_from_ftms(state, parsed)

            await client.start_notify(FTMS_CHAR_INDOOR_BIKE_DATA, on_indoor_bike_data)
            logger.info("FTMS indoor bike data notifications enabled")

            try:
                payload = build_ftms_control_command(FTMS_OP_REQUEST_CONTROL)
                await client.write_gatt_char(FTMS_CHAR_CONTROL_POINT, payload, response=True)
            except Exception as exc:
                logger.warning("FTMS request control failed: {}", exc)
            try:
                payload = build_ftms_control_command(FTMS_OP_START_OR_RESUME)
                await client.write_gatt_char(FTMS_CHAR_CONTROL_POINT, payload, response=True)
                logger.info("FTMS start_or_resume sent")
            except Exception as exc:
                logger.warning("FTMS start_or_resume failed: {}", exc)

            await asyncio.Future()
    finally:
        stop_event.set()
        ant_thread.join(timeout=2.0)


def main() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level} | {message}")
    asyncio.run(run_bridge())


if __name__ == "__main__":
    main()

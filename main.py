import itertools
import sys
import threading
import time
from collections import deque
from typing import Optional

import objc
from loguru import logger
from openant.easy.channel import Channel
from openant.easy.node import Node

from CoreBluetooth import (
    CBAdvertisementDataLocalNameKey,
    CBCharacteristicWriteWithResponse,
    CBManagerStatePoweredOn,
    CBCentralManager,
    CBUUID,
)
from Foundation import NSDate, NSData, NSRunLoop, NSObject

ANTPLUS_NETWORK = 0
ANTPLUS_KEY = list(bytes.fromhex("B9A521FBBD72C345"))

DEVICE_NAME = "UnixFit SB-700"
DEVICE_NUMBER = 700
DEVICE_TYPE = 0x11
TRANSMISSION_TYPE = 0x05

RF_FREQ = 57
CHANNEL_PERIOD = 8192  # 4 Hz

FTMS_DEVICE_NAME = "SB-700"
FTMS_SERVICE_UUID = "1826"
FTMS_CHAR_INDOOR_BIKE_DATA = "2ad2"
FTMS_CHAR_CONTROL_POINT = "2ad9"

FTMS_OP_REQUEST_CONTROL = 0x00
FTMS_OP_START_OR_RESUME = 0x07

FE_TYPE_TRAINER = 25
FE_STATE_READY = 2
FE_STATE_IN_USE = 3

OUTLIER_WINDOW = 3
OUTLIER_RESET_S = 3.0


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


class BridgeState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.speed_mps: Optional[float] = None
        self.cadence_rpm: Optional[int] = None
        self.instant_power: Optional[int] = None
        self.distance_m: Optional[int] = None
        self.elapsed_time_s: Optional[int] = None
        self.last_ftms_update: Optional[float] = None
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
    now = time.monotonic()
    with state.lock:
        last_update = state.last_ftms_update
        state.last_ftms_update = now
        if last_update is not None and now - last_update > OUTLIER_RESET_S:
            state.speed_filter.reset()
            state.cadence_filter.reset()
            state.power_filter.reset()
        if data.get("instant_speed") is not None:
            speed_mps = float(data["instant_speed"]) / 3.6
            filtered_speed = state.speed_filter.update(speed_mps)
            if filtered_speed is not None:
                state.speed_mps = filtered_speed
        if data.get("instant_cadence") is not None:
            cadence = float(data["instant_cadence"])
            filtered_cadence = state.cadence_filter.update(cadence)
            if filtered_cadence is not None:
                state.cadence_rpm = int(round(filtered_cadence))
        if data.get("instant_power") is not None:
            power = float(data["instant_power"])
            filtered_power = state.power_filter.update(power)
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


def build_ftms_control_command(opcode: int) -> NSData:
    payload = bytes([opcode])
    return NSData.dataWithBytes_length_(payload, len(payload))


class FTMSClient(NSObject):
    __pyobjc_protocols__ = [
        objc.protocolNamed("CBCentralManagerDelegate"),
        objc.protocolNamed("CBPeripheralDelegate"),
    ]

    def initWithState_targetName_(self, state: BridgeState, target_name: str):
        self = objc.super(FTMSClient, self).init()
        if self is None:
            return None
        self.state = state
        self.target_name = target_name
        self.scanning = False
        self.data_count = 0
        self.central = CBCentralManager.alloc().initWithDelegate_queue_options_(
            self, None, None
        )
        self.peripheral = None
        self.char_bike = None
        self.char_control = None
        self.ftms_uuid = CBUUID.UUIDWithString_(FTMS_SERVICE_UUID)
        self.bike_uuid = CBUUID.UUIDWithString_(FTMS_CHAR_INDOOR_BIKE_DATA)
        self.control_uuid = CBUUID.UUIDWithString_(FTMS_CHAR_CONTROL_POINT)
        return self

    def centralManagerDidUpdateState_(self, central):
        if central.state() != CBManagerStatePoweredOn:
            logger.warning("Bluetooth not ready: {}", central.state())
            return
        logger.info("BLE scanning for {}", self.target_name)
        central.scanForPeripheralsWithServices_options_([self.ftms_uuid], None)
        self.scanning = True

    def centralManager_didDiscoverPeripheral_advertisementData_RSSI_(
        self, central, peripheral, advertisementData, _rssi
    ):
        name = advertisementData.get(CBAdvertisementDataLocalNameKey) or peripheral.name()
        if not name or self.target_name not in name:
            return
        logger.info("BLE found {}", name)
        self.peripheral = peripheral
        self.peripheral.setDelegate_(self)
        central.stopScan()
        self.scanning = False
        central.connectPeripheral_options_(peripheral, None)

    def centralManager_didConnectPeripheral_(self, _central, peripheral):
        logger.info("BLE connected; discovering services")
        peripheral.discoverServices_(None)

    def centralManager_didFailToConnectPeripheral_error_(
        self, _central, _peripheral, error
    ):
        logger.warning("BLE connect failed: {}", error)

    def centralManager_didDisconnectPeripheral_error_(
        self, _central, _peripheral, error
    ):
        logger.warning("BLE disconnected: {}", error)
        self.peripheral = None
        self.char_bike = None
        self.char_control = None

    def ensure_scan(self) -> None:
        if self.peripheral is not None or self.scanning:
            return
        if self.central.state() != CBManagerStatePoweredOn:
            return
        logger.info("BLE scanning for {}", self.target_name)
        self.central.scanForPeripheralsWithServices_options_([self.ftms_uuid], None)
        self.scanning = True

    def peripheral_didDiscoverServices_(self, peripheral, error):
        if error:
            logger.warning("Service discovery failed: {}", error)
            return
        services = peripheral.services() or []
        if not services:
            logger.warning("No BLE services discovered")
        for service in services:
            uuid_str = service.UUID().UUIDString().lower()
            logger.info("BLE service: {}", uuid_str)
            if uuid_str == FTMS_SERVICE_UUID:
                logger.info("BLE FTMS service matched; discovering characteristics")
                peripheral.discoverCharacteristics_forService_(None, service)

    def peripheral_didDiscoverCharacteristicsForService_error_(
        self, peripheral, service, error
    ):
        if error:
            logger.warning("Characteristic discovery failed: {}", error)
            return
        chars = service.characteristics() or []
        if not chars:
            logger.warning("No BLE characteristics discovered for service")
        for char in chars:
            uuid = char.UUID().UUIDString().lower()
            logger.info("BLE characteristic: {}", uuid)
            if uuid == FTMS_CHAR_INDOOR_BIKE_DATA:
                self.char_bike = char
                logger.info("BLE subscribing to indoor bike data")
                peripheral.setNotifyValue_forCharacteristic_(True, char)
            elif uuid == FTMS_CHAR_CONTROL_POINT:
                self.char_control = char
        if self.char_control:
            self._send_control(FTMS_OP_REQUEST_CONTROL)
            self._send_control(FTMS_OP_START_OR_RESUME)

    def peripheral_didUpdateNotificationStateForCharacteristic_error_(
        self, _peripheral, characteristic, error
    ):
        if error:
            logger.warning("Notify failed: {}", error)
            return
        logger.info("Notify enabled: {}", characteristic.UUID().UUIDString())

    def peripheral_didUpdateValueForCharacteristic_error_(
        self, _peripheral, characteristic, error
    ):
        if error:
            logger.warning("Notify error: {}", error)
            return
        if characteristic.UUID().UUIDString().lower() != FTMS_CHAR_INDOOR_BIKE_DATA:
            return
        data = bytes(characteristic.value())
        self.data_count += 1
        if self.data_count <= 5:
            logger.info("FTMS data packet {} bytes={}", self.data_count, list(data))
        parsed = parse_indoor_bike_data(data)
        update_state_from_ftms(self.state, parsed)

    def peripheral_didWriteValueForCharacteristic_error_(
        self, _peripheral, _characteristic, error
    ):
        if error:
            logger.warning("Control write failed: {}", error)

    def _send_control(self, opcode: int) -> None:
        if not self.char_control or not self.peripheral:
            return
        payload = build_ftms_control_command(opcode)
        self.peripheral.writeValue_forCharacteristic_type_(
            payload, self.char_control, CBCharacteristicWriteWithResponse
        )


def run_corebluetooth(state: BridgeState, stop_event: threading.Event) -> None:
    client = FTMSClient.alloc().initWithState_targetName_(state, FTMS_DEVICE_NAME)
    if client is None:
        logger.error("Failed to initialize CoreBluetooth client")
        return
    run_loop = NSRunLoop.currentRunLoop()
    while not stop_event.is_set():
        client.ensure_scan()
        run_loop.runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.1))


def main() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level} | {message}")

    state = BridgeState()
    stop_event = threading.Event()
    ant_thread = threading.Thread(
        target=run_ant_bridge, args=(state, stop_event), daemon=True
    )
    ant_thread.start()

    try:
        run_corebluetooth(state, stop_event)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        ant_thread.join(timeout=2.0)


if __name__ == "__main__":
    main()

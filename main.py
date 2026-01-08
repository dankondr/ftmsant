import time
import itertools
import threading
import queue
from openant.easy.node import Node
from openant.easy.channel import Channel

ANTPLUS_NETWORK = 0
# Standard ANT+ network key (B9 A5 21 FB BD 72 C3 45).
ANTPLUS_KEY = list(bytes.fromhex("B9A521FBBD72C345"))

DEVICE_NUMBER = 12345
DEVICE_TYPE = 0x11
TRANSMISSION_TYPE = 0x05

RF_FREQ = 57
CHANNEL_PERIOD = 8192  # 4 Hz

MANUFACTURER_ID = 0xFFFF
MODEL_NUMBER = 1
HW_REVISION = 1
SW_REVISION = 1
SW_REVISION_SUP = 0xFF
SERIAL_NUMBER = 1

REQUEST_PAGE = 0x46
REQUEST_COMMAND_DATA_PAGE = 0x01
REQUESTED_RESPONSE_COUNT_MASK = 0x7F
REQUESTED_RESPONSE_ACK_MASK = 0x80

FE_TYPE_TRAINER = 25
FE_STATE_IN_USE = 3

FE_CAP_BASIC_RESISTANCE = 1 << 0
FE_CAP_TARGET_POWER = 1 << 1
FE_CAP_SIMULATION = 1 << 2

TRAINER_CAPABILITIES = [
    0x36,
    0xFF,
    0xFF,
    0xFF,
    0xFF,
    0xFF,
    0xFF,
    FE_CAP_BASIC_RESISTANCE | FE_CAP_TARGET_POWER,
]


def _le16(value: int) -> list[int]:
    return [value & 0xFF, (value >> 8) & 0xFF]


def _le32(value: int) -> list[int]:
    return [
        value & 0xFF,
        (value >> 8) & 0xFF,
        (value >> 16) & 0xFF,
        (value >> 24) & 0xFF,
    ]


def build_page_10(elapsed_time: int, speed_mps: float) -> list[int]:
    speed = int(speed_mps * 1000.0) & 0xFFFF
    capabilities = 0x00
    fe_state = (FE_STATE_IN_USE & 0x0F) << 4
    return [
        0x10,
        FE_TYPE_TRAINER & 0xFF,
        elapsed_time & 0xFF,
        0xFF,
        speed & 0xFF,
        (speed >> 8) & 0xFF,
        0xFF,
        capabilities | fe_state,
    ]


def build_page_19(
    event: int, cadence_rpm: int, accumulated_power: int, instant_power: int
) -> list[int]:
    instant_power &= 0x0FFF
    trainer_status = 0x00
    flags = 0x00
    fe_state = (FE_STATE_IN_USE & 0x0F) << 4
    return [
        0x19,
        event & 0xFF,
        cadence_rpm & 0xFF,
        accumulated_power & 0xFF,
        (accumulated_power >> 8) & 0xFF,
        instant_power & 0xFF,
        ((trainer_status & 0x0F) << 4) | ((instant_power >> 8) & 0x0F),
        (fe_state & 0xF0) | (flags & 0x0F),
    ]


def build_page_50() -> list[int]:
    return [
        0x50,
        0xFF,
        0xFF,
        HW_REVISION & 0xFF,
        *_le16(MANUFACTURER_ID),
        *_le16(MODEL_NUMBER),
    ]


def build_page_51() -> list[int]:
    return [
        0x51,
        0xFF,
        SW_REVISION_SUP & 0xFF,
        SW_REVISION & 0xFF,
        *_le32(SERIAL_NUMBER),
    ]


def build_page_47(
    last_command_page: int,
    last_command_sequence: int,
    last_command_status: int,
    last_command_response: list[int],
) -> list[int]:
    return [
        0x47,
        last_command_page & 0xFF,
        last_command_sequence & 0xFF,
        last_command_status & 0xFF,
        *last_command_response,
    ]


def build_page_fc() -> list[int]:
    return [0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]


def build_page(page: int, **kwargs) -> list[int] | None:
    if page == 0x10:
        return build_page_10(kwargs["elapsed_time"], kwargs["speed_mps"])
    if page == 0x19:
        return build_page_19(
            kwargs["event"],
            kwargs["cadence_rpm"],
            kwargs["accumulated_power"],
            kwargs["instant_power"],
        )
    if page == 0x36:
        return TRAINER_CAPABILITIES[:]
    if page == 0x47:
        return build_page_47(
            kwargs["last_command_page"],
            kwargs["last_command_sequence"],
            kwargs["last_command_status"],
            kwargs["last_command_response"],
        )
    if page == 0x50:
        return build_page_50()
    if page == 0x51:
        return build_page_51()
    if page == 0xFC:
        return build_page_fc()
    return None


def send_response(channel: Channel, payload: list[int], use_ack: bool) -> None:
    if use_ack:
        try:
            channel._ant.send_acknowledged_data(channel.id, payload)
            print("TX ack page=0x%02X payload=%s" % (payload[0], payload))
            return
        except Exception as exc:
            print("TX ack failed, falling back to broadcast: %s" % (exc,))
    channel.send_broadcast_data(payload)
    print("TX resp page=0x%02X payload=%s" % (payload[0], payload))


def main():
    node = Node()
    print("Node created; starting dispatch loop thread")
    dispatch_thread = threading.Thread(
        target=node.start, name="ant-dispatch", daemon=True
    )
    dispatch_thread.start()
    node.get_capabilities()

    # Set ANT+ network key on the node
    node.set_network_key(ANTPLUS_NETWORK, ANTPLUS_KEY)

    ch = node.new_channel(Channel.Type.BIDIRECTIONAL_TRANSMIT)

    ch.set_id(DEVICE_NUMBER, DEVICE_TYPE, TRANSMISSION_TYPE)
    ch.set_period(CHANNEL_PERIOD)
    ch.set_rf_freq(RF_FREQ)

    ch.open()
    print(
        "ANT+ channel open: dev=%s type=0x%02X tx=0x%02X freq=%s period=%s"
        % (DEVICE_NUMBER, DEVICE_TYPE, TRANSMISSION_TYPE, RF_FREQ, CHANNEL_PERIOD)
    )

    requested_pages: queue.SimpleQueue[tuple[int, int, bool, int]] = queue.SimpleQueue()
    last_command_page = 0xFF
    last_command_sequence = 0xFF
    last_command_status = 0xFF
    last_command_response = [0xFF, 0xFF, 0xFF, 0xFF]

    def on_rx(data):
        nonlocal last_command_page, last_command_sequence, last_command_status, last_command_response
        values = list(data)
        page = values[0] if values else None
        if page == REQUEST_PAGE and len(values) >= 8:
            response_control = values[5]
            requested_page = values[6]
            command_type = values[7]
            transmit_count = response_control & REQUESTED_RESPONSE_COUNT_MASK
            use_ack = (response_control & REQUESTED_RESPONSE_ACK_MASK) != 0
            requested_pages.put((requested_page, transmit_count, use_ack, command_type))
            print(
                "RX request page=0x%02X count=%s ack=%s cmd=0x%02X raw=%s"
                % (requested_page, transmit_count, use_ack, command_type, values)
            )
            return
        if page in (0x30, 0x31, 0x32, 0x33) and len(values) >= 8:
            last_command_page = page
            if last_command_sequence == 0xFF:
                last_command_sequence = 0
            else:
                last_command_sequence = (last_command_sequence + 1) % 255
            last_command_status = 0x00
            if page == 0x30:
                last_command_response = [0xFF, 0xFF, 0xFF, values[7]]
            elif page == 0x31:
                last_command_response = [0xFF, 0xFF, values[6], values[7]]
            elif page == 0x32:
                last_command_response = [0xFF, values[5], values[6], values[7]]
            elif page == 0x33:
                last_command_response = [0xFF, values[5], values[6], values[7]]
            print("RX control page=0x%02X raw=%s" % (page, values))
            return
        print("RX:", data)

    ch.on_broadcast_data = on_rx
    ch.on_acknowledge_data = on_rx
    ch.on_burst_data = on_rx

    pages = itertools.cycle([0x10, 0x19, 0x50, 0x51])
    page25_event = 0
    elapsed_time = 0
    speed_mps = 0.0
    cadence_rpm = 85
    instant_power = 180
    accumulated_power = 0

    try:
        while True:
            try:
                requested_page, transmit_count, use_ack, command_type = (
                    requested_pages.get_nowait()
                )
            except queue.Empty:
                requested_page = None

            if requested_page is not None:
                if command_type != REQUEST_COMMAND_DATA_PAGE:
                    print(
                        "RX request ignored (unsupported cmd=0x%02X) raw page=0x%02X"
                        % (command_type, requested_page)
                    )
                else:
                    payload = build_page(
                        requested_page,
                        event=page25_event,
                        elapsed_time=elapsed_time,
                        speed_mps=speed_mps,
                        cadence_rpm=cadence_rpm,
                        accumulated_power=accumulated_power,
                        instant_power=instant_power,
                        last_command_page=last_command_page,
                        last_command_sequence=last_command_sequence,
                        last_command_status=last_command_status,
                        last_command_response=last_command_response,
                    )

                    if payload is None:
                        print(
                            "RX request page=0x%02X has no handler; ignoring"
                            % (requested_page,)
                        )
                    else:
                        send_response(ch, payload, use_ack)

                    count = transmit_count if transmit_count > 0 else 1
                    if count > 1:
                        requested_pages.put(
                            (requested_page, count - 1, use_ack, command_type)
                        )

            page = next(pages)
            if page == 0x19:
                accumulated_power = (accumulated_power + instant_power) & 0xFFFF
                page25_event = (page25_event + 1) & 0xFF

            payload = build_page(
                page,
                event=page25_event,
                elapsed_time=elapsed_time,
                speed_mps=speed_mps,
                cadence_rpm=cadence_rpm,
                accumulated_power=accumulated_power,
                instant_power=instant_power,
                last_command_page=last_command_page,
                last_command_sequence=last_command_sequence,
                last_command_status=last_command_status,
                last_command_response=last_command_response,
            )

            if payload is not None:
                print(
                    "TX page=0x%02X event=%s payload=%s"
                    % (page, page25_event, payload)
                )
                ch.send_broadcast_data(payload)

            elapsed_time = (elapsed_time + 1) & 0xFF
            time.sleep(0.25)
    finally:
        ch.close()
        node.stop()
        dispatch_thread.join(timeout=2.0)


if __name__ == "__main__":
    main()

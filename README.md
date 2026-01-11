FTMS to ANT+ bridge for a bike trainer using BLE via pycycling.

Configuration:
- Set `FTMS_DEVICE_NAME` to a substring of the BLE device name (default: SB-700).
- Optionally set `FTMS_DEVICE_ADDRESS` to connect directly by address/UUID.
- Optional outlier filters:
  - `SPEED_MAX_UP_MPS_S` (default 2.5)
  - `CADENCE_MAX_UP_RPM_S` (default 40)
  - `POWER_MAX_UP_W_S` (default 300)

Run:
`python3 main.py`

"""Read power-meter values over Modbus TCP, store in InfluxDB, and send Telegram alerts."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

# CONFIG
MODBUS_IP = "192.168.0.7"
PORT = 502
SLAVE_ID = 1

INFLUX_URL = "http://localhost:8086"
TOKEN = "your_token"
ORG = "power"
BUCKET = "energy"

TELEGRAM_TOKEN = "your_bot_token"
CHAT_ID = "your_chat_id"

POLL_INTERVAL_SECONDS = 10
ALERT_COOLDOWN_SECONDS = 300


@dataclass
class MeterData:
    voltage: float
    current: float
    power_kw: float
    pf: float
    freq: float


def send_telegram(msg: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        response = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logging.error("Failed to send Telegram alert: %s", exc)


def read_meter(client: ModbusTcpClient) -> MeterData | None:
    try:
        rr = client.read_holding_registers(0, 20, slave=SLAVE_ID)
    except ModbusException as exc:
        logging.error("Modbus read failed: %s", exc)
        return None

    if rr.isError():
        logging.error("Modbus response returned error: %s", rr)
        return None

    data = rr.registers

    # Example mapping (change as per your meter manual)
    return MeterData(
        voltage=data[0] / 10,
        current=data[1] / 10,
        power_kw=data[2] / 10,
        pf=data[3] / 100,
        freq=data[4] / 100,
    )


def write_influx(write_api: Any, data: MeterData) -> None:
    point = (
        Point("energy")
        .field("voltage", data.voltage)
        .field("current", data.current)
        .field("power_kw", data.power_kw)
        .field("pf", data.pf)
        .field("freq", data.freq)
        .time(time.time_ns(), WritePrecision.NS)
    )
    write_api.write(bucket=BUCKET, record=point)


def check_alerts(data: MeterData, last_sent: dict[str, float]) -> dict[str, float]:
    now = time.time()

    def can_send(key: str) -> bool:
        return now - last_sent.get(key, 0.0) >= ALERT_COOLDOWN_SECONDS

    if data.power_kw < 5 and can_send("power_drop"):
        send_telegram("⚠️ Power Drop / Shutdown Detected!")
        last_sent["power_drop"] = now

    if data.voltage < 200 and can_send("low_voltage"):
        send_telegram("⚠️ Low Voltage Alert!")
        last_sent["low_voltage"] = now

    if data.pf < 0.7 and can_send("poor_pf"):
        send_telegram("⚠️ Poor Power Factor!")
        last_sent["poor_pf"] = now

    return last_sent


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    with ModbusTcpClient(MODBUS_IP, port=PORT) as client, InfluxDBClient(
        url=INFLUX_URL,
        token=TOKEN,
        org=ORG,
    ) as influx:
        write_api = influx.write_api()
        last_alert_sent: dict[str, float] = {}

        while True:
            meter = read_meter(client)

            if meter is not None:
                write_influx(write_api, meter)
                last_alert_sent = check_alerts(meter, last_alert_sent)
                logging.info("Data: %s", meter)
            else:
                if time.time() - last_alert_sent.get("comm_fail", 0.0) >= ALERT_COOLDOWN_SECONDS:
                    send_telegram("🚨 Meter Communication Failed!")
                    last_alert_sent["comm_fail"] = time.time()

            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

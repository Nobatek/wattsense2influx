"""Import Wattsense data into InfluxDB"""
import base64
import csv
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from urllib import parse
from zoneinfo import ZoneInfo

import influxdb_client
import requests
from dotenv import load_dotenv


class WattsenseAuth(requests.auth.AuthBase):
    """Wattsense authentication"""
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def __call__(self, r):
        timestamp = int(time.time() * 1000)
        url_parts = parse.urlparse(r.url)
        query = parse.parse_qs(url_parts.query)
        query_str = "&".join(
            [f"{key}={value}" for key in query for value in query[key]]
        )
        message = "\n".join(
            [
                str(e)
                for e in [
                    r.method,
                    url_parts.path,
                    (query_str if query_str else None),
                    (json.dumps(r.body) if r.body else None),
                    timestamp,
                ]
                if e is not None
            ]
        )
        hmac_hash = base64.b64encode(
            hmac.new(
                self.api_secret.encode(), message.encode(), hashlib.sha512
            ).digest()
        ).decode()

        r.headers["X-API-Auth"] = f"{self.api_key}:{hmac_hash}"
        r.headers["X-API-Timestamp"] = str(timestamp)

        return r


def get_data(
    req_session, device_id, since, until, property_ids=None, size_per_request=500
):
    """Get device properties data"""
    params = {
        "since": int(since.timestamp()) * 1000,
        "until": int(until.timestamp()) * 1000,
        "size": size_per_request,
    }
    if property_ids:
        params["property"] = property_ids

    elements = []
    res = req_session.get(
        WS_URL + "/v1/devices/" + device_id + "/properties", params=params
    )
    i = 0
    while res.status_code == 200:
        i += 1
        elements.extend(res.json())
        if res.links.get("next"):
            next_url_query = parse.urlparse(res.links["next"]["url"]).query
            # parse next url query parameters
            params = {}
            properties = []
            for kv in next_url_query.split("&"):
                k, v = kv.split("=")
                if k == "property":
                    properties.append(v)
                else:
                    params[k] = v
                params["property"] = properties
            res = req_session.get(
                WS_URL + "/v1/devices/" + device_id + "/properties", params=params
            )
        else:
            break

    return elements


def read_ws(metadata_file_path, labels, start_dt, end_dt):
    """Read data from Wattsense"""
    with requests.session() as session:
        session.auth = WattsenseAuth(WS_API_KEY, WS_API_SECRET)

        points = []
        with open(metadata_file_path) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=",")
            for row in reader:
                device_id = row["DeviceId"]
                equipment_id = row["EquipmentId"]
                property_id = row["PropertyId"]
                metadata = {l: row[l] for l in labels}

                logger.debug("Get data for %s %s %s", device_id, equipment_id, property_id)
                result = get_data(
                    session,
                    device_id,
                    start_dt,
                    end_dt,
                    property_ids=[property_id],
                    size_per_request=999,
                )

                for item in result:
                    time_dt = dt.datetime.fromtimestamp(
                        item["timestamp"] / 1000, tz=dt.timezone.utc
                    )
                    point = {
                        "measurement": INFLUXDB_MEASUREMENT,
                        "tags": {l: metadata[l] for l in labels},
                        "time": time_dt,
                        "fields": {"value": float(item["payload"])}
                    }
                    points.append(point)

    return points


def ws2influx(metadata_file_path, labels, start_dt, end_dt):
    """Read data from Wattsense and write to InfluxDB"""

    logger.info("Read from Wattsense")
    records = read_ws(metadata_file_path, labels, start_dt, end_dt)

    with influxdb_client.InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    ) as client, client.write_api() as write_api:
        logger.info("Write %s data points to Influx", len(records))
        if not DRY_RUN:
            write_api.write(bucket=INFLUXDB_BUCKET, record=records)
        else:
            logger.warning("Dry run enabled, no data written")


def delete_data(start_dt, end_dt):
    """May be used manually (e.g. to clear measurement for tests)"""

    with influxdb_client.InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    ) as client:
        client.delete_api().delete(
            start_dt,
            end_dt,
            f'_measurement={INFLUXDB_MEASUREMENT}',
            bucket=INFLUXDB_BUCKET,
            org=INFLUXDB_ORG,
        )


if __name__ == "__main__":

    load_dotenv()

    LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(message)s'
    LOG_FILE = os.getenv("LOG_FILE")
    LOG_LEVEL = os.getenv("LOG_LEVEL")
    logging.basicConfig(level="INFO", filename=LOG_FILE, format=LOG_FORMAT)
    logger = logging.getLogger(__name__)

    DRY_RUN = (os.getenv("DRY_RUN") == "true")
    TIME_MARGIN = dt.timedelta(seconds=int(os.getenv("TIME_MARGIN")))
    TZ = ZoneInfo(os.getenv("TZ"))

    WS_URL = os.getenv("WS_URL")
    WS_API_KEY = os.getenv("WS_API_KEY")
    WS_API_SECRET = os.getenv("WS_API_SECRET")

    INFLUXDB_URL = os.getenv("INFLUXDB_URL")
    INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
    INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
    INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
    INFLUXDB_MEASUREMENT = os.getenv("INFLUXDB_MEASUREMENT")

    METADATA_FILES_DIR = Path(os.getenv("METADATA_FILES_DIR"))
    LABELS = os.getenv("LABELS").split(",")
    STATUS_FILE = os.getenv("STATUS_FILE")

    logger.info("== Wattsense 2 InfluxDB ==")

    now = dt.datetime.now(tz=TZ)

    with open(STATUS_FILE) as f_p:
        status = json.load(f_p)

    try:
        last_exec_time = dt.datetime.fromisoformat(status["last_exec_time"])
    except KeyError:
        last_exec_time = now

    start_dt = last_exec_time - TIME_MARGIN
    end_dt = now

    logger.info("Start: %s", start_dt)
    logger.info("End:   %s", end_dt)

    for _, _, files in METADATA_FILES_DIR.walk():
        for filename in files:
            if Path(filename).suffix == ".csv":
                logger.info("Load %s", filename)
                ws2influx(METADATA_FILES_DIR / filename, LABELS, start_dt, end_dt)

    logger.info("Update status")

    status["last_exec_time"] = now.isoformat()

    with open(STATUS_FILE, "w") as f_p:
        json.dump(status, f_p, indent=2)

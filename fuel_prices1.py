#!/usr/bin/env python3
"""
UK Fuel Price Scraper — Fuel Finder statutory API
Fetches E10 and B7 prices via OAuth 2.0 client credentials,
pages through all stations and price records, derives county
from postcode prefix, and writes fuel_data.json.

Credentials are read from environment variables:
  FUEL_CLIENT_ID
  FUEL_CLIENT_SECRET

Schedule this script to run hourly (e.g. via Task Scheduler on Windows,
or cron on Linux) and place fuel_data.json in the same folder as the
dashboard HTML file.
"""

import json
import os
import re
import sys
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests

FUEL_CLIENT_ID     = os.environ.get("FUEL_CLIENT_ID", "")
FUEL_CLIENT_SECRET = os.environ.get("FUEL_CLIENT_SECRET", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL     = "https://www.fuel-finder.service.gov.uk"
TOKEN_URL    = f"{BASE_URL}/api/v1/oauth/generate_access_token"
REFRESH_URL  = f"{BASE_URL}/api/v1/oauth/regenerate_access_token"
STATIONS_URL = f"{BASE_URL}/api/v1/pfs"
PRICES_URL   = f"{BASE_URL}/api/v1/pfs/fuel-prices"

BATCH_SIZE    = 500
RATE_DELAY    = 2.0
TOKEN_SKEW    = 30
TIMEOUT       = 60
CUTOFF_HOURS  = 36          # only include stations updated within this window

POSTCODE_COUNTY = {
    "AB": "Aberdeenshire",         "AL": "Hertfordshire",
    "B":  "West Midlands",         "BA": "Somerset",
    "BB": "Lancashire",            "BD": "West Yorkshire",
    "BH": "Dorset",                "BL": "Greater Manchester",
    "BN": "East Sussex",           "BR": "Greater London",
    "BS": "Bristol",               "CA": "Cumbria",
    "CB": "Cambridgeshire",        "CF": "Cardiff",
    "CH": "Cheshire",              "CM": "Essex",
    "CO": "Essex",                 "CR": "Greater London",
    "CT": "Kent",                  "CV": "West Midlands",
    "CW": "Cheshire",              "DA": "Greater London",
    "DD": "Dundee",                "DE": "Derbyshire",
    "DG": "Dumfries and Galloway", "DH": "County Durham",
    "DL": "County Durham",         "DN": "South Yorkshire",
    "DT": "Dorset",                "DY": "West Midlands",
    "E":  "Greater London",        "EC": "Greater London",
    "EH": "Edinburgh",             "EN": "Hertfordshire",
    "EX": "Devon",                 "FK": "Stirlingshire",
    "FY": "Lancashire",            "G":  "Glasgow",
    "GL": "Gloucestershire",       "GU": "Surrey",
    "HA": "Greater London",        "HD": "West Yorkshire",
    "HG": "North Yorkshire",       "HP": "Hertfordshire",
    "HR": "Herefordshire",         "HS": "Western Isles",
    "HU": "East Yorkshire",        "HX": "West Yorkshire",
    "IG": "Greater London",        "IP": "Suffolk",
    "IV": "Inverness",             "KA": "Ayrshire",
    "KT": "Greater London",        "KW": "Caithness",
    "KY": "Fife",                  "L":  "Merseyside",
    "LA": "Lancashire",            "LD": "Powys",
    "LE": "Leicestershire",        "LL": "North Wales",
    "LN": "Lincolnshire",          "LS": "West Yorkshire",
    "LU": "Bedfordshire",          "M":  "Greater Manchester",
    "ME": "Kent",                  "MK": "Buckinghamshire",
    "ML": "Lanarkshire",           "N":  "Greater London",
    "NE": "Tyne and Wear",         "NG": "Nottinghamshire",
    "NN": "Northamptonshire",      "NP": "Gwent",
    "NR": "Norfolk",               "NW": "Greater London",
    "OL": "Greater Manchester",    "OX": "Oxfordshire",
    "PA": "Renfrewshire",          "PE": "Cambridgeshire",
    "PH": "Perthshire",            "PL": "Devon",
    "PO": "Hampshire",             "PR": "Lancashire",
    "RG": "Berkshire",             "RH": "Surrey",
    "RM": "Greater London",        "S":  "South Yorkshire",
    "SA": "Swansea",               "SE": "Greater London",
    "SG": "Hertfordshire",         "SK": "Cheshire",
    "SL": "Berkshire",             "SM": "Greater London",
    "SN": "Wiltshire",             "SO": "Hampshire",
    "SP": "Wiltshire",             "SR": "Tyne and Wear",
    "SS": "Essex",                 "ST": "Staffordshire",
    "SW": "Greater London",        "SY": "Shropshire",
    "TA": "Somerset",              "TD": "Scottish Borders",
    "TF": "Shropshire",            "TN": "Kent",
    "TQ": "Devon",                 "TR": "Cornwall",
    "TS": "Cleveland",             "TW": "Greater London",
    "UB": "Greater London",        "W":  "Greater London",
    "WA": "Cheshire",              "WC": "Greater London",
    "WD": "Hertfordshire",         "WF": "West Yorkshire",
    "WN": "Greater Manchester",    "WR": "Worcestershire",
    "WS": "West Midlands",         "WV": "West Midlands",
    "YO": "North Yorkshire",       "ZE": "Shetland",
}

FUEL_MAP = {
    "E10": "E10", "E10_STANDARD": "E10",
    "B7":  "B7",  "B7_STANDARD":  "B7", "B7P": "B7", "DIESEL": "B7",
    "E5":  "E5",  "E5_SUPER":     "E5",
    "SDV": "SDV", "SDV_SUPER":    "SDV",
}

HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}


class TokenManager:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        if self._access_token and time.time() < self._expires_at:
            return self._access_token
        if self._refresh_token:
            self._do_refresh()
        else:
            self._do_new()
        return self._access_token  # type: ignore[return-value]

    def _store(self, data: dict) -> None:
        inner = data.get("data", data)
        self._access_token = inner["access_token"]
        self._refresh_token = inner.get("refresh_token") or self._refresh_token
        expires_in = int(inner.get("expires_in", 3600))
        self._expires_at = time.time() + max(60, expires_in) - TOKEN_SKEW
        log.info("Token OK — valid for ~%ds", expires_in)

    def _do_new(self) -> None:
        log.info("Requesting new OAuth token...")
        for attempt in range(1, 4):
            try:
                resp = requests.post(
                    TOKEN_URL,
                    json={"client_id": self.client_id, "client_secret": self.client_secret},
                    headers=HEADERS,
                    timeout=TIMEOUT,
                )
                resp.raise_for_status()
                self._store(resp.json())
                return
            except (requests.Timeout, requests.ConnectionError) as exc:
                log.warning("Token request attempt %d/3 failed: %s", attempt, exc)
                if attempt == 3:
                    raise
                time.sleep(5 * attempt)

    def _do_refresh(self) -> None:
        log.info("Refreshing OAuth token...")
        try:
            resp = requests.post(
                REFRESH_URL,
                json={"client_id": self.client_id, "refresh_token": self._refresh_token},
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            self._store(resp.json())
        except requests.HTTPError as exc:
            log.warning("Token refresh failed (%s) — re-authenticating", exc)
            self._refresh_token = None
            self._do_new()


def api_get(url: str, token: str, params: Optional[dict] = None) -> "list | dict":
    hdrs = {**HEADERS, "Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=hdrs, params=params, timeout=TIMEOUT)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        log.warning("Rate limited — sleeping %ds", retry_after)
        time.sleep(retry_after)
        return api_get(url, token, params)

    if not resp.ok:
        log.error("HTTP %s — response body: %s", resp.status_code, resp.text)

    resp.raise_for_status()
    return resp.json()


def fetch_all_pages(url: str, tm: TokenManager, label: str = "",
                    extra_params: Optional[dict] = None) -> list:
    records = []
    batch = 1

    while True:
        if batch > 1:
            time.sleep(RATE_DELAY)

        token = tm.get_token()
        log.info("  %s  batch %d...", label or url.split("/")[-1], batch)

        params = {"batch-number": batch}
        if extra_params:
            params.update(extra_params)
        data = api_get(url, token, params=params)

        if isinstance(data, list):
            page = data
        else:
            page = (
                data.get("data")
                or data.get("forecourts")
                or data.get("stations")
                or data.get("prices")
                or []
            )

        records.extend(page)
        log.info("    -> %d records this batch  (running total: %d)", len(page), len(records))

        if len(page) < BATCH_SIZE:
            break

        batch += 1

    return records


def postcode_area(postcode: str) -> Optional[str]:
    m = re.match(r"^([A-Z]{1,2})", (postcode or "").strip().upper())
    return m.group(1) if m else None


def county_from_postcode(postcode: str) -> str:
    area = postcode_area(postcode)
    if not area:
        return "Unknown"
    return POSTCODE_COUNTY.get(area, f"{area} area")


def parse_price(val) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        n = float(val)
    except (ValueError, TypeError):
        return None
    if n <= 0:
        return None
    return round(n if n > 10 else n * 100, 2)


def build_station_index(stations_raw: list) -> dict:
    index: dict = {}
    for s in stations_raw:
        node_id = str(s.get("node_id") or "").strip()
        if not node_id:
            continue

        loc = s.get("location") or {}
        postcode = str(loc.get("postcode") or "").strip().upper()

        county = str(loc.get("county") or "").strip()
        if not county:
            county = county_from_postcode(postcode)

        town    = str(loc.get("city") or loc.get("address_line_2") or "").strip()
        address = str(loc.get("address_line_1") or "").strip()

        index[node_id] = {
            "node_id":      node_id,
            "name":         s.get("trading_name") or s.get("brand_name") or node_id,
            "brand":        s.get("brand_name") or "",
            "postcode":     postcode,
            "address":      address,
            "town":         town,
            "county":       county or "Unknown",
            "E10":          None,
            "B7":           None,
            "last_updated": None,   # ISO timestamp of most recent price change
        }
    return index


def parse_timestamp(val) -> Optional[str]:
    """Return an ISO-8601 UTC string, or None if unparseable."""
    if not val:
        return None
    # Normalise common JS date strings: "Sun Mar 29 2026 17:55:28 GMT+0000 (...)"
    # Strip the parenthesised timezone name so strptime can handle it.
    s = re.sub(r"\s*\(.*\)\s*$", "", str(val).strip())
    for fmt in (
        "%a %b %d %Y %H:%M:%S GMT%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    log.debug("Could not parse timestamp: %r", val)
    return None


def apply_prices(index: dict, prices_raw: list) -> None:
    for record in prices_raw:
        node_id = str(record.get("node_id") or "").strip()
        if node_id not in index:
            continue

        for fp in record.get("fuel_prices") or []:
            raw_type = str(fp.get("fuel_type") or "").upper().replace("-", "_").replace(" ", "_")
            fuel = FUEL_MAP.get(raw_type)
            if fuel in ("E10", "B7"):
                p = parse_price(fp.get("price"))
                if p is not None:
                    index[node_id][fuel] = p

                # Capture the effective timestamp for whichever fuel type this is
                ts = parse_timestamp(
                    fp.get("price_change_effective_timestamp")
                    or fp.get("submission_timestamp")
                )
                if ts:
                    cur = index[node_id]["last_updated"]
                    # Keep the most recent timestamp across all fuel types
                    if cur is None or ts > cur:
                        index[node_id]["last_updated"] = ts


def build_output(index: dict) -> dict:
    """
    Output structure consumed by dashboard.html:
    {
      "generated_at": "2026-03-31T10:00:00Z",
      "total_stations": 7561,
      "stations": [ { node_id, name, brand, postcode, address, town, county, E10, B7 }, ... ],
      "county_summary": {
        "Greater London": {
          "station_count": 412,
          "avg_E10": 148.2,
          "avg_B7": 155.1,
          "most_expensive_E10": { ...station... },
          "most_expensive_B7":  { ...station... }
        }, ...
      }
    }
    """
    stations = list(index.values())

    by_county: dict = {}
    for s in stations:
        county = s["county"]
        if county == "Unknown":
            continue
        by_county.setdefault(county, []).append(s)

    county_summary = {}
    for county, rows in sorted(by_county.items()):
        e10_prices = [s["E10"] for s in rows if s["E10"] is not None]
        b7_prices  = [s["B7"]  for s in rows if s["B7"]  is not None]

        most_exp_e10 = max(
            (s for s in rows if s["E10"] is not None),
            key=lambda x: x["E10"], default=None
        )
        most_exp_b7 = max(
            (s for s in rows if s["B7"] is not None),
            key=lambda x: x["B7"], default=None
        )

        county_summary[county] = {
            "station_count":      len(rows),
            "avg_E10":            round(sum(e10_prices) / len(e10_prices), 1) if e10_prices else None,
            "avg_B7":             round(sum(b7_prices)  / len(b7_prices),  1) if b7_prices  else None,
            "most_expensive_E10": most_exp_e10,
            "most_expensive_B7":  most_exp_b7,
        }

    return {
        "generated_at":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_stations": len(stations),
        "stations":       stations,
        "county_summary": county_summary,
    }


def main() -> None:
    if not FUEL_CLIENT_ID or not FUEL_CLIENT_SECRET:
        sys.exit(
            "Error: Set FUEL_CLIENT_ID and FUEL_CLIENT_SECRET environment variables.\n"
            "  PowerShell:  $env:FUEL_CLIENT_ID='your_id'\n"
            "  PowerShell:  $env:FUEL_CLIENT_SECRET='your_secret'"
        )

    tm = TokenManager(FUEL_CLIENT_ID, FUEL_CLIENT_SECRET)
    t0 = time.time()

    log.info("=== Fetching station metadata ===")
    stations_raw = fetch_all_pages(STATIONS_URL, tm, label="stations")
    log.info("Total raw station records: %d", len(stations_raw))

    time.sleep(RATE_DELAY)

    log.info("=== Fetching fuel prices (last %dh) ===", CUTOFF_HOURS)
    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=CUTOFF_HOURS)
    cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    log.info("Effective start timestamp: %s UTC", cutoff_str)
    prices_raw = fetch_all_pages(
        PRICES_URL, tm, label="prices",
        extra_params={"effective-start-timestamp": cutoff_str},
    )
    log.info("Total raw price records: %d", len(prices_raw))

    log.info("=== Building output ===")
    index = build_station_index(stations_raw)
    apply_prices(index, prices_raw)

    with_e10 = sum(1 for s in index.values() if s["E10"] is not None)
    with_b7  = sum(1 for s in index.values() if s["B7"]  is not None)
    log.info("Stations with E10: %d  |  with B7: %d", with_e10, with_b7)

    output = build_output(index)

    out_path = Path("fuel_data.json")
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    elapsed = time.time() - t0
    log.info(
        "Done in %.1fs — wrote %s  (%d counties, %d total stations)",
        elapsed, out_path, len(output["county_summary"]), output["total_stations"],
    )


if __name__ == "__main__":
    main()
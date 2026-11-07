"""
Download the raw datasets from the CSO (Central Statistics Office, Ireland).

    RIQ02 - RTB Average Monthly Rent Report (rent by place, bedrooms, property type, quarter)
    HPM09 - Residential Property Price Index (monthly, by area and type of home)

Both are published under CC BY 4.0. Files are saved to data/raw/ (which is git-ignored,
so the big raw files never get committed).

Usage:
    python download_data.py
"""

import urllib.request
from pathlib import Path

BASE_URL = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{code}/CSV/1.0/en"
TABLES = ["RIQ02", "HPM09"]
RAW_DIR = Path(__file__).parent / "data" / "raw"


def download(code: str) -> Path:
    url = BASE_URL.format(code=code)
    target = RAW_DIR / f"{code}.csv"
    print(f"Downloading {code} ...")
    request = urllib.request.Request(url, headers={"User-Agent": "irish-rent-trends-project"})
    with urllib.request.urlopen(request, timeout=60) as response:
        target.write_bytes(response.read())
    print(f"  saved {target} ({target.stat().st_size / 1024:.0f} KB)")
    return target


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for code in TABLES:
        try:
            download(code)
        except Exception as error:  # keep going so one failure doesn't hide the other
            print(f"  FAILED for {code}: {error}")


if __name__ == "__main__":
    main()

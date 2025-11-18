import argparse
import sys
import time
from typing import Optional, List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://masothue.com"
UA = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9",
}


def fetch_by_mst(mst: str, timeout=15.0) -> Dict[str, str]:
    url = f"{BASE_URL}/{mst}"

    r = requests.get(url, headers=UA, timeout=timeout)
    if r.status_code != 200:
        raise Exception(f"HTTP {r.status_code} cho {url}")

    soup = BeautifulSoup(r.text, "html.parser")

    data = {
        "tax_id": mst,
        "masothue_url": url
    }

    # Lấy tên doanh nghiệp từ <h1>
    h1 = soup.find("h1")
    if h1:
        data["Tên doanh nghiệp"] = h1.get_text(strip=True)

    # Lấy mọi bảng 2 cột
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["th", "td"])
            if len(tds) != 2:
                continue
            label = tds[0].get_text(" ", strip=True)
            value = tds[1].get_text(" ", strip=True)
            data[label] = value

    return data


def enrich_excel(input_path, output_path, column="tax_id",
                 sheet=0, delay=1.0, timeout=15.0):

    df = pd.read_excel(input_path, sheet_name=sheet)

    if column not in df.columns:
        raise Exception(f"Không tìm thấy cột {column}")

    results = []
    total = len(df)

    for idx, v in enumerate(df[column], start=1):
        mst = str(v).strip()
        if not mst:
            continue

        try:
            info = fetch_by_mst(mst, timeout=timeout)
            print(f"[{idx}/{total}] ✅ {mst}")
            results.append(info)
        except Exception as ex:
            print(f"[{idx}/{total}] ❌ {mst}: {ex}")

        time.sleep(delay)

    if not results:
        raise Exception("Không lấy được dữ liệu nào.")

    newdf = pd.DataFrame(results)
    merged = df.merge(newdf, how="left", left_on=column, right_on="tax_id")
    merged.to_excel(output_path, index=False)
    print("Đã lưu:", output_path)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--column", default="tax_id")
    parser.add_argument("--sheet", default=0)
    parser.add_argument("--output", default="result.xlsx")
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=15.0)
    args = parser.parse_args(argv)

    enrich_excel(args.input, args.output, args.column,
                 args.sheet, args.delay, args.timeout)


if __name__ == "__main__":
    main()

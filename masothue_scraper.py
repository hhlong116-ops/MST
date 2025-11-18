"""
Utility to fetch business details from https://masothue.com for a list of tax IDs.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://masothue.com"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
)


class ScrapeError(Exception):
    pass


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tải thông tin từ masothue.com"
    )
    parser.add_argument("input")
    parser.add_argument("--column", default="tax_id")
    parser.add_argument("--sheet", default=0)
    parser.add_argument("--output", default="masothue_results.xlsx")
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=15.0)
    return parser.parse_args(argv)


LINK_RE = re.compile(r"^/([\d-]+)-")


def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def get_tax_id_from_url(url: str) -> Optional[str]:
    """
    Lấy MST từ URL: /0100105052-011-chi-nhanh-...
    """
    m = LINK_RE.match(url)
    if not m:
        return None
    return m.group(1)  # "0100105052-011"


def fetch_detail(detail_url: str, raw_tax_id: str, timeout: float):
    """
    Truy cập trang chi tiết và đọc dữ liệu.
    Không còn tìm MST trong <table> vì HTML đã thay đổi.
    Chỉ cần so MST từ URL.
    """
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(detail_url, headers=headers, timeout=timeout)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    data = {
        "raw_tax_id": raw_tax_id,
        "masothue_url": detail_url
    }

    # Tên doanh nghiệp (thường ở <h1>)
    h1 = soup.find("h1")
    if h1:
        data["Tên doanh nghiệp"] = h1.get_text(strip=True)

    # Lấy bảng dữ liệu
    tables = soup.find_all("table")
    for tb in tables:
        for tr in tb.find_all("tr"):
            tds = tr.find_all(["th", "td"])
            if len(tds) == 2:
                label = tds[0].get_text(" ", strip=True)
                value = tds[1].get_text(" ", strip=True)
                if label not in data:
                    data[label] = value

    return data


def fetch_tax_info(raw_tax_id: str, timeout: float = 15.0) -> Dict[str, str]:
    """
    Quy trình:
    1. Query search
    2. Lấy tất cả link chi tiết
    3. Chọn link nào có MST trong URL trùng (theo digits)
    4. Scrape dữ liệu
    """
    query = raw_tax_id.strip()
    if not query:
        raise ScrapeError("MST rỗng")

    headers = {"User-Agent": USER_AGENT}
    search_url = f"{BASE_URL}/Search/?q={query}"
    resp = requests.get(search_url, headers=headers, timeout=timeout)
    if resp.status_code != 200:
        raise ScrapeError("Không tải được trang Search")

    soup = BeautifulSoup(resp.text, "html.parser")

    input_digits = digits_only(raw_tax_id)

    # Thu link ứng viên
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if LINK_RE.match(href):
            links.append(href)

    if not links:
        raise ScrapeError(f"Không tìm thấy link cho {raw_tax_id}")

    # Chọn đúng link theo MST trong URL
    for href in links:
        mst_in_url = get_tax_id_from_url(href)
        if not mst_in_url:
            continue

        # So theo digits
        if digits_only(mst_in_url) == input_digits:
            detail_url = href if href.startswith("http") else BASE_URL + href
            data = fetch_detail(detail_url, raw_tax_id, timeout)
            if data:
                return data

    raise ScrapeError(f"Không có link MST khớp với {raw_tax_id}")


def enrich_excel(input_path, output_path, column, sheet, delay, timeout):
    df = pd.read_excel(input_path, sheet_name=sheet)
    if column not in df.columns:
        raise ValueError(f"Không thấy cột {column}")

    results = []
    total = len(df[column])

    for idx, raw in enumerate(df[column], start=1):
        tax_id = str(raw).strip()
        if not tax_id:
            continue
        try:
            info = fetch_tax_info(tax_id, timeout)
            results.append(info)
            print(f"[{idx}/{total}] ✅ {tax_id}")
        except Exception as e:
            print(f"[{idx}/{total}] ❌ {tax_id}: {e}")

        time.sleep(delay)

    if not results:
        raise ScrapeError("Không có dữ liệu nào.")

    out_df = df.merge(
        pd.DataFrame(results),
        how="left",
        left_on=column,
        right_on="raw_tax_id"
    )
    out_df.to_excel(output_path, index=False)
    print("Đã lưu:", output_path)
    return out_df


def main(argv=None):
    args = parse_args(argv)
    try:
        enrich_excel(
            args.input, args.output, args.column, args.sheet,
            args.delay, args.timeout
        )
    except Exception as e:
        print(e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

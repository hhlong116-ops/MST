"""
Scraper masothue.com:
- Đọc danh sách MST từ file Excel
- Với mỗi MST -> gọi endpoint auto search -> lấy link chi tiết
- Vào trang chi tiết đó, lấy thông tin và ghi ra file Excel mới
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://masothue.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9",
}


class ScrapeError(Exception):
    pass


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tra cứu thông tin doanh nghiệp từ masothue.com theo danh sách MST trong file Excel."
    )
    parser.add_argument("input", help="Đường dẫn file Excel đầu vào.")
    parser.add_argument(
        "--column",
        default="tax_id",
        help="Tên cột chứa MST trong file Excel (mặc định: tax_id).",
    )
    parser.add_argument(
        "--sheet",
        default=0,
        help="Tên hoặc index sheet (mặc định: 0).",
    )
    parser.add_argument(
        "--output",
        default="masothue_results.xlsx",
        help="File Excel kết quả (mặc định: masothue_results.xlsx).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay (giây) giữa các request để tránh bị chặn (mặc định: 1s).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Timeout cho mỗi request HTTP (mặc định: 15s).",
    )
    return parser.parse_args(argv)


def find_detail_url_for_tax_id(tax_id: str, timeout: float = 15.0) -> str:
    """
    Gọi endpoint auto search của masothue:
      https://masothue.com/Search/?type=auto&q=<MST>
    Sau đó tìm <a href="..."> có chứa '/<MST>-' trong href.
    Ví dụ:
      tax_id = '0100106264'
      href   = '/0100106264-cong-ty-co-phan-van-tai-duong-sat-ha-noi'
    """

    # dùng endpoint type=auto đúng như trong JSON-LD của site
    search_url = f"{BASE_URL}/Search/?type=auto&q={tax_id}"
    resp = requests.get(search_url, headers=HEADERS, timeout=timeout)
    if resp.status_code != 200:
        raise ScrapeError(f"Không tải được {search_url} (HTTP {resp.status_code})")

    soup = BeautifulSoup(resp.text, "html.parser")

    needle = f"/{tax_id}-"  # chuỗi cần tìm trong href

    candidate = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if needle in href:
            candidate = href
            break

    if candidate is None:
        raise ScrapeError(f"Không tìm thấy link chứa MST {tax_id} trên trang auto search.")

    if candidate.startswith("http"):
        return candidate
    return BASE_URL + candidate


def fetch_tax_info(tax_id: str, timeout: float = 15.0) -> Dict[str, str]:
    """
    Từ MST:
      1. Tìm URL chi tiết bằng find_detail_url_for_tax_id()
      2. Vào URL, parse HTML, lấy thông tin bảng 2 cột
    """

    detail_url = find_detail_url_for_tax_id(tax_id, timeout=timeout)

    resp = requests.get(detail_url, headers=HEADERS, timeout=timeout)
    if resp.status_code != 200:
        raise ScrapeError(f"Không tải được {detail_url} (HTTP {resp.status_code})")

    soup = BeautifulSoup(resp.text, "html.parser")

    data: Dict[str, str] = {
        "tax_id": tax_id,
        "masothue_url": detail_url,
    }

    # Tên doanh nghiệp: thường là <h1>
    title = soup.find("h1")
    if title:
        data["Tên doanh nghiệp"] = title.get_text(strip=True)

    # Lấy các bảng dạng 2 cột
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["th", "td"])
            if len(tds) != 2:
                continue
            label = tds[0].get_text(" ", strip=True)
            value = tds[1].get_text(" ", strip=True)
            if label and label not in data:
                data[label] = value

    if len(data) <= 2:
        # chỉ có tax_id + masothue_url -> coi như không có dữ liệu
        raise ScrapeError(f"Không đọc được dữ liệu hữu ích cho MST {tax_id}.")

    return data


def enrich_excel(
    input_path: str,
    output_path: str,
    column: str = "tax_id",
    sheet: str | int = 0,
    delay: float = 1.0,
    timeout: float = 15.0,
) -> pd.DataFrame:
    df = pd.read_excel(input_path, sheet_name=sheet)
    if column not in df.columns:
        raise ValueError(
            f"Không tìm thấy cột '{column}' trong file Excel. Các cột có: {list(df.columns)}"
        )

    total = len(df)
    results: List[Dict[str, str]] = []

    for idx, raw in enumerate(df[column], start=1):
        tax_id = str(raw).strip()
        if not tax_id:
            continue
        try:
            info = fetch_tax_info(tax_id, timeout=timeout)
            results.append(info)
            print(f"[{idx}/{total}] ✅ {tax_id} → {info.get('Tên doanh nghiệp', '')}")
        except Exception as exc:
            print(f"[{idx}/{total}] ❌ {tax_id}: {exc}")
        time.sleep(delay)

    if not results:
        raise ScrapeError("Không lấy được dữ liệu nào. Vui lòng kiểm tra danh sách mã.")

    result_df = pd.DataFrame(results)
    output_df = df.merge(result_df, how="left", left_on=column, right_on="tax_id")
    output_df.to_excel(output_path, index=False)
    print(f"Đã lưu kết quả vào {output_path}")
    return output_df


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        enrich_excel(
            input_path=args.input,
            output_path=args.output,
            column=args.column,
            sheet=args.sheet,
            delay=args.delay,
            timeout=args.timeout,
        )
        return 0
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

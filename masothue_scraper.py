"""
Utility to fetch business details from https://masothue.com for a list of tax IDs
and export the results to an Excel spreadsheet.
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
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
)


class ScrapeError(Exception):
    """Raised when a tax ID cannot be scraped."""


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Tải thông tin doanh nghiệp từ masothue.com và xuất ra file Excel mới."
        )
    )
    parser.add_argument(
        "input",
        help="Đường dẫn đến file Excel chứa danh sách mã số thuế.",
    )
    parser.add_argument(
        "--column",
        default="tax_id",
        help=(
            "Tên cột trong file Excel chứa mã số thuế. Mặc định: tax_id. "
            "Có thể trỏ đến bất kỳ cột nào chứa mã số thuế."
        ),
    )
    parser.add_argument(
        "--sheet",
        default=0,
        help=(
            "Tên hoặc chỉ số sheet cần đọc trong Excel. Mặc định là sheet đầu tiên."
        ),
    )
    parser.add_argument(
        "--output",
        default="masothue_results.xlsx",
        help="Đường dẫn file Excel xuất kết quả. Mặc định: masothue_results.xlsx",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Độ trễ (giây) giữa các request để tránh bị chặn. Mặc định: 1 giây.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Timeout (giây) cho mỗi request HTTP. Mặc định: 15 giây.",
    )
    return parser.parse_args(argv)


def fetch_tax_info(tax_id: str, timeout: float = 15.0) -> Dict[str, str]:
    """
    Fetch details for a single tax ID.

    The function is resilient to small HTML changes by collecting all two-column
    table rows (th/td or td/td) into a dictionary.
    """

    url = f"{BASE_URL}/{tax_id}"
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "vi-VN,vi;q=0.9"}
    response = requests.get(url, headers=headers, timeout=timeout)

    if response.status_code != 200:
        raise ScrapeError(f"Không thể tải {url} (mã {response.status_code}).")

    soup = BeautifulSoup(response.text, "html.parser")

    # Lấy tên doanh nghiệp nếu có.
    title = soup.find("h1")
    data: Dict[str, str] = {"tax_id": tax_id}
    if title:
        data["Tên doanh nghiệp"] = title.get_text(strip=True)

    # Thu thập tất cả các bảng dữ liệu dạng 2 cột.
    tables = soup.find_all("table")
    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) != 2:
                continue
            label = cells[0].get_text(" ", strip=True)
            value = cells[1].get_text(" ", strip=True)
            if label and label not in data:
                data[label] = value

    if len(data) == 1:  # chỉ chứa tax_id
        raise ScrapeError(f"Không tìm thấy dữ liệu cho mã {tax_id} tại {url}.")

    return data


def enrich_excel(
    input_path: str,
    output_path: str,
    column: str = "tax_id",
    sheet: str | int = 0,
    delay: float = 1.0,
    timeout: float = 15.0,
) -> pd.DataFrame:
    """
    Read the Excel file, fetch each tax ID's info, and write results to a new file.
    """

    df = pd.read_excel(input_path, sheet_name=sheet)
    if column not in df.columns:
        raise ValueError(
            f"Không tìm thấy cột '{column}' trong file Excel. Các cột có: {list(df.columns)}"
        )

    results: List[Dict[str, str]] = []
    for idx, tax_id in enumerate(df[column].astype(str)):
        tax_id = tax_id.strip()
        if not tax_id:
            continue
        try:
            info = fetch_tax_info(tax_id, timeout=timeout)
            results.append(info)
            print(f"[{idx+1}/{len(df)}] ✅ {tax_id} - {info.get('Tên doanh nghiệp', 'đã lấy dữ liệu')}")
        except Exception as exc:  # noqa: BLE001 - want to show any scraping error
            print(f"[{idx+1}/{len(df)}] ❌ {tax_id}: {exc}")
        time.sleep(delay)

    if not results:
        raise ScrapeError("Không lấy được dữ liệu nào. Vui lòng kiểm tra danh sách mã.")

    results_df = pd.DataFrame(results)

    # Kết hợp dữ liệu cũ và mới theo cột mã số thuế.
    output_df = df.merge(results_df, how="left", left_on=column, right_on="tax_id")
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
    except Exception as exc:  # noqa: BLE001 - CLI entry point should show any error
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

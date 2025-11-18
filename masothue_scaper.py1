"""
Scraper masothue.com:
- Đọc danh sách MST từ file Excel
- Với mỗi MST -> gọi https://masothue.com/Search/?type=auto&q=<MST>
  (endpoint này trả về luôn trang chi tiết)
- Parse HTML, lấy thông tin và ghi ra file Excel mới
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
    """Lỗi trong quá trình scrape masothue."""
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
        help="Tên hoặc index sheet cần đọc (mặc định: 0).",
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


def fetch_tax_info(tax_id: str, timeout: float = 15.0) -> Dict[str, str]:
    """
    Gọi trực tiếp:
      https://masothue.com/Search/?type=auto&q=<tax_id>

    Endpoint này trả về luôn trang chi tiết.
    Parse:
    - <h1>: Tên doanh nghiệp
    - Các bảng 2 cột: 'Mã số thuế', 'Trạng thái', 'Địa chỉ', ...
    """

    tax_id = (tax_id or "").strip()
    if not tax_id:
        raise ScrapeError("MST rỗng.")

    url = f"{BASE_URL}/Search/?type=auto&q={tax_id}"
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    if resp.status_code != 200:
        raise ScrapeError(f"Không tải được {url} (HTTP {resp.status_code}).")

    soup = BeautifulSoup(resp.text, "html.parser")

    # Check MST xuất hiện trong trang (nới lỏng: so sánh theo chuỗi số, bỏ '-')
    page_text = soup.get_text(" ", strip=True)
    norm_mst = tax_id.replace("-", "")
    if norm_mst and norm_mst not in page_text.replace("-", ""):
        raise ScrapeError(f"Không thấy MST {tax_id} trong nội dung trang.")

    data: Dict[str, str] = {
        "tax_id": tax_id,      # luôn là string
        "masothue_url": resp.url,  # URL thực tế (có thể đã canonical/redirect)
    }

    # Tên doanh nghiệp: thường là <h1>
    title = soup.find("h1")
    if title:
        data["Tên doanh nghiệp"] = title.get_text(strip=True)

    # Các bảng 2 cột
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
    """
    Đọc Excel, tra từng MST, ghép dữ liệu và ghi ra file mới.

    Quan trọng: ép cột MST trong Excel về kiểu string để merge đúng,
    tránh trường hợp Excel tự convert sang number.
    """

    # Ép riêng cột MST về string
    df = pd.read_excel(
        input_path,
        sheet_name=sheet,
        dtype={column: str},
    )
    df[column] = df[column].str.strip()

    if column not in df.columns:
        raise ValueError(
            f"Không tìm thấy cột '{column}' trong file Excel. Các cột có: {list(df.columns)}"
        )

    total = len(df)
    results: List[Dict[str, str]] = []

    for idx, raw in enumerate(df[column], start=1):
        tax_id = (raw or "").strip()
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

    # Đảm bảo tax_id trong result_df là string
    result_df["tax_id"] = result_df["tax_id"].astype(str).str.strip()

    # Merge theo string ↔ string
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

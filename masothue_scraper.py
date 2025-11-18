"""
Utility to fetch business details from https://masothue.com for a list of tax IDs
and export the results to an Excel spreadsheet.
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


# Link chi tiết thường dạng /0100106264-cong-ty-... hoặc /0100106264-011-chi-nhanh-...
DETAIL_LINK_RE = re.compile(r"^/[\d-]+-")


def digits_only(s: str) -> str:
    """Lấy chuỗi chỉ gồm chữ số từ MST (bỏ dấu, khoảng trắng, ký tự khác)."""
    return re.sub(r"\D", "", s or "")


def extract_company_data_from_detail(
    detail_url: str,
    raw_tax_id: str,
    timeout: float,
) -> Optional[Dict[str, str]]:
    """
    Truy cập trang chi tiết, kiểm tra 'Mã số thuế' trên trang có khớp với
    MST đầu vào (sau khi bỏ dấu) không. Nếu KHÔNG khớp → trả None.
    Nếu khớp → trả dict dữ liệu đầy đủ (bao gồm Trạng thái, Địa chỉ, ...).
    """
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "vi-VN,vi;q=0.9"}
    resp = requests.get(detail_url, headers=headers, timeout=timeout)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    data: Dict[str, str] = {
        "raw_tax_id": raw_tax_id,
        "masothue_url": detail_url,
    }

    # Lấy tên doanh nghiệp (h1)
    title = soup.find("h1")
    if title:
        data["Tên doanh nghiệp"] = title.get_text(strip=True)

    # Quét tất cả các bảng 2 cột để lấy thông tin
    tax_id_on_page: Optional[str] = None

    tables = soup.find_all("table")
    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) != 2:
                continue
            label = cells[0].get_text(" ", strip=True)
            value = cells[1].get_text(" ", strip=True)
            if not label:
                continue

            # Lưu MST từ trang
            if "Mã số thuế" in label and not tax_id_on_page:
                tax_id_on_page = value

            # Lưu các trường khác (Trạng thái, Địa chỉ, ...)
            if label not in data:
                data[label] = value

    # Nếu không đọc được MST trên trang → coi như không hợp lệ
    if not tax_id_on_page:
        return None

    data["tax_id_on_page"] = tax_id_on_page

    # So sánh MST chỉ theo chữ số (bỏ dấu, bỏ khoảng trắng)
    input_digits = digits_only(raw_tax_id)
    page_digits = digits_only(tax_id_on_page)

    if not input_digits or not page_digits or input_digits != page_digits:
        # Không khớp MST → không dùng kết quả này
        return None

    # Có ít nhất MST hợp lệ + vài trường nữa thì mới coi là có dữ liệu
    if len(data) <= 3:  # raw_tax_id, masothue_url, tax_id_on_page
        return None

    return data


def fetch_tax_info(raw_tax_id: str, timeout: float = 15.0) -> Dict[str, str]:
    """
    Fetch details for a single tax ID.

    Quy trình:
    1. Gọi trang search:  https://masothue.com/Search/?q=<raw_tax_id>
    2. Lấy danh sách các link chi tiết ứng viên.
    3. Lần lượt vào từng link, đọc 'Mã số thuế' trên trang và so sánh với MST đầu vào
       (so sánh theo chữ số). Chỉ khi KHỚP mới trả dữ liệu.
    """

    query = raw_tax_id.strip()
    if not query:
        raise ScrapeError("Mã số thuế rỗng.")

    headers = {"User-Agent": USER_AGENT, "Accept-Language": "vi-VN,vi;q=0.9"}

    # BƯỚC 1: TÌM CÁC LINK ỨNG VIÊN QUA TRANG SEARCH
    search_url = f"{BASE_URL}/Search/?q={query}"
    search_resp = requests.get(search_url, headers=headers, timeout=timeout)
    if search_resp.status_code != 200:
        raise ScrapeError(
            f"Không thể tải trang tìm kiếm {search_url} (mã {search_resp.status_code})."
        )

    search_soup = BeautifulSoup(search_resp.text, "html.parser")

    candidate_links: List[str] = []
    for a in search_soup.find_all("a", href=True):
        href = a["href"]
        if DETAIL_LINK_RE.match(href):
            if href not in candidate_links:
                candidate_links.append(href)

    if not candidate_links:
        raise ScrapeError(
            f"Không tìm thấy bất kỳ link chi tiết nào cho MST {raw_tax_id} trên {search_url}."
        )

    # BƯỚC 2: LẦN LƯỢT THỬ TỪNG LINK, CHỈ GIỮ LINK CÓ MST TRÊN TRANG KHỚP VỚI INPUT
    for href in candidate_links[:20]:  # giới hạn tối đa 20 link để tránh quá tải
        detail_url = href if href.startswith("http") else BASE_URL + href
        info = extract_company_data_from_detail(detail_url, raw_tax_id, timeout=timeout)
        if info is not None:
            return info

    # Nếu duyệt hết mà không có link nào MST khớp → báo lỗi
    raise ScrapeError(
        f"Không tìm thấy kết quả có MST khớp với {raw_tax_id} trên masothue.com."
    )


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
    total = len(df[column])

    for idx, raw_value in enumerate(df[column], start=1):
        raw_tax_id = str(raw_value).strip()
        if not raw_tax_id:
            continue
        try:
            info = fetch_tax_info(raw_tax_id, timeout=timeout)
            results.append(info)
            print(
                f"[{idx}/{total}] ✅ {raw_tax_id} → "
                f"{info.get('Tên doanh nghiệp', 'đã lấy dữ liệu')}"
            )
        except Exception as exc:  # noqa: BLE001 - log mọi lỗi scrape
            print(f"[{idx}/{total}] ❌ {raw_tax_id}: {exc}")
        time.sleep(delay)

    if not results:
        raise ScrapeError("Không lấy được dữ liệu nào. Vui lòng kiểm tra danh sách mã.")

    results_df = pd.DataFrame(results)

    # Join theo cột gốc (tax_id) ↔ raw_tax_id để giữ nguyên dữ liệu input
    output_df = df.merge(
        results_df,
        how="left",
        left_on=column,
        right_on="raw_tax_id",
    )
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

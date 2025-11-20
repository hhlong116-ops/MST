# masothue.com scraper

Tiện ích Python để lấy thông tin doanh nghiệp từ [masothue.com](https://masothue.com/) dựa trên danh sách mã số thuế và xuất dữ liệu sang Excel.

## Cài đặt

```bash
pip install -r requirements.txt
```

## Sử dụng

1. Chuẩn bị file Excel có cột chứa mã số thuế, ví dụ:

   | tax_id        | ghi_chu |
   | ------------- | ------- |
   | 0100100100    | ví dụ 1 |
   | 0305029405    | ví dụ 2 |

2. Chạy tiện ích và chỉ định cột chứa MST:

   ```bash
   python masothue_scraper.py duong_dan_file.xlsx --column tax_id --output ket_qua.xlsx
   ```

   Tham số chính:
   * `duong_dan_file.xlsx`: file Excel đầu vào chứa danh sách mã số thuế.
   * `--column`: tên cột chứa mã số thuế (mặc định `tax_id`).
   * `--sheet`: tên hoặc chỉ số sheet muốn đọc (mặc định sheet đầu tiên).
   * `--output`: file Excel kết quả (mặc định `masothue_results.xlsx`).
   * `--delay`: độ trễ giữa các request (mặc định 1 giây) để hạn chế bị chặn.
   * `--timeout`: timeout cho mỗi request (mặc định 15 giây).

3. Xem file kết quả

   File `ket_qua.xlsx` giữ nguyên dữ liệu gốc và thêm các cột thông tin lấy từ masothue.com (tên doanh nghiệp, địa chỉ, ngày cấp…).

Ví dụ đọc một sheet có tên "MST" và chờ 2 giây giữa các lần gọi:

```bash
python masothue_scraper.py duong_dan_file.xlsx --sheet MST --column tax_id --delay 2
```

---

# Newborn product market research prototype

This repository also provides a **local-only** workflow to analyze baby/newborn products using pre-exported datasets (no live scraping). The pipeline aggregates social engagement, matches posts to catalog items, and powers an interactive Streamlit dashboard.

## Folder structure

```
app.py                 # Streamlit dashboard
data_pipeline.py       # End-to-end aggregation script
src/
  utils_text.py        # Text cleaning + fuzzy matching helpers
```

## Input schemas

Place the following CSV files in the working directory (paths can be overridden with CLI arguments):

**1) Social posts (`social_posts.csv`)**
- `post_id`, `image_id`, `image_url`, `caption`, `hashtags`, `likes`, `comments`, `posted_at`, `platform`

**2) Product catalog / prices (`products_catalog.csv`)**
- `product_id`, `product_name`, `brand`, `model`, `category`, `price`, `currency`, `url`, `rating`, `marketplace`

**3) Optional image matches (`image_matches.csv`)**
- `image_id`, `product_id`, `score`

_All files are assumed to come from official APIs or approved export tools. The code does not scrape any websites or automate logins._

## Running the data pipeline

```bash
python data_pipeline.py \
  --social-posts social_posts.csv \
  --products products_catalog.csv \
  --image-matches image_matches.csv \
  --output aggregated_products.csv
```

What it does:
- Cleans captions/hashtags, filters posts about baby/newborn topics using configurable keywords.
- Infers categories and potential brand/model mentions.
- Matches posts to catalog products via optional image matches, then text similarity.
- Aggregates social engagement metrics and merges price statistics plus up to three sample URLs per product.
- Writes `aggregated_products.csv` for the dashboard.

## Launching the Streamlit dashboard

```bash
streamlit run app.py
```

Dashboard features:
- Sidebar filters for category, brand, price range, and engagement thresholds.
- KPIs: distinct products, total posts, and top categories.
- Sortable product table with median price and up to three reference price links.
- Charts: top categories, brand breakdown within a category, and recent post counts per product.

If the aggregated file is missing, the app will prompt you to run `data_pipeline.py` first.

## Customization tips
- Extend the keyword lists in `data_pipeline.py` (`BABY_KEYWORDS`, `CATEGORY_KEYWORDS`) to fit your market vocabulary.
- Adjust fuzzy matching thresholds in `src/utils_text.py` if you want stricter or looser text matching.
- Swap in different CSV filenames via CLI flags; the code expects local files only.

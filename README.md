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

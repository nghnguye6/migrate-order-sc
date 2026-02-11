# Hướng Dẫn Xuất Dữ Liệu và Migrate Đơn Hàng

## Bước 1: Xuất Dữ Liệu Từ Database
Sử dụng query để xuất dữ liệu từ database.

## Bước 2: Giải Nén và Thiết Lập Môi Trường
1. Tải code về và giải nén file.
2. Chạy lệnh sau để thiết lập môi trường ảo:
    ```bash
    python3 -m venv /path/to/extracted_folder
    ```
    Điều này sẽ tạo ra file `pyvenv.cfg`.

## Bước 3: Kích Hoạt Môi Trường Ảo
Chuyển đến thư mục vừa giải nén và chạy lệnh sau:
```bash
source bin/activate
```

## Bước 4: Migrate Dữ Liệu
Chạy lệnh sau để migrate dữ liệu:
```bash
python3 script.py {file_order_data_export_từ_database} {số_dòng_trên_1_file}
```
*Lưu ý: Nếu bạn đặt số dòng là 5000, dữ liệu sẽ được chia thành các file mỗi file 5000 dòng.*

## Bước 5: Kiểm Tra Kết Quả
Sau khi migrate xong, trong thư mục hiện tại sẽ có thêm một file zip. Giải nén file này và import vào Shopify.

## Xử Lý Các Trường Hợp Lỗi Khi Import Vào Shopify
1. Sau khi import xong, Matrixify sẽ cho phép bạn tải file kết quả import.
2. Tải về và kiểm tra cột "Import Result" để lọc các trường hợp "OK" và xóa đi.
3. Kiểm tra cột "Import Comment" để xem lý do bị lỗi. Dựa vào lý do này, sửa lại file và thực hiện import lại.

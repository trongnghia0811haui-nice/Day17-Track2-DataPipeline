# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:Trần Trọng Nghĩa** …  **Lớp:** Track2_E403  **Ngày:** 2026-08-17  
**Git repo:** https://github.com/trongnghia0811haui-nice/Day17-Track2-DataPipeline

---

## 0 · Kết quả `make verify`

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 28.5s
  run 2/3 … 27.6s
  run 3/3 … 26.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt

```

Kết quả `make quick` hiện tại: **4/4 tiêu chí chính đạt**. `make verify` đầy đủ
ba lượt cần được chạy sau cùng để ghi lại toàn bộ checksum ổn định.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau retry, `gold_training_set` có 38.750 hàng thay vì 12.480; cùng một `ticket_id` xuất hiện nhiều lần. |
| **Nguyên nhân** | Model incremental không khai báo khóa duy nhất và chiến lược thay thế, nên dbt sinh ghi append/`INSERT`; retry cùng partition và CDC update tiếp tục thêm bản ghi cho cùng entity. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`; trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng · sau: 12.480 hàng · không còn ticket trùng · `make verify` cần bổ sung checksum của cả 3 lượt. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định nhưng chỉ có 8.645 hàng, thiếu 455 cặp ngày/khách hàng ở các ngày cũ. |
| **P99 độ trễ đo được** | **2,7258333333333336 ngày** (xấp xỉ **2,7258 ngày**); tỷ lệ late là 5,05%. |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 để bao phủ độ trễ quan sát được; `max` là 2,9446875 ngày nhưng dùng max làm window cố định sẽ khiến mọi lượt chạy sau quét rộng hơn cần thiết. |
| **Nguyên nhân** | Điều kiện incremental chỉ lấy `event_date > max(event_date)` của bảng đích, nên event đến muộn thuộc ngày cũ không bao giờ được tính lại. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`, mở lookback 3 ngày, khai báo khóa ghép `(event_date, customer_id)` và dùng `delete+insert` để thay thế nhóm được tính lại. |
| **Bằng chứng** | Trước: 8.645 hàng · sau: 9.100 hàng · rows đúng theo expected. |

P99 được chọn thay vì `max` vì P99 bao phủ phần lớn dữ liệu trễ với chi phí quét ổn
định; dùng `max` có thể khiến một outlier làm tăng chi phí mọi lượt chạy sau.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có 6.606 hàng sai/null; `quarantine_tickets` rỗng dù có 312 CDC record lỗi. |
| **Nguyên nhân** | Macro chỉ `try_cast`, nên không nhận diện schema evolution dạng nhãn chữ và vẫn chấp nhận số ngoài miền; Silver lại xếp hạng trước khi lọc, còn quarantine luôn dùng `where false`. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Số `1..4` giữ nguyên; `urgent/high/medium/low` map lần lượt thành `1/2/3/4`; `P1/P2/0/5/-1/unknown/rỗng/NULL` trả `NULL` và đưa vào quarantine. |
| **Cách khắc phục** | Sửa macro dùng chung, lọc bản ghi không chuẩn hóa trước `row_number()`, bật điều kiện quarantine, bật contract và test `not_null`/`accepted_values` trong Silver. |
| **Bằng chứng** | `quarantine_tickets`: 0 → 312 · `dbt test`: 9/9 → 11/11 pass · priority sạch · Silver vẫn đủ 12.480 ticket. |

Bronze giữ raw record để audit; lỗi được chặn và định tuyến ở Silver để vài trăm
record hỏng không làm dừng việc xử lý dữ liệu hợp lệ còn lại.

---

## 4 · Bài mở rộng trong `EXTRA.md`

| Bài | Nguyên nhân | Cách khắc phục | Bằng chứng |
|---|---|---|---|
| **A — Dashboard Parquet** | 5.000 file nhỏ, không partition; filter ngày bọc trong `strftime`. | `tools/compact.py`: partition theo `event_date`, sort theo ngày/khách hàng, row group 4.096; `queries/dashboard.sql`: dùng dataset mới và predicate sargable. | Rows scanned `5.000.000 → 9.324` (536,3×), files `5.000 → 14`, hash không đổi. |
| **B — Consumer crash** | Commit offset trước khi ghi tạo at-most-once và làm mất batch khi process chết. | `ingest/consumer.py`: ghi trước, commit offset sau; `event_id` là primary key và upsert bằng `ON CONFLICT DO UPDATE`. | 20.000/20.000 message, không mất, không trùng, `C == A`, bài B đạt. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Grain, natural key và SQL write strategy của incremental model; đặc biệt kiểm tra retry có tạo append hay không. |
| 2 | Phân bố event-time/ingestion-time và P99 trước khi chọn lookback. |
| 3 | Raw values, data contract, thứ tự quarantine/ranking và khả năng giữ lại bản ghi hợp lệ trước đó. |


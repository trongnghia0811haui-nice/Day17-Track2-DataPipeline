# PROJECT MEMORY — LAB 17 Data Pipeline Engineering

> Đọc file này ở đầu phiên làm việc tiếp theo. Đây là bản tổng hợp trạng thái
> repository sau khi đã rà soát toàn bộ file mã nguồn, cấu hình, tài liệu và test
> vào ngày 2026-08-17. File này là ghi chú điều phối, không thay thế source of
> truth: nếu có khác biệt, code hiện tại và kết quả `make verify` được ưu tiên.

## 1. Trạng thái repository tại thời điểm ghi nhớ

- Repository: `Day17-Track2-DataPipeline` — lab 17 về Data Pipeline Engineering.
- Workspace: `D:/Tailieu-2026/vin/Day17_lab/Day17-Track2-DataPipeline`.
- Git: nhánh `main`, đang cùng `origin/main`, HEAD là commit `a0a8012`
  (`Rút gọn lab còn 3 nhiệm vụ...`).
- Trước khi tạo file này, working tree sạch; chưa có logic nào trong project
  được sửa. File này là artifact mới cần được giữ lại cho phiên sau.
- Các file runtime bị ignore và hiện không có trong repository: `seed/*.jsonl`,
  `warehouse.duckdb`, `data/`, `.venv/`, `dbt/target/`, `dbt/logs/`.
- Chưa chạy `make setup`, `make pipeline` hay `make verify` trong lần audit
  này; các số liệu “ban đầu” bên dưới là trạng thái được tài liệu lab cam kết.
  Muốn xác nhận bằng runtime, chạy lại setup/pipeline rồi dùng query hoặc verify.

## 2. Mục đích của project

Đây là bài lab chẩn đoán và sửa một pipeline của nền tảng AI hỗ trợ khách hàng,
không phải bài xây dựng hệ thống từ đầu. Hạ tầng thật được mô phỏng local:

```text
Postgres tickets (CDC) ─┐
S3 transcripts (JSON)  ─┼─> Bronze ─> Silver ─┬─> gold_doc_chunks    -> RAG index
Kafka events/feedback ─┘                     ├─> gold_training_set  -> Classifier
                                             └─> gold_feature_daily -> Routing agent
```

Mục tiêu chính:

1. Transform idempotent: chạy lại nhiều lần không nhân bản dữ liệu.
2. Xử lý late-arriving data bằng lookback window.
3. Dùng data contract để giữ schema và quarantine bản ghi lỗi thay vì dừng cả
   pipeline.

Kho dữ liệu là DuckDB; dbt tạo Silver/Gold. Kafka, S3 và Postgres chỉ được mô
phỏng bằng JSONL seed và commit log trên đĩa. Không cần Docker hay cloud.

## 3. Cách chạy và vòng đời dữ liệu

Yêu cầu: Python 3.11+, GNU `make`, các package trong `requirements.txt`:
`duckdb>=1.4,<2`, `dbt-core>=1.9,<2`, `dbt-duckdb>=1.9,<2`.

Các lệnh quan trọng:

```bash
make setup       # tạo .venv, cài package, sinh seed và expected counts
make pipeline    # replay 14 ngày, một lượt
make quick       # verify một lượt
make verify      # xoá warehouse, chạy 3 lượt, kiểm tra ổn định/đúng số hàng
make dbt-test    # chỉ chạy dbt test
make reset       # xoá warehouse.duckdb và WAL, giữ seed/data
make clean       # xoá warehouse, dbt target/logs và data/crash
make seed-extra  # sinh 5.000 parquet nhỏ cho bài mở rộng A
make explain     # đo dashboard (sau seed-extra)
make compact     # chạy tools/compact.py (sau khi hiện thực bài A)
make crash-test  # kiểm tra consumer (bài mở rộng B)
```

`tools/run_pipeline.py` chạy các ngày `2026-08-03` đến `2026-08-16` theo thứ tự.
Với mỗi ngày: `load_day()` nạp các dòng có `_ingested_at` trong ngày vào Bronze,
đóng kết nối DuckDB, rồi gọi dbt với `var("run_date")`. Bronze xoá partition
`_batch_date` trước khi insert nên bản thân loader đã idempotent.

`tools/verify.py` mặc định xoá warehouse, chạy pipeline 3 lần, chụp checksum
cho bốn bảng, so row count với `expected/`, chạy dbt test, kiểm tra invariant,
đọc DAG bằng AST và (nếu có baseline) kiểm tra dashboard. Mặc định script luôn
thoát mã 0 dù bài chưa đạt; dùng `--strict` cho CI.

Lưu ý môi trường: Makefile dùng `SHELL := /bin/bash` và layout `.venv/bin/...`,
trong khi phiên audit hiện ở PowerShell Windows. Khi chạy thật cần Git Bash/WSL
hoặc một môi trường có GNU make, bash và layout lệnh tương thích.

## 4. Dữ liệu seed và các con số bất biến

`seed/generate.py` là file không được sửa. Dữ liệu được sinh deterministic bằng
DuckDB và `ROW_NUMBER`, nên các máy phải ra cùng kết quả.

- 14 ngày: `2026-08-03` → `2026-08-16`.
- 650 customer; `C0001` có tên `ACME` và bị skew lớn.
- Ticket tạo mới: `12,735`.
- `255` ticket bị delete ngay trong ngày, không vào Gold.
- Ticket sống kỳ vọng: `12,480`.
- `998` ticket có update hợp lệ; thêm `312` ticket có update bị lỗi priority.
  Tổng CDC update có thể gặp là `1,310`; CDC gồm `c`, `u`, `d`.
- `312` bản ghi CDC lỗi priority là số kỳ vọng của quarantine.
- Transcript: `5,200`, mỗi transcript có 6 chunk ⇒ `31,200` doc chunks.
- Event có đủ mọi tổ hợp `(event_date, customer_id)` ⇒ `14 × 650 = 9,100`
  dòng feature cuối cùng.
- Có `455` tổ hợp chỉ có dữ liệu về muộn; dữ liệu late chỉ được sinh đến
  `2026-08-13` (`day_idx <= 10`). Ngoài ra có late record ngẫu nhiên với tỷ lệ
  2.5% (`25/1000`) trong giai đoạn đó.
- Delay bình thường là `delay_bucket * 21` giây (xấp xỉ 0–5.8 giờ). Delay late
  là `155520 + delay_bucket * 99` giây (xấp xỉ 1.8–2.95 ngày). P99 phải được
  đo bằng query trên `bronze_events`, không được tự điền số chưa đo vào report.
- Bài mở rộng A sinh `5,000` file parquet nhỏ trong `data/gold_events/`.

Expected counts được lưu đồng thời trong `expected/summary.json` và các file
`.count`, không được chỉnh sửa chúng:

| Bảng | Expected |
|---|---:|
| `gold_training_set` | 12,480 |
| `gold_feature_daily` | 9,100 |
| `gold_doc_chunks` | 31,200 |
| `quarantine_tickets` | 312 |

Baseline bài mở rộng A hiện có trong `expected/dashboard_baseline.json`:
`rows_scanned=5,000,000`, `rows_on_disk=130,683`, `files=5,000`,
`result_hash=4379e4c5d9f3`, `result_rows=1`.

## 5. Bản đồ file

### Tài liệu và cấu hình

- `README.md`: bối cảnh, triệu chứng, yêu cầu và tiêu chí chính.
- `GUIDE.md`: trình tự điều tra; không đưa lời giải hoàn chỉnh nhưng chứa các
  câu hỏi và pseudo-code cần bám theo.
- `RUBRIC.md`: chấm điểm; cấm sửa `expected/`, `seed/generate.py`,
  `tools/verify.py`, `tools/explain.py`, `tools/common.py`.
- `EXTRA.md`: bài thưởng A (Parquet layout) và B (consumer delivery semantics).
- `REPORT_TEMPLATE.md`: format báo cáo triệu chứng → root cause → fix → bằng
  chứng; nhiệm vụ 2 bắt buộc có P99.
- `Makefile`: entrypoint cho setup, seed, pipeline, verify, dbt test và extras.
- `requirements.txt`: dependencies; `.gitignore`: loại runtime artifacts.

### Ingest và dữ liệu nguồn

- `seed/generate.py`: dựng bảng tạm và ghi `tickets_cdc.jsonl`,
  `events.jsonl`, `transcripts.jsonl`; `--extra` còn ghi dataset parquet nhỏ.
- `ingest/load_bronze.py`: đọc seed vào `raw_*` một lần; tạo `bronze_*` và
  xoá/nạp lại partition theo `_batch_date`. Bronze giữ payload gốc, đặc biệt
  `priority_raw` là VARCHAR.
- `ingest/log_client.py`: Kafka-lite; `poll()` đọc JSONL, `position` tăng,
  `commit()` ghi offset vào JSON. Không cần sửa cho ba nhiệm vụ chính.
- `ingest/consumer.py`: consumer bài mở rộng B; DDL hiện không có unique key,
  `write_batch()` dùng INSERT thuần.

### dbt

- `dbt/dbt_project.yml`: project `lab17`; Silver mặc định materialized table;
  `run_date` mặc định `2026-08-03`.
- `dbt/profiles.yml`: adapter DuckDB; path lấy từ `LAB17_DB`, 4 threads.
- `dbt/models/sources.yml`: khai báo ba bảng Bronze.
- `dbt/models/silver/silver_events.sql`: dedup `event_id` theo `_ingested_at`,
  thêm `event_date` và `ingested_date`; không phải file lỗi.
- `dbt/models/silver/silver_transcripts.sql`: dedup `transcript_id`; không lỗi.
- `dbt/models/silver/silver_tickets.sql`: lấy trạng thái ticket mới nhất từ CDC;
  hiện đang rank trước khi loại priority lỗi — phải sửa thứ tự logic.
- `dbt/models/silver/quarantine_tickets.sql`: chọn bản ghi CDC lỗi; hiện
  `where false` nên luôn rỗng.
- `dbt/models/silver/schema.yml`: contract hiện `enforced: false`; test
  `priority` đang comment.
- `dbt/macros/normalize_priority.sql`: macro dùng chung cho Silver và
  quarantine; hiện chỉ `try_cast` và chưa map nhãn chữ.
- `dbt/models/gold/gold_training_set.sql`: incremental theo `_ingested_at` của
  `run_date`, grain 1 row/ticket; thiếu key/strategy.
- `dbt/models/gold/gold_feature_daily.sql`: incremental grain
  `(event_date, customer_id)`; hiện chỉ lọc `event_date > max(event_date)`.
- `dbt/models/gold/gold_doc_chunks.sql`: table materialization, 1 row/chunk;
  nhóm đối chứng, không được làm hỏng.
- Các `schema.yml` Gold hiện chỉ có test `chunk_id` unique/not_null; Gold
  training/feature chủ yếu được `tools/verify.py` kiểm tra.

### DAG, query và tooling

- `dags/ai_training_pipeline.py`: DAG production chỉ để đọc; hiện
  `catchup=True`, `max_active_runs` bị bỏ comment. `tools/check_dag.py` dùng AST
  và yêu cầu `catchup is False` cùng `max_active_runs == 1`.
- `tools/common.py`: đường dẫn, ngày chạy, kết nối DuckDB, expected counts,
  checksum. Checksum dùng các cột:
  - training: `ticket_id, priority, category, status`
  - feature: `event_date, customer_id, n_events, p95_latency_ms`
  - chunks: `chunk_id, transcript_id, chunk_index`
  - quarantine: `ticket_id, cdc_seq`
- `tools/run_pipeline.py`: orchestration local, reset warehouse và dbt runner.
- `tools/verify.py`: vòng phản hồi/chấm tự động; không sửa file này.
- `tools/check_dag.py`: AST checker của DAG.
- `queries/dashboard.sql`: query ACME ngày `2026-08-09`, đọc
  `data/gold_events/*.parquet`, hiện filter `strftime(event_time, ...)`.
- `tools/explain.py`: đo rows scanned, rows on disk, file count, result hash;
  ép DuckDB 1 thread.
- `tools/compact.py`: skeleton bài A, chưa có logic COPY.
- `tools/crash_test.py`: chạy consumer một mạch, crash ở batch 7, restart và
  so sánh tổng row/distinct event_id.

## 6. Trạng thái lỗi ban đầu và mục tiêu

Theo README, trạng thái skeleton ban đầu là:

| Bảng/kiểm tra | Ban đầu | Kỳ vọng | Nguyên nhân cần điều tra |
|---|---:|---:|---|
| `gold_training_set` | 38,750, không ổn định | 12,480 | incremental append/thiếu key |
| `gold_feature_daily` | 8,645, ổn định nhưng thiếu | 9,100 | không có lookback cho late data |
| `gold_doc_chunks` | 31,200, ổn định | 31,200 | nhóm đối chứng |
| `quarantine_tickets` | 0 | 312 | `where false` |
| priority sai/null trong Silver | 6,606 dòng | 0 | `try_cast` và chưa lọc |
| training duplicate ticket | 12,480 ticket lặp | 0 | ghi incremental không idempotent |
| dbt test | 9/9 pass | pass và >9 test | test priority đang comment |
| DAG | `True / None` | `False / 1` | cấu hình catchup/concurrency |

Không được nhầm “ỔN ĐỊNH” với “ĐÚNG”: một bảng feature hiện ổn định vì luôn
thiếu cùng một tập late data.

## 7. Việc cần làm chính (chưa thực hiện)

### Nhiệm vụ 1 — training set phình sau retry

Hiện trạng:

- `gold_training_set` là entity table, grain 1 row/`ticket_id`.
- Model là `incremental`, có `on_schema_change='fail'` nhưng chưa có
  `unique_key` và `incremental_strategy`.
- Lọc theo `_ingested_at`/`run_date` là có chủ đích để backfill partition, không
  được xoá bỏ.
- Vì thiếu key, dbt-duckdb sinh kiểu ghi append; chạy lại cùng partition sẽ
  thêm row cũ. CDC update còn làm cùng ticket xuất hiện ở nhiều ngày.

Hướng xử lý cần xác nhận bằng `make verify`: merge theo natural key
`ticket_id` (không dùng append; strategy cụ thể phải tương thích dbt-duckdb),
giữ nguyên WHERE run_date. Trong DAG đặt `catchup=False` và
`max_active_runs=1`. Hai tham số DAG chỉ giảm nguy cơ retry/backfill đồng thời;
root cause vẫn là incremental write không idempotent.

### Nhiệm vụ 2 — feature daily thiếu ngày cũ

Hiện trạng:

- Grain là hai cột `(event_date, customer_id)`.
- Điều kiện hiện tại: `event_date > (select max(event_date) from {{ this }})`.
- Khi event xảy ra ngày cũ nhưng tới kho muộn, nó không còn lớn hơn max ngày đã
  có nên bị bỏ qua vĩnh viễn.

Trước khi sửa, đo P50/P95/P99/max và tỷ lệ late bằng query trong `GUIDE.md`:

```sql
select
  quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) p50,
  quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) p95,
  quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) p99,
  max(date_diff('second', event_time, _ingested_at)/86400.0) max_delay,
  avg(case when _ingested_at > event_time + interval 1 day then 1.0 else 0 end) late_rate
from bronze_events;
```

Sau đó mở lookback đủ bao phủ P99, không nhất thiết dùng max vì max có thể là
outlier và làm mọi lượt chạy sau tốn thêm chi phí. Khi mở window, cùng một nhóm
ngày/customer được tính lại; phải khai báo composite key
`['event_date', 'customer_id']` và dùng cơ chế replace (`merge` hoặc
`delete+insert` phù hợp adapter), nếu không row sẽ cộng dồn và phá tính ổn định.

### Nhiệm vụ 3 — priority đổi kiểu và bản ghi lỗi

Macro phải phân biệt ba nhóm:

| Nhóm | Giá trị | Xử lý |
|---|---|---|
| Số hợp lệ | `'1'`, `'2'`, `'3'`, `'4'` | giữ thành integer |
| Nhãn hợp lệ do schema evolution | `urgent`, `high`, `medium`, `low` | map lần lượt 1, 2, 3, 4 |
| Lỗi thật | `P1`, `P2`, `0`, `5`, `-1`, rỗng, `NULL`, `unknown` | trả `NULL`, quarantine |

Các bước bắt buộc:

1. Sửa `normalize_priority.sql` thành CASE đủ ba nhóm. Macro này phải được
   dùng giống nhau ở Silver và quarantine.
2. Trong `silver_tickets.sql`, lọc dòng có macro trả NULL **trước** khi
   `row_number()` theo `(event_time desc, cdc_seq desc)`. Sau đó mới chọn row mới
   nhất và loại `op='d'`. Chỉ bỏ bản ghi CDC hỏng, không làm mất cả ticket có
   trạng thái hợp lệ trước đó.
3. Trong `quarantine_tickets.sql`, thay `where false` bằng điều kiện macro trả
   NULL; grain là 1 row cho 1 CDC record, không phải 1 row/ticket.
4. Trong `silver/schema.yml`, đổi `contract.enforced` thành `true`; bật test
   `priority` gồm `not_null` và `accepted_values: [1,2,3,4]` với `quote: false`.
   Contract kiểm tra kiểu, test kiểm tra miền giá trị — cần cả hai.

Thiết kế: Bronze nên giữ raw row để audit/điều tra, Silver là nơi chuẩn hoá và
định tuyến lỗi. Không để vài trăm record hỏng chặn hơn 130 nghìn event và
31.200 chunk hợp lệ; quarantine là hàng đợi để xử lý sau.

## 8. Bài mở rộng (chưa làm)

### A — dashboard Parquet

`data/gold_events/` hiện có 5.000 file nhỏ, không partition, thứ tự ngẫu nhiên.
Query filter theo customer và ngày nhưng path không mang thông tin ngày, đồng
thời đang bọc `event_time` trong `strftime`, nên pruning kém. `tools/compact.py`
có skeleton `COPY ... TO ...`: cần dataset mới partition theo cột ngày, sort
theo ngày/customer để row-group min/max hữu ích, chọn row-group phù hợp, rồi sửa
`queries/dashboard.sql` sang dataset mới và predicate sargable. Kết quả hash phải
giữ nguyên; rows scanned phải giảm ít nhất 10 lần, số file phải giảm.

### B — consumer crash giữa batch

Hiện `consumer.commit()` chạy trước `maybe_crash()` và `write_batch()`.
Crash ở điểm đó tạo at-most-once và làm mất batch vì offset đã commit nhưng row chưa
ghi. Đổi thành ghi trước, commit sau sẽ là at-least-once; khi replay, INSERT thuần
hiện tại tạo duplicate. Cần thêm constraint unique/primary key cho `event_id`
và ghi idempotent bằng `ON CONFLICT`; `DO UPDATE` bảo toàn nội dung mới khi
message replay đã thay đổi, còn `DO NOTHING` bỏ qua nội dung mới. `log_client.py`
chỉ mô phỏng transport và không cần sửa.

## 9. Cách kiểm chứng cuối cùng

Sau mỗi thay đổi nhỏ nên chạy `make quick`; trước khi kết thúc chạy:

```bash
make clean
make setup
make verify
```

Kết quả cuối cần đạt:

- 4/4 tiêu chí trong verify.
- Ba checksum liên tiếp giống nhau cho training, feature, chunks và quarantine.
- Counts đúng 12,480 / 9,100 / 31,200 / 312.
- `silver_tickets` đủ 12,480 ticket, `priority` không NULL và chỉ thuộc 1..4.
- `dbt test` pass với tổng test lớn hơn 9.
- DAG check báo `False / 1`.
- Báo cáo nhiệm vụ 2 có P99 đo được và giải thích vì sao chọn lookback.

Các file tuyệt đối không sửa để “làm cho test xanh”: `expected/*`,
`seed/generate.py`, `tools/common.py`, `tools/verify.py`, `tools/explain.py`.
Được phép sửa trong phạm vi lab: `dbt/`, `dags/`, `ingest/`, `queries/`,
`tools/compact.py`.

## 10. Việc nên làm ngay ở phiên sau

1. Đọc file này và chạy `git status --short` để phân biệt thay đổi cũ với thay
   đổi mới.
2. Nếu cần số liệu runtime, chạy `make setup`, `make quick` và lưu P99 trước
   khi chỉnh code.
3. Sửa lần lượt nhiệm vụ 1 → 2 → 3, mỗi lần dùng `make quick`.
4. Dùng `make verify` để xác nhận thay đổi của một nhiệm vụ không phá nhóm đối
   chứng `gold_doc_chunks` hay nhiệm vụ trước.
5. Chỉ làm EXTRA sau khi ba nhiệm vụ chính đạt; không sửa expected để khớp số.


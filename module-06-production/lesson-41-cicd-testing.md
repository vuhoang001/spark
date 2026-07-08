# Lesson 41 — CI/CD cho Spark: test PySpark, packaging

> Module 6 · Production Engineering · Tuần 22 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Tại sao **test Spark khó** (cần SparkSession, chậm, phụ thuộc hạ tầng) và bộ kỹ thuật hóa giải: **session-scoped pytest fixture** + `local[2]` + cấu trúc code tách pure function.
- **Cấu trúc project chuẩn**: `src/` chứa transformation là hàm thuần "DataFrame vào → DataFrame ra", `jobs/` chỉ là lớp vỏ I/O — ranh giới này quyết định code có test được hay không.
- Ba tầng test: **unit** (transformation, chispa), **integration** (đọc/ghi Iceberg thật vào thư mục tạm), **data contract** (schema assertion).
- **Packaging**: `.whl` + `--py-files` vs Docker image — khi nào dùng gì.
- **CI/CD hoàn chỉnh**: GitHub Actions từ lint → test → build → push → deploy, và **canary run** trước khi thay production.

Sau bài này bạn phải làm được:

- Viết `conftest.py` chuẩn mà mọi project PySpark sau này của bạn copy lại dùng.
- Refactor một job "cục gạch 200 dòng trong main()" thành các hàm test được, kèm test pass trong <30 giây.
- Giải thích cho team vì sao "job chạy được trên notebook" chưa phải là done, và deploy thẳng lên production không qua canary là đánh bạc.

Kiến thức dùng trong thực tế: đây là ranh giới cứng giữa "data scripter" và "data engineer". Ở mọi công ty nghiêm túc, code Spark không có test thì không qua được code review; pipeline không có CI thì mỗi lần deploy là một lần nín thở. Interviewer senior hỏi "bạn test Spark job thế nào?" để lọc ứng viên chỉ quen chạy notebook.

---

## 2. Why

### Vấn đề: tại sao đa số code Spark ngoài kia không có test?

Ba lý do người ta viện dẫn — và lời đáp của senior:

1. **"Cần cả cluster mới chạy được"** — Sai. `local[2]` là một cluster thu nhỏ hoàn chỉnh trong 1 JVM: có shuffle, có partition, có Catalyst. Logic transformation đúng trên `local[2]` với 20 dòng dữ liệu thì đúng trên 200 node với 2 tỷ dòng (những gì KHÁC nhau — memory, skew — là chuyện tuning lesson 40, không phải chuyện logic).
2. **"Khởi động SparkSession mất 10–15 giây, 50 test = 10 phút"** — Đúng, nếu ngây thơ tạo session mỗi test. Fix: fixture `scope="session"` — trả phí khởi động MỘT lần cho cả bộ test. 50 test còn ~30 giây.
3. **"Code của tôi toàn đọc S3/Kafka/Iceberg, test kiểu gì?"** — Đây mới là lý do thật, và nó là **lỗi thiết kế**, không phải giới hạn của Spark. Code trộn I/O với logic thì không test được — ở mọi ngôn ngữ, mọi framework. Giải pháp là kiến trúc (§3.2), không phải tool.

### Nếu không có test + CI thì sao?

Chuỗi domino kinh điển: sửa "một dòng nhỏ" trong logic dedup → không có test nào chặn → deploy chiều thứ 6 → job chạy XONG, không lỗi (tệ hơn cả crash!) → silver layer mất 3% đơn hàng → 5 ngày sau finance phát hiện số liệu lệch → truy ngược mất 2 ngày, backfill mất 3 ngày, niềm tin vào data team mất 6 tháng. Bug dữ liệu khác bug service ở chỗ: **nó không kêu**. Không có exception, không có alert — chỉ có dữ liệu sai lặng lẽ chảy xuống downstream. Test là hàng rào duy nhất bắt được nó TRƯỚC khi chảy.

> **Analogy nhà máy nước**: transformation là các van xử lý, dữ liệu là nước. Service backend có bug thì user thấy lỗi ngay (nước không chảy). Pipeline có bug thì nước VẪN chảy — nhưng bẩn. Uống vài tuần mới phát bệnh, và lúc đó không biết bẩn từ van nào. Test từng van với mẫu nước chuẩn trước khi lắp là cách duy nhất.

### Trade-off (senior phải cân được)

| Được | Mất |
|---|---|
| Bug logic chết từ trong trứng, refactor không run sợ | Viết test tốn ~30–50% thời gian viết code (trả trước, lãi về sau) |
| Code buộc phải tách I/O/logic → dễ đọc, dễ tái sử dụng | Phải kỷ luật kiến trúc — notebook-style "viết đến đâu chạy đến đó" không còn |
| CI chặn code hỏng trước khi vào main | Duy trì CI: test flaky, dependency drift — cũng là việc |
| Deploy tự động, lặp lại được, có canary | Không test được MỌI THỨ: skew, OOM, hành vi trên 2 TB — test không thay thế monitoring (lesson 39–40) |

---

## 3. Theory

### 3.1. Kim tự tháp test cho data pipeline

```
                    ▲  ÍT — CHẬM — ĐẮT
                   ╱ ╲
                  ╱ E2E╲        chạy cả pipeline trên staging cluster,
                 ╱ test ╲       dữ liệu như thật (trước release lớn)
                ╱─────────╲
               ╱ INTEGRATION╲    đọc/ghi format thật (Iceberg/Parquet)
              ╱    test      ╲   vào thư mục tạm — bắt bug I/O, schema,
             ╱                 ╲  merge logic (giây → phút)
            ╱───────────────────╲
           ╱   DATA CONTRACT     ╲  schema assertion: cột, kiểu, nullability
          ╱       test            ╲  — hàng rào giữa các layer (mili-giây)
         ╱─────────────────────────╲
        ╱        UNIT test          ╲  từng transformation thuần:
       ╱   (nhiều nhất, nhanh nhất)  ╲  DF nhỏ vào → assert DF ra (giây)
      ╱───────────────────────────────╲
                    NHIỀU — NHANH — RẺ
```

Tỷ lệ lành mạnh cho một Spark project: ~70% unit, ~20% integration + contract, ~10% E2E. Junior hay làm ngược: không có unit test nào, chỉ có "chạy thử cả pipeline trên dev" — tức là chỉ có tầng đắt nhất, chậm nhất.

### 3.2. Cấu trúc project chuẩn — kiến trúc quyết định testability

```
spark-project/
├── src/
│   └── olist_pipeline/
│       ├── __init__.py
│       ├── transformations/          # ← TIM của project: PURE FUNCTIONS
│       │   ├── __init__.py
│       │   ├── cleaning.py           #    nhận DataFrame → trả DataFrame
│       │   └── revenue.py            #    KHÔNG read, KHÔNG write, KHÔNG spark.conf
│       ├── jobs/                     # ← lớp vỏ mỏng: I/O + ghép transformation
│       │   └── silver_orders.py      #    đọc nguồn → gọi transform → ghi đích
│       └── io/                       # ← reader/writer tách riêng (mock/swap được)
│           └── catalog.py
├── tests/
│   ├── conftest.py                   # fixture SparkSession dùng chung
│   ├── unit/test_cleaning.py
│   ├── integration/test_silver_job.py
│   └── contracts/test_schemas.py
├── pyproject.toml                    # metadata + dependencies + build .whl
└── .github/workflows/ci.yml
```

Nguyên tắc vàng — **transformation là pure function trên DataFrame**:

```python
# TESTABLE — nhận DF, trả DF, không đụng thế giới bên ngoài
def add_delivery_days(orders: DataFrame) -> DataFrame: ...

# KHÔNG TESTABLE — trộn I/O, config, logic vào một cục
def process():
    df = spark.read.csv("s3://prod-bucket/orders/")   # cần S3 thật
    ...30 dòng logic...
    df.write.saveAsTable("silver.orders")             # ghi vào prod!
```

Hàm thuần nhận DataFrame thì test truyền vào DataFrame 5 dòng tạo tay; hàm tự đi đọc S3 thì test phải có... S3. Job file chỉ còn là "bản lắp ráp": `write(transform(read()))` — mỗi mảnh thay thế được độc lập. Đây cũng chính là lý do `io/` tách riêng: integration test swap catalog prod bằng catalog trỏ vào thư mục tạm.

### 3.3. Ba tầng test — mỗi tầng bắt loại bug gì

**Unit test** bắt bug LOGIC: điều kiện `when` ngược, join key sai, quên xử lý null, làm tròn sai. Input là DataFrame vài dòng tạo tay — trong đó BẮT BUỘC có dòng ác: null, trùng, giá trị biên. So sánh DataFrame có 2 trường phái: (a) `chispa.assert_df_equality` — diff đẹp, chỉ ra đúng ô sai; (b) tay trần `sorted(df.collect()) == sorted(expected)` — không thêm dependency. Cả hai đều phải nhớ: **DataFrame không có thứ tự dòng đảm bảo** → luôn sort trước khi so (hoặc `ignore_row_order=True`).

**Integration test** bắt bug I/O: schema ghi ra khác schema khai, MERGE INTO sai điều kiện, partition spec sai, quyền catalog. Chạy job THẬT nhưng đọc/ghi vào `tmp_path` của pytest (Iceberg hadoop catalog trỏ file system local — không cần service nào).

**Data contract test** bắt bug GIAO KHO: layer gold tin rằng silver có cột `order_id: string not-null` — niềm tin đó phải là một assertion chạy trong CI, không phải lời hứa miệng. Khi ai đó đổi schema silver, test của HỌ đỏ ngay lập tức thay vì job gold của BẠN chết lúc 2 giờ sáng. (Đây là phiên bản compile-time của data quality checks runtime ở lesson 35 — cần cả hai.)

### 3.4. Packaging — code lên cluster bằng đường nào

Executor cần import được code của bạn. Ba cấp tiến hóa:

| Cách | Cơ chế | Khi dùng |
|---|---|---|
| `--py-files src.zip` | Zip source, Spark phát cho executor, thêm vào PYTHONPATH | Nhanh gọn, ổn cho project nhỏ KHÔNG có dependency ngoài |
| **`.whl` + `--py-files`** | Build wheel chuẩn từ `pyproject.toml`, có version | Chuẩn mực khi dependency toàn pure-Python; version gắn vào artifact |
| **Docker image** | Đóng băng cả Python + libs (kể cả C-extension: numpy/pandas/pyarrow) + code + cả Spark | **Chuẩn production hiện đại** (K8s, EMR on EKS): build 1 lần, chạy y hệt ở mọi nơi, rollback = đổi tag |

Cái bẫy lớn nhất của `--py-files`: nó KHÔNG cài dependency — wheel của bạn cần `holidays==0.47` thì executor không tự có. Với dependency có C-extension, đường duy nhất tử tế là Docker image (hoặc conda/venv archive `--archives` — cách cũ trên YARN). Quy tắc quyết định: **có dependency ngoài stdlib+pyspark → nghĩ Docker trước**.

### 3.5. CD và canary — deploy không phải là điểm kết thúc của CI

Deploy job Spark = trỏ orchestrator (Airflow, lesson 36) vào **artifact có version mới** (image tag / wheel version). Nguyên tắc: Airflow DAG chỉ chứa THAM CHIẾU version, không chứa logic — đổi version là một dòng diff, rollback là revert dòng đó.

**Canary run** — nghi thức bắt buộc trước khi thay production:

```
merge → build image v1.8.0 → deploy vào canary DAG
   → canary chạy job v1.8.0 trên INPUT THẬT (hoặc mẫu), ghi ra BẢNG BÓNG
   → so kết quả với output của v1.7.x: row count, checksum các cột tiền,
     schema, duration (không chậm hơn 20%?)
   → PASS → promote v1.8.0 vào production DAG; FAIL → prod vẫn chạy v1.7.x,
     không ai bị đánh thức
```

Vì sao cần canary khi đã có cả rừng test? Vì test chạy trên dữ liệu BẠN NGHĨ RA, canary chạy trên dữ liệu THẬT — nơi sống của những ca không ai nghĩ ra (encoding lạ, key mới xuất hiện, volume tăng 10×). Test bắt bug đã biết trước hình dạng; canary bắt bug chưa biết hình dạng.

---

## 4. Internal

### Chuyện gì xảy ra khi pytest chạy một test PySpark?

```
① pytest thu thập tests, thấy fixture `spark` scope="session"
        │
② LẦN ĐẦU TIÊN một test cần `spark`: fixture chạy —
   JVM khởi động, SparkContext local[2] mọc lên NGAY TRONG
   process pytest (driver = pytest process; executor = thread
   trong cùng JVM). Trả phí ~10s đúng MỘT lần.
        │
③ Mỗi test: createDataFrame (nhỏ, nhanh) → transformation
   → Catalyst optimize y như thật → action (collect/so sánh)
   → job chạy trên 2 thread local. Mili-giây đến vài giây.
        │
④ Test sau DÙNG LẠI session — không JVM mới. Đây là lý do
   fixture phải "session-scoped" và cũng là NGUỒN RỦI RO:
   trạng thái rò giữa các test (bảng temp, cache, catalog) —
   test phải tự dọn hoặc dùng tên/namespace riêng.
        │
⑤ Hết phiên: fixture yield xong → spark.stop() → JVM tắt.
```

Hai hệ quả thực dụng: (1) `local[2]` chứ không phải `local[1]` — 2 thread mới lộ bug phụ thuộc thứ tự partition (ví dụ dedup bằng `dropDuplicates` rồi tin rằng "dòng đầu tiên" là dòng mới nhất — với 1 partition tình cờ đúng, 2 partition sai ngay); (2) config test nên đặt `spark.sql.shuffle.partitions=2` — mặc định 200 partition cho 10 dòng dữ liệu khiến mỗi shuffle tốn 200 task rỗng, bộ test chậm 5–10×.

### CI pipeline chảy thế nào

```
push/PR → GitHub Actions runner (VM sạch)
  ① checkout + setup Python + cache pip (không cache = mỗi lần cài lại từ đầu)
  ② ruff: lint + format check (fail nhanh nhất đứng đầu — fail ở đây
     khỏi tốn tiền chạy test)
  ③ pytest: unit + contract (nhanh) → integration (chậm hơn)
     — JVM local[2] chạy thẳng trên runner: KHÔNG cần cluster, chỉ cần Java
  ④ build: wheel (giây) và/hoặc docker build (phút — sau test để khỏi
     build thứ sẽ vứt đi)
  ⑤ push image lên registry, tag = version + git SHA (truy vết được
     image nào từ commit nào)
  ⑥ CD: cập nhật version ở canary → canary pass → promote production
```

---

## 5. API

### `conftest.py` — trái tim của bộ test (copy về mọi project)

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession dùng chung cho CẢ bộ test — khởi động 1 lần duy nhất."""
    session = (SparkSession.builder
        .master("local[2]")                # 2 thread: đủ lộ bug partition
        .appName("pytest-pipeline")
        .config("spark.sql.shuffle.partitions", "2")   # 200 → 2: test nhanh 5-10×
        .config("spark.ui.enabled", "false")            # test không cần UI
        .config("spark.sql.session.timeZone", "UTC")    # kết quả không phụ thuộc máy
        .getOrCreate())
    yield session
    session.stop()
```

- **Ý nghĩa**: `scope="session"` = một instance cho cả phiên pytest. `yield` tách phần setup/teardown.
- **Pitfall**: quên `scope="session"` (mặc định là `function`) → mỗi test một JVM → bộ test 50 test mất 10 phút và bạn kết luận nhầm "test Spark không khả thi".

### `chispa.assert_df_equality`

```python
from chispa import assert_df_equality

assert_df_equality(actual_df, expected_df,
                   ignore_row_order=True,      # DataFrame không đảm bảo thứ tự!
                   ignore_nullable=True)       # nullable flag hay lệch vặt vãnh
```

- **Ý nghĩa**: so 2 DataFrame cả schema lẫn data; fail thì in bảng diff tô màu đúng ô lệch — tiết kiệm 10 phút dò mắt mỗi lần đỏ.
- **Khi dùng**: mọi unit test transformation. (`pip install chispa` — thư viện test-only, không theo lên production.)
- **Pitfall**: quên `ignore_row_order=True` → test flaky: pass máy này fail máy kia tùy plan chọn thứ tự nào.

### `tmp_path` (pytest built-in) + Iceberg hadoop catalog

```python
warehouse = tmp_path / "warehouse"   # pytest phát thư mục tạm, tự dọn sau test
```

- **Ý nghĩa**: integration test ghi Iceberg/Parquet vào thư mục dùng-một-lần — không đụng bất kỳ hạ tầng thật nào, chạy được cả trên CI runner trần.

### `spark-submit --py-files` (đường packaging nhẹ)

```bash
poetry build   # hoặc: python -m build → dist/olist_pipeline-1.2.0-py3-none-any.whl
spark-submit --master spark://spark-master:7077 \
  --py-files dist/olist_pipeline-1.2.0-py3-none-any.whl \
  src/olist_pipeline/jobs/silver_orders.py
```

- **Pitfall**: wheel chỉ mang CODE CỦA BẠN — dependency bên thứ ba không tự đến. Có numpy/pandas trong dependency? → Docker image (§3.4).

---

## 6. Demo nhỏ

Một transformation + một unit test — trọn vẹn chu trình trong 40 dòng:

```python
# src/olist_pipeline/transformations/cleaning.py
from pyspark.sql import DataFrame, functions as F

def dedup_orders(orders: DataFrame) -> DataFrame:
    """Giữ bản ghi MỚI NHẤT cho mỗi order_id (CDC có thể phát trùng)."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("order_id").orderBy(F.desc("updated_at"))
    return (orders
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))
```

```python
# tests/unit/test_cleaning.py
from datetime import datetime
from chispa import assert_df_equality
from olist_pipeline.transformations.cleaning import dedup_orders

def test_dedup_giu_ban_ghi_moi_nhat(spark):          # nhận fixture từ conftest
    orders = spark.createDataFrame([
        ("o1", "shipped",   datetime(2026, 7, 1, 10, 0)),
        ("o1", "delivered", datetime(2026, 7, 2, 9, 0)),   # mới hơn → phải thắng
        ("o2", "created",   datetime(2026, 7, 1, 8, 0)),   # không trùng → giữ nguyên
    ], "order_id string, status string, updated_at timestamp")

    expected = spark.createDataFrame([
        ("o1", "delivered", datetime(2026, 7, 2, 9, 0)),
        ("o2", "created",   datetime(2026, 7, 1, 8, 0)),
    ], "order_id string, status string, updated_at timestamp")

    assert_df_equality(dedup_orders(orders), expected,
                       ignore_row_order=True, ignore_nullable=True)
```

```bash
$ pytest tests/unit/ -q
.                                                    [100%]
1 passed in 8.42s        # lần sau cùng session: ~0.5s/test
```

Thử tự phá: đổi `F.desc` thành `F.asc` trong transformation — chispa in diff chỉ đúng ô `delivered/shipped` lệch. Đó chính là bug "dedup giữ nhầm bản cũ" mà ở §2 làm mất 3% đơn hàng — ở đây nó chết trong 8 giây, giá 0 đồng.

---

## 7. Production Example

Quy trình một thay đổi đi từ laptop đến production ở một team DE chuẩn (mô hình phổ biến ở các công ty dùng Spark trên K8s):

```
Dev sửa transformations/revenue.py + thêm test  (laptop, pytest 30s)
   ↓ push branch, mở PR
CI trên PR: ruff → pytest (unit+contract+integration) → build image thử
   ↓ xanh + 1 approve review
merge main → CI build image ghcr.io/team/olist-pipeline:1.8.0-a1b2c3d → push
   ↓ CD bot mở PR sang repo airflow-dags: canary_silver_orders → image 1.8.0
CANARY (chạy đêm đó, song song prod):
   input thật → ghi silver_orders__canary (bảng bóng)
   job so sánh: row_count lệch 0.0%, sum(price) lệch 0đ, schema khớp,
   duration 12m vs 11m baseline → PASS
   ↓ promote: PR đổi image của DAG production → 1.8.0
Prod chạy 1.8.0. Rollback nếu cần = revert 1 dòng image tag.
```

Chi tiết đáng học nhất: **hai lớp lưới khác nhau cho hai loại bug**. Bộ test (lưới 1) từng bắt bug logic làm tròn tiền về 0 khi giá null — bug "biết trước hình dạng". Canary (lưới 2) từng chặn một release mà MỌI TEST XANH: thư viện parse date bump minor version, đổi cách hiểu format 2 chữ số năm, chỉ sai với ~200 đơn hàng cũ format lạ trong dữ liệu thật — không test nào nghĩ ra nổi ca này, nhưng phép so `sum()` giữa bảng bóng và prod lệch là canary đỏ ngay. Prod không hề hấn gì, và không ai phải mở lesson 40 lúc 3 giờ sáng.

---

## 8. Hands-on Lab

**Mục tiêu**: dựng mini-project có cấu trúc chuẩn + 3 tầng test, chạy pytest trong venv local; đóng gói wheel và submit lên cluster Docker.

Lab này chạy pytest bằng **venv local** của repo (test không cần cluster — đó chính là điểm hay); chỉ bước cuối cùng mới đụng cluster.

### Bước 0 — chuẩn bị venv

```bash
cd ~/Documents/data-engineering/spark-mastery
source venv/bin/activate
pip install pyspark==3.4.1 pytest chispa build
python -c "import pyspark; print(pyspark.__version__)"   # phải là 3.4.1 — khớp cluster
mkdir -p labs/lab41
```

(pytest cần Java trên máy — `java -version`; chưa có thì `sudo apt install openjdk-17-jre-headless`.)

### Bước 1 — dựng skeleton trong `labs/lab41/`

```
labs/lab41/
├── src/olist_pipeline/{__init__.py, transformations/, jobs/}
├── tests/{conftest.py, unit/, integration/, contracts/}
└── pyproject.toml
```

`conftest.py`: copy nguyên từ §5. `pyproject.toml` tối thiểu:

```toml
[project]
name = "olist-pipeline"
version = "0.1.0"
requires-python = ">=3.9"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
pythonpath = ["src"]          # pytest import được olist_pipeline không cần cài
testpaths = ["tests"]
```

### Bước 2 — viết transformation + unit test

Trong `transformations/revenue.py`, viết 2 pure function:

1. `delivered_only(orders)` — lọc `order_status == "delivered"`.
2. `revenue_by_month(orders, items)` — join theo `order_id`, cột `month` từ `order_purchase_timestamp` (format `yyyy-MM`), agg `sum(price)` làm `revenue` (round 2). (Bạn đã viết logic này ở lab01 — giờ nó thành hàm thuần có test.)

Viết `tests/unit/test_revenue.py` với TỐI THIỂU các ca ác: order không delivered phải biến mất; order delivered nhưng KHÔNG có item nào (inner join phải loại nó — hay bạn muốn left join? quyết định và test hóa quyết định đó); 2 item cùng 1 order phải cộng dồn. Dùng chispa.

```bash
cd labs/lab41 && pytest tests/unit -v
```

### Bước 3 — data contract test

`tests/contracts/test_schemas.py`: định nghĩa `EXPECTED_REVENUE_SCHEMA` bằng `StructType` tường minh; test gọi `revenue_by_month` trên input rỗng-nhưng-đúng-schema (`spark.createDataFrame([], schema=...)`) và assert `output.schema == EXPECTED_REVENUE_SCHEMA`. Đây là hợp đồng: ai đổi tên cột `revenue` → test đỏ ngay tại PR của người đó.

### Bước 4 — integration test với thư mục tạm

`tests/integration/test_silver_job.py`: viết job function `run(spark, source_path, target_path)` trong `jobs/silver_revenue.py` (đọc CSV → 2 transformation → ghi Parquet partition theo `month`). Test: ghi 2 file CSV mini vào `tmp_path`, gọi `run`, đọc lại Parquet, assert nội dung + kiểm partition folder `month=...` tồn tại. (Có thời gian: làm bản Iceberg — `pip install` không đủ, cần jar `iceberg-spark-runtime-3.4_2.12` qua `spark.jars.packages` trong một fixture spark riêng, warehouse trỏ `tmp_path`, catalog type `hadoop`.)

### Bước 5 — build wheel, submit lên cluster

```bash
python -m build          # → dist/olist_pipeline-0.1.0-py3-none-any.whl
```

Viết `labs/lab41/submit_silver.py` (entrypoint mỏng: tạo session, gọi `olist_pipeline.jobs.silver_revenue.run(spark, "/workspace/data/olist", "/workspace/labs/lab41/out")`). Submit thủ công để thấy `--py-files` hoạt động (make run không truyền được flag này):

```bash
cd ~/Documents/data-engineering/spark-mastery && make up
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /workspace/labs/lab41/dist/olist_pipeline-0.1.0-py3-none-any.whl \
  /workspace/labs/lab41/submit_silver.py
```

Kiểm chứng thú vị: bỏ `--py-files` chạy lại → `ModuleNotFoundError` nổ **ở đâu**, driver hay executor? (Ngẫm rồi hãy chạy.)

### Bước 6 — viết CI workflow (chưa cần push GitHub)

Tạo `labs/lab41/.github/workflows/ci.yml` theo mẫu đầy đủ:

```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]

jobs:
  lint-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11", cache: pip }
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "17" }
      - run: pip install pyspark==3.4.1 pytest chispa ruff build
      - run: ruff check src tests && ruff format --check src tests
      - run: pytest tests -v
      - run: python -m build          # wheel build phải luôn xanh

  build-push-image:
    needs: lint-test                  # test đỏ thì không build — fail fast
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    permissions: { packages: write, contents: read }
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/build-push-action@v6
        with:
          context: .
          push: true
          tags: ghcr.io/${{ github.repository }}/olist-pipeline:${{ github.sha }}

  deploy-canary:
    needs: build-push-image
    runs-on: ubuntu-latest
    steps:
      # thực tế: mở PR đổi image tag trong repo airflow-dags (canary DAG),
      # hoặc gọi API trigger canary run; prod CHỈ đổi sau khi canary pass
      - run: echo "trigger canary với image ${{ github.sha }}"
```

Kèm `Dockerfile` 5 dòng: `FROM apache/spark:3.4.1` → `USER root` → `RUN pip install ...` → `COPY dist/*.whl` + `RUN pip install /tmp/*.whl`. Giải thích trong NOTES.md: vì sao build image SAU test, vì sao tag bằng git SHA, canary so sánh những gì trước khi promote.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Vì sao fixture SparkSession phải `scope="session"`? Điều gì xảy ra nếu để mặc định?
2. "Pure function nhận DataFrame trả DataFrame" — vì sao cấu trúc này là điều kiện tiên quyết của unit test? Cho 1 ví dụ hàm KHÔNG test được và refactor nó.
3. Kể 2 loại bug mà unit test KHÔNG bắt được nhưng canary bắt được.

**Medium** — Mở rộng lab: thêm transformation `flag_late_delivery(orders)` (cột `is_late` = delivered_date > estimated_date; chú ý null ở cả hai cột — đơn chưa giao!). Viết trước 4 unit test theo TDD (test đỏ → viết code → xanh): đúng hạn, trễ, thiếu delivered_date, thiếu estimated_date. Quyết định nghiệp vụ null trả gì (null? false?) và ghi quyết định đó thành docstring + test.

**Hard** — Test tính **idempotency** (lesson 36 gặp lại): sửa `silver_revenue.run` sang chế độ ghi đè partition (`partitionOverwriteMode=dynamic` hoặc MERGE nếu bạn làm bản Iceberg). Viết integration test chạy `run()` HAI LẦN liên tiếp cùng input rồi assert kết quả y hệt chạy một lần (row count + nội dung). Sau đó làm test khó hơn: lần 2 với input đã sửa 1 dòng — kết quả phải phản ánh bản sửa, không nhân đôi.

**Production Challenge** — Chọn 1 job "cục gạch" thật của bạn (job dài nhất trong labs cũ của khóa này, hoặc job công ty). Refactor theo cấu trúc §3.2: liệt kê từng hàm tách ra được, viết test cho 2 hàm quan trọng nhất, đo coverage phần transformations (`pytest --cov`). Viết 10 dòng "migration note": phần nào KHÔNG tách được (vì sao — dính SparkSession? dính config?), và kế hoạch trả nợ.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Performance của... bộ test — vì bộ test chậm là bộ test chết (không ai chạy nữa):

| Thủ phạm test chậm | Fix | Tác động |
|---|---|---|
| Session mỗi test (quên scope) | `scope="session"` | 50 test: 10 phút → 30 giây |
| `shuffle.partitions=200` mặc định | set `2` trong fixture | mỗi groupBy/join: 200 task → 2 task, nhanh 5–10× |
| Test đọc file CSV to "cho giống thật" | `createDataFrame` 5–10 dòng — unit test cần CA ÁC, không cần DUNG LƯỢNG | giây → mili-giây |
| Bật Spark UI trong test | `spark.ui.enabled=false` | bớt vài giây khởi động + hết cảnh CI fail vì port bận |
| Integration test trộn lẫn unit | tách thư mục; CI chạy unit trước, integration sau (fail fast) | feedback đỏ sớm hơn |

Và performance của CI: cache pip (`cache: pip` trong setup-python) tiết kiệm 1–3 phút/run; build Docker đặt SAU test (không build thứ sẽ vứt); layer image xếp ít-đổi-trước (base + deps) nhiều-đổi-sau (code) để cache layer ăn tối đa — image rebuild vài giây thay vì vài phút. Mốc lành mạnh: **PR feedback < 5 phút**. Trên 10 phút, dev bắt đầu "gộp nhiều thay đổi một PR cho đỡ chờ" — chính là hành vi làm tăng rủi ro mà CI sinh ra để giảm.

---

## 11. Spark UI

Bài này UI đóng vai phụ nhưng có 2 việc đáng làm:

1. **Soi bộ test của chính bạn**: tạm đổi `spark.ui.enabled` thành `true` trong conftest, thêm breakpoint/`input()` ở một test, mở :4040 — bạn sẽ thấy fixture của mình là một application thật: tab Jobs đầy job li ti của từng `createDataFrame`/`collect`, và nếu quên set `shuffle.partitions=2` sẽ thấy stage 200 task cho 5 dòng dữ liệu — nhìn một lần là nhớ mãi vì sao test chậm. (Nhớ trả lại `false`.)
2. **Xác nhận `--py-files` ở bước 5 lab**: tab **Environment** → mục "Classpath Entries"/`spark.submit.pyFiles` — thấy wheel của bạn được phát đi. Đây cũng là nơi kiểm tra khi gặp `ModuleNotFoundError` trên cluster: wheel có thực sự lên xe không, version có đúng không (deploy nhầm wheel cũ là lỗi CD kinh điển — tag bằng git SHA sinh ra để trị đúng bệnh này).

Tab Environment nói chung là "birth certificate" của mỗi run production: image tag, py-files, mọi config — chụp nó vào log đầu job (in `spark.sparkContext.getConf().getAll()` có chọn lọc) để sự cố lesson 40 có thêm hiện trường.

---

## 12. Common Mistakes

1. **Test transformation bằng cách... chạy cả job trên dev cluster.** Đó là E2E, tầng đắt nhất — feedback 20 phút thay vì 2 giây, và không cô lập được hàm nào sai.
2. **Quên `scope="session"`** → kết luận nhầm "test PySpark chậm lắm, thôi bỏ". Bug một dòng, hậu quả cả văn hóa team.
3. **So sánh DataFrame không sort / không `ignore_row_order`** → test flaky, pass-fail ngẫu nhiên theo plan → team mất niềm tin vào test → mute test → còn tệ hơn không có.
4. **Test chỉ có happy path.** 5 dòng input đẹp đẽ đều pass; null, trùng lặp, chuỗi rỗng, timezone — nơi bug thật sống — không có mặt. Quy tắc: mỗi test ít nhất một "dòng ác".
5. **Version pyspark trong venv lệch version cluster** (3.5 local, 3.4.1 cluster) → test xanh, production nổ vì API/behavior khác. Pin `pyspark==3.4.1` khớp image — và để Docker image hóa giải hẳn họ lỗi này.
6. **`--py-files` mang theo wheel nhưng tưởng nó cài cả dependency.** Không — nó chỉ thêm vào PYTHONPATH. Numpy/pandas trong deps? Docker image.
7. **CD không có canary, deploy thẳng prod chiều thứ 6.** Mọi test đều xanh vẫn không nói gì về dữ liệu thật ngày mai. Bảng bóng + so sánh + promote là ba bước rẻ hơn một đêm on-call.
8. **Test phụ thuộc lẫn nhau qua session dùng chung** (test A tạo temp view, test B đọc nó) → chạy cả bộ thì xanh, chạy lẻ `-k test_b` thì đỏ. Mỗi test tự lo dữ liệu của mình.

---

## 13. Interview

**Junior:**

1. *Test PySpark khó ở đâu và giải quyết thế nào?* — Khó: cần SparkSession (JVM, ~10s khởi động), code thường dính I/O hạ tầng. Giải: fixture pytest `scope="session"` với `local[2]` — trả phí khởi động một lần; tách transformation thành pure function nhận/trả DataFrame để test không cần hạ tầng.
2. *Unit test và integration test khác nhau gì trong ngữ cảnh Spark?* — Unit: một transformation thuần, input createDataFrame tay, bắt bug logic, mili-giây. Integration: chạy job với đọc/ghi format thật (Parquet/Iceberg) vào thư mục tạm, bắt bug I/O/schema/merge, giây-phút.
3. *So sánh 2 DataFrame trong test thế nào cho đúng?* — chispa `assert_df_equality` với `ignore_row_order=True` (DataFrame không đảm bảo thứ tự dòng), hoặc collect + sort cả hai rồi so. So cả schema, không chỉ data.
4. *Vì sao nên `local[2]` chứ không `local[1]` khi test?* — 2 thread = nhiều partition thật sự → lộ bug phụ thuộc thứ tự/phân bố partition (dedup "lấy dòng đầu", agg không giao hoán) mà 1 partition che giấu.

**Mid:**

5. *Cấu trúc project PySpark thế nào để testable? Vì sao?* — `transformations/` chứa pure function DataFrame→DataFrame (không read/write/config); `jobs/` là vỏ mỏng ghép đọc→transform→ghi; `io/` tách reader/writer để swap được trong integration test. Vì unit test chỉ khả thi khi logic không dính I/O — testability là hệ quả của kiến trúc, không phải của tool.
6. *`--py-files` với wheel có giới hạn gì, khi nào phải chuyển Docker image?* — `--py-files` chỉ phát code lên PYTHONPATH của executor, KHÔNG cài dependency; dependency C-extension (numpy/pyarrow) càng không. Chuyển Docker khi: có dependency ngoài, cần đồng nhất môi trường driver/executor/CI, cần rollback bằng image tag. Docker đóng băng Python + libs + code + Spark — build một lần chạy mọi nơi.
7. *Data contract test là gì, khác data quality check runtime chỗ nào?* — Contract test: assertion schema/cột/kiểu của output một layer, chạy trong CI trên input rỗng-đúng-schema — bắt thay đổi PHÁ VỠ hợp đồng ngay tại PR của người đổi. Quality check runtime (lesson 35): kiểm dữ liệu thật lúc chạy (null %, range) — bắt dữ liệu bẩn. Cần cả hai: một cái gác code, một cái gác data.
8. *Thiết kế CI pipeline cho Spark project — các stage và thứ tự?* — Lint (ruff, rẻ nhất trước) → unit + contract test → integration test → build wheel/image (sau test — không build thứ sẽ vứt) → push registry tag theo git SHA → deploy canary. Nguyên tắc: fail fast, artifact bất biến truy vết được về commit, prod chỉ đổi sau canary pass.

**Senior:**

9. *Mọi test đều xanh mà production vẫn ra số sai — kể các lỗ hổng và cách bịt.* — (a) Test chỉ phủ dữ liệu tưởng tượng: dữ liệu thật có encoding/format/key lạ → bịt bằng canary chạy input thật ghi bảng bóng, so row count/checksum với baseline trước khi promote; (b) lệch môi trường: pyspark version test ≠ cluster → bịt bằng Docker image thống nhất + pin version; (c) bug chỉ hiện ở scale (skew, OOM, spill) — test chức năng không bắt được → bịt bằng monitoring + baseline metrics (lesson 39) và thiết kế chịu lỗi (lesson 40); (d) upstream đổi schema âm thầm → contract test hai phía + schema registry. Ý cần toát ra: test là điều kiện cần, không phải đủ — dây chuyền phòng thủ là test → canary → monitoring.
10. *Team 5 DE toàn "notebook engineer", pipeline prod chưa có test nào. Chiến lược đưa test + CI vào trong một quý?* — Không big-bang rewrite. (1) Tuần 1: conftest chuẩn + CI chỉ chạy lint và test hiện có (rỗng) — làm đường ray trước; (2) luật mới: mọi PR sửa logic phải kèm test cho phần sửa (không đòi phủ code cũ); (3) chọn 1 job đau nhất, mob-refactor tách transformations làm mẫu — team học qua ví dụ nội bộ, không qua slide; (4) contract test cho các bảng nhiều consumer nhất — ROI cao nhất; (5) canary cho pipeline doanh thu trước tiên; (6) đo và khoe: số bug chặn ở CI, thời gian feedback — văn hóa đổi khi thấy lợi, không khi bị ép. Trọng tâm senior: đây là bài toán quản lý thay đổi, công nghệ chỉ là 30%.

---

## 14. Summary

### Mindmap

```
                        CI/CD CHO SPARK
                              │
    ┌───────────────┬─────────┴─────────┬────────────────────┐
    ▼               ▼                   ▼                    ▼
 TESTABLE        3 TẦNG TEST         PACKAGING            CI/CD
 ARCHITECTURE       │                   │                    │
    │            unit: pure fn       zip py-files         ruff → pytest
 transformations  + chispa            (nhỏ, no deps)      → build → push
 = pure fn       integration:        .whl + --py-files    (tag = git SHA)
 DF in → DF out   tmp_path +          (code, KHÔNG deps)  → canary: bảng
 jobs/ = vỏ I/O   Iceberg hadoop     Docker image          bóng, so sánh,
 conftest:        catalog             (chuẩn prod:         rồi mới promote
 scope=session,  contract:            deps + rollback      rollback = revert
 local[2],        schema assert       = đổi tag)           image tag
 shuffle=2
```

### Checklist trước khi gõ "Continue"

- [ ] Viết lại được conftest.py chuẩn từ trí nhớ (4 config và lý do từng cái).
- [ ] Giải thích được vì sao pure function DataFrame→DataFrame là điều kiện của testability.
- [ ] Phân biệt unit / integration / contract test — mỗi tầng bắt loại bug nào.
- [ ] Biết `--py-files` KHÔNG cài dependency và khi nào bắt buộc Docker.
- [ ] Viết được skeleton GitHub Actions: lint → test → build → push → canary.
- [ ] Giải thích được canary bắt loại bug mà test không bắt được — và ngược lại.
- [ ] Lab: bộ test pass trong venv local < 1 phút, wheel submit lên cluster chạy được.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 42 — Cost & capacity: spot instances, autoscaling, khi nào KHÔNG dùng Spark.**

Bạn đã biết viết job đúng (test), deploy an toàn (CI/CD), chữa khi ốm (debug). Mảnh cuối của bức tranh production là thứ ít được dạy nhất nhưng sếp quan tâm nhất: **tiền**. Cluster 20 node chạy 24/7 cho job tổng cộng 3 giờ/ngày là đốt ~85% ngân sách vào không khí. Bài sau trả lời: đo cost per job bằng gì, spot instance rẻ 60–90% nhưng dùng sao cho khỏi mất dữ liệu giữa chừng, autoscaling cấu hình thế nào, serverless đáng tiền không — và câu hỏi senior nhất: bài toán này có đáng dùng Spark không, hay một con DuckDB 10 giây là xong?

> Gõ **"Continue"** khi sẵn sàng.

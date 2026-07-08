# Lesson 11 — Complex types: array/map/struct, explode, JSON

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 6 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Ba kiểu dữ liệu phức hợp của Spark: **ArrayType, MapType, StructType** — và khi nào dữ liệu thật buộc bạn dùng chúng.
- Truy cập nested field bằng **dot notation**, index array, key của map.
- Bộ ba "duỗi phẳng" dữ liệu: **explode / explode_outer / posexplode** — và chiều ngược lại: **collect_list / collect_set**.
- Parse JSON: **from_json, to_json, schema_of_json, get_json_object** — vũ khí chính khi làm việc với Kafka.
- Kỹ thuật **flatten** JSON lồng nhau thành bảng phẳng chuẩn warehouse.
- Case thật 100%: parse **Debezium envelope** (`before`/`after`/`op`/`ts_ms`/`source`) — thứ bạn sẽ dùng nguyên xi ở Project 2 (CDC Lakehouse).
- NULL trong struct/array cư xử thế nào, và schema evolution khi struct thêm field.

Sau bài này bạn phải làm được:

- Nhìn một message JSON bất kỳ từ Kafka → viết được `StructType` schema và `from_json` để bung nó ra bảng phẳng.
- Giải thích cho đồng nghiệp: explode làm số dòng thay đổi ra sao, và tại sao `explode` "nuốt mất" dòng có array rỗng.
- Xử lý được tình huống "hôm qua JSON có 5 field, hôm nay nguồn thêm field thứ 6" mà pipeline không vỡ.

Kiến thức dùng trong thực tế: **rất nhiều**. Dữ liệu hiện đại không phẳng — event từ Kafka, log từ API, document từ MongoDB, payload CDC từ Debezium đều là JSON lồng nhau. DE không thạo complex types thì chỉ xử lý được dữ liệu "đẹp", mà dữ liệu đẹp thì... không cần DE.

---

## 2. Why

### Vấn đề: thế giới không phẳng

Bảng SQL cổ điển là hình chữ nhật: mỗi ô một giá trị vô hướng (số, chuỗi, ngày). Nhưng dữ liệu nguồn thời nay trông thế này — một event từ hệ thống đặt hàng:

```json
{
  "order_id": "abc123",
  "customer": {"id": "c9", "city": "Ha Noi", "tier": "gold"},
  "items": [
    {"sku": "SP01", "qty": 2, "price": 150000},
    {"sku": "SP07", "qty": 1, "price": 99000}
  ],
  "tags": {"channel": "app", "campaign": "tet2026"}
}
```

Một đơn hàng có **nhiều** item (quan hệ 1-n nằm NGAY TRONG một dòng), customer là một **cụm** field, tags là cặp key-value tùy biến. Nếu ép mọi thứ thành cột phẳng ngay từ lúc đọc, bạn phải quyết định trước "tối đa bao nhiêu item?" — thiết kế kiểu `item_1_sku, item_2_sku, ... item_20_sku` là thảm họa kinh điển.

Spark giải quyết bằng cách cho **kiểu dữ liệu phức hợp là công dân hạng nhất**: một ô của DataFrame có thể chứa cả struct, array, map — và Parquet/Iceberg lưu trữ được chúng ở dạng columnar hẳn hoi (bạn đã thấy ở lesson 6).

### Nếu không có complex types thì sao?

- Bạn phải lưu JSON như một **string** rồi mỗi lần cần thì regex/substring — chậm, dễ vỡ, không có schema kiểm soát.
- Hoặc normalize sớm thành nhiều bảng — nhưng ở tầng bronze, tách bảng sớm nghĩa là mất khả năng replay nguyên bản.
- Hoặc dùng công cụ khác cho phần JSON rồi vá lại — pipeline chắp vá 2 công cụ.

### Trade-off (Senior phải thuộc)

| Được | Mất |
|---|---|
| Giữ nguyên hình dạng dữ liệu nguồn ở bronze | Query nested chậm hơn cột phẳng (dù Parquet đã tối ưu tốt) |
| Schema kiểm soát chặt (from_json + StructType) | Schema dài dòng, phải bảo trì khi nguồn đổi |
| 1-n nằm gọn trong 1 dòng, không cần join sớm | explode nhân số dòng — dễ nổ dữ liệu nếu bất cẩn |
| BI/analyst đọc bảng phẳng ở silver/gold | Ai đó phải flatten — người đó là bạn |

> Nguyên tắc medallion: **bronze giữ nested (trung thực với nguồn), silver/gold phẳng dần (thân thiện với người dùng)**. Flatten là công việc của tầng silver — chính là bài hôm nay.

---

## 3. Theory

### 3.1. Ba kiểu phức hợp

| Kiểu | Chứa gì | Analogy | Ví dụ |
|---|---|---|---|
| **StructType** | Cụm field cố định, mỗi field có tên + kiểu riêng | Cái hộp có ngăn dán nhãn | `customer: {id, city, tier}` |
| **ArrayType** | Danh sách phần tử **cùng kiểu**, độ dài tùy ý | Chuỗi hạt cùng loại | `items: [item, item, ...]` |
| **MapType** | Cặp key→value, key tùy biến lúc runtime | Từ điển tra cứu | `tags: {"channel": "app"}` |

Chúng **lồng nhau tùy ý**: array của struct (phổ biến nhất), struct chứa array, map có value là struct...

```
Row của DataFrame "orders_raw"
┌──────────┬─────────────────────────┬──────────────────────────────────┬────────────────────┐
│ order_id │ customer (STRUCT)       │ items (ARRAY<STRUCT>)            │ tags (MAP)         │
│ string   │ ┌────┬───────┬──────┐   │ ┌───────────────┬──────────────┐ │ channel → "app"    │
│ "abc123" │ │ id │ city  │ tier │   │ │{SP01, 2, 150k}│{SP07, 1, 99k}│ │ campaign → "tet26" │
│          │ │ c9 │ Ha Noi│ gold │   │ └───────────────┴──────────────┘ │                    │
│          │ └────┴───────┴──────┘   │   phần tử 0        phần tử 1     │                    │
└──────────┴─────────────────────────┴──────────────────────────────────┴────────────────────┘
```

**Struct vs Map — câu hỏi phỏng vấn ưa thích**: struct khi **tập field biết trước và cố định** (schema kiểm soát, Parquet lưu mỗi field một cột riêng → column pruning hoạt động); map khi **key động, không biết trước** (ví dụ custom attributes người dùng tự đặt). Nếu bạn biết trước key, dùng map là tự bỏ đi lợi ích columnar.

### 3.2. Truy cập nested field

```
customer.city          ← dot notation vào struct
items[0].sku           ← index array (0-based) rồi vào struct
tags['channel']        ← key của map (không có key → NULL)
```

Dot notation hoạt động ở cả DataFrame API (`F.col("customer.city")`) lẫn SQL. Lấy field từ struct ra thành cột riêng = một bước flatten.

### 3.3. explode — nhân dòng theo array

`explode(items)` biến **1 dòng có array N phần tử → N dòng**, mỗi dòng một phần tử; các cột khác được **lặp lại**:

```
TRƯỚC explode (1 dòng):
┌──────────┬────────────────────────────────┐
│ order_id │ items                          │
│ abc123   │ [{SP01,2,150k}, {SP07,1,99k}]  │
└──────────┴────────────────────────────────┘
            │ explode(items) → item
            ▼
SAU explode (2 dòng — order_id LẶP LẠI):
┌──────────┬────────────────┐
│ order_id │ item           │
│ abc123   │ {SP01, 2, 150k}│
│ abc123   │ {SP07, 1, 99k} │
└──────────┴────────────────┘
```

Ba biến thể — khác nhau ở cách đối xử với array rỗng/NULL:

| Hàm | Array rỗng hoặc NULL | Kèm vị trí phần tử |
|---|---|---|
| `explode` | **Dòng biến mất** (như inner join) | Không |
| `explode_outer` | Dòng được giữ, cột item = NULL (như left join) | Không |
| `posexplode` / `posexplode_outer` | Như trên | Có cột `pos` (0-based) |

> **Pitfall nguy hiểm nhất bài này**: `explode` trên đơn hàng không có item → đơn hàng **bốc hơi khỏi kết quả**. Đếm doanh thu thì không sao, nhưng đếm SỐ ĐƠN thì sai âm thầm. Ở silver, mặc định cân nhắc `explode_outer` trước, chỉ dùng `explode` khi chủ đích bỏ dòng rỗng.

Chiều ngược lại: `groupBy(...).agg(collect_list(col))` gom nhiều dòng thành 1 array (giữ trùng lặp, **không đảm bảo thứ tự**), `collect_set` thì khử trùng lặp. Đây là cách "gấp" bảng phẳng về nested — hữu ích khi xuất dữ liệu cho API/document store.

### 3.4. JSON: string ↔ struct

Từ Kafka, cột `value` là **chuỗi JSON** (thực chất là binary phải cast string). Chuỗi thì Spark không hiểu cấu trúc — phải parse:

```
value (STRING)                              value (STRUCT — Spark hiểu, Catalyst tối ưu được)
'{"order_id":"abc",     ── from_json ──▶    {order_id: "abc", amount: 5.0}
  "amount":5.0}'        ◀── to_json ───
```

- **from_json(col, schema)**: string → struct theo schema bạn khai. Dòng nào parse fail → **NULL** (mặc định mode PERMISSIVE), không throw — vừa là tính năng vừa là bẫy (mất dữ liệu âm thầm nếu không kiểm tra NULL sau parse).
- **to_json(col)**: struct/array/map → string JSON. Dùng khi ghi ngược ra Kafka.
- **schema_of_json(lit(sample))**: đưa 1 mẫu JSON, Spark trả về chuỗi DDL schema — công cụ **thăm dò lúc dev**, không dùng production (đoán từ 1 mẫu thì mẫu thiếu field là schema thiếu field).
- **get_json_object(col, '$.path')**: móc 1 giá trị bằng JSONPath, trả string, **không cần khai schema**. Tiện khi chỉ cần 1-2 field (ví dụ lấy `op` để route message), nhưng mỗi lần gọi parse lại chuỗi từ đầu — gọi 10 lần cho 10 field là parse 10 lần. Cần nhiều field → `from_json` một lần rồi dot notation.

### 3.5. NULL trong nested — 3 tầng khác nhau

Phân biệt kỹ, vì chúng trông giống nhau khi `show()` nhưng nghĩa khác nhau:

1. **Cả struct NULL**: `customer` = NULL → `customer.city` cũng NULL (Spark không throw NPE như Java — dot notation "trượt" qua NULL an toàn).
2. **Struct tồn tại, field bên trong NULL**: `customer = {id: "c9", city: NULL}` — struct có, dữ liệu thiếu một ngăn.
3. **Array**: NULL array ≠ array rỗng `[]` ≠ array chứa phần tử NULL `[NULL]`. `explode` nuốt cả NULL lẫn `[]`; `size(NULL)` trả **-1** (với `spark.sql.legacy.sizeOfNull=true`, mặc định Spark 3.x trả -1... hãy tự kiểm chứng trong lab) còn `size([])` trả 0.

### 3.6. Schema evolution — struct thêm field

Nguồn upstream thêm field mới vào JSON (chuyện xảy ra hàng tháng ở công ty thật). Với `from_json`:

- Schema bạn khai **thiếu** field mới → field mới bị **lờ đi lặng lẽ** (không lỗi, không cảnh báo — dữ liệu rơi).
- Schema bạn khai **thừa** field (nguồn chưa gửi) → field đó = **NULL** cho message cũ (an toàn).

⇒ Chiến lược production: **khai schema theo phiên bản MỚI NHẤT của nguồn** ngay khi biết nguồn sắp đổi — message cũ tự động NULL field mới, message mới có đủ. Kết hợp: giữ nguyên chuỗi raw ở bronze (cột `value` gốc) để khi phát hiện rơi field còn replay lại được.

---

## 4. Internal

### from_json chạy ở đâu, nhanh vì sao

`from_json` là **built-in expression của Catalyst**, thực thi bằng Jackson parser **ngay trong JVM executor** — không rời JVM, không serialize sang Python (khác hẳn UDF — lesson 12 sẽ đau đớn hơn). Nó cũng tham gia codegen của Tungsten như mọi built-in khác.

```
Executor JVM
┌───────────────────────────────────────────────────────┐
│ partition của Kafka records                           │
│   value(binary) ─cast─▶ string ─Jackson─▶ InternalRow │
│                                    (struct theo schema)│
│   dòng parse fail ──▶ NULL struct (PERMISSIVE)        │
└───────────────────────────────────────────────────────┘
        không có process Python nào ở đây cả
```

### explode là generator, không phải projection

Trong physical plan, explode xuất hiện là node **`Generate explode(items)`**. Điểm quan trọng: explode là **narrow transformation** — mỗi partition tự bung dòng của mình, KHÔNG gây shuffle. Nhưng nó **nhân số dòng**: partition 128 MB chứa array trung bình 50 phần tử → sau explode thành ~50× số dòng. Số partition không đổi mà mỗi partition phình to → task chậm/OOM. Thuốc: `repartition` sau explode, hoặc select bớt cột TRƯỚC khi explode (mọi cột đi kèm đều bị lặp lại N lần).

### Nested trong Parquet: pruning vẫn sống

Parquet lưu `customer.city` như một cột lá riêng. Query chỉ đụng `customer.city` → Spark đọc đúng cột lá đó (nested column pruning, mặc định bật từ Spark 3.0 qua `spark.sql.optimizer.nestedSchemaPruning.enabled`). Đây là lý do struct ăn đứt map và ăn đứt "JSON string": map và string là black box, pruning bó tay.

---

## 5. API

### `StructType` / `ArrayType` / `MapType` — khai schema

```python
from pyspark.sql.types import *

item_schema = StructType([
    StructField("sku",   StringType(),  True),
    StructField("qty",   IntegerType(), True),
    StructField("price", DoubleType(),  True),
])
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer", StructType([
        StructField("id",   StringType(), True),
        StructField("city", StringType(), True),
    ]), True),
    StructField("items", ArrayType(item_schema), True),
    StructField("tags",  MapType(StringType(), StringType()), True),
])
```
- **Pitfall**: quên `True` (nullable) → khai `False` mà dữ liệu có NULL thì hành vi không đảm bảo (Spark không thực sự enforce not-null khi đọc, nhưng optimizer có thể tin lời khai của bạn và tối ưu sai).

### `F.col("a.b")` — dot notation

```python
df.select("order_id", F.col("customer.city").alias("city"))
```
- **Pitfall**: cột tên có dấu chấm THẬT (ví dụ `"user.name"` là tên cột phẳng) sẽ bị hiểu nhầm là nested → phải dùng backtick `` F.col("`user.name`") ``.

### `F.explode` / `F.explode_outer` / `F.posexplode`

```python
df.select("order_id", F.explode_outer("items").alias("item")) \
  .select("order_id", "item.sku", "item.qty", "item.price")
```
- **Pitfall 1**: `explode` bỏ rơi dòng array rỗng/NULL — sai count âm thầm (đã nói ở Theory, nhắc lại vì đây là bug production có thật).
- **Pitfall 2**: hai `explode` trong CÙNG một `select` → lỗi `Only one generator allowed`. Muốn explode 2 array → 2 bước, và cẩn thận: kết quả là **tích Descartes** N×M dòng.

### `F.collect_list` / `F.collect_set`

```python
df_flat.groupBy("order_id").agg(F.collect_list("sku").alias("skus"))
```
- **Pitfall**: không đảm bảo thứ tự phần tử (phụ thuộc thứ tự dòng đến sau shuffle). Cần thứ tự → gom struct có kèm cột sort rồi `array_sort`, hoặc dùng window. Và: gom cả triệu phần tử vào 1 array của 1 key = OOM executor.

### `F.from_json` / `F.to_json` / `F.schema_of_json`

```python
parsed = df.withColumn("data", F.from_json(F.col("value").cast("string"), order_schema))
# kiểm tra parse fail:
parsed.filter(F.col("data").isNull() & F.col("value").isNotNull())
```
- **Pitfall**: PERMISSIVE mode trả NULL khi fail — **phải** đếm/ghi lại số dòng NULL sau parse (dead letter), nếu không dữ liệu hỏng biến mất không dấu vết.

### `F.get_json_object(col, path)`

```python
df.withColumn("op", F.get_json_object(F.col("value").cast("string"), "$.payload.op"))
```
- **Pitfall**: trả về STRING luôn (kể cả số) → nhớ cast; gọi nhiều lần = parse nhiều lần.

### Hàm array/map hay dùng kèm

`F.size`, `F.array_contains`, `F.flatten` (array của array → array), `F.transform(col, lambda)` (map từng phần tử — higher-order function, chạy trong JVM, KHÔNG phải Python UDF), `F.map_keys`, `F.map_values`, `F.explode(map_col)` → 2 cột key/value.

---

## 6. Demo nhỏ

```
Input:  2 chuỗi JSON đơn hàng (1 cái có 2 items, 1 cái items rỗng)
   ↓    from_json với schema tường minh
   ↓    explode_outer items
Output: bảng phẳng — đơn rỗng vẫn còn mặt
```

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("demo11").master("local[2]").getOrCreate()

raw = spark.createDataFrame([
    ('{"order_id":"A1","customer":{"city":"Ha Noi"},"items":[{"sku":"SP01","qty":2},{"sku":"SP07","qty":1}]}',),
    ('{"order_id":"A2","customer":{"city":"Da Nang"},"items":[]}',),
], ["value"])

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer", StructType([StructField("city", StringType())])),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()), StructField("qty", IntegerType())]))),
])

flat = (raw
    .withColumn("d", F.from_json("value", schema))
    .select("d.order_id", F.col("d.customer.city").alias("city"),
            F.explode_outer("d.items").alias("item"))
    .select("order_id", "city", "item.sku", "item.qty"))

flat.show()
# +--------+-------+----+----+
# |order_id|   city| sku| qty|
# |      A1| Ha Noi|SP01|   2|
# |      A1| Ha Noi|SP07|   1|
# |      A2|Da Nang|NULL|NULL|   ← explode_outer giữ A2; explode thường thì A2 biến mất
# +--------+-------+----+----+
spark.stop()
```

Đổi `explode_outer` thành `explode` chạy lại — A2 bốc hơi. Hãy tận mắt thấy một lần để không bao giờ quên.

---

## 7. Production Example

Đây chính là tầng parse của repo `../kafka-flink` của bạn: Debezium bắt thay đổi từ PostgreSQL, đẩy vào Kafka mỗi thay đổi một **envelope** JSON:

```json
{
  "payload": {
    "before": null,
    "after":  {"order_id": "abc", "status": "delivered", "amount": 150.0},
    "source": {"db": "olist", "table": "orders", "lsn": 123456, "ts_ms": 1720400000000},
    "op": "c",
    "ts_ms": 1720400000123
  }
}
```

Giải mã envelope — bảng tra Senior phải thuộc:

| op | Nghĩa | before | after |
|---|---|---|---|
| `c` | insert (create) | NULL | dòng mới |
| `u` | update | dòng cũ | dòng mới |
| `d` | delete | dòng bị xóa | **NULL** |
| `r` | snapshot (read lần đầu) | NULL | dòng hiện tại |

Pipeline thật (bạn sẽ viết bản streaming ở Project 2, hôm nay viết bản batch):

```
Kafka topic "olist.public.orders"
   ↓  value là bytes → cast string
   ↓  from_json(value, debezium_schema)          ← schema: payload{before, after, source, op, ts_ms}
   ↓  chọn dòng: op='d' lấy before, còn lại lấy after
   ↓  flatten: select after.* + op + ts_ms + source.lsn
Iceberg bronze (giữ cả op để tầng sau MERGE)
```

Vì sao doanh nghiệp làm vậy: `before`/`after` là struct **cùng schema với bảng nguồn** — khai một lần, dùng cho cả hai; `ts_ms` + `source.lsn` cho phép sắp thứ tự sự kiện khi cùng một dòng bị update 2 lần trong 1 batch (lấy bản có lsn lớn nhất — bạn đã học window function ở lesson 10, `row_number` desc chính là để làm việc này).

---

## 8. Hands-on Lab

**Mục tiêu**: parse Debezium envelope bằng batch, flatten thành bảng phẳng, xử lý đủ 3 op và schema evolution.

### Bước 1 — tạo dữ liệu Debezium giả lập: `labs/lab11/gen_debezium.py`

```python
# Tạo file JSON lines mô phỏng topic CDC của bảng orders
import json, random, time

ops = []
for i in range(1000):
    row = {"order_id": f"ord{i:04d}", "status": "created", "amount": round(random.uniform(10, 500), 2)}
    ops.append({"payload": {"before": None, "after": row,
                "source": {"db": "olist", "table": "orders", "lsn": i * 10, "ts_ms": 1720400000000 + i},
                "op": "c", "ts_ms": 1720400000000 + i}})
    if i % 5 == 0:  # 20% đơn được update sang delivered
        new = dict(row, status="delivered")
        ops.append({"payload": {"before": row, "after": new,
                    "source": {"db": "olist", "table": "orders", "lsn": i * 10 + 5, "ts_ms": 1720400500000 + i},
                    "op": "u", "ts_ms": 1720400500000 + i}})
    if i % 50 == 0:  # vài đơn bị xóa
        ops.append({"payload": {"before": row, "after": None,
                    "source": {"db": "olist", "table": "orders", "lsn": i * 10 + 9, "ts_ms": 1720400900000 + i},
                    "op": "d", "ts_ms": 1720400900000 + i}})

with open("/workspace/labs/lab11/debezium_orders.jsonl", "w") as f:
    for o in ops:
        f.write(json.dumps(o) + "\n")
print(f"Đã ghi {len(ops)} messages")
```

### Bước 2 — parse & flatten: `labs/lab11/parse_debezium.py`

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("lab11-debezium").getOrCreate()

row_schema = StructType([          # schema của bảng nguồn — dùng chung before/after
    StructField("order_id", StringType()),
    StructField("status",   StringType()),
    StructField("amount",   DoubleType()),
])
dbz_schema = StructType([StructField("payload", StructType([
    StructField("before", row_schema),
    StructField("after",  row_schema),
    StructField("source", StructType([
        StructField("db", StringType()), StructField("table", StringType()),
        StructField("lsn", LongType()),  StructField("ts_ms", LongType())])),
    StructField("op",    StringType()),
    StructField("ts_ms", LongType()),
]))])

raw = spark.read.text("/workspace/labs/lab11/debezium_orders.jsonl")
parsed = raw.withColumn("d", F.from_json("value", dbz_schema))

# QC bắt buộc sau from_json: có dòng nào parse fail không?
bad = parsed.filter(F.col("d").isNull()).count()
print(f"Parse fail: {bad} dòng")   # phải = 0

flat = (parsed
    .select(F.col("d.payload.op").alias("op"),
            F.col("d.payload.ts_ms").alias("ts_ms"),
            F.col("d.payload.source.lsn").alias("lsn"),
            # delete thì after=NULL → lấy ảnh từ before
            F.when(F.col("d.payload.op") == "d", F.col("d.payload.before"))
             .otherwise(F.col("d.payload.after")).alias("row"))
    .select("op", "ts_ms", "lsn", "row.*"))       # row.* bung struct thành cột phẳng

flat.show(10, truncate=False)
flat.groupBy("op").count().show()
flat.write.mode("overwrite").parquet("/workspace/labs/lab11/out/orders_cdc_flat")
spark.stop()
```

### Bước 3 — chạy

```bash
python3 labs/lab11/gen_debezium.py   # hoặc chạy trong container nếu muốn
make run-local F=labs/lab11/parse_debezium.py
```

### Bước 4 — schema evolution drill

Sửa `gen_debezium.py`: thêm field `"channel": "app"` vào `row` (mô phỏng nguồn thêm cột). Chạy lại parse **KHÔNG sửa schema** → quan sát: không lỗi, nhưng `channel` biến mất. Rồi thêm `StructField("channel", StringType())` vào `row_schema`, chạy lại → message cũ có `channel = NULL`, message mới có giá trị. Ghi nhận xét vào `labs/lab11/NOTES.md`.

### Bước 5 — quan sát Spark UI (:4040)

Tab SQL → mở query plan: tìm node `Generate` nếu bạn có explode, xác nhận from_json KHÔNG tạo stage mới (narrow). Đếm số stage của job cuối.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Struct vs Map: khi nào dùng cái nào? Tại sao struct thân thiện với Parquet hơn?
2. Vẽ bảng: explode vs explode_outer vs posexplode xử lý array rỗng/NULL khác nhau thế nào.
3. Debezium envelope: op='d' thì dữ liệu dòng nằm ở `before` hay `after`? Tại sao Debezium thiết kế vậy?

**Medium** — Lấy `flat` từ lab: một `order_id` có thể xuất hiện nhiều lần (c rồi u rồi d). Dùng window function (lesson 10) lấy **trạng thái cuối cùng** của mỗi order theo `lsn` lớn nhất, rồi loại các order có op cuối = 'd'. Đây chính là logic "materialize CDC thành bảng hiện hành" — kiểm tra: số order còn lại + số order bị xóa = 1000?

**Hard** — Nguồn thêm field lồng sâu: `after.shipping = {carrier, fee}` (struct trong struct). Cập nhật schema, flatten thành `shipping_carrier`, `shipping_fee`; chứng minh message cũ ra NULL cả hai. Sau đó viết hàm Python `flatten_struct(df)` tự động bung MỌI struct thành cột phẳng với tên `cha_con` (đệ quy qua `df.schema`) — chạy được trên DataFrame bất kỳ.

**Production Challenge** — Mở repo `../kafka-flink`, tìm code parse Debezium hiện có (gợi ý: grep `from_json` hoặc `payload` trong thư mục processing). Review 10 dòng: schema khai đủ field chưa? Có kiểm tra parse-fail không? Có giữ raw value ở bronze không? Đề xuất 2 cải tiến.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| `from_json` với schema tường minh | Nhanh | Jackson trong JVM, một lượt parse, tham gia codegen. |
| `get_json_object` × 10 field | Chậm ×10 | Mỗi call parse lại string từ đầu. Nhiều field → from_json một lần. |
| `schema_of_json` trong production | Nguy hiểm | Đoán schema từ mẫu; và nếu dùng kiểu infer trên cả cột thì thêm một job quét dữ liệu. |
| explode array to (nghìn phần tử) | Bẫy memory | Số dòng nhân N ngay trong partition — select bớt cột trước khi explode, repartition sau. |
| `collect_list` trên key skew | Bẫy OOM | 1 key có 10M dòng → 1 array 10M phần tử trong 1 task. |
| Query `customer.city` trên Parquet nested | Nhanh | Nested column pruning — chỉ đọc cột lá cần thiết. |
| Lưu JSON dưới dạng string rồi parse mỗi query | Chậm nhất | Parse lặp lại mãi mãi. Parse một lần ở silver, lưu struct/cột phẳng. |

Câu tự vấn của bài này: *"explode này nhân số dòng lên bao nhiêu, và có dòng nào bị nó nuốt mất không?"*

---

## 11. Spark UI

Bài này dùng **tab SQL / DataFrame** (mở khóa từ lesson 9, hôm nay nhìn thêm):

- Mở query → tìm node **`Generate explode(...)`**: đây là explode của bạn. Nhìn số **output rows** của node này so với node trước — chính là hệ số nhân dòng. Trước 1.000, sau 50.000 → array trung bình 50 phần tử.
- `from_json` KHÔNG hiện thành node riêng — nó nằm trong `Project`. Xác nhận không có Exchange (shuffle) nào sinh ra chỉ vì parse JSON.
- Tab **Stages**: nếu sau explode task duration phình to bất thường → đó là dấu hiệu cần repartition sau explode.

---

## 12. Common Mistakes

1. **Dùng `explode` khi cần `explode_outer`** → dòng array rỗng biến mất, count sai âm thầm. Bug production có thật, khó truy vì không có lỗi nào được ném ra.
2. **Không kiểm tra NULL sau `from_json`** → message hỏng thành NULL lặng lẽ, mất dữ liệu không dấu vết. Luôn đếm và log số dòng parse-fail.
3. **`get_json_object` rải 10 lần cho 10 field** → parse lại chuỗi 10 lần. Cần ≥2-3 field thì `from_json` một lần.
4. **Dùng `schema_of_json`/infer schema trong production** → schema trôi theo mẫu dữ liệu, hôm nay đủ field mai thiếu field. Schema phải nằm trong code, được review như code.
5. **Explode sớm khi chưa cần** → nhân dòng rồi mới filter/join, chi phí gấp N. Filter và select bớt cột TRƯỚC explode.
6. **MapType cho dữ liệu có key cố định** → mất column pruning, mất kiểm soát kiểu per-field. Key biết trước = struct.
7. **Quên rằng collect_list không có thứ tự** → array items lúc đúng thứ tự lúc không, tùy shuffle. Cần thứ tự thì sort tường minh.

---

## 13. Interview

**Junior:**

1. *StructType, ArrayType, MapType khác nhau thế nào?* — Struct: cụm field cố định có tên/kiểu riêng (cái hộp có ngăn dán nhãn). Array: danh sách phần tử cùng kiểu, độ dài tùy ý. Map: key→value với key động lúc runtime. Chúng lồng nhau tùy ý; array-của-struct là hình dạng phổ biến nhất của event data.
2. *explode làm gì? explode vs explode_outer?* — explode biến 1 dòng có array N phần tử thành N dòng, cột khác lặp lại. Khác nhau ở array rỗng/NULL: explode bỏ dòng đó (như inner join), explode_outer giữ dòng với giá trị NULL (như left join).
3. *from_json cần gì để hoạt động? Dòng JSON hỏng thì sao?* — Cần schema (StructType hoặc chuỗi DDL). Mặc định PERMISSIVE: dòng parse fail trả NULL chứ không throw — nên phải chủ động kiểm tra NULL sau parse.
4. *Truy cập field trong struct thế nào?* — Dot notation: `col("customer.city")` hoặc `customer.city` trong SQL; struct NULL thì kết quả NULL, không lỗi. Array dùng index `[0]`, map dùng `['key']`.

**Mid:**

5. *Khi nào dùng MapType thay vì StructType? Trade-off?* — Map khi tập key không biết trước/động (custom attributes). Trade-off: mất column pruning trên Parquet (map là black box với reader), mất kiểm soát kiểu theo từng field, query chậm hơn. Key cố định → luôn struct.
6. *get_json_object vs from_json — chọn thế nào?* — get_json_object: không cần schema, tiện lấy 1-2 field (routing), nhưng trả string và parse lại chuỗi mỗi lần gọi. from_json: parse một lần ra struct có kiểu, Catalyst tối ưu tiếp được. Cần nhiều field hoặc dùng lặp lại → from_json.
7. *explode có gây shuffle không? Rủi ro performance của nó là gì?* — Không — explode là narrow transformation (node Generate), mỗi partition tự bung. Rủi ro: nhân số dòng trong cùng partition → partition phình to, task chậm/OOM; mọi cột đi kèm bị lặp N lần. Giải pháp: select tối thiểu cột trước explode, repartition sau.
8. *Nguồn JSON thêm field mới — pipeline from_json của bạn phản ứng ra sao?* — Schema thiếu field mới → field bị lờ đi lặng lẽ (rơi dữ liệu, không lỗi). Schema khai trước field mà nguồn chưa gửi → NULL an toàn. Nên chủ động thêm field vào schema sớm và giữ raw string ở bronze để replay.

**Senior:**

9. *Thiết kế tầng parse cho topic Debezium: những quyết định quan trọng?* — (a) Khai row schema một lần dùng cho cả before/after; (b) chọn ảnh dữ liệu theo op: delete lấy before, còn lại after — và GIỮ cột op cho tầng MERGE; (c) dedupe trong batch bằng window theo key với lsn/ts_ms desc để lấy sự kiện cuối; (d) đếm parse-fail + dead letter; (e) bronze giữ nguyên raw value để replay khi schema evolution làm rơi field. Trả lời thiếu (e) là chưa từng bị production dạy cho một bài.
10. *Bảng 1 dòng/đơn hàng với cột items là array 10k phần tử cho vài key lớn — vấn đề và cách xử lý?* — Vấn đề kép: (a) explode nhân dòng không đều → partition skew, vài task rùa bò; (b) collect_list chiều ngược lại tạo array khổng lồ trong 1 task → OOM. Xử lý: explode xong repartition theo key phù hợp; với skew nặng dùng salting (lesson 19); cân nhắc thiết kế lại grain — lưu order-item level ở silver thay vì ôm array, array chỉ để trình bày ở serving layer.

---

## 14. Summary

### Mindmap

```
                        COMPLEX TYPES & JSON
                               │
     ┌──────────────┬──────────┴──────────┬────────────────────┐
     ▼              ▼                     ▼                    ▼
  3 KIỂU         DUỖI & GẤP           JSON PARSE          PRODUCTION
     │              │                     │                    │
  Struct: ngăn   explode: 1→N dòng    from_json + schema   Debezium envelope
  dán nhãn       explode_outer: giữ   → struct trong JVM   before/after/op
  Array: 1-n     dòng rỗng            NULL nếu fail        op='d' → lấy before
  trong 1 dòng   posexplode: kèm pos  get_json_object:     dedupe theo lsn
  Map: key động  collect_list/set:    1 field, parse lại   schema evolution:
  (biết key →    N dòng → 1 array     schema_of_json:      thiếu→rơi, thừa→NULL
  dùng struct!)  (không thứ tự!)      chỉ để dev           bronze giữ raw
```

### Checklist trước khi gõ "Continue"

- [ ] Chọn được struct/array/map cho 3 tình huống dữ liệu khác nhau và nói được lý do.
- [ ] Giải thích được explode nuốt dòng array rỗng thế nào và khi nào phải dùng explode_outer.
- [ ] Viết được from_json với StructType tường minh + kiểm tra parse-fail.
- [ ] Flatten được Debezium envelope, xử lý đúng op='d' (lấy before).
- [ ] Đã làm schema evolution drill: thấy tận mắt field bị rơi khi schema thiếu.
- [ ] Nhìn được node Generate và output rows trong tab SQL của Spark UI.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 12 — UDF vs built-in vs pandas UDF.**

Hôm nay bạn flatten JSON hoàn toàn bằng built-in functions — mọi thứ chạy gọn trong JVM. Nhưng sẽ có ngày logic của bạn không có built-in nào làm được: validate số điện thoại theo chuẩn Việt Nam, gọi thư viện Python tính khoảng cách địa lý... và bạn sẽ viết UDF. Lesson 12 cho bạn thấy cái giá thật của một dòng `@udf`: dữ liệu bị lôi ra khỏi JVM, serialize từng dòng sang process Python, Catalyst mù tịt không tối ưu được gì — job chậm gấp 5-50 lần mà code trông vẫn "sạch". Và lối thoát: pandas UDF vectorized với Arrow. Đây là bài học đắt tiền nhất về performance trong Module 2.

> Gõ **"Continue"** khi sẵn sàng.

"""schemas.py — Schema TƯỜNG MINH cho Mini Project 1 (kết quả của bài A21).

Đây KHÔNG phải file để chạy. Nó là *hằng số* của cả project: mọi script khác
import từ đây, để chỉ có MỘT nơi định nghĩa "dữ liệu Olist trông như thế nào".

VÌ SAO không dùng inferSchema trong code nộp (đề cấm, −10 điểm):
  1. inferSchema là một ACTION TRÁ HÌNH: Spark phải quét (gần) hết file MỘT LẦN
     chỉ để đoán kiểu, rồi quét LẠI lần nữa để đọc thật. Job lười biếng bỗng
     đọc file 2 lần.
  2. Nó đoán theo dữ liệu HÔM NAY. Tháng sau file đổi (cột toàn số bỗng có chữ)
     -> kiểu đổi im lặng -> downstream vỡ mà không ai biết vì sao.
  3. Nó đoán SAI theo những cách nguy hiểm: id "00123" -> Integer 123 (mất số 0),
     tiền -> Double (sai số nhị phân khi cộng dồn).

Cách làm đúng (mẹo lesson 5, chính là bài A21):
  chạy inferSchema ĐÚNG MỘT LẦN ở máy dev -> copy schema ra -> SỬA TAY -> đóng băng vào file này.
  Script `exercises/a21_schema_infer_then_fix.py` làm bước "chạy một lần" đó và
  in ra bảng so sánh "Spark đoán gì / tôi sửa thành gì / vì sao".
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Tên cột hứng nguyên văn dòng hỏng. Muốn dùng _corrupt_record thì PHẢI khai nó
# trong schema (StringType, nullable) — Spark không tự thêm cột này cho bạn.
CORRUPT_COL = "_corrupt_record"


# ---------------------------------------------------------------------------
# ORDERS — olist_orders_dataset.csv (8 cột)
# ---------------------------------------------------------------------------
# nullable=True ở KHẮP NƠI, kể cả order_id. Xem ghi chú NULLABLE ở cuối file:
# nullable=False là một LỜI HỨA với Spark chứ không phải một phép kiểm tra.
ORDERS = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    # 4 cột dưới đây inferSchema đoán ra... TimestampType (vì format ISO chuẩn).
    # Nhưng nếu file có 1 dòng ngày lỗi, inferSchema sẽ tụt xuống StringType cho
    # CẢ CỘT — im lặng. Khai tay thì dòng lỗi thành NULL + _corrupt_record: có dấu vết.
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])

ORDERS_CORRUPT = StructType(ORDERS.fields + [StructField(CORRUPT_COL, StringType(), True)])


# ---------------------------------------------------------------------------
# ORDER_ITEMS — olist_order_items_dataset.csv (7 cột)
# ---------------------------------------------------------------------------
ORDER_ITEMS = StructType([
    StructField("order_id", StringType(), True),
    # order_item_id: SỐ THẬT (1,2,3... thứ tự món trong đơn) -> Integer là đúng.
    # Đây là cột duy nhất trong dataset mà "nhìn giống số" VÀ "thật sự là số".
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", TimestampType(), True),
    # price / freight_value: Double.
    # PRODUCTION dùng DecimalType(10,2): Double là nhị phân, 0.1+0.2 != 0.3.
    # Cộng dồn triệu dòng tiền bằng Double -> lệch xu -> kế toán không ký.
    # Ở bài học giữ Double vì Olist chỉ ~112k dòng và mọi tutorial dùng Double;
    # nhưng ĐÂY LÀ MỘT KHOẢN NỢ KỸ THUẬT CÓ Ý THỨC, không phải sơ suất.
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

ORDER_ITEMS_CORRUPT = StructType(
    ORDER_ITEMS.fields + [StructField(CORRUPT_COL, StringType(), True)]
)


# ---------------------------------------------------------------------------
# CUSTOMERS — olist_customers_dataset.csv (5 cột)
# ---------------------------------------------------------------------------
CUSTOMERS = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    # customer_zip_code_prefix: inferSchema đoán Integer -> BẪY KINH ĐIỂN.
    # Mã bưu chính Brazil có số 0 đứng đầu ("01310"). Integer nuốt số 0 -> "1310".
    # Quy tắc: cái gì KHÔNG BAO GIỜ đem đi CỘNG/TRỪ thì không phải số. Nó là NHÃN.
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

CUSTOMERS_CORRUPT = StructType(
    CUSTOMERS.fields + [StructField(CORRUPT_COL, StringType(), True)]
)


# ---------------------------------------------------------------------------
# BRONZE: mọi thứ là String
# ---------------------------------------------------------------------------
# Dùng ở A23 để chứng minh một điều rợn người: cùng một file bẩn, đọc bằng schema
# CÓ KIỂU thì Spark bắt được dòng hỏng; đọc bằng schema TOÀN STRING (kiểu "bronze
# cho an toàn") thì dòng lệch cột LỌT SẠCH — order_status thành "São Paulo" mà
# không ai báo gì. "String cho an toàn" là an toàn cho PIPELINE, không an toàn
# cho DỮ LIỆU.
ORDERS_ALL_STRING = StructType(
    [StructField(f.name, StringType(), True) for f in ORDERS.fields]
)

ORDERS_ALL_STRING_CORRUPT = StructType(
    ORDERS_ALL_STRING.fields + [StructField(CORRUPT_COL, StringType(), True)]
)


# ---------------------------------------------------------------------------
# Bảng bằng chứng cho A21 — "Spark đoán gì / tôi sửa thành gì / vì sao"
# ---------------------------------------------------------------------------
# Để ở đây (không ở exercise) vì đây là *quyết định thiết kế schema*, mà schema
# thì sống ở file này. a21 chỉ đối chiếu bảng này với thứ inferSchema thật sự đoán.
FIX_TABLE = [
    # (bảng, cột, Spark đoán, tôi sửa thành, vì sao)
    ("customers", "customer_zip_code_prefix", "IntegerType", "StringType",
     "Mã bưu chính có số 0 đứng đầu ('01310'). Integer nuốt số 0 -> sai vĩnh viễn, không cứu được."),
    ("orders", "order_id", "StringType", "StringType (giữ, nhưng nullable=True)",
     "Kiểu đúng rồi. Cái phải sửa là nullable: KHÔNG khai False dù nó là khoá — xem ghi chú NULLABLE."),
    ("order_items", "order_item_id", "IntegerType", "IntegerType (giữ)",
     "Số thật (thứ tự món trong đơn), có đem đi so sánh/max. Giữ Integer là đúng."),
    ("order_items", "price", "DoubleType", "DoubleType (nợ kỹ thuật, đúng phải là DecimalType(10,2))",
     "Tiền bằng Double = sai số nhị phân khi SUM triệu dòng. Bài học chấp nhận Double; production phải Decimal."),
    ("order_items", "freight_value", "DoubleType", "DoubleType (nợ kỹ thuật, như trên)",
     "Cùng lý do với price."),
    ("orders", "order_purchase_timestamp", "TimestampType (may mắn)", "TimestampType (khai tay)",
     "inferSchema chỉ đoán đúng vì file HÔM NAY sạch. Một dòng 'hôm qua' là cả cột tụt xuống String — im lặng."),
    ("orders", "order_delivered_customer_date", "TimestampType", "TimestampType (khai tay)",
     "Cột này có NULL thật (đơn chưa giao). Khai tay để chắc chắn NULL là NULL, không phải chuỗi rỗng."),
    ("orders", "_corrupt_record", "(không có)", "StringType — tự thêm",
     "inferSchema KHÔNG BAO GIỜ sinh cột này. Không khai = không có quarantine = dòng hỏng biến mất im lặng."),
]


# ---------------------------------------------------------------------------
# GHI CHÚ NULLABLE — cái bẫy đắt nhất của lesson 5
# ---------------------------------------------------------------------------
# StructField("order_id", StringType(), False)  <-- False = "tôi HỨA cột này không bao giờ null"
#
# Spark KHÔNG kiểm tra lời hứa đó khi đọc CSV. Nó TIN bạn, rồi Catalyst tối ưu
# DỰA TRÊN niềm tin đó (bỏ các phép check null, đổi outer join thành inner...).
# Dữ liệu vẫn có null -> kết quả SAI IM LẶNG, không exception, không cảnh báo.
#
# -> Trong project này MỌI cột đều nullable=True. Ràng buộc "order_id không null"
#    được thực thi bằng một CHECK THẬT trong quality gate (A38), không phải bằng
#    một chữ False trong schema.
# -> Bài a21 chạy một thí nghiệm chứng minh: khai False cho một cột có null thật,
#    xem có ai báo lỗi không (spoiler: không).
NULLABLE_STRICT_ORDERS = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    # LỜI HỨA SAI CỐ Ý: cột này có ~3000 null thật trong Olist (đơn chưa giao xong).
    StructField("order_delivered_customer_date", TimestampType(), False),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])

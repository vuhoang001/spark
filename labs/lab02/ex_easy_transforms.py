import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
PRODUCTS_FILE = ROOT / "data" / "olist" / "olist_products_dataset.csv"

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Lab02 - Easy Transforms")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    products = spark.read.csv(str(PRODUCTS_FILE), header=True, inferSchema=True)

    t0 = time.time()
    result = (
        products
        # (1) NARROW — filter xét từng dòng độc lập, không cần dữ liệu của partition khác,
        #     không shuffle. 1 dòng vào -> 0 hoặc 1 dòng ra, ngay tại partition đó.
        .filter(F.col("product_weight_g") > 1000)

        # (2) NARROW — withColumn tính trên chính các cột của từng dòng (l*h*w),
        #     mỗi output partition chỉ phụ thuộc đúng 1 input partition -> không shuffle.
        .withColumn(
            "volume_cm3",
            F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"),
        )

        # (3) NARROW — select + rename chỉ chọn/đổi tên cột, thuần map theo dòng, không shuffle.
        .select(
            F.col("product_id"),
            F.col("product_category_name").alias("category"),
            F.col("product_weight_g").alias("weight_g"),
            F.col("volume_cm3"),
        )

        # (4) NARROW — na.drop loại dòng có null, vẫn xét từng dòng riêng lẻ, không shuffle.
        .na.drop(subset=["category", "volume_cm3"])

        # (5) WIDE — distinct phải gom các bản trùng nằm rải khắp các partition về
        #     cùng chỗ để khử trùng => cần SHUFFLE (repartition theo hash của category).
        .select("category").distinct()
    )
    t_build = time.time() - t0

    # ↓↓↓ ĐÚNG 1 ACTION duy nhất kích cả chuỗi trên chạy ↓↓↓
    result.show(20, truncate=False)

    print(f"Dựng 5 transformation mất: {t_build:.4f}s (gần 0 vì lazy)")
    spark.stop()

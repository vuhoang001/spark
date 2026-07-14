"""A28 — JDBC partitioned read. (Bài ○ tuỳ chọn — làm phần LÝ THUYẾT, có lý do.)

Chạy (KHÔNG cần Spark, KHÔNG cần cluster — python thuần):
    python3 labs/mini-project-1/exercises/a28_jdbc_partitioned_read.py

VÌ SAO KHÔNG CHẠY THẬT — kiểm chứng, không phải lười:
  1. Repo CÓ postgres, nhưng ở `docker-compose.yaml` (stack kafka/debezium), KHÔNG
     phải `docker-compose.spark.yaml`. Hai compose = hai docker network khác nhau
     -> container spark-submit không gọi được host `postgres` (trừ khi nối network).
  2. Image apache/spark:3.4.1 KHÔNG có JDBC driver của Postgres:
        docker exec spark-mastery-spark-submit-1 ls /opt/spark/jars | grep -i postgres
     -> rỗng. Không driver = `java.lang.ClassNotFoundException: org.postgresql.Driver`.
     Muốn chạy thật phải: --packages org.postgresql:postgresql:42.7.3 (cần mạng).
  => Đề CHO PHÉP: "Không có DB? Vẫn làm được phần lý thuyết: viết ra 8 câu SQL mà
     Spark SẼ sinh ra". Script này KHÔNG viết tay 8 câu đó — nó CÀI ĐẶT LẠI đúng
     thuật toán của Spark (JDBCRelation.columnPartition, Scala) rồi để máy sinh ra.
     Chép tay thì chỉ chứng minh mình biết chép; cài lại thuật toán thì chứng minh
     mình hiểu nó cắt dải kiểu gì — và đó mới là thứ cứu bạn khỏi skew.

Output: 8 câu WHERE + phân tích skew + checklist trước khi bắn 8 connection vào DB thật.
"""


def column_partition(column, lower_bound, upper_bound, num_partitions):
    """Cài lại thuật toán chia dải của Spark 3.x.

    Nguồn: sql/core/.../datasources/jdbc/JDBCRelation.scala — hàm columnPartition().
    Ba chi tiết mà người chép tay luôn làm sai:

      (a) stride = upperBound/N - lowerBound/N   <-- CHIA TRƯỚC RỒI TRỪ, và là
          phép chia SỐ NGUYÊN (Long). KHÔNG phải (upper-lower)/N.
          Với lower=1, upper=100000, N=8: 100000/8 - 1/8 = 12500 - 0 = 12500.
          (Nếu tính (100000-1)/8 = 12499 -> bạn ra 8 câu SQL SAI so với thực tế.)

      (b) Dải ĐẦU không có cận dưới, và nó GÁNH LUÔN `OR col IS NULL`.
          -> Cột partition có nhiều NULL = task 0 ôm hết đống NULL đó. Skew ngay.

      (c) Dải CUỐI không có cận trên -> nó hứng MỌI giá trị > upperBound.
          -> lowerBound/upperBound KHÔNG lọc dữ liệu. Đặt sai = task cuối ôm cả bảng.
    """
    if upper_bound - lower_bound < num_partitions:
        # Spark tự hạ N xuống: không thể có nhiều dải hơn số giá trị.
        num_partitions = upper_bound - lower_bound

    stride = upper_bound // num_partitions - lower_bound // num_partitions

    clauses = []
    current = lower_bound
    for i in range(num_partitions):
        l_bound = f"{column} >= {current}" if i != 0 else None
        current += stride
        u_bound = f"{column} < {current}" if i != num_partitions - 1 else None

        if u_bound is None:
            where = l_bound
        elif l_bound is None:
            where = f"{u_bound} or {column} is null"
        else:
            where = f"{l_bound} AND {u_bound}"
        clauses.append(where)
    return stride, clauses


def main():
    COL, LO, HI, N = "order_sk", 1, 100000, 8

    print("=" * 78)
    print("A28 — 8 câu SQL mà Spark SẼ sinh ra")
    print(f"partitionColumn={COL}  lowerBound={LO}  upperBound={HI}  numPartitions={N}")
    print("=" * 78)
    stride, clauses = column_partition(COL, LO, HI, N)
    print(f"\nstride = {HI}//{N} - {LO}//{N} = {HI // N} - {LO // N} = {stride}\n")
    print("| task | câu SQL Spark gửi xuống DB (mỗi task = 1 connection riêng) |")
    print("|---|---|")
    for i, w in enumerate(clauses):
        print(f"| {i} | `SELECT ... FROM public.orders WHERE {w}` |")

    print("\nĐỌC KỸ task 0 và task 7 — đó là 2 chỗ duy nhất KHÔNG đối xứng:")
    print(f"  task 0: `{clauses[0]}`   <- gánh luôn mọi dòng NULL và mọi dòng < {LO}")
    print(f"  task {N-1}: `{clauses[-1]}`            <- gánh mọi dòng >= {HI} (không có cận trên!)")

    # ---------------------------------------------------------------
    print("\n" + "=" * 78)
    print("(a) KHÔNG có partitionColumn thì sao?")
    print("=" * 78)
    print("""
    df = spark.read.jdbc(url, "public.orders", properties=props)
    df.rdd.getNumPartitions()   ->  1     (KHÔNG cần chạy cũng biết: Spark
                                           không có căn cứ nào để cắt dải)

    1 partition = 1 task = 1 connection = 1 core làm việc, 5 core kia ngồi chơi.
    Bảng 50 triệu dòng: một luồng TCP kéo về, rồi TOÀN BỘ đi qua 1 executor.
    Và tệ hơn: 1 partition đó phải VỪA RAM của executor đó. Không vừa -> OOM/spill.

    => Số partition khi đọc JDBC KHÔNG do dữ liệu quyết định (khác hẳn đọc file,
       nơi maxPartitionBytes chia file hộ bạn). Đọc JDBC, BẠN phải tự chia.""")

    print("=" * 78)
    print("(b) CÓ đủ 4 option -> 8 partition -> 8 connection SONG SONG")
    print("=" * 78)
    print("""
    df = (spark.read.format("jdbc")
          .option("url", "jdbc:postgresql://postgres:5432/olist")
          .option("dbtable", "public.orders")
          .option("user", "postgres").option("password", "***")
          .option("partitionColumn", "order_sk")   # PHẢI là numeric/date/timestamp
          .option("lowerBound", "1")
          .option("upperBound", "100000")
          .option("numPartitions", "8")
          .load())
    df.rdd.getNumPartitions()   ->  8

    Cách NHÌN THẤY 8 câu SELECT đó nếu có Postgres thật:
        ALTER SYSTEM SET log_statement = 'all';   SELECT pg_reload_conf();
        docker logs -f <postgres_container> | grep 'order_sk'
    Bốn option này là MỘT GÓI: thiếu 1 trong 4 -> Spark im lặng quay về 1 partition.
    Không cảnh báo. Bạn chỉ biết khi nhìn getNumPartitions().""")

    # ---------------------------------------------------------------
    print("=" * 78)
    print("(c) CÂU ĐỀ HỎI: nếu partitionColumn LỆCH thì chuyện gì xảy ra?")
    print("=" * 78)
    # Mô phỏng số: id tăng dần, nhưng 90% dữ liệu dồn vào 10% id cuối
    # (đúng như đời thật: bảng orders của một shop tăng trưởng -> đơn mới nhiều hơn đơn cũ).
    total = 10_000_000
    hot_from = 90_000          # 90% số dòng có order_sk >= 90000
    rows_hot, rows_cold = int(total * 0.9), int(total * 0.1)
    print(f"""
    Giả định (đúng với mọi bảng của công ty đang tăng trưởng):
      - order_sk chạy 1..100000 (đều về GIÁ TRỊ)
      - nhưng {rows_hot:,} / {total:,} dòng ({0.9:.0%}) có order_sk >= {hot_from:,}
        (shop bán chạy dần, đơn mới dồn về cuối dải id)

    Spark cắt dải theo GIÁ TRỊ của cột, KHÔNG theo SỐ DÒNG — nó không biết đếm
    phân bố, và nó cũng không hỏi database. Kết quả:""")
    print()
    print("| task | dải order_sk | số dòng ước tính | ghi chú |")
    print("|---|---|---|---|")
    for i, w in enumerate(clauses):
        lo_i = LO + i * stride
        hi_i = lo_i + stride
        if i < N - 1:
            # 7 dải đầu chia nhau 10% dữ liệu "nguội"
            est = rows_cold // (N - 1)
            note = "nhàn"
        else:
            est = rows_hot
            note = "**ÔM 90% BẢNG — cả job chờ MỘT task này**"
        print(f"| {i} | [{lo_i:,} .. {hi_i if i < N-1 else '∞'}) | ~{est:,} | {note} |")

    print(f"""
    Hậu quả cụ thể:
      - 7 task xong trong vài giây rồi NGỒI CHƠI; 1 task cày {rows_hot:,} dòng.
      - Thời gian job = thời gian của task chậm nhất, KHÔNG phải trung bình.
        Song song 8 connection mà nhanh lên gần như 0% -> tưởng "Spark chậm",
        thực ra là "tôi chia dải sai".
      - Task đó phải nhét {rows_hot:,} dòng vào RAM 1 executor -> spill hoặc OOM.
      - Trên Spark UI: Max/Median của Task Duration >> 3 -> dấu hiệu skew kinh điển.

    CÁCH SỬA (theo thứ tự ưu tiên):
      1. Chọn cột chia có phân bố ĐỀU VỀ SỐ DÒNG: hash(id) % N, hoặc một cột
         surrogate rải đều. Không dùng id tăng dần của bảng đang tăng trưởng.
      2. Chia theo NGÀY nếu dữ liệu đều theo ngày (partitionColumn kiểu date/timestamp
         được Spark hỗ trợ) — thường đều hơn id nhiều.
      3. Không có cột đều? Dùng `.option("query", ...)` đẩy hẳn SQL xuống DB,
         hoặc để DB tự export ra file rồi Spark đọc file (nhiều lúc đây là câu trả lời đúng).

    BA ĐIỀU KHÔNG BAO GIỜ QUÊN VỀ JDBC:
      1. lowerBound/upperBound KHÔNG LỌC DỮ LIỆU. Chúng chỉ cắt dải. Dòng nằm ngoài
         khoảng vẫn được đọc — rơi vào dải đầu (kèm NULL) hoặc dải cuối.
      2. numPartitions = số CONNECTION ĐỒNG THỜI ĐẤM VÀO DATABASE. numPartitions=100
         trên DB production = 100 connection = DBA đi tìm bạn. Đọc từ replica.
      3. Thiếu 1 trong 4 option -> im lặng quay về 1 partition. Luôn kiểm bằng
         df.rdd.getNumPartitions() ngay sau khi đọc.
""")


if __name__ == "__main__":
    main()

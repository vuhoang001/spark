"""A33 — MỔ FILE PARQUET BẰNG PYARROW: nhìn tận mắt thứ lesson 6 mô tả.

CHẠY (KHÔNG cần Spark — đây là python thuần, đọc file trên đĩa):
    docker exec spark-mastery-spark-submit-1 python3 \
        /workspace/labs/mini-project-1/exercises/a33_parquet_anatomy.py
    # chỉ định file/thư mục khác:
    docker exec spark-mastery-spark-submit-1 python3 \
        /workspace/labs/mini-project-1/exercises/a33_parquet_anatomy.py /workspace/data/bench/a31/gzip

⚠️ pyarrow KHÔNG có sẵn trong image apache/spark:3.4.1. Cài trước (1 lần / mỗi lần
   container được tạo lại — pip install ghi vào lớp ghi của container, `make down` là mất):
       docker exec spark-mastery-spark-submit-1 pip install pyarrow

PHỤ THUỘC: cần MỘT thư mục Parquet đã tồn tại. Ưu tiên (tự dò theo thứ tự):
    1. data/output/silver/orders_clean   (Checkpoint 2 — có partition order_date=...)
    2. data/bench/cp3/orders_parquet     (do src/benchmark.py đẻ ra)
    3. data/bench/a30/orders             (do A30 đẻ ra)
=> chạy A30 hoặc benchmark.py TRƯỚC, hoặc truyền đường dẫn vào tham số.

BA CÂU HỎI ĐỀ BÀI BẮT TRẢ LỜI (script tự tính, không phải đoán):
  (a) cột nào nén tốt nhất, vì sao?   -> nhìn cột 'tỉ lệ nén' + 'encoding'
  (b) min/max của order_date có khớp tên thư mục partition không?
  (c) file có mấy row group — có đạt 128MB/row group như lý thuyết không, điều đó nói gì?
"""

import os
import sys

CANDIDATES = [
    "/workspace/data/output/silver/orders_clean",
    "/workspace/data/bench/cp3/orders_parquet",
    "/workspace/data/bench/a30/orders",
    "/workspace/data/bench/a31/snappy",
]


def find_parquet_files(root):
    out = []
    for r, _d, fs in os.walk(root):
        for f in fs:
            if f.startswith("part-") and f.endswith(".parquet"):
                out.append(os.path.join(r, f))
    return sorted(out)


def human(n):
    n = float(n)
    for u in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024 or u == "GB":
            return f"{n:.1f} {u}"
        n /= 1024


def main():
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("CHẠY LỖI: ModuleNotFoundError: No module named 'pyarrow'")
        print("Cài: docker exec spark-mastery-spark-submit-1 pip install pyarrow")
        sys.exit(1)

    arg = sys.argv[1] if len(sys.argv) > 1 else None
    if arg and os.path.isfile(arg):
        files, root = [arg], os.path.dirname(arg)
    else:
        root = arg or next((c for c in CANDIDATES if find_parquet_files(c)), None)
        if not root:
            print("CHẠY LỖI: không tìm thấy thư mục Parquet nào trong:")
            for c in CANDIDATES:
                print("   -", c)
            print("Chạy trước: make run-local F=labs/mini-project-1/exercises/a30_column_pruning.py")
            sys.exit(1)
        files = find_parquet_files(root)

    print("=" * 78)
    print(f"A33 — MỔ PARQUET  |  thư mục: {root}")
    print("=" * 78)
    total_bytes = sum(os.path.getsize(f) for f in files)
    print(f"Số file part-*.parquet : {len(files)}")
    print(f"Tổng dung lượng        : {human(total_bytes)} ({total_bytes:,} byte)")
    print(f"Trung bình mỗi file    : {human(total_bytes / max(len(files), 1))}")

    # Chọn file to nhất để mổ: file bé nhất có thể chỉ có 1-2 dòng -> thống kê vô nghĩa.
    target = max(files, key=os.path.getsize)
    pf = pq.ParquetFile(target)
    m = pf.metadata

    print("\n" + "-" * 78)
    print(f"FILE ĐEM MỔ (file to nhất): {target}")
    print("-" * 78)
    print(f"  num_rows        : {m.num_rows:,}")
    print(f"  num_columns     : {m.num_columns}")
    print(f"  num_row_groups  : {m.num_row_groups}")
    print(f"  created_by      : {m.created_by}")
    print(f"  serialized_size : {m.serialized_size:,} byte (footer — metadata, KHÔNG phải dữ liệu)")
    print(f"  file size       : {os.path.getsize(target):,} byte")
    print(f"  => footer chiếm {100 * m.serialized_size / os.path.getsize(target):.2f}% file. "
          "Nhớ con số này: nó là CHI PHÍ CỐ ĐỊNH mỗi file -> đó là lý do 10.000 file nhỏ")
    print("     tốn nhiều dung lượng hơn 600 file to dù cùng số dòng (bài A35).")

    # ---------------------------------------------------------------- cột
    # Cộng dồn qua MỌI row group -> ra chân dung của cả file, không phải rg[0].
    cols = {}
    for g in range(m.num_row_groups):
        rg = m.row_group(g)
        for i in range(rg.num_columns):
            c = rg.column(i)
            d = cols.setdefault(c.path_in_schema, {
                "comp": 0, "uncomp": 0, "compression": c.compression,
                "enc": set(), "min": None, "max": None, "nulls": 0,
            })
            d["comp"] += c.total_compressed_size
            d["uncomp"] += c.total_uncompressed_size
            d["enc"].update(c.encodings or ())
            st = c.statistics
            if st is not None:
                d["nulls"] += st.null_count or 0
                if st.has_min_max:
                    d["min"] = st.min if d["min"] is None else min(d["min"], st.min)
                    d["max"] = st.max if d["max"] is None else max(d["max"], st.max)

    tot_c = sum(d["comp"] for d in cols.values())
    tot_u = sum(d["uncomp"] for d in cols.values())

    print("\n" + "=" * 78)
    print("BẢNG A33.1 — GIẢI PHẪU TỪNG CỘT (dán vào report)")
    print("=" * 78)
    hdr = ["cột", "codec", "encoding", "nén (byte)", "thô (byte)", "tỉ lệ nén",
           "% file", "nulls", "min", "max"]
    print("| " + " | ".join(hdr) + " |")
    print("|" + "|".join("---" for _ in hdr) + "|")
    for name, d in sorted(cols.items(), key=lambda x: -x[1]["comp"]):
        ratio = d["uncomp"] / max(d["comp"], 1)
        enc = ",".join(sorted(str(e) for e in d["enc"]))
        mn = str(d["min"])[:19] if d["min"] is not None else "—"
        mx = str(d["max"])[:19] if d["max"] is not None else "—"
        print(f"| {name} | {d['compression']} | {enc} | {d['comp']:,} | {d['uncomp']:,} "
              f"| {ratio:.1f}× | {100 * d['comp'] / max(tot_c, 1):.1f}% | {d['nulls']:,} | {mn} | {mx} |")
    print(f"| **TỔNG** |  |  | {tot_c:,} | {tot_u:,} | {tot_u / max(tot_c, 1):.1f}× | 100% |  |  |  |")

    # ---------------------------------------------------------------- row group
    print("\n" + "=" * 78)
    print("BẢNG A33.2 — ROW GROUP")
    print("=" * 78)
    print("| # | rows | size nén | size thô |")
    print("|---|---|---|---|")
    for g in range(min(m.num_row_groups, 20)):
        rg = m.row_group(g)
        print(f"| {g} | {rg.num_rows:,} | {human(rg.total_byte_size)} | "
              f"{rg.total_byte_size:,} byte |")
    avg_rg = os.path.getsize(target) / max(m.num_row_groups, 1)

    # ---------------------------------------------------------------- 3 nhận xét
    print("\n" + "=" * 78)
    print("BA NHẬN XÉT ĐỀ BÀI YÊU CẦU (số do script tính, không phải cảm tính)")
    print("=" * 78)

    best = max(cols.items(), key=lambda x: x[1]["uncomp"] / max(x[1]["comp"], 1))
    worst = min(cols.items(), key=lambda x: x[1]["uncomp"] / max(x[1]["comp"], 1))
    print(f"(a) NÉN TỐT NHẤT: `{best[0]}` — {best[1]['uncomp'] / max(best[1]['comp'], 1):.1f}× "
          f"(encoding: {','.join(sorted(str(e) for e in best[1]['enc']))})")
    print(f"    NÉN TỆ NHẤT : `{worst[0]}` — {worst[1]['uncomp'] / max(worst[1]['comp'], 1):.1f}×")
    print("    VÌ SAO: cột ít giá trị phân biệt (order_status chỉ 8 giá trị) -> Parquet dùng")
    print("    DICTIONARY encoding: lưu 1 từ điển 8 chuỗi + mỗi dòng chỉ là 1 số nguyên nhỏ,")
    print("    rồi RLE/bit-pack số nguyên đó -> nén khủng khiếp. Ngược lại `order_id` là hash")
    print("    32 ký tự DUY NHẤT mỗi dòng -> từ điển to bằng dữ liệu -> Parquet bỏ dictionary,")
    print("    rơi về PLAIN -> gần như không nén được. Đây là lý do 'cột nào nén tốt' phụ thuộc")
    print("    CARDINALITY, không phụ thuộc kiểu dữ liệu.")

    print(f"\n(b) MIN/MAX vs TÊN THƯ MỤC PARTITION:")
    part = [p for p in target.split("/") if "=" in p]
    if part:
        print(f"    file nằm trong partition: {part}")
        # order_date là cột partition -> Parquet KHÔNG lưu nó trong file (nằm ở tên thư mục)
        if not any(k.startswith("order_date") for k in cols):
            print("    -> cột `order_date` KHÔNG xuất hiện trong bảng A33.1. ĐÚNG như lý thuyết:")
            print("       cột partition được 'nén' xuống thành TÊN THƯ MỤC, không lưu lặp lại")
            print("       trong từng dòng. Đó là một dạng nén miễn phí — và cũng là lý do")
            print("       partition pruning không cần đọc file nào cả (A36).")
        ts = [k for k in cols if "timestamp" in k or k.endswith("_at") or k.endswith("_date")]
        for k in ts[:3]:
            print(f"       min/max của `{k}` trong file: {cols[k]['min']} .. {cols[k]['max']}")
        print("       -> đối chiếu: chúng PHẢI nằm trong đúng ngày ghi trên tên thư mục.")
    else:
        print("    file này KHÔNG nằm trong thư mục partition (bảng ghi phẳng).")
        print("    Muốn kiểm ý (b), chạy lại script với thư mục có partition:")
        print("      ... a33_parquet_anatomy.py /workspace/data/bench/cp3/orders_parquet")

    print(f"\n(c) ROW GROUP: file có {m.num_row_groups} row group, "
          f"trung bình {human(avg_rg)}/row group.")
    if avg_rg < 100 * 2**20:
        print(f"    KHÔNG đạt 128MB/row group như lý thuyết. Vì sao? Vì cả FILE mới có "
              f"{human(os.path.getsize(target))} — không đủ dữ liệu để lấp đầy 1 row group.")
        print("    ĐIỀU ĐÓ NÓI GÌ: dataset Olist quá bé so với thang đo Parquet được thiết kế.")
        print("    Hệ quả trực tiếp: mọi cơ chế 'bỏ qua row group' (A32) gần như KHÔNG có đất")
        print("    diễn ở đây, và mọi con số 'giây' trong project này đều bị overhead cố định")
        print("    lấn át. Muốn thấy Parquet phát huy đúng sức -> dữ liệu ×100 (A40).")
    else:
        print("    Đạt xấp xỉ ngưỡng lý thuyết 128MB.")


if __name__ == "__main__":
    main()

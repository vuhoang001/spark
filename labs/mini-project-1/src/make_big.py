#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_big.py — SINH DỮ LIỆU ×100 cho bài A40.  ~10 TRIỆU đơn, ~1.7 GB CSV.

    python3 labs/mini-project-1/src/make_big.py                  # ×100, 20 file part
    python3 labs/mini-project-1/src/make_big.py --factor 10      # thử nhanh
    python3 labs/mini-project-1/src/make_big.py --factor 100 --parts 20

Đầu ra:  data/big/orders_100x/part-00000.csv ... part-00019.csv
         (THƯ MỤC, mỗi file CÓ header -> spark.read.option("header",True).csv(<thư mục>))

===========================================================================
VÌ SAO PYTHON THUẦN CHỨ KHÔNG PHẢI SPARK crossJoin?
===========================================================================
Đề gợi ý `spark.range(100).crossJoin(orders)`. Cách đó CHẠY ĐƯỢC, nhưng trên máy
này nó là một canh bạc: máy còn ~8 GB RAM, cluster chỉ có ~2.0 GB tổng cho dữ
liệu (2 executor × 1048 MB). crossJoin + ghi 1.7 GB CSV rất dễ spill, và nếu
lỡ có collect/broadcast nhầm chỗ thì OOM.

Script này ghi STREAMING: đọc 99.441 dòng gốc vào RAM MỘT LẦN (~17 MB, rẻ), rồi
lặp 100 vòng, mỗi vòng ghi thẳng xuống đĩa và KHÔNG giữ lại gì. Đỉnh RAM ~vài
trăm MB bất kể factor là 100 hay 1000.

=> Đây KHÔNG phải né bài học của đề. crossJoin vẫn là bài học (xem A40: nó tạo
   bao nhiêu task, wide transformation ra sao). Nhưng SINH dữ liệu và HỌC về
   crossJoin là hai việc khác nhau — trộn chúng vào nhau thì lúc job chết bạn
   không biết chết vì thiết kế pipeline hay vì cái máy sinh dữ liệu.

===========================================================================
LÀM NHIỄU THẾ NÀO CHO ĐÚNG
===========================================================================
1. order_id: gắn hậu tố replica  ->  10 triệu order_id KHÁC NHAU.
   Nếu không làm, distinct(order_id) = 99.441 và mọi phép dedup/join sẽ cho kết
   quả vô nghĩa — bạn sẽ "tối ưu" một pipeline đang xử lý dữ liệu giả.

2. Ngày: dịch NGẪU NHIÊN nhưng phải nằm TRONG CÙNG cửa sổ 2016-09-04..2018-10-17
   của Olist gốc. Vì sao quan trọng: silver partitionBy(order_date). Nếu ×100 mà
   giữ nguyên ngày, ta chỉ nhân mỗi partition-ngày lên 100 lần — vẫn 774 partition,
   mỗi cái to gấp 100. Nếu trải ra 100 năm, ta được 36.500 partition tí hon
   (small-file hell). Cả hai đều là bài toán GIẢ. Giữ nguyên cửa sổ ngày = giữ
   nguyên SỐ partition, chỉ tăng KÍCH THƯỚC mỗi partition -> đúng cái mà "×100
   lượng đơn hàng" nghĩa là trong đời thực.

3. Cả 5 cột timestamp của MỘT dòng dịch CÙNG một delta -> giữ nguyên thứ tự
   nhân quả (mua < duyệt < giao < nhận). Dịch mỗi cột một kiểu = sinh ra dữ liệu
   phi vật lý (đơn được giao trước khi đặt), và mọi kiểm tra chất lượng ở A38 sẽ
   báo đỏ vì lỗi của CHÍNH TA, không phải lỗi của pipeline.
"""

import argparse
import csv
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SRC_CSV = os.path.join(ROOT, "data", "olist", "olist_orders_dataset.csv")
BIG_DIR = os.path.join(ROOT, "data", "big", "orders_100x")

COLS = ["order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"]

TS_IDX = [3, 4, 5, 6, 7]           # 5 cột timestamp
FMT = "%Y-%m-%d %H:%M:%S"

# Cửa sổ thời gian THẬT của Olist (sẽ được đo lại từ dữ liệu, đây chỉ là mặc định)
WIN_LO = datetime(2016, 9, 4)
WIN_HI = datetime(2018, 10, 17)


def load_source(src):
    """Đọc file gốc 1 lần, parse sẵn timestamp. Trả về list các dòng đã chuẩn bị."""
    rows = []
    lo, hi = None, None
    with open(src, "r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        header = next(r)
        if header != COLS:
            print("[canh bao] header khac ky vong:\n  {}\n  {}".format(header, COLS),
                  file=sys.stderr)
        for rec in r:
            if len(rec) != 8:
                continue                       # file gốc sạch, nhưng thủ sẵn
            ts = []
            for i in TS_IDX:
                v = rec[i].strip()
                ts.append(datetime.strptime(v, FMT) if v else None)
            purchase = ts[0]
            if purchase is not None:
                if lo is None or purchase < lo:
                    lo = purchase
                if hi is None or purchase > hi:
                    hi = purchase
            rows.append((rec[0], rec[1], rec[2], ts))
    return rows, lo, hi


def build_shift_bounds(rows, lo, hi):
    """
    Với mỗi dòng, tính khoảng dịch (số ngày) hợp lệ để order_purchase_timestamp
    sau khi dịch VẪN nằm trong [lo, hi]. Tính TRƯỚC 1 lần -> vòng lặp 10 triệu
    dòng bên trong chỉ còn randint + cộng timedelta (rẻ).
    """
    # CẨN THẬN — timedelta.days LÀM TRÒN XUỐNG (về phía âm vô cực), không phải về 0.
    # Cận dưới cần làm tròn LÊN (ceil), cận trên cần làm tròn XUỐNG (floor).
    # Nếu dùng .days cho cả hai, cận dưới bị nới rộng thêm tối đa 1 ngày và dữ liệu
    # tràn ra NGOÀI cửa sổ (đã dính thật: min ra 2016-09-03 thay vì 2016-09-04,
    # đẻ ra 775 ngày phân biệt thay vì 774 -> thừa một partition-ngày tí hon).
    bounds = []
    for (_, _, _, ts) in rows:
        p = ts[0]
        if p is None:
            bounds.append((0, 0))
        else:
            blo = math.ceil((lo - p).total_seconds() / 86400.0)
            bhi = math.floor((hi - p).total_seconds() / 86400.0)
            if bhi < blo:
                blo = bhi = 0
            bounds.append((blo, bhi))
    return bounds


def generate(factor=100, parts=20, src=SRC_CSV, dst=BIG_DIR, seed=42):
    t0 = time.time()
    print("[make_big] đọc nguồn: {}".format(src))
    rows, lo, hi = load_source(src)
    n_src = len(rows)
    lo = lo or WIN_LO
    hi = hi or WIN_HI
    print("[make_big]   {:,} dòng gốc".format(n_src))
    print("[make_big]   cửa sổ ngày THẬT: {} .. {}  ({} ngày)".format(
        lo.date(), hi.date(), (hi - lo).days))

    bounds = build_shift_bounds(rows, lo, hi)

    if parts > factor:
        parts = factor                      # mỗi part ít nhất 1 replica trọn vẹn
    reps_per_part = (factor + parts - 1) // parts

    os.makedirs(dst, exist_ok=True)
    for old in os.listdir(dst):             # dọn lần chạy trước -> tránh cộng dồn
        if old.endswith(".csv"):
            os.remove(os.path.join(dst, old))

    rnd = random.Random(seed)
    total = 0
    files = 0
    day = timedelta(days=1)

    for pi in range(parts):
        r_lo = pi * reps_per_part
        r_hi = min(factor, r_lo + reps_per_part)
        if r_lo >= r_hi:
            break
        path = os.path.join(dst, "part-{:05d}.csv".format(pi))
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            w.writerow(COLS)                # MỖI file một header (Spark strip từng file)
            buf = []
            for rep in range(r_lo, r_hi):
                sfx = "{:03d}".format(rep)  # hậu tố replica -> order_id duy nhất
                for (oid, cid, status, ts), (blo, bhi) in zip(rows, bounds):
                    d = rnd.randint(blo, bhi) * day if bhi > blo else timedelta(0)
                    out = [oid + sfx, cid, status]
                    for t in ts:
                        out.append((t + d).isoformat(sep=" ") if t is not None else "")
                    buf.append(out)
                    if len(buf) >= 50_000:  # ghi theo lô -> không phình RAM
                        w.writerows(buf)
                        buf.clear()
            if buf:
                w.writerows(buf)
                buf.clear()
        files += 1
        total += (r_hi - r_lo) * n_src
        sz = os.path.getsize(path)
        print("[make_big]   {} : replica {:>3}..{:<3} | {:>9,} dòng | {:>6.1f} MB | {:.0f}s".format(
            os.path.basename(path), r_lo, r_hi - 1, (r_hi - r_lo) * n_src,
            sz / 1e6, time.time() - t0))

    el = time.time() - t0
    tot_sz = sum(os.path.getsize(os.path.join(dst, x)) for x in os.listdir(dst))
    print()
    print("[make_big] XONG trong {:.1f}s".format(el))
    print("[make_big]   thư mục   : {}".format(dst))
    print("[make_big]   số file   : {}".format(files))
    print("[make_big]   số dòng   : {:,} data ( = {:,} × {} ) + {} header".format(
        total, n_src, factor, files))
    print("[make_big]   dung lượng: {:,} bytes = {:.2f} GB".format(tot_sz, tot_sz / 2**30))
    return dst, total, tot_sz


def main():
    ap = argparse.ArgumentParser(description="Sinh data/big/orders_100x/ cho A40")
    ap.add_argument("--factor", type=int, default=100, help="hệ số nhân (mặc định 100)")
    ap.add_argument("--parts", type=int, default=20, help="số file CSV đầu ra (mặc định 20)")
    ap.add_argument("--out", default=BIG_DIR)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    if not os.path.exists(SRC_CSV):
        print("KHÔNG THẤY FILE GỐC: {}".format(SRC_CSV), file=sys.stderr)
        raise SystemExit(2)

    generate(factor=args.factor, parts=args.parts, dst=args.out, seed=args.seed)


if __name__ == "__main__":
    main()

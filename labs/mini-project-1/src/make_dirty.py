#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_dirty.py — SINH DỮ LIỆU BẨN cho bài A23.

    python3 labs/mini-project-1/src/make_dirty.py            # 2000 dòng (mặc định)
    python3 labs/mini-project-1/src/make_dirty.py --full     # toàn bộ 99.441 dòng
    python3 labs/mini-project-1/src/make_dirty.py --report labs/mini-project-1/results/a23_injected.md

Python THUẦN, không cần Spark, không cần container.

VÌ SAO CHỈ 2000 DÒNG?
  File bẩn không phải để đo hiệu năng — nó để bạn NHÌN THẤY dòng bẩn bằng mắt.
  2000 dòng thì `sed -n '101p' file.csv` ra ngay dòng bẩn số 1, đối chiếu được với
  bảng báo cáo. 99.441 dòng thì bạn chỉ tin lời script nói. Bằng chứng phải KIỂM
  TRA ĐƯỢC bằng tay, nếu không nó không phải bằng chứng.
  (Cần bản đầy đủ để test scale thì có --full.)

TRIẾT LÝ 6 LOẠI BẨN:
  Loại 1,2,5,6 = lỗi CẤU TRÚC  -> số field sai/parser vấp -> Spark BẮT ĐƯỢC
                                  (_corrupt_record hoặc NULL hàng loạt).
  Loại 3       = lỗi KIỂU      -> đúng 8 field, nhưng "hom qua" không phải timestamp
                                  -> với schema CÓ KIỂU: Spark bắt được.
                                  -> với schema TOÀN STRING: LỌT.
  Loại 4b      = lỗi NGỮ NGHĨA -> ĐÚNG 8 field, MỌI field đều là string hợp lệ,
                                  nhưng GIÁ TRỊ LỆCH SANG PHẢI một cột.
                                  -> KHÔNG có cách nào bắt được bằng schema.
                                  Đây là loại nguy hiểm nhất và là toàn bộ lý do
                                  tồn tại của A38 (data quality gate).
"""

import argparse
import csv
import io
import os
import sys

# repo root = .../spark-mastery  (file này ở labs/mini-project-1/src/)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

SRC_CSV = os.path.join(ROOT, "data", "olist", "olist_orders_dataset.csv")
DIRTY_DIR = os.path.join(ROOT, "data", "dirty")
DIRTY_CSV = os.path.join(DIRTY_DIR, "orders_dirty.csv")
REPORT_MD = os.path.join(ROOT, "labs", "mini-project-1", "results", "a23_injected.md")

DEFAULT_ROWS = 2000

# Header CHUẨN của olist_orders_dataset.csv — 8 cột.
HEADER = ('"order_id","customer_id","order_status","order_purchase_timestamp",'
          '"order_approved_at","order_delivered_carrier_date",'
          '"order_delivered_customer_date","order_estimated_delivery_date"')

N_COLS = 8

# ---------------------------------------------------------------------------
# SÁU LOẠI BẨN
# ---------------------------------------------------------------------------
# Mỗi phần tử: (mã loại, mô tả ngắn, dòng CSV THÔ, số token THẬT khi tách bằng dấu phẩy)
#   ntok = -1  -> parser tự quyết (dòng ngoặc kép lệch), không đếm cơ học được.
#
# order_id của mỗi dòng bẩn bắt đầu bằng "dirtyNN" -> tra ngược được trong Spark:
#   bronze.filter(F.col("order_id").contains("dirty04b"))
DIRTY_ROWS = [
    (
        "L1_thieu_cot",
        "Thiếu cột: xoá 2 field cuối (chỉ còn 6/8)",
        "dirty01missingcols,cust01,delivered,2018-07-02 10:00:00,"
        "2018-07-02 11:00:00,2018-07-03 09:00:00",
        6,
    ),
    (
        "L2_thua_cot",
        "Thừa cột: nhét 1 field lạ vào GIỮA (thành 9/8)",
        "dirty02extracols,cust02,delivered,FIELD_LA_TU_DAU_RA,2018-07-02 10:00:00,"
        "2018-07-02 11:00:00,2018-07-03 09:00:00,2018-07-10 00:00:00,"
        "2018-07-12 00:00:00",
        9,
    ),
    (
        "L3_sai_kieu",
        "Sai kiểu: order_purchase_timestamp = 'hom qua' (vẫn ĐÚNG 8 field)",
        "dirty03badtype,cust03,delivered,hom qua,2018-07-02 11:00:00,"
        "2018-07-03 09:00:00,2018-07-10 21:00:00,2018-07-12 00:00:00",
        8,
    ),
    (
        # 4a — phẩy trong text, KHÔNG bù trừ -> thừa token -> Spark thấy ngay.
        "L4a_phay_trong_text_thua_token",
        "Dấu phẩy trong text không có ngoặc kép, không bù trừ -> 9 token",
        "dirty04acomma,cust04a,Sao Paulo, SP,2018-07-02 10:00:00,"
        "2018-07-02 11:00:00,2018-07-03 09:00:00,2018-07-10 21:00:00,"
        "2018-07-12 00:00:00",
        9,
    ),
    (
        # 4b — LOẠI NGUY HIỂM NHẤT. Dấu phẩy tách 1 field thành 2, ĐỒNG THỜI thiếu
        # 1 field cuối -> tổng vẫn ĐÚNG 8 token. Không parser nào kêu ca.
        # Nhưng mọi giá trị từ cột 3 trở đi LỆCH SANG PHẢI một ô:
        #     order_status             <- "Sao Paulo"  (string hợp lệ -> IM LẶNG)
        #     order_purchase_timestamp <- " SP"        (typed schema -> NULL;
        #                                               string schema -> IM LẶNG)
        #     order_approved_at        <- ngày mua thật ... (lệch dây chuyền)
        # => Với bronze schema TOÀN STRING: 0 lỗi, 0 NULL, 0 _corrupt_record.
        #    Dữ liệu SAI đi thẳng vào kho và không ai biết. Mãi mãi.
        "L4b_phay_trong_text_lech_cot",
        "Dấu phẩy trong text + thiếu 1 field cuối -> ĐỦ 8 token nhưng LỆCH CỘT",
        "dirty04bcomma,cust04b,Sao Paulo, SP,2018-07-02 10:00:00,"
        "2018-07-02 11:00:00,2018-07-03 09:00:00,2018-07-10 21:00:00",
        8,
    ),
    (
        # Ngoặc kép mở mà không đóng. Mặc định multiLine=false -> univocity parse
        # từng dòng, dấu " lệch nuốt phần còn lại CỦA DÒNG ĐÓ.
        # Nếu bật multiLine=true, nó nuốt sang CÁC DÒNG SAU -> hỏng dây chuyền.
        # Đó là lý do multiLine=true không phải quyết định vô hại.
        "L5_ngoac_kep_lech",
        'Ngoặc kép mở " mà không đóng',
        'dirty05badquote,"cust05,delivered,2018-07-02 10:00:00,'
        "2018-07-02 11:00:00,2018-07-03 09:00:00,2018-07-10 21:00:00,"
        "2018-07-12 00:00:00",
        -1,
    ),
    (
        "L6a_dong_trong",
        "Dòng trống hoàn toàn",
        "",
        0,
    ),
    (
        "L6b_header_lap",
        "Header lặp lại ở GIỮA file (kinh điển khi `cat` nhiều file lại)",
        HEADER,
        8,
    ),
]

# Marker để tra ngược trong Spark (A23/A24 dùng lại).
MARKERS = ["dirty01", "dirty02", "dirty03", "dirty04a", "dirty04b", "dirty05"]


def _ntok(raw):
    """Số token khi tách THÔ bằng dấu phẩy (không hiểu ngoặc kép) — góc nhìn 'ngây thơ'."""
    return 0 if raw == "" else len(raw.split(","))


def _parsed_tokens(raw):
    """Số token theo CSV parser THẬT (hiểu ngoặc kép) — góc nhìn của Spark/univocity."""
    if raw == "":
        return 0
    try:
        return len(next(csv.reader(io.StringIO(raw))))
    except Exception:
        return -1


def generate(n_rows=DEFAULT_ROWS, src=SRC_CSV, dst=DIRTY_CSV):
    """
    Lấy n_rows dòng data đầu của file gốc, chèn 8 dòng bẩn RẢI ĐỀU, ghi ra dst.

    Chèn rải đều (không dồn một chỗ) là CỐ Ý: dồn một chỗ thì cả 8 dòng bẩn rơi
    vào cùng 1 partition -> không giống thực tế, và che mất chuyện _corrupt_record
    phân bố ra sao giữa các task.

    Trả về (dst, report) với report = list các tuple:
        (mã loại, mô tả, số dòng trong file bẩn (1-based, kể cả header),
         token thô, token theo parser, nguyên văn)
    """
    with open(src, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    header, data = lines[0], lines[1:]
    n_src = len(data)

    if n_rows is None or n_rows <= 0 or n_rows > n_src:
        n_rows = n_src
    data = data[:n_rows]

    # Rải đều: khoảng cách = n_rows / (số dòng bẩn + 1)
    step = max(1, len(data) // (len(DIRTY_ROWS) + 1))
    inject_at = {(i + 1) * step: row for i, row in enumerate(DIRTY_ROWS)}

    out = [header]
    report = []
    for idx, line in enumerate(data, start=1):
        out.append(line)
        if idx in inject_at:
            code, desc, raw, ntok_declared = inject_at[idx]
            out.append(raw)
            # len(out) = số dòng 1-based của dòng vừa chèn (vì out[0] là header)
            report.append((code, desc, len(out), ntok_declared,
                           _parsed_tokens(raw), raw))

    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")

    return dst, report, (len(data), len(out))


def render_report(report, counts, dst, as_markdown=True):
    n_data, n_total = counts
    L = []
    L.append("# A23 — Bảng dòng bẩn đã tiêm")
    L.append("")
    L.append("Sinh bởi `labs/mini-project-1/src/make_dirty.py` (python thuần).")
    L.append("")
    L.append("| file | giá trị |")
    L.append("|---|---|")
    L.append("| nguồn | `data/olist/olist_orders_dataset.csv` |")
    L.append("| đích | `{}` |".format(os.path.relpath(dst, ROOT)))
    L.append("| dòng data sạch | {:,} |".format(n_data))
    L.append("| dòng bẩn tiêm vào | {} |".format(len(report)))
    L.append("| TỔNG dòng file (kể cả header) | {:,} |".format(n_total))
    L.append("")
    L.append("Schema đúng = **8 cột**. Mọi con số khác 8 ở cột `token (thô)` là một cái bẫy "
             "*cấu trúc*. Cái bẫy THẬT SỰ là dòng có **đúng 8 token mà vẫn sai**: `L4b`.")
    L.append("")
    L.append("| # | dòng số | loại bẩn | token (thô) | token (parser) | nội dung |")
    L.append("|---|---|---|---|---|---|")
    for i, (code, desc, lineno, ntok, ptok, raw) in enumerate(report, start=1):
        shown = raw if raw else "*(dòng trống)*"
        if len(shown) > 72:
            shown = shown[:72] + "…"
        shown = shown.replace("|", "\\|")
        tok = "?" if ntok == -1 else str(ntok)
        pt = "?" if ptok == -1 else str(ptok)
        L.append("| {} | **{}** | `{}` — {} | {} | {} | `{}` |".format(
            i, lineno, code, desc, tok, pt, shown))
    L.append("")
    L.append("## Vì sao L4b là loại nguy hiểm nhất")
    L.append("")
    L.append("Dòng `dirty04bcomma` có **đúng 8 token** — bằng đúng số cột của schema. "
             "Không parser nào có cớ để kêu ca. Nhưng vì `Sao Paulo, SP` bị dấu phẩy "
             "xé làm hai *và* dòng thiếu mất field cuối, mọi giá trị từ cột 3 trở đi "
             "**lệch sang phải một ô**:")
    L.append("")
    L.append("| cột | giá trị ĐÚNG phải là | giá trị THẬT SỰ nhận được |")
    L.append("|---|---|---|")
    L.append("| `order_id` | dirty04bcomma | dirty04bcomma ✅ |")
    L.append("| `customer_id` | cust04b | cust04b ✅ |")
    L.append("| `order_status` | (một status hợp lệ) | `Sao Paulo` ❌ **string hợp lệ → IM LẶNG** |")
    L.append("| `order_purchase_timestamp` | 2018-07-02 10:00:00 | ` SP` ❌ |")
    L.append("| `order_approved_at` | 2018-07-02 11:00:00 | 2018-07-02 10:00:00 ❌ lệch |")
    L.append("| `order_estimated_delivery_date` | (ngày dự kiến) | 2018-07-10 21:00:00 ❌ lệch |")
    L.append("")
    L.append("**Hệ quả theo schema dùng để đọc:**")
    L.append("")
    L.append("| schema đọc | L4b bị bắt? | vì sao |")
    L.append("|---|---|---|")
    L.append("| Bronze — **toàn String** | ❌ **KHÔNG** | mọi token đều là string hợp lệ. "
             "Không `_corrupt_record`, không NULL, không exception. Dữ liệu sai đi thẳng vào kho. |")
    L.append("| Silver — **có kiểu** (Timestamp) | ⚠️ *một phần* | ` SP` không parse được → "
             "`order_purchase_timestamp` = NULL. Ta chỉ thấy **triệu chứng** (NULL), "
             "không thấy **bệnh** (lệch cột). `order_status = 'Sao Paulo'` vẫn lọt nguyên. |")
    L.append("")
    L.append("→ `_corrupt_record` bắt lỗi **cấu trúc**, không bắt lỗi **ngữ nghĩa**. "
             "Đó chính xác là lý do phải có **data quality gate (A38)**: một cái test "
             "`order_status IN (danh sách hợp lệ)` sẽ tóm được L4b trong một nốt nhạc, "
             "còn schema thì không bao giờ.")
    L.append("")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Sinh data/dirty/orders_dirty.csv cho A23")
    ap.add_argument("--rows", type=int, default=DEFAULT_ROWS,
                    help="số dòng data sạch lấy từ file gốc (mặc định 2000)")
    ap.add_argument("--full", action="store_true",
                    help="dùng TOÀN BỘ file gốc (99.441 dòng) thay vì --rows")
    ap.add_argument("--out", default=DIRTY_CSV, help="file CSV bẩn đầu ra")
    ap.add_argument("--report", default=REPORT_MD,
                    help="file markdown báo cáo dòng bẩn (đặt '' để tắt)")
    args = ap.parse_args()

    n_rows = None if args.full else args.rows

    if not os.path.exists(SRC_CSV):
        print("KHÔNG THẤY FILE GỐC: {}".format(SRC_CSV), file=sys.stderr)
        raise SystemExit(2)

    dst, report, counts = generate(n_rows=n_rows, dst=args.out)
    n_data, n_total = counts

    print("[make_dirty] nguồn : {}".format(SRC_CSV))
    print("[make_dirty] đích  : {}".format(dst))
    print("[make_dirty]   data sạch : {:,} dòng".format(n_data))
    print("[make_dirty]   dòng bẩn  : {}".format(len(report)))
    print("[make_dirty]   TỔNG      : {:,} dòng (kể cả header)".format(n_total))
    print("[make_dirty]   dung lượng: {:,} bytes".format(os.path.getsize(dst)))
    print()

    md = render_report(report, counts, dst)
    print(md)

    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            f.write(md)
        print("[make_dirty] đã ghi báo cáo: {}".format(args.report))


if __name__ == "__main__":
    main()

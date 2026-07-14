# A23 — Bảng dòng bẩn đã tiêm

Sinh bởi `labs/mini-project-1/src/make_dirty.py` (python thuần).

| file | giá trị |
|---|---|
| nguồn | `data/olist/olist_orders_dataset.csv` |
| đích | `data/dirty/orders_dirty.csv` |
| dòng data sạch | 2,000 |
| dòng bẩn tiêm vào | 8 |
| TỔNG dòng file (kể cả header) | 2,009 |

Schema đúng = **8 cột**. Mọi con số khác 8 ở cột `token (thô)` là một cái bẫy *cấu trúc*. Cái bẫy THẬT SỰ là dòng có **đúng 8 token mà vẫn sai**: `L4b`.

| # | dòng số | loại bẩn | token (thô) | token (parser) | nội dung |
|---|---|---|---|---|---|
| 1 | **224** | `L1_thieu_cot` — Thiếu cột: xoá 2 field cuối (chỉ còn 6/8) | 6 | 6 | `dirty01missingcols,cust01,delivered,2018-07-02 10:00:00,2018-07-02 11:00…` |
| 2 | **447** | `L2_thua_cot` — Thừa cột: nhét 1 field lạ vào GIỮA (thành 9/8) | 9 | 9 | `dirty02extracols,cust02,delivered,FIELD_LA_TU_DAU_RA,2018-07-02 10:00:00…` |
| 3 | **670** | `L3_sai_kieu` — Sai kiểu: order_purchase_timestamp = 'hom qua' (vẫn ĐÚNG 8 field) | 8 | 8 | `dirty03badtype,cust03,delivered,hom qua,2018-07-02 11:00:00,2018-07-03 0…` |
| 4 | **893** | `L4a_phay_trong_text_thua_token` — Dấu phẩy trong text không có ngoặc kép, không bù trừ -> 9 token | 9 | 9 | `dirty04acomma,cust04a,Sao Paulo, SP,2018-07-02 10:00:00,2018-07-02 11:00…` |
| 5 | **1116** | `L4b_phay_trong_text_lech_cot` — Dấu phẩy trong text + thiếu 1 field cuối -> ĐỦ 8 token nhưng LỆCH CỘT | 8 | 8 | `dirty04bcomma,cust04b,Sao Paulo, SP,2018-07-02 10:00:00,2018-07-02 11:00…` |
| 6 | **1339** | `L5_ngoac_kep_lech` — Ngoặc kép mở " mà không đóng | ? | 2 | `dirty05badquote,"cust05,delivered,2018-07-02 10:00:00,2018-07-02 11:00:0…` |
| 7 | **1562** | `L6a_dong_trong` — Dòng trống hoàn toàn | 0 | 0 | `*(dòng trống)*` |
| 8 | **1785** | `L6b_header_lap` — Header lặp lại ở GIỮA file (kinh điển khi `cat` nhiều file lại) | 8 | 8 | `"order_id","customer_id","order_status","order_purchase_timestamp","orde…` |

## Vì sao L4b là loại nguy hiểm nhất

Dòng `dirty04bcomma` có **đúng 8 token** — bằng đúng số cột của schema. Không parser nào có cớ để kêu ca. Nhưng vì `Sao Paulo, SP` bị dấu phẩy xé làm hai *và* dòng thiếu mất field cuối, mọi giá trị từ cột 3 trở đi **lệch sang phải một ô**:

| cột | giá trị ĐÚNG phải là | giá trị THẬT SỰ nhận được |
|---|---|---|
| `order_id` | dirty04bcomma | dirty04bcomma ✅ |
| `customer_id` | cust04b | cust04b ✅ |
| `order_status` | (một status hợp lệ) | `Sao Paulo` ❌ **string hợp lệ → IM LẶNG** |
| `order_purchase_timestamp` | 2018-07-02 10:00:00 | ` SP` ❌ |
| `order_approved_at` | 2018-07-02 11:00:00 | 2018-07-02 10:00:00 ❌ lệch |
| `order_estimated_delivery_date` | (ngày dự kiến) | 2018-07-10 21:00:00 ❌ lệch |

**Hệ quả theo schema dùng để đọc:**

| schema đọc | L4b bị bắt? | vì sao |
|---|---|---|
| Bronze — **toàn String** | ❌ **KHÔNG** | mọi token đều là string hợp lệ. Không `_corrupt_record`, không NULL, không exception. Dữ liệu sai đi thẳng vào kho. |
| Silver — **có kiểu** (Timestamp) | ⚠️ *một phần* | ` SP` không parse được → `order_purchase_timestamp` = NULL. Ta chỉ thấy **triệu chứng** (NULL), không thấy **bệnh** (lệch cột). `order_status = 'Sao Paulo'` vẫn lọt nguyên. |

→ `_corrupt_record` bắt lỗi **cấu trúc**, không bắt lỗi **ngữ nghĩa**. Đó chính xác là lý do phải có **data quality gate (A38)**: một cái test `order_status IN (danh sách hợp lệ)` sẽ tóm được L4b trong một nốt nhạc, còn schema thì không bao giờ.

# Lesson 37 — Deployment: client vs cluster mode, standalone/YARN/Kubernetes

> Module 6 · Production Engineering · Tuần 20 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Giải phẫu `spark-submit`**: mọi flag quan trọng bạn sẽ gõ trong 10 năm tới — `--master`, `--deploy-mode`, `--conf`, `--packages`, `--py-files`, `--files`.
- **Client mode vs cluster mode**: driver nằm Ở ĐÂU trong từng mode, và tại sao câu hỏi này quyết định job của bạn sống hay chết lúc 3 giờ sáng.
- **Standalone vs YARN vs Kubernetes**: bảng so sánh chi tiết, khi nào chọn cái nào.
- **Spark on Kubernetes**: driver pod → executor pods, spark-operator, docker image, service account.
- **Dynamic allocation** + shuffle tracking trên K8s.
- **Config precedence**: code < spark-submit < spark-defaults.conf... sai thứ tự này là debug cả buổi.
- **Quản lý dependency Python**: venv / pex / docker image — nỗi đau riêng của PySpark.

Sau bài này bạn phải làm được:

- Đọc một lệnh `spark-submit` dài 15 dòng của công ty và giải thích từng flag.
- Vẽ diagram vị trí driver trong client mode và cluster mode, trên cả YARN lẫn K8s.
- Trả lời: "Spark UI của job này đang ở đâu?" trong mọi mode — kể cả khi job đã chết.
- Submit job lên cluster standalone Docker của repo này bằng cả hai đường (`make run` và tay trần).

Kiến thức dùng trong thực tế: **mỗi lần deploy**. Từ lesson 1 đến 36 bạn viết code; từ hôm nay code đó phải RỜI KHỎI máy bạn và sống một mình trong cluster. 80% sự cố production đầu đời của DE là deploy sai mode, thiếu dependency, hoặc config bị đè mà không biết.

---

## 2. Why

### Câu chuyện quen thuộc

Job của bạn chạy ngon trên laptop bằng `local[4]`. Sếp bảo: "đưa lên production, chạy 2h sáng mỗi ngày". Bạn SSH vào một máy trong cluster, gõ `spark-submit app.py`, thấy chạy, tắt terminal đi ngủ. Sáng dậy: **job chết ngay lúc bạn đóng SSH**. Vì sao? Vì bạn chạy **client mode** — driver sống trong terminal session của bạn, terminal chết là driver chết, driver chết là application chết (lesson 1!).

Deployment là tập hợp các câu hỏi mà `local[*]` đã che giấu suốt 36 bài:

1. Driver chạy ở đâu? Ai nuôi nó khi bạn ngủ?
2. Ai cấp executor? Standalone, YARN hay Kubernetes?
3. Code + thư viện Python của bạn làm sao đến được TỪNG executor?
4. Config lấy từ đâu khi có 3 nơi cùng khai một key?
5. Job chết lúc 3h sáng — Spark UI (port 4040 của driver) cũng chết theo, xem lại bằng gì?

### Nếu không học bài này thì sao?

- Deploy client mode từ máy cá nhân → job chết theo laptop/VPN.
- Quên `--py-files` → `ModuleNotFoundError` **trên executor** (còn local thì chạy ngon — vì local mọi thứ chung một máy).
- Hardcode `master("local[4]")` trong code → job "chạy thành công" trên production nhưng thực ra chạy 1 máy, chậm gấp 50 lần, không ai nhận ra suốt 3 tháng (chuyện thật, rất phổ biến).
- Không biết config precedence → sửa `spark-defaults.conf` mãi mà job không đổi hành vi, vì DevOps đã set `--conf` trong script submit đè lên.

### Trade-off tổng quát của các lựa chọn deploy

| Được | Mất |
|---|---|
| Cluster mode: driver sống độc lập, production-grade | Khó debug hơn (log ở xa, không print ra terminal) |
| Client mode: debug tương tác, thấy output ngay | Driver chết theo máy submit; network driver↔executor xa |
| K8s: chuẩn hiện đại, isolation tốt, docker image | Vận hành phức tạp hơn, phải hiểu pod/service account |
| YARN: trưởng thành, ecosystem Hadoop | Gắn chặt Hadoop, đang thoái trào ở công ty mới |

> Bài học Senior: **deploy mode không phải chi tiết kỹ thuật, nó là quyết định về "ai chịu trách nhiệm nuôi driver"**. Notebook/debug → client. Scheduled production job → cluster. Không có ngoại lệ đáng nhớ nào.

---

## 3. Theory

### 3.1. Giải phẫu spark-submit — lệnh quan trọng nhất sự nghiệp

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \      # ① NƠI xin tài nguyên (cluster manager)
  --deploy-mode client \                     # ② driver chạy Ở ĐÂU
  --name daily-revenue \                     # tên hiện trên UI
  --conf spark.executor.memory=4g \          # ③ config lẻ, key=value, lặp lại được
  --conf spark.executor.cores=2 \
  --conf spark.sql.shuffle.partitions=200 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \  # ④ jar từ Maven
  --py-files deps.zip,utils.py \             # ⑤ code Python gửi kèm cho executor
  --files config.prod.yaml \                 # ⑥ file dữ liệu/config gửi kèm
  /workspace/jobs/daily_revenue.py \         # ⑦ application chính
  --run-date 2026-07-08                      # ⑧ argument CỦA APP (sau file là của bạn)
```

Bảng flag phải thuộc lòng:

| Flag | Ý nghĩa | Ghi chú sống còn |
|---|---|---|
| `--master` | Cluster manager: `local[N]`, `spark://host:7077`, `yarn`, `k8s://https://...` | KHÔNG hardcode trong code |
| `--deploy-mode` | `client` (mặc định) hoặc `cluster` | Quyết định vị trí driver |
| `--conf key=value` | Mọi config Spark | Lặp nhiều lần được; đè spark-defaults.conf |
| `--name` | Tên app trên UI/history | Đặt tên có ý nghĩa, đừng để "Simple App" |
| `--packages` | Maven coordinates, tự tải jar + dependency | Cần internet lúc submit; production nên bake vào image |
| `--jars` | Jar local, phân phát cho driver + executor | Khi jar không có trên Maven |
| `--py-files` | `.py`, `.zip`, `.egg` — thêm vào `PYTHONPATH` của executor | KHÔNG mang được thư viện C (numpy, pandas)! |
| `--files` | File bất kỳ → thư mục làm việc của executor | Đọc bằng tên file trần, hoặc `SparkFiles.get()` |
| `--driver-memory` / `--executor-memory` | RAM heap | Lesson 38 tính toán chi tiết |
| `--executor-cores` / `--num-executors` | Core mỗi executor / số executor (YARN) | Standalone dùng `spark.cores.max` thay num-executors |

> **Analogy chuyển nhà**: `spark-submit` là hợp đồng với công ty vận chuyển. `--master` là chọn hãng xe (Standalone/YARN/K8s). `--deploy-mode` là bạn đi cùng xe (client) hay giao chìa khóa cho tài xế (cluster). `--py-files`/`--files` là thùng đồ gửi kèm — quên thùng nào là đến nơi thiếu đồ nấy, và bạn chỉ phát hiện khi mở thùng (runtime, trên executor).

### 3.2. Client mode vs Cluster mode — driver nằm ở đâu?

```
CLIENT MODE                                CLUSTER MODE
(driver ở máy submit)                      (driver ở trong cluster)

┌─ máy bạn / edge node ────┐               ┌─ máy bạn / edge node ────┐
│  spark-submit            │               │  spark-submit            │
│    └─ DRIVER (main())    │               │    └─ chỉ là NGƯỜI ĐƯA   │
│       Spark UI :4040     │               │       THƯ — gửi app rồi  │
└──────────┬───────────────┘               │       có thể TẮT MÁY     │
           │ điều phối task                └──────────┬───────────────┘
           │ qua network ▼                            ▼
┌─ CLUSTER ────────────────┐               ┌─ CLUSTER ────────────────┐
│ [executor] [executor]    │               │  [DRIVER] ← sống ở đây   │
│ [executor]               │               │   Spark UI :4040 ở đây   │
└──────────────────────────┘               │  [executor] [executor]   │
                                           └──────────────────────────┘
laptop sleep/VPN rớt → CHẾT CẢ APP         máy submit tắt → app vẫn chạy
```

| | Client mode | Cluster mode |
|---|---|---|
| Driver ở đâu | Máy chạy `spark-submit` | Một node/pod trong cluster |
| `print()`/log driver | Ra thẳng terminal | Nằm trong log của cluster (yarn logs / kubectl logs) |
| Máy submit chết | **App chết** | App vẫn sống |
| Network driver↔executor | Xa (qua mạng văn phòng/VPN) → chậm khi `collect`, broadcast | Gần (cùng datacenter) |
| Dùng khi | Notebook, pyspark shell, debug, dev | **Mọi production job** (Airflow, cron, operator) |
| Lưu ý đặc biệt | Mặc định của spark-submit | **Standalone KHÔNG hỗ trợ cluster mode cho Python** — sẽ gặp trong lab! |

Quy tắc quyết định (trả lời interview 10 giây): *"Ai cần tương tác với driver? Người → client. Máy → cluster."*

### 3.3. Standalone vs YARN vs Kubernetes — chọn hãng xe nào

| Tiêu chí | Standalone | YARN | Kubernetes |
|---|---|---|---|
| Là gì | Cluster manager có sẵn trong Spark | Resource manager của Hadoop | Container orchestrator tổng quát |
| Cài đặt | Dễ nhất — start-master.sh + start-worker.sh | Cần cả hệ Hadoop | Cần cluster K8s (thường có sẵn ở công ty) |
| Resource isolation | Yếu — executor các app chung máy, không cgroup | Container YARN (cgroup) — khá | **Mạnh nhất** — pod, namespace, quota, limit |
| Multi-tenancy / queue | Gần như không | Queue, fair/capacity scheduler — trưởng thành | Namespace + ResourceQuota + priority class |
| Dynamic allocation | Có (cần external shuffle service) | Có, chuẩn chỉnh nhất (NodeManager chạy shuffle service) | Có từ Spark 3.0 qua **shuffle tracking** (không cần external shuffle service) |
| Dependency | Cài lib trên từng node (đau khổ) | Cài trên node hoặc ship archive | **Docker image** — nghiệm đúng một lần, chạy mọi nơi |
| Ecosystem | Học tập, cluster nhỏ | Thế giới Hadoop/HDFS/Hive on-prem | Thế giới cloud-native; xu hướng chính hiện nay |
| Ai đang dùng | Repo này (để học!), team nhỏ | Ngân hàng, telco, hệ on-prem lâu đời | Công ty mới, cloud, EMR on EKS, mọi nền tảng hiện đại |

Phiên dịch sang lời khuyên:
- **Học / PoC / cluster 3 máy**: Standalone — như repo này đang chạy.
- **Công ty đã có Hadoop**: YARN — đừng cãi nhau với hạ tầng có sẵn.
- **Xây mới 2026**: Kubernetes — isolation tốt, dependency bằng image, đội infra đã biết vận hành K8s.

### 3.4. Spark on Kubernetes — nhìn gần

```
spark-submit --master k8s://https://<api-server>:6443 \
             --deploy-mode cluster ...
        │
        ▼ ① gọi K8s API tạo DRIVER POD
┌─ Kubernetes cluster ───────────────────────────────────┐
│                                                        │
│   ┌────────────────┐   ② driver (dùng SERVICE ACCOUNT │
│   │  DRIVER POD    │      có quyền tạo pod) tự gọi     │
│   │  image: my-app │      K8s API xin executor          │
│   └───────┬────────┘                                    │
│           ▼ ③ K8s scheduler đặt executor pod lên node   │
│   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐    │
│   │ EXECUTOR POD │ │ EXECUTOR POD │ │ EXECUTOR POD │    │
│   └──────────────┘ └──────────────┘ └──────────────┘    │
│   ④ app xong → executor pod bị xóa, driver pod ở lại    │
│     trạng thái Completed (giữ log để xem lại)           │
└────────────────────────────────────────────────────────┘
```

4 mảnh ghép phải chuẩn bị:
1. **Image**: chứa Spark + Python + THƯ VIỆN của bạn (numpy, pandas...). Đây là cách quản lý dependency sạch nhất — build một lần, driver và executor giống nhau tuyệt đối.
2. **Service account**: driver pod cần quyền `create/delete pods` trong namespace — `--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark`.
3. **Namespace + resource quota**: ranh giới giữa team này và team khác.
4. **spark-operator** (Kubeflow/Google): thay vì gõ spark-submit, bạn viết **YAML `SparkApplication`** và `kubectl apply`. Operator lo submit, restart policy, TTL, expose UI. Đây là cách phổ biến nhất chạy Spark on K8s production — hợp gu GitOps (YAML nằm trong repo, review được).

### 3.5. Dynamic allocation — thuê xe theo giờ

Tĩnh: xin 20 executor từ đầu đến cuối — giai đoạn đọc dữ liệu cần cả 20, giai đoạn ghi kết quả chỉ cần 2, còn 18 con ngồi chơi vẫn tính tiền.

```
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2       # sàn
spark.dynamicAllocation.maxExecutors=20      # trần
spark.dynamicAllocation.executorIdleTimeout=60s   # rảnh 60s → trả máy
# Trên K8s (Spark 3+), thay external shuffle service bằng:
spark.dynamicAllocation.shuffleTracking.enabled=true
```

Vấn đề cốt lõi: executor bị thu hồi mà đang giữ **shuffle file** thì stage sau đọc từ đâu? YARN giải bằng external shuffle service (NodeManager giữ file hộ). K8s giải bằng **shuffle tracking** — Spark theo dõi executor nào còn giữ shuffle data thì KHÔNG thu hồi cho tới khi data hết được cần đến (trade-off: co lại chậm hơn).

### 3.6. Config precedence — ai đè ai

```
độ ưu tiên TĂNG dần →

spark-defaults.conf   <   spark-submit --conf   <   SparkConf trong CODE
(mặc định của cluster,    (per-job, do người         (thắng tất cả — vì thế
 admin quản lý)            submit/Airflow set)        ĐỪNG hardcode trong code!)
```

Nghịch lý phải khắc cốt: code thắng, **nên production không được set config trong code**. Vì config trong code thì DevOps/Airflow không đè nổi — muốn đổi memory phải sửa code, review, release. Chuẩn: code chỉ `SparkSession.builder.getOrCreate()` trơn, mọi config đẩy ra tầng submit. Ngoại lệ chấp nhận được: config gắn liền logic (như `spark.sql.session.timeZone` mà logic phụ thuộc).

### 3.7. Dependency Python — nỗi đau riêng của PySpark

`--py-files` chỉ ship được **pure Python**. Numpy/pandas/pyarrow có phần biên dịch C → 3 con đường:

| Cách | Cơ chế | Khi dùng |
|---|---|---|
| Cài sẵn trên node | admin pip install trên mọi worker | Standalone/YARN nhỏ; dễ trôi version giữa các node |
| **venv/conda đóng gói** (`venv-pack`/`conda-pack`) | nén cả môi trường thành `.tar.gz`, ship bằng `--archives env.tar.gz#env` + `PYSPARK_PYTHON=./env/bin/python` | YARN; không cần đụng vào node |
| **PEX** | đóng cả deps thành 1 file thực thi | Tương tự, một file duy nhất |
| **Docker image** | bake tất cả vào image | K8s — cách sạch nhất, khuyến nghị 2026 |

### 3.8. Spark UI ở đâu trong từng mode?

| Tình huống | UI sống (job đang chạy) | UI sau khi job chết |
|---|---|---|
| local / client mode | `:4040` trên máy submit | History Server (nếu bật event log — lesson 39) |
| cluster mode YARN | link "ApplicationMaster" trong YARN RM UI (proxy tới 4040 của driver) | History Server |
| cluster mode K8s | port-forward driver pod: `kubectl port-forward <driver-pod> 4040:4040` | History Server (event log ghi ra S3/PVC) |
| Standalone master UI | `:8080` — thấy worker, app đang chạy, app đã xong (link log) | vẫn `:8080` + History Server |

Ghi nhớ: **4040 là của DRIVER** — driver ở đâu, 4040 ở đó, driver chết là 4040 chết. Muốn xem "hậu sự" phải có event log + history server — đó là lesson 39.

---

## 4. Internal

Chuyện gì xảy ra khi submit **cluster mode trên YARN** (so sánh với chuỗi lesson 1):

```
① spark-submit --master yarn --deploy-mode cluster app.py
        │  (máy submit chỉ upload resource + gửi yêu cầu tới ResourceManager)
② YARN ResourceManager chọn 1 NodeManager, cấp container đầu tiên
        │
③ Trong container đó, ApplicationMaster khởi động = chính là DRIVER
        │  (máy submit lúc này đã có thể tắt — vai trò xong)
④ Driver xin RM thêm container cho executor
⑤ Executor khởi động trong các container, đăng ký ngược về driver
⑥ Từ đây giống hệt lesson 1: DAG → stage → task → kết quả
⑦ App xong → AM báo RM → container được thu hồi,
   event log (nếu bật) đã nằm lại trên HDFS/S3 cho History Server
```

Trên **K8s cluster mode**: thay "RM cấp container" bằng "K8s API tạo pod"; driver pod dùng service account tự tạo executor pod; app xong thì executor pod bị xóa, driver pod chuyển `Completed` (giữ được `kubectl logs`).

Còn **client mode**: bước ②–③ không xảy ra — driver đã chạy sẵn tại máy submit ngay khi lệnh được gõ, chỉ có executor được cấp trong cluster. Đó là lý do client mode nhìn "nhanh khởi động" hơn, và cũng là lý do nó mong manh.

Chi tiết nhỏ mà interviewer thích: trong YARN client mode vẫn có một ApplicationMaster "gầy" trong cluster — nhưng nó chỉ làm nhiệm vụ xin container hộ, KHÔNG chứa driver.

---

## 5. API

### `spark-submit` (điểm lại phần 3.1 — đây là API chính của bài)

- **Pitfall**: argument sau file `.py` là của app bạn, TRƯỚC file là của Spark. Đặt `--conf` sau tên file → Spark không đọc, app nhận được arg lạ, cả hai đều im lặng sai.

### `SparkSession.builder` — viết kiểu production

```python
from pyspark.sql import SparkSession

# ĐÚNG chuẩn production: không master, không memory config trong code
spark = (SparkSession.builder
         .appName("daily-revenue")     # appName được phép — nhưng --name vẫn đè được nó? KHÔNG:
         .getOrCreate())               # code thắng — thêm lý do để code càng trơn càng tốt
```

- **Pitfall**: `getOrCreate()` nghĩa là nếu session đã tồn tại (shell, notebook), config bạn builder thêm **bị bỏ qua im lặng**. Đừng ngạc nhiên khi config "không ăn" trong pyspark shell.

### `SparkFiles` — đọc file gửi bằng `--files`

```python
from pyspark import SparkFiles
path = SparkFiles.get("config.prod.yaml")   # đường dẫn thật trên node hiện tại
```

- **Khi dùng**: file config, file lookup nhỏ cần cho cả driver lẫn executor.
- **Pitfall**: `--files` KHÔNG dành cho dữ liệu — dữ liệu đọc bằng `spark.read` từ storage chung.

### `--archives` + PYSPARK_PYTHON — ship nguyên môi trường Python

```bash
venv-pack -o env.tar.gz          # nén venv hiện tại
spark-submit \
  --archives env.tar.gz#env \
  --conf spark.pyspark.python=./env/bin/python \
  app.py
```

- **Ý nghĩa**: `#env` = giải nén thành thư mục tên `env` trong working dir của executor.
- **Pitfall**: venv phải được pack trên **cùng OS/arch** với node cluster (pack trên Mac ARM, chạy trên Linux x86 → nổ).

---

## 6. Demo nhỏ

Nhìn config precedence bằng mắt thường, ngay trên cluster Docker của repo:

```
Input:  1 file py in ra giá trị config
   ↓    khai config ở 2 tầng khác nhau (submit vs code)
Output: tầng nào thắng?
```

```python
# labs/lab37/precedence.py
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("demo-precedence")
         .config("spark.sql.shuffle.partitions", "8")   # tầng CODE
         .getOrCreate())

print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))
print("app.name           =", spark.conf.get("spark.app.name"))
spark.stop()
```

```bash
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=200 \
  --name ten-tu-submit \
  /workspace/labs/lab37/precedence.py
# → shuffle.partitions = 8      (CODE thắng --conf)
# → app.name           = demo-precedence   (appName trong code thắng --name)
```

Kết luận rút ra ngay: config trong code là "vua" — và vì thế production code phải là ông vua **không ra lệnh gì cả**.

---

## 7. Production Example

Pipeline thật của một công ty chạy Spark on K8s bằng spark-operator — file YAML nằm trong Git, deploy bằng ArgoCD:

```yaml
# spark-app.yaml — kubectl apply -f là job chạy
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: daily-revenue
  namespace: data-jobs
spec:
  type: Python
  mode: cluster
  image: registry.cty.vn/de/spark-jobs:3.4.1-v42   # code + deps bake sẵn
  mainApplicationFile: local:///app/jobs/daily_revenue.py
  sparkVersion: 3.4.1
  driver:
    cores: 1
    memory: 2g
    serviceAccount: spark          # quyền tạo executor pod
  executor:
    instances: 10
    cores: 4
    memory: 8g
  sparkConf:
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: s3a://logs/spark-events   # cho History Server (lesson 39)
    spark.dynamicAllocation.enabled: "true"
    spark.dynamicAllocation.shuffleTracking.enabled: "true"
    spark.dynamicAllocation.maxExecutors: "20"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 2
```

Vì sao doanh nghiệp xếp hình như vậy:
1. **Image có version** (`-v42`): rollback = đổi 1 dòng YAML. Dependency Python không bao giờ lệch giữa driver/executor.
2. **cluster mode + operator**: không có "máy của ai đó" trong đường găng; operator restart hộ khi fail.
3. **Event log ra S3**: UI sống chết gì cũng còn hồ sơ — nguyên liệu của lesson 39.
4. **YAML trong Git**: đổi memory là một pull request có reviewer, không phải ai đó SSH sửa tay.

Còn với Airflow (rất phổ biến): `SparkSubmitOperator` bản chất chỉ là máy gõ `spark-submit` hộ bạn — mọi flag bạn học hôm nay map 1-1 vào tham số của operator (`conf=`, `py_files=`, `files=`, `deploy_mode=`).

---

## 8. Hands-on Lab

**Mục tiêu**: sờ tận tay client mode, chứng kiến giới hạn cluster mode của standalone, dùng `--files`/`--py-files`, và biết đích xác UI nào ở đâu.

### Bước 0 — bật cluster, tạo lab

```bash
make up          # spark-master :8080/:7077, 1 worker (1 core/1G), container spark-submit
mkdir -p labs/lab37
```

### Bước 1 — viết app có dependency riêng

```python
# labs/lab37/helpers.py  — module "thư viện nội bộ" của bạn
def vnd(x: float) -> str:
    return f"{x:,.0f} VND"
```

```python
# labs/lab37/submit_anatomy.py
import sys
from pyspark import SparkFiles
from pyspark.sql import SparkSession, functions as F
from helpers import vnd                      # đến từ --py-files!

spark = SparkSession.builder.appName("lab37").getOrCreate()
print(">>> master     =", spark.conf.get("spark.master"))
print(">>> deployMode =", spark.conf.get("spark.submit.deployMode"))
print(">>> config file gửi kèm:", open(SparkFiles.get("greeting.txt")).read().strip())

df = spark.createDataFrame([("HN", 120.0), ("SG", 480.0), ("HN", 300.0)], ["city", "amount"])
total = df.agg(F.sum("amount")).first()[0]
print(">>> Tổng doanh thu:", vnd(total * 25000))
input(">>> Đang giữ driver sống. Mở http://localhost:4040 và http://localhost:8080 rồi Enter...")
spark.stop()
```

```bash
echo "Xin chao tu --files" > labs/lab37/greeting.txt
```

### Bước 2 — submit CLIENT mode lên cluster (chính là điều `make run` vẫn làm)

```bash
docker exec -it spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --py-files /workspace/labs/lab37/helpers.py \
  --files /workspace/labs/lab37/greeting.txt \
  /workspace/labs/lab37/submit_anatomy.py
```

Trong lúc script dừng ở `input()`: mở **:4040** (UI của driver — driver đang ở container spark-submit, nên port 4040 của container đó được map ra) và **:8080** (master UI — thấy app `lab37` RUNNING, worker nào cấp executor).

### Bước 3 — thử CLUSTER mode và... học từ thất bại

```bash
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  /workspace/labs/lab37/submit_anatomy.py
# → Error: Cluster deploy mode is currently not supported for python
#   applications on standalone clusters.
```

Đây KHÔNG phải lab hỏng — đây là kiến thức: **standalone không chạy được cluster mode cho Python**. Muốn cluster mode với PySpark → YARN hoặc K8s. Ghi câu này vào NOTES.md; nó là câu hỏi phỏng vấn thật.

### Bước 4 — chứng minh "client mode chết theo máy submit"

Chạy lại bước 2, và trong lúc app đang treo ở `input()`, mở terminal khác:

```bash
docker restart spark-mastery-spark-submit-1   # "tắt laptop"
```

Mở :8080 → app chuyển FINISHED/FAILED ngay: driver chết kéo cả app chết. Nếu đây là YARN/K8s cluster mode thì restart máy submit không hề hấn gì.

### Bước 5 — quan sát, ghi NOTES.md

1. Trên :8080, app đã chết có link vào log executor — đọc thử stdout của executor.
2. Bỏ `--py-files` chạy lại → đọc kỹ `ModuleNotFoundError` xảy ra ở đâu (driver hay executor? tại sao lần này ở driver? gợi ý: import ở top-level chạy trong driver trước).
3. `make run F=labs/lab37/submit_anatomy.py` fail vì thiếu `--py-files` — hiểu vì sao Makefile hiện tại chưa đủ cho app nhiều file.

---

## 9. Assignment

**Easy** — (bám ROADMAP) Submit job local mode (`make run-local`) vs client mode lên cluster (`make run`): port 4040 mỗi trường hợp do process nào phục vụ, nằm trong container nào? App nào xuất hiện trên :8080, app nào không, vì sao? Trả lời bằng chữ của bạn kèm screenshot 2 UI.

**Medium** — (bám ROADMAP) Quyết định mode cho 3 ngữ cảnh, mỗi cái 3–5 dòng lập luận: (a) dev đang viết job mới trên laptop, dữ liệu mẫu 200 MB; (b) staging của công ty đang dùng Hadoop, job chạy hằng đêm do Airflow trigger; (c) production xây mới trên EKS, 40 job/ngày của 3 team. Chỉ rõ: master gì, deploy-mode gì, dependency Python quản lý bằng gì, UI xem ở đâu khi job chết.

**Hard** — (bám ROADMAP) Viết lệnh `spark-submit` hoàn chỉnh cho job giả định trên YARN cluster mode: driver 2G, 10 executor × 4 core × 8G, thêm 2G overhead mỗi executor, bật dynamic allocation trần 20, ship venv bằng `--archives`, bật event log ra `hdfs:///spark-logs`. Chú thích TỪNG dòng vì sao có mặt. (Không cần chạy — YARN không có trong repo; đây là bài "viết đơn thuốc".)

**Production Challenge** — Sửa `Makefile` của repo: thêm target `run-deps` nhận `F=` và tự động: zip toàn bộ thư mục chứa file đó thành `deps.zip`, submit với `--py-files deps.zip`. Kiểm chứng bằng app lab37 (import `helpers`) chạy được qua `make run-deps F=labs/lab37/submit_anatomy.py`. Nộp diff Makefile + output.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Deployment tưởng là chuyện vận hành, nhưng dính performance trực tiếp:

| Quyết định | Ảnh hưởng | Tại sao |
|---|---|---|
| Client mode qua VPN/WAN | `collect`, `broadcast`, `toPandas` chậm thê thảm | Mọi byte driver↔executor đi qua đường mạng xa nhất hệ thống |
| `--packages` lúc submit | Khởi động chậm + phụ thuộc Maven repo sống | Mỗi lần submit là một lần resolve/download; production bake vào image |
| Không dynamic allocation cho job nhiều pha | Trả tiền executor ngồi chơi | Pha ghi kết quả chỉ cần 10% tài nguyên pha xử lý |
| Dynamic allocation + shuffle tracking (K8s) | Cluster co lại CHẬM hơn kỳ vọng | Executor giữ shuffle file không được thu — hành vi đúng, đừng tưởng bug |
| Hardcode `local[4]` sót trong code | Job "chạy được" nhưng 1 máy | Code thắng submit — lỗi im lặng đắt nhất chương này |

Câu tự vấn mới từ hôm nay, trước mọi lần deploy: *"driver của tôi nằm đâu, ai nuôi nó, và byte nào phải đi qua đường mạng dài nhất?"*

---

## 11. Spark UI

Bài này mở khóa **Master UI :8080** (của standalone) và củng cố bản đồ UI:

**Master UI :8080** — nhìn gì:
- **Workers**: mỗi worker còn bao nhiêu core/memory — job WAITING thường vì xin quá số này (lesson 38 khai thác tiếp).
- **Running Applications**: app nào đang chiếm bao nhiêu core — standalone mặc định app đầu **chiếm hết core** (`spark.cores.max` không set!), app sau xếp hàng. Thử submit 2 app cùng lúc để chứng kiến.
- **Completed Applications**: link tới log executor của app đã chết — nơi đọc stacktrace khi 4040 đã tắt.

**Driver UI :4040** — điều mới cần để ý hôm nay: tab **Environment** → mục *Spark Properties* cho biết giá trị config CUỐI CÙNG sau khi ba tầng đè nhau. Nghi ngờ config không ăn? Đừng đoán — mở Environment tab. Đây là thao tác debug config số 1.

Bản đồ đầy đủ: 4040 = driver đang sống; 8080 = master standalone; YARN RM UI = 8088 (proxy tới driver); History Server = 18080 (người chết kể chuyện — lesson 39).

---

## 12. Common Mistakes

1. **Chạy production bằng client mode từ máy cá nhân/edge node không giám sát** → job chết theo SSH session. Production = cluster mode, không thương lượng.
2. **Hardcode `master`/memory trong code** → tầng submit không đè nổi (code thắng!), job "chạy" sai cấu hình hàng tháng trời không ai biết.
3. **Quên `--py-files`/không đóng gói deps** → `ModuleNotFoundError` chỉ xuất hiện trên cluster. Nhớ: local mode che giấu MỌI vấn đề phân phối code.
4. **Dùng `--py-files` cho numpy/pandas** → lib có C extension không ship kiểu này được. Dùng archives (venv-pack) hoặc docker image.
5. **Đặt argument của Spark SAU tên file .py** → Spark lờ đi không báo lỗi, app nhận arg lạ. Trật tự: flags → file → app args.
6. **Quên `spark.cores.max` trên standalone** → app đầu tiên nuốt cả cluster, app thứ hai WAITING vô hạn. (K8s/YARN có quota nên đỡ hơn — một lý do người ta rời standalone.)
7. **Tin rằng sửa `spark-defaults.conf` là đủ** → bị `--conf` trong script Airflow đè. Luôn kiểm chứng bằng Environment tab, đừng kiểm chứng bằng niềm tin.

---

## 13. Interview

**Junior:**

1. *Client mode và cluster mode khác nhau thế nào? Khi nào dùng cái nào?* — Khác vị trí driver: client = driver ở máy submit (debug, notebook, thấy output ngay, chết theo máy submit); cluster = driver trong cluster (production, sống độc lập máy submit). Người tương tác → client; máy scheduler trigger → cluster.
2. *Kể các giá trị `--master` bạn biết.* — `local[N]` (1 máy, N thread), `spark://host:7077` (standalone), `yarn` (đọc địa chỉ RM từ HADOOP_CONF_DIR), `k8s://https://api-server` (Kubernetes).
3. *`--files` và `--py-files` khác gì nhau?* — `--py-files`: code Python (.py/.zip/.egg), được thêm vào PYTHONPATH để import; `--files`: file dữ liệu/config bất kỳ, đặt vào working dir của executor, đọc qua `SparkFiles.get()`. Cả hai đều KHÔNG dành cho dữ liệu lớn.
4. *Job chạy cluster mode, `print()` trong driver in ra đâu?* — Không ra terminal submit; nằm trong log driver của cluster: `yarn logs -applicationId ...` trên YARN, `kubectl logs <driver-pod>` trên K8s, link log trên master UI với standalone.

**Mid:**

5. *Config precedence trong Spark? Hệ quả thực hành?* — spark-defaults.conf < spark-submit `--conf` < SparkConf trong code. Hệ quả: production code KHÔNG set config (trừ thứ gắn logic) để tầng deploy toàn quyền; debug config bằng Environment tab thay vì đoán.
6. *Spark UI ở đâu khi job chạy cluster mode trên K8s? Khi job đã chết?* — Đang chạy: port 4040 của driver pod, xem qua `kubectl port-forward` (hoặc ingress/operator expose). Đã chết: History Server đọc event log (S3/HDFS/PVC) — nếu không bật event log thì mất trắng.
7. *Dynamic allocation cần điều kiện gì để hoạt động an toàn? Trên K8s khác gì YARN?* — Vấn đề là shuffle file trên executor bị thu hồi. YARN: external shuffle service trên NodeManager giữ file hộ. K8s (Spark 3+): `shuffleTracking.enabled=true` — không thu executor còn giữ shuffle data đang cần, đổi lại co giãn xuống chậm hơn.
8. *Đưa numpy/pandas lên executor bằng cách nào? Vì sao `--py-files` không đủ?* — `--py-files` chỉ chuyển source Python, còn numpy có binary C biên dịch theo platform. Giải pháp: cài sẵn trên node, ship venv/conda đóng gói qua `--archives` + `spark.pyspark.python`, PEX, hoặc (chuẩn K8s) bake vào docker image dùng chung cho driver+executor.

**Senior:**

9. *Công ty đang on-prem YARN, muốn chuyển Spark sang K8s — bạn đánh giá gì và lộ trình ra sao?* — Được: isolation pod, dependency bằng image, hạ tầng thống nhất với phần còn lại của công ty, không nuôi Hadoop chỉ để chạy Spark. Rủi ro: shuffle performance (không external shuffle service — cần shuffle tracking hoặc đánh giá remote shuffle service), đội vận hành phải biết K8s, job cũ giả định HDFS locality. Lộ trình: chọn vài job stateless chuyển trước bằng spark-operator, bật event log ra object storage + History Server dùng chung, đo benchmark shuffle-heavy job trước khi cam kết, giữ hai hệ song song một quý.
10. *Standalone cluster mode không hỗ trợ Python — nếu buộc phải chạy PySpark "kiểu cluster mode" trên standalone thì làm gì?* — Thừa nhận giới hạn: driver Python phải là client mode. Workaround thực dụng: chạy chính lệnh spark-submit client mode BÊN TRONG cluster (container/systemd/supervisor trên một node cluster, hoặc trong pod nếu docker) để "máy submit" là hạ tầng được giám sát chứ không phải laptop — đạt được mục tiêu thật sự của cluster mode là "driver được hạ tầng nuôi". Hoặc trung thực hơn: nếu cần cluster mode thật, đổi cluster manager (K8s/YARN) — đó là lý do standalone chỉ hợp cluster nhỏ/học tập.

---

## 14. Summary

### Mindmap

```
                       DEPLOYMENT (L37)
                            │
    ┌───────────────┬───────┴─────────┬────────────────────┐
    ▼               ▼                 ▼                    ▼
spark-submit     DEPLOY MODE      CLUSTER MANAGER      DEPENDENCY & CONFIG
    │               │                 │                    │
 --master        client:          Standalone (học)      precedence:
 --deploy-mode    driver@submit,  YARN (Hadoop,          defaults < --conf < code
 --conf           chết theo máy    queue chín)           → code phải TRƠN
 --packages      cluster:         K8s (pod, image,      Python deps:
 --py-files       driver@cluster,  service account,      py-files (pure py)
 --files          production       spark-operator,       archives+venv-pack
 UI: 4040=driver  (standalone:     shuffle tracking)     docker image (K8s)
 8080=master       KHÔNG python!)  dynamic allocation
```

### Checklist trước khi gõ "Continue"

- [ ] Đọc hiểu từng flag của một lệnh spark-submit 10 dòng bất kỳ.
- [ ] Vẽ được diagram client vs cluster mode, chỉ đúng chỗ driver đứng.
- [ ] Nói được 3 khác biệt Standalone/YARN/K8s và chọn đúng cho 3 ngữ cảnh.
- [ ] Giải thích config precedence và vì sao production code không set config.
- [ ] Biết 3 cách đưa dependency Python lên executor và giới hạn của `--py-files`.
- [ ] Đã tự tay chứng kiến: standalone từ chối cluster mode Python; driver chết kéo app chết.
- [ ] Chỉ đúng UI cần mở trong 4 tình huống (local/client/cluster-đang-chạy/đã-chết).

---

## 15. Next Lesson

**Lesson 38 — Resource sizing: executor/core/memory calculation.**

Hôm nay bạn đã biết CÁCH xin tài nguyên (`--executor-memory`, `--executor-cores`, `--num-executors`) — nhưng xin BAO NHIÊU? Đây là câu hỏi phỏng vấn kinh điển nhất của Spark: *"cluster 10 node, mỗi node 16 core 64 GB — bạn cấu hình executor thế nào?"*. Trả lời sai là loại thẳng, vì nó lộ ra bạn chưa từng deploy thật. Lesson 38 giải bài toán này từng bước như một lời giải mẫu: chừa gì cho OS, vì sao 5 core/executor là con số vàng, memoryOverhead trốn ở đâu (đặc biệt với PySpark), và fat executor vs tiny executor thua thiệt nhau chỗ nào.

Sai sizing thì mọi tuning ở module 3 đều vô nghĩa — như độ xe đua nhưng đổ nhầm dầu ăn.

> Gõ **"Continue"** khi sẵn sàng.

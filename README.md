# Data Engineering Project: Building a Telco Churn Analysis Pipeline
End-to-end Telco Churn Data Engineering Project using Docker, Python, Apache Airflow, PostgreSQL, and Spark. This project automates data ingestion, processing, and storage through a scalable pipeline. Airflow orchestrates the workflow, Spark handles batch processing, and PostgreSQL serves as the primary database.

[Dataset: Telco Customer Churn (dibimbing)](https://drive.google.com/open?id=1y_IwdVNOsWFFc9DqBfnVimC6BAMPTt46&usp=drive_copy)
[Dataset: Telco Customer Churn (Kaggle)](https://www.kaggle.com/datasets/blastchar/telco-customer-churn/data))

## Dashboard

![Dashboard](https://github.com/hijirdella/Telco-Churn-Data-Pipeline/blob/650b81429232dc00eee52ae4f22179bacc341648/Picture/Dashboard.jpg)
[Dashboard: Telco Customer Churn](https://lookerstudio.google.com/reporting/c5149b81-33e2-400e-9e64-0ab860ce73de)


## 1. Architecture
![Architecture](https://github.com/hijirdella/Telco-Churn-Data-Pipeline/blob/cb5cd2723092ca9fa8e44b5e37eada8e53a98360/Picture/Architecture.png)

Arsitektur pipeline data untuk analisis churn pelanggan di sektor telekomunikasi menggunakan beberapa komponen utama:

**Airflow:**
- **Scheduler:** Mengatur penjadwalan tugas-tugas (tasks) dalam pipeline.
- **Webserver & Worker:** Mengelola eksekusi dan hasil dari tugas yang dikirimkan ke Spark.

**PostgreSQL (db):**
- Berfungsi sebagai basis data utama untuk menyimpan data mentah dan hasil transformasi.
- Berkomunikasi dengan Spark untuk mendapatkan data yang dibutuhkan.

**Spark Cluster:**
- **Master:** Mengelola eksekusi job dari Spark dan mengirimkan tugas-tugas ke node worker.
- **Worker:** Memproses tugas yang diberikan oleh master dan mengembalikan status pekerjaan.

**Docker:**
- Seluruh komponen (Airflow, PostgreSQL, dan Spark) dijalankan dalam container Docker untuk konsistensi lingkungan dan kemudahan dalam deployment.

**Alur Kerja:**
1. **Airflow** berkomunikasi dengan **PostgreSQL** untuk mengambil data yang dibutuhkan.
2. **Airflow** kemudian mengirimkan tugas ke **Spark** untuk melakukan transformasi data dan analisis churn.
3. **Spark Master** akan mengelola dan membagi tugas ke **Spark Worker** untuk diproses.
4. Hasil pemrosesan dikembalikan ke **PostgreSQL** atau disimpan sesuai kebutuhan, lalu status pekerjaan dilaporkan kembali ke **Airflow** untuk menyelesaikan tugas.
5. Hasil pemrosesan juga disimpan dalam bentuk **CSV** di folder **result**.
6. File **CSV** tersebut kemudian di-load ke **Looker** untuk visualisasi dan analisis lebih lanjut.

**Kesimpulan:**
Arsitektur ini memungkinkan pipeline yang terstruktur dan scalable untuk proses analisis churn pelanggan di sektor telekomunikasi menggunakan orchestrasi **Airflow**, pemrosesan batch **Spark**, dan penyimpanan data dengan **PostgreSQL**, semuanya dikelola menggunakan **Docker**. Dengan tambahan alur penyimpanan hasil dalam bentuk **CSV** dan integrasi dengan **Looker**, arsitektur ini mendukung analisis berbasis data yang lebih dalam dan lebih mudah divisualisasikan.

## 2. DAG for Telco Customer Churn Analysis using Spark
![DAG](https://github.com/hijirdella/Telco-Churn-Data-Pipeline/blob/cb5cd2723092ca9fa8e44b5e37eada8e53a98360/Picture/output.png)

Berikut adalah visualisasi DAG (Directed Acyclic Graph) untuk Telco Customer Churn Analysis menggunakan Spark. 

Alur ini mencakup beberapa tahapan utama:
Start: Memulai alur proses data pipeline.
Extract Data: Mengambil data mentah dari sumber (database atau file).
Transform Data: Mengolah dan membersihkan data menggunakan Spark.
Load Data: Memuat data yang telah diproses ke dalam database.
Churn Analysis: Menganalisis data untuk prediksi churn.
End: Mengakhiri alur proses.


### How to use this Repo
1. Clone This Repo.
2. Run `make docker-build` for x86 user, or `make docker-build-arm` for arm chip user.

---
```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## docker-build-arm             - Build Docker Images (arm64) including its inter-container network.
## postgres                     - Run a Postgres container
## spark                        - Run a Spark cluster, rebuild the postgres container, then create the destination tables
## clean                        - Cleanup all running containers related to the challenge.
```

---

# Telco-Churn-Data-Pipeline
End-to-end Telco Churn Data Engineering Project using Docker, Python, Apache Airflow, PostgreSQL, and Spark. This project automates data ingestion, processing, and storage through a scalable pipeline. Airflow orchestrates the workflow, Spark handles batch processing, and PostgreSQL serves as the primary database.


[Dataset: Telco Customer Churn](https://drive.google.com/open?id=1y_IwdVNOsWFFc9DqBfnVimC6BAMPTt46&usp=drive_copy)

## 1. Architecture
![Architecture](https://github.com/hijirdella/Telco-Churn-Data-Pipeline/blob/cb5cd2723092ca9fa8e44b5e37eada8e53a98360/Picture/Architecture.png)

Arsitektur pipeline data yang digunakan untuk analisis churn pelanggan di sektor telekomunikasi, menggunakan beberapa komponen utama:

Airflow:
Scheduler: Mengatur penjadwalan tugas-tugas (tasks) dalam pipeline.
Webserver & Worker: Mengelola eksekusi dan hasil dari tugas yang dikirimkan ke Spark.

PostgreSQL (db):
Berfungsi sebagai basis data utama untuk menyimpan data mentah dan hasil transformasi.
Berkomunikasi dengan Spark untuk mendapatkan data yang dibutuhkan.

Spark Cluster:
Master: Mengelola eksekusi job dari Spark dan mengirimkan tugas-tugas ke node worker.
Worker: Memproses tugas yang diberikan oleh master dan mengembalikan status pekerjaan.

Docker:
Seluruh komponen (Airflow, PostgreSQL, dan Spark) dijalankan dalam container Docker untuk konsistensi lingkungan dan kemudahan dalam deployment.

Alur Kerja:
Airflow berkomunikasi dengan PostgreSQL untuk mengambil data yang dibutuhkan.
Airflow kemudian mengirimkan tugas ke Spark untuk melakukan transformasi data dan analisis churn.
Spark Master akan mengelola dan membagi tugas ke Spark Worker untuk diproses.
Hasil pemrosesan dikembalikan ke PostgreSQL atau disimpan sesuai kebutuhan, lalu status pekerjaan dilaporkan kembali ke Airflow untuk menyelesaikan tugas.

Kesimpulan:
Arsitektur ini memungkinkan pipeline yang terstruktur dan scalable untuk proses analisis churn pelanggan di sektor telekomunikasi menggunakan orchestrasi Airflow, pemrosesan batch Spark, dan penyimpanan data dengan PostgreSQL, semuanya dikelola menggunakan Docker.

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
## jupyter                      - Spinup jupyter notebook for testing and validation purposes.
## airflow                      - Spinup airflow scheduler and webserver.
## kafka                        - Spinup kafka cluster (Kafka+Zookeeper).
## datahub                      - Spinup datahub instances.
## metabase                     - Spinup metabase instance.
## clean                        - Cleanup all running containers related to the challenge.
```

---
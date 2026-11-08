# Laporan Milestone 2a / Komparasi Kinerja PySpark
**Mata Kuliah:** Analitik dan Skalabilitas Data (MK35)  
**Nama Mahasiswa:** Inyoman Theo Ardiles Rada  
**NIM:** 42430018  
**Program Studi:** Teknologi Informasi, Universitas Pendidikan Nasional (Undiknas)  
**Run ID yang Dianalisis:** `kelas-1m` (Skala Penuh 1 Juta Event) & `smoke-a` (Skala Uji 20.000 Event)  
**Repositori GitHub:** [theorada/uts-skalabilitas-data-42430018-theo-ardiles](https://github.com/theorada/uts-skalabilitas-data-42430018-theo-ardiles.git)

---

## 1. Lingkungan Pengujian
* **Perangkat Keras:** Apple MacBook (Apple Silicon M-Series, arsitektur ARM64 / Darwin), RAM fisik ≥ 12 GB.
* **Versi Docker:** Docker Engine 29.8.1, build 4a63305.
* **Distributed Engine:** Apache Spark 3.5.3 (mode local[4], alokasi driver `spark.driver.memory=3g`).
* **Message Broker:** Redpanda v26.2.5 (kompatibel Kafka API, single-broker dev-container, smp=1).
* **Object Storage & Table Format:** Chainguard MinIO Object Storage & Tabulario Iceberg REST Catalog 1.6.0 (Apache Iceberg 1.7.1).
* **Parameter Uji Kunci:**
  * `spark.sql.shuffle.partitions`: 8
  * `spark.sql.adaptive.enabled`: false (dinonaktifkan secara sengaja untuk mengukur mitigasi skew murni)
  * `spark.sql.autoBroadcastJoinThreshold`: -1 (dinonaktifkan pada baseline untuk mematikan konversi broadcast otomatis)
  * `salt-buckets`: 16

![Status Layanan Docker](./foto/docker_ps.jpg)
*Gambar 1.1: Status seluruh kontainer lab aktif via Docker Compose.*

---

## 2. Topologi dan Aliran Data
Aliran data end-to-end dibangun melintasi 5 tahapan utama dalam jaringan Docker `labnet`:
1. **Event Ingestion (Producer):** Skrip `01_producer.py` menghasilkan data transaksi sintetis dengan ketimpangan key kota dan anomali data, kemudian mengirimkannya ke topik `ecommerce.orders` pada broker Redpanda.
2. **Batch Archival (Consumer):** Skrip `02_consumer_raw_minio.py` membaca pesan dari topik dengan semantik *at-least-once*, mengemas batch transaksi ke format JSON Lines, dan menyimpannya ke MinIO bucket `raw` (`s3a://raw/events/run_id=.../partition=.../*.jsonl`).
3. **Data Cleaning & Quality Control:** Skrip `04_spark_pipeline.py` membaca dataset raw dari MinIO dengan skema eksplisit, membersihkan anomali, membuang duplikasi transaksi, dan meng-cache dataframe bersih di memori.
4. **Analisis Optimasi:** Spark menjalankan benchmark komparatif: Baseline GroupBy, Skew GroupBy sebelum salting, Skew GroupBy setelah two-stage salting, SortMergeJoin, dan BroadcastHashJoin.
5. **Curated Iceberg Storage:** Menuliskan tabel fakta dan metrik bisnis terkurasi ke format tabel Apache Iceberg (`lake.curated.orders_clean`) pada MinIO bucket `warehouse` dengan partisi kolom tanggal (`sale_date`).

```text
[01_producer.py] 
       │ (1.000.000 event JSON)
       ▼
[Redpanda Broker :9092] (Topik: ecommerce.orders, 3 partisi)
       │ (Consumer poll batch)
       ▼
[02_consumer_raw_minio.py] 
       │ (At-least-once JSONL)
       ▼
[MinIO Raw Bucket] (s3a://raw/events/run_id=kelas-1m/partition=...)
       │ (spark.read.schema(schema).json)
       ▼
[Apache Spark 3.5.3 Engine] ─── ETL, Cleaning & In-Memory Cache
       │                                │
       ├─► Benchmarking Komputasi       │
       │                                ▼
       └─► [Iceberg REST Catalog + MinIO Warehouse] (lake.curated.orders_clean)
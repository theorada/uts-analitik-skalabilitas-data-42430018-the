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
* **Distributed Engine:** Apache Spark 3.5.3 (mode `local[4]`, alokasi driver `spark.driver.memory=3g`).
* **Message Broker:** Redpanda v26.2.5 (kompatibel Kafka API, single-broker dev-container, `smp=1`).
* **Object Storage & Table Format:** Chainguard MinIO Object Storage & Tabulario Iceberg REST Catalog 1.6.0 (Apache Iceberg 1.7.1).
* **Parameter Uji Kunci:**
  * `spark.sql.shuffle.partitions`: 8
  * `spark.sql.adaptive.enabled`: false (dinonaktifkan secara sengaja untuk mengukur mitigasi skew murni)
  * `spark.sql.autoBroadcastJoinThreshold`: -1 (dinonaktifkan pada baseline untuk mematikan konversi broadcast otomatis)
  * `salt-buckets`: 16

![Status Layanan Docker](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2000.18.10.jpg)
*Gambar 1.1: Status seluruh kontainer lab aktif via Docker Compose.*

---

## 2. Topologi dan Aliran Data
Aliran data end-to-end dibangun melintasi 5 tahapan utama dalam jaringan Docker `labnet`:
1. **Event Ingestion (Producer):** Skrip `01_producer.py` menghasilkan data transaksi sintetis dengan ketimpangan key kota dan anomali data, kemudian mengirimkannya ke topik `ecommerce.orders` pada broker Redpanda.
2. **Batch Archival (Consumer):** Skrip `02_consumer_raw_minio.py` membaca pesan dari topik dengan semantik *at-least-once*, mengemas batch transaksi ke format JSON Lines, dan menyimpannya ke MinIO bucket `raw` (`s3a://raw/events/run_id=.../partition=.../*.jsonl`).
3. **Data Cleaning & Quality Control:** Skrip `04_spark_pipeline.py` membaca dataset raw dari MinIO dengan skema eksplisit, membersihkan anomali, membuang duplikasi transaksi, dan meng-cache dataframe bersih di memori.
4. **Analisis Optimasi:** Spark menjalankan benchmark komparatif: Baseline GroupBy, Skew GroupBy sebelum salting, Skew GroupBy setelah two-stage salting, SortMergeJoin, dan BroadcastHashJoin.
5. **Curated Iceberg Storage:** Menuliskan tabel fakta dan metrik bisnis terkurasi ke format tabel Apache Iceberg (`lake.curated.orders_clean`) pada MinIO bucket `warehouse` dengan partisi kolom tanggal (`sale_date`).

| Bukti Aliran Data Ingestion (Redpanda) | Bukti Penyimpanan Objek Mentah (MinIO) |
| :---: | :---: |
| ![Redpanda Console](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2000.41.54.jpg) | ![MinIO Raw Bucket](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2000.48.37.jpg) |
| *Gambar 2.1: Topik Redpanda dengan 3 partisi.* | *Gambar 2.2: Berkas transaksi JSON Lines di bucket raw.* |

---

## 3. Validasi dan Kualitas Data
Proses pembersihan data mengevaluasi seluruh transaksi mentah terhadap kriteria validasi bisnis dan deduplikasi:

| Metrik Kualitas Data | Nilai (`smoke-a`) | Nilai (`kelas-1m`) | Penjelasan Logika Filter |
| :--- | :---: | :---: | :--- |
| **Raw Records Ingested** | 20.000 | 1.000.000 | Total data mentah yang terbaca dari bucket `raw`. |
| **Valid Clean Records** | 19.960 | 998.005 | Baris yang lolos seluruh aturan validasi data. |
| **Rejected / Duplicates** | 40 | 1.995 | Baris tidak valid atau duplikat yang disaring keluar. |
| **Integritas Record** | 100% Lolos | 100% Lolos | Raw Count = Clean Count + Invalid Count. |

### Logika Pembersihan Data:
1. `order_id.isNotNull()` dan `city != ""`: Menyaring record di mana field kota bernilai string kosong (disimulasikan pada baris kelipatan 997).
2. `amount.isNotNull() & (amount >= 0)`: Menyaring transaksi bernilai amount `NULL` (disimulasikan pada baris kelipatan 1009).
3. `quantity > 0` dan `product_id.between(1, 200)`: Memvalidasi integritas numerik barang dan referensi ID produk katalog.
4. `dropDuplicates(["run_id", "order_id"])`: Menjamin sifat *idempotent* untuk menanggulangi duplikasi akibat pengiriman ulang (*replay*) consumer Kafka bersemantik *at-least-once*.

---

## 4. Analisis Execution Plan dan Shuffle
Pemeriksaan physical execution plan dan visualisasi DAG dilakukan melalui Spark Web UI (`http://localhost:4040`):

![Spark Web UI Queries](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2019.55.59.jpg)
*Gambar 4.1: Daftar kueri komputasi yang diselesaikan pada Spark Web UI.*

![Spark Stages](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2001.19.45.jpg)
*Gambar 4.2: Rincian durasi task dan pembagian shuffle read pada Stage 17.*

* **Operator Exchange (Shuffle):** Terdeteksi pemanggilan `Exchange hashpartitioning` yang memicu pemindahan data antar-thread memori. Spark membagi data ke dalam 8 partisi sesuai konfigurasi `spark.sql.shuffle.partitions=8`.
* **HashAggregate:** Sebelum fase shuffle terjadi, Spark menjalankan partial aggregation lokal pada setiap partisi asal untuk mengurangi volume record yang harus dikirim melalui jaringan, sehingga menghemat bandwidth I/O shuffle.

---

## 5. Penanganan Data Skew
Dataset transaksi sengaja dirancang memiliki ketimpangan distribusi key yang tajam, di mana sekitar 80% transaksi berpusat di kota **Denpasar**, sedangkan Jakarta dan Surabaya hanya menyumbang masing-masing sekitar 10%.

### Tabel Perbandingan Distribusi Baris per Partisi (`kelas-1m`):

| Partisi ID | Jumlah Baris Sebelum Salting (Skew) | Jumlah Baris Sesudah Salting (Salted) | Keterangan Distribusi |
| :---: | :---: | :---: | :--- |
| `0` | 0 | 131.033 | Terisi merata setelah salting |
| `1` | 0 | 92.957 | Terisi merata setelah salting |
| `2` | 99.801 | 281.164 | Beban terdistribusi |
| `3` | **798.404** | 218.970 | **Bottleneck Denpasar terpecah** |
| `4` | 0 | 12.622 | Terisi merata setelah salting |
| `5` | 0 | 62.440 | Terisi merata setelah salting |
| `6` | 99.800 | 68.203 | Beban terdistribusi |
| `7` | 0 | 130.616 | Terisi merata setelah salting |
| **Evaluasi** | **Skew Index = 6.40 (5 Kosong)** | **Skew Index = 2.25 (0 Kosong)** | **Validasi Salting OK** |

| Bukti Terminal Sebelum Salting | Bukti Terminal Sesudah Salting |
| :---: | :---: |
| ![Skew Before Salting](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2020.38.22.png) | ![Skew After Salting](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2020.41.23.png) |
| *Gambar 5.1: Ketimpangan partisi sebelum salting.* | *Gambar 5.2: Perataan beban baris setelah Two-Stage Salting.* |

### Mekanisme Two-Stage Key Salting:
1. **Tahap 1 (Salt Key):** Nilai acak seragam ditambahkan ke kunci utama dengan formula `salt = pmod(xxhash64(order_id, 42), 16)`. Data kemudian diagregasi berdasarkan kunci gabungan `(city, salt)`. Volume masif transaksi Denpasar yang semula membebani partisi 3 berhasil disebar ke 16 bucket virtual.
2. **Tahap 2 (Global Aggregation):** Agregat parsial dari setiap salt dikonsolidasikan kembali berdasarkan `city` murni untuk menghasilkan metrik akhir.
3. **Validasi:** Terminal mencetak log `VALIDASI_SALTING_OK`, membuktikan total omzet dan jumlah pesanan per kota tetap presisi dan identik dengan hasil baseline.

---

## 6. Broadcast Join vs SortMergeJoin
Pengujian join membandingkan performa antara tabel fakta transaksi besar (`cleaned`, 998.005 baris) dengan tabel dimensi referensi produk (`dim`, 200 baris, ukuran data 1025.6 KiB):

| SortMergeJoin (Baseline Non-Broadcast) | BroadcastHashJoin (Optimasi) |
| :---: | :---: |
| ![SortMergeJoin DAG](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2020.44.23.png) | ![BroadcastHashJoin DAG](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2020.45.20.png) |
| *Gambar 6.1: Mengharuskan Shuffle Exchange & Sort.* | *Gambar 6.2: Menggunakan BroadcastExchange tanpa shuffle.* |

* **SortMergeJoin (Plan D):** Memerlukan pertukaran shuffle (`Exchange`) dan pengurutan (`Sort`) pada tabel fakta transaksi maupun tabel dimensi. Waktu eksekusi: **1.7815 detik**.
* **BroadcastHashJoin (Plan E):** Tabel dimensi disebarkan ke setiap worker via operator `BroadcastExchange`. Tabel fakta transaksi besar dapat langsung dipindai tanpa fase shuffle. Waktu eksekusi: **0.7796 detik**.
* **Peningkatan Kecepatan:** Memberikan percepatan (**Speedup**) sebesar **2.29x lipat** pada pengujian 1 juta event (dan 4.14x pada smoke test).

---

## 7. Partition Pruning dan Iceberg
Tabel dikurasi ke format Apache Iceberg dengan partisi kolom tanggal (`PARTITIONED BY (sale_date)`).

Bukti *Partition Pruning* ditunjukkan melalui berkas `plan_partition_pruning.txt` saat kueri difilter untuk tanggal transaksi tertentu (`sale_date = '2026-09-05'`):

![Partition Pruning Plan](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2020.00.54.png)
*Gambar 7.1: Physical Plan membuktikan pendorongan filter partisi langsung ke layer pemindaian Iceberg.*

* **Analisis Pruning:** Filter `sale_date = 20701` (representasi internal epoch untuk tanggal 2026-09-05) langsung didorong ke layer storage Iceberg (*PushedFilters*).
* Spark tidak memindai seluruh file Parquet (*full-table scan*), melainkan hanya membaca manifest metadata yang berkaitan dengan tanggal tersebut, meminimalkan overhead pembacaan I/O.

---

## 8. Benchmark Komparatif
Data pengukuran waktu eksekusi aktual dari berkas `benchmarks.csv` dan log audit:

| Skenario Eksperimen | Durasi `smoke-a` (20k) | Durasi `kelas-1m` (1M) | Speedup / Rasio | Temuan Kinerja |
| :--- | :---: | :---: | :---: | :--- |
| **A. GroupBy Baseline** | 1.3017 s | 1.8867 s | Baseline | Agregasi standar dengan shuffle bawaan. |
| **B. Skew Sebelum Salting** | 0.3332 s | 0.8393 s | Baseline Skew | Partisi tidak merata; 5 partisi kosong dan 1 partisi menampung 79.8% beban. |
| **C. Skew Setelah Salting** | 0.5700 s | 0.8359 s | **1.00x** (1m) | Berhasil meratakan beban ke seluruh 8 partisi (0 partisi kosong). |
| **D. SortMergeJoin** | 1.8108 s | 1.7815 s | Baseline Join | Full shuffle dan sorting pada kedua tabel. |
| **E. BroadcastHashJoin** | 0.4376 s | 0.7796 s | **2.29x** (1m) | **Sangat signifikan.** Meniadakan shuffle tabel transaksi besar. |

---

## 9. Kesimpulan
1. **Broadcast Hash Join Sangat Efektif:** Mengeliminasi shuffle pada tabel fakta besar menghasilkan percepatan komputasi konsisten sebesar **2.29x lipat** pada skala 1 juta baris.
2. **Karakteristik Key Salting pada Mode Lokal:** Teknik salting terbukti secara matematis meratakan data (menurunkan *skew index* dari **6.40** ke **2.25**). Namun, pada mode `local[4]`, durasi eksekusi relatif setara (**1.00x**) akibat overhead kalkulasi fungsi hash. Manfaat waktu salting akan terlihat nyata pada klaster multi-node fisik untuk mencegah *straggler task*.
3. **Efisiensi Format Iceberg:** Skema partisi berbasis tanggal pada Apache Iceberg berhasil membuktikan mekanisme *partition pruning*, di mana query filtering tanggal tertentu langsung memangkas pembacaan berkas Parquet yang tidak relevan.

---

## Lampiran: Bukti Audit Terminal

![Audit Kelulusan Pipeline](./hasilscreenshoot/Tangkapan%20Layar%202026-11-08%20pukul%2020.53.03.jpg)
*Gambar 9.1: Log audit akhir menyatakan seluruh artefak konsisten dan lulus uji (`AUDIT_OK`).*

* **Repositori GitHub:** [theorada/uts-skalabilitas-data-42430018-theo-ardiles](https://github.com/theorada/uts-skalabilitas-data-42430018-theo-ardiles.git)
* **Google Drive Bukti & Artefak:** [Folder Bukti Pengujian](https://drive.google.com/drive/folders/1asQJm4pf8gOgoyFvmypXRrMbnmuuGM_o?usp=sharing)
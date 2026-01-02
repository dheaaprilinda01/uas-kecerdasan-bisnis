# Battery Charge Recommendation Data Lake

Proyek ini membangun Data Lake menggunakan **Docker**, **Apache Airflow**, dan **Minio**.

# MATA KULIAH KECERDASAN BISNIS
1. Mario Franca Wijaya (2210817310009)
2. Dhea Aprilinda Utami (2210817220019)


## Komponen
1. **Minio**: Sebagai Storage Layer (Data Lake).
2. **Apache Airflow**: Sebagai Orchestrator untuk proses ELT dan ETL.
3. **PostgreSQL**: Sebagai metadata database untuk Airflow.

## Sumber Data
1. **Weather API**: Data cuaca real-time (Banjarmasin).
2. **SQL Charging Logs**: Riwayat pengisian daya dari `data_charging.sql`.
3. **CSV Activity Logs**: Log aktivitas penggunaan aplikasi dan baterai.
4. **CSV Smartphone Specs**: Spesifikasi teknis berbagai smartphone.

## Arsitektur Data
- **Bronze Layer**: Data mentah dari semua sumber dimuat ke Minio (`datalake/bronze/`).
- **Silver Layer**: Data yang telah dibersihkan dan diseragamkan (`datalake/silver/`).
- **Gold Layer**: Data hasil analisis akhir untuk rekomendasi (`datalake/gold/`).

## Cara Menjalankan
1. Pastikan Docker dan Docker Compose sudah terinstal.
2. Jalankan perintah:
   ```bash
   docker-compose up -d
   ```
3. Buka Airflow Web UI di `http://localhost:8080` (User: `admin`, Pass: `admin`).
4. Aktifkan DAG `battery_data_lake_dag`.
5. Hasil analisis dapat dilihat di Minio Console `http://localhost:9001` (User: `minioadmin`, Pass: `minioadmin`) pada bucket `datalake/gold/recommendations/final_recommendation.csv`.
6. Dashboard Visualisasi dapat diakses melalui Streamlit di `http://localhost:8501`.

## Dashboard Streamlit
Dashboard ini menampilkan:
- Metrik ringkasan penggunaan baterai.
- Rekomendasi preskriptif untuk setiap perangkat.
- Tren penggunaan baterai harian.
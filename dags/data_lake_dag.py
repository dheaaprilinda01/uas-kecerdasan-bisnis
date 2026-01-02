from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import os
import io
import json
import psycopg2

# Configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "datalake"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_minio_client():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)

def init_minio():
    try: get_minio_client().create_bucket(Bucket=BUCKET_NAME)
    except: pass

# --- BRONZE LAYER ---
def elt_weather_api():
    with open('/opt/airflow/API.txt', 'r') as f: url = f.read().strip()
    respon = requests.get(url).json()
    get_minio_client().put_object(Bucket=BUCKET_NAME, Key="bronze/weather/current_weather.json", Body=json.dumps(respon))

def elt_sql_data():
    # Connect to the real database "battery_analytics_db"
    koneksi = psycopg2.connect(
        host="postgres",
        database="battery_analytics_db",
        user="airflow",
        password="airflow"
    )
    kursor = koneksi.cursor()
    
    # Extract Charging Logs
    kursor.execute("SELECT * FROM charging_logs")
    data_pengisian = kursor.fetchall()
    df_pengisian_mentah = pd.DataFrame(data_pengisian, columns=['charge_id', 'device_id', 'plug_in_time', 'plug_out_time', 'start_level', 'end_level', 'charge_temp'])
    penyangga_logs = io.StringIO()
    df_pengisian_mentah.to_csv(penyangga_logs, index=False)
    get_minio_client().put_object(Bucket=BUCKET_NAME, Key="bronze/charging_logs/logs.csv", Body=penyangga_logs.getvalue())
    
    # Extract Devices
    kursor.execute("SELECT * FROM devices")
    data_perangkat = kursor.fetchall()
    df_perangkat_mentah = pd.DataFrame(data_perangkat, columns=['device_id', 'device_type', 'brand_name', 'model'])
    penyangga_dev = io.StringIO()
    df_perangkat_mentah.to_csv(penyangga_dev, index=False)
    get_minio_client().put_object(Bucket=BUCKET_NAME, Key="bronze/devices/devices.csv", Body=penyangga_dev.getvalue())
    
    koneksi.close()

def elt_csv_activity():
    df = pd.read_csv('/opt/airflow/data_source/log_aktivitas_baterai_hp.csv', sep=';')
    penyangga = io.StringIO()
    df.to_csv(penyangga, index=False)
    get_minio_client().put_object(Bucket=BUCKET_NAME, Key="bronze/activity_logs/activity.csv", Body=penyangga.getvalue())

def elt_csv_specs():
    df = pd.read_csv('/opt/airflow/data_source/smartphones.csv', sep=';')
    penyangga = io.StringIO()
    df.to_csv(penyangga, index=False)
    get_minio_client().put_object(Bucket=BUCKET_NAME, Key="bronze/smartphone_specs/specs.csv", Body=penyangga.getvalue())

# --- SILVER LAYER ---
def etl_silver_layer():
    s3 = get_minio_client()
    
    # Activity
    df_aktivitas = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="bronze/activity_logs/activity.csv")['Body'].read()))
    if df_aktivitas['battery_usage_percent'].dtype == 'object': df_aktivitas['battery_usage_percent'] = df_aktivitas['battery_usage_percent'].str.replace(',', '.').astype(float)
    df_aktivitas['activity_date'] = pd.to_datetime(df_aktivitas['activity_date'], dayfirst=True)
    
    # Charging Logs (From DB CSV dump)
    df_cas = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="bronze/charging_logs/logs.csv")['Body'].read()))
    df_cas['plug_in'] = pd.to_datetime(df_cas['plug_in_time'])
    df_cas['plug_out'] = pd.to_datetime(df_cas['plug_out_time'])
    df_cas = df_cas.rename(columns={'start_level': 'start_lvl', 'end_level': 'end_lvl', 'charge_temp': 'temp'})
    
    # Devices (From DB CSV dump)
    df_perangkat = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="bronze/devices/devices.csv")['Body'].read()))

    # Specs
    df_spek = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="bronze/smartphone_specs/specs.csv")['Body'].read()))
    
    # Save Cleaned
    for nama, data in [('activity', df_aktivitas), ('charging', df_cas), ('specs', df_spek), ('devices', df_perangkat)]:
        penyangga = io.StringIO(); data.to_csv(penyangga, index=False)
        s3.put_object(Bucket=BUCKET_NAME, Key=f"silver/{nama}/clean_{nama}.csv", Body=penyangga.getvalue())

# --- GOLD LAYER ---
def etl_gold_analysis():
    s3 = get_minio_client()
    df_aktivitas = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="silver/activity/clean_activity.csv")['Body'].read()))
    df_cas = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="silver/charging/clean_charging.csv")['Body'].read()))
    df_spek = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="silver/specs/clean_specs.csv")['Body'].read()))
    df_perangkat = pd.read_csv(io.BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key="silver/devices/clean_devices.csv")['Body'].read()))
    
    try:
        data_cuaca = json.loads(s3.get_object(Bucket=BUCKET_NAME, Key="bronze/weather/current_weather.json")['Body'].read().decode('utf-8'))
        suhu_saat_ini, lokasi = data_cuaca['current']['temp_c'], data_cuaca['location']['name']
    except: suhu_saat_ini, lokasi = 30.0, "Unknown"

    # Map Device ID to Model Name using the devices table
    peta_perangkat = pd.Series(df_perangkat.model.values, index=df_perangkat.device_id).to_dict()

    # Historical Rate
    df_cas['durasi'] = (pd.to_datetime(df_cas['plug_out']) - pd.to_datetime(df_cas['plug_in'])).dt.total_seconds() / 60
    df_cas = df_cas[df_cas['durasi'] > 0]
    df_cas['laju'] = (df_cas['end_lvl'] - df_cas['start_lvl']) / df_cas['durasi']
    rata_rata_laju = df_cas.groupby('device_id')['laju'].mean().to_dict()

    # Historical Charging Hour Pattern (Mode)
    df_cas['jam_cas'] = pd.to_datetime(df_cas['plug_in']).dt.hour
    pola_jam_cas = df_cas.groupby('device_id')['jam_cas'].agg(lambda x: x.mode()[0] if not x.mode().empty else None).to_dict()

    data_harian = df_aktivitas.groupby(['device_id', 'activity_date'])['battery_usage_percent'].sum().reset_index()
    hasil_akhir = []
    for id_perangkat in data_harian['device_id'].unique():
        harian_perangkat = data_harian[data_harian['device_id'] == id_perangkat]
        rata_rata_penggunaan = harian_perangkat['battery_usage_percent'].mean()
        model = peta_perangkat.get(id_perangkat, "Unknown")
        
        # Spec Lookup
        cocok = df_spek[df_spek['model'].str.contains(model.split('(')[0].strip(), case=False, na=False)]
        kapasitas, watt = 4000, 15
        if not cocok.empty:
            kapasitas = cocok.iloc[0].get('battery_capacity', 4000)
            if pd.isna(kapasitas): kapasitas = 4000
            watt = cocok.iloc[0].get('fast_charging', 15)
            if pd.isna(watt) or watt == 0: watt = 20 if "iPhone" in model else 15
        
        # Smart Estimation
        menit_teoretis = (0.6 * kapasitas * 3.7 / 1000) / (watt * 0.85) * 60
        laju_historis = rata_rata_laju.get(id_perangkat, 1.0)
        if pd.isna(laju_historis) or laju_historis <= 0: laju_historis = 1.0
        menit_historis = 60 / laju_historis
        estimasi_menit = int((menit_teoretis * 0.4) + (menit_historis * 0.6))

        persen_cas_dibutuhkan = 60 # Target 20% to 80%
        siklus_dibutuhkan = rata_rata_penggunaan / persen_cas_dibutuhkan
        
        batas_aman_suhu = 40.0
        apakah_aman = suhu_saat_ini < batas_aman_suhu
        
        # Get Historical Charging Hour Preference
        jam_kebiasaan = pola_jam_cas.get(id_perangkat)
        if jam_kebiasaan is None: jam_kebiasaan = 22 # Default if no data
        jam_kebiasaan = int(jam_kebiasaan)

        # Build recommendation based on calculated metrics
        if siklus_dibutuhkan > 2.0:
            profil = "Extreme User"
            waktu_terbaik = f"{jam_kebiasaan:02d}:00, {(jam_kebiasaan+8)%24:02d}:00 & {(jam_kebiasaan+16)%24:02d}:00"
            saran = f"Dengan rata-rata pengurasan baterai sebesar {rata_rata_penggunaan:.1f}% per hari (>120%), perangkat Anda dikategorikan sangat intensif. Disarankan melakukan 3 kali pengisian daya sehari (pagi, siang, dan malam) guna menjaga kesehatan baterai di rentang 20-80%."
        elif siklus_dibutuhkan > 1.0:
            profil = "Heavy User"
            waktu_terbaik = f"{jam_kebiasaan:02d}:00 & {(jam_kebiasaan+12)%24:02d}:00"
            saran = f"Penggunaan harian Anda mencapai {rata_rata_penggunaan:.1f}% (60%-120%). Disarankan pengisian daya dua kali sehari (pagi dan sore) untuk menjaga kesehatan sel baterai dalam jangka panjang."
        else:
            profil = "Regular User"
            waktu_terbaik = f"{jam_kebiasaan:02d}:00"
            saran = f"Konsumsi daya harian Anda stabil di angka {rata_rata_penggunaan:.1f}% (<60%). Cukup lakukan satu kali pengisian daya harian."

        saran_cuaca = f"Kondisi Banjarmasin saat ini ({suhu_saat_ini:.1f}°C) {'mendukung' if apakah_aman else 'berisiko untuk'} pengisian daya cepat. Ambang batas panas (overheat) berada pada suhu lingkungan di atas {batas_aman_suhu}°C."
        
        rekomendasi_lengkap = f"{saran} {saran_cuaca}"

        for baris in harian_perangkat.itertuples():
            hasil_akhir.append({
                "device_id": id_perangkat, "model": model, "date": baris.activity_date,
                "daily_usage": baris.battery_usage_percent, "avg_weekly_usage": rata_rata_penggunaan,
                "best_time_to_charge": waktu_terbaik, "temp_status": f"Aman ({suhu_saat_ini+5:.1f}°C)", 
                "recommendation": f"[{profil}] {rekomendasi_lengkap}",
                "charge_estimation_20_80": f"{estimasi_menit} Menit",
                "outdoor_temp": f"{suhu_saat_ini:.1f}°C", "location": lokasi
            })
            
    df_gold = pd.DataFrame(hasil_akhir)
    penyangga_gold = io.StringIO(); df_gold.to_csv(penyangga_gold, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key="gold/recommendations/final_recommendation.csv", Body=penyangga_gold.getvalue())

with DAG('battery_data_lake_dag', default_args=default_args, schedule_interval=timedelta(days=1), catchup=False) as dag:
    task_inisialisasi = PythonOperator(task_id='inisialisasi_minio', python_callable=init_minio)
    task_ekstrak_cuaca = PythonOperator(task_id='ekstrak_cuaca_api', python_callable=elt_weather_api)
    task_ekstrak_sql = PythonOperator(task_id='ekstrak_database_sql', python_callable=elt_sql_data)
    task_ekstrak_aktivitas = PythonOperator(task_id='ekstrak_log_aktivitas', python_callable=elt_csv_activity)
    task_ekstrak_specs = PythonOperator(task_id='ekstrak_spesifikasi_hp', python_callable=elt_csv_specs)
    task_transformasi_silver = PythonOperator(task_id='transformasi_layer_silver', python_callable=etl_silver_layer)
    task_analisis_gold = PythonOperator(task_id='analisis_layer_gold', python_callable=etl_gold_analysis)
    
    task_inisialisasi >> [task_ekstrak_cuaca, task_ekstrak_sql, task_ekstrak_aktivitas, task_ekstrak_specs] >> task_transformasi_silver >> task_analisis_gold
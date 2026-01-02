import streamlit as st
import pandas as pd
import boto3
import io
import os
import requests
from requests.auth import HTTPBasicAuth

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake")
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"

st.set_page_config(page_title="Battery Insight Dashboard", layout="wide")

st.title("Battery Insight & Recommendation")
st.markdown("Dashboard analisis penggunaan baterai mingguan dan rekomendasi preskriptif (Data Source: Minio Gold Layer).")

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )



@st.cache_data(ttl=60)
def load_data(refresh_key=0):
    s3 = get_minio_client()
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key="gold/recommendations/final_recommendation.csv")
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        # Check if it's the correct version
        if 'outdoor_temp' not in df.columns:
            return "OLD_VERSION"
        return df
    except Exception as e:
        return None

if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0

df_all = load_data(st.session_state.refresh_count)


if df_all is None:
    st.error("Data Gold belum tersedia di Minio. Silakan klik Refresh.")
elif isinstance(df_all, str) and df_all == "OLD_VERSION":
    st.warning("Data di Minio masih versi lama. Sedang menunggu Airflow selesai memproses...")
    st.info("Mohon tunggu 10-20 detik lalu klik Refresh lagi.")
else:
    # Sidebar Selection
    device_options = df_all[['device_id', 'model']].drop_duplicates()
    device_labels = {row['device_id']: f"{row['device_id']} - {row['model']}" for _, row in device_options.iterrows()}
    
    selected_device_id = st.sidebar.selectbox("Pilih Perangkat", options=list(device_labels.keys()), format_func=lambda x: device_labels[x], key="device_selector")
    
    df = df_all[df_all['device_id'] == selected_device_id].copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
    
    model_name = df['model'].iloc[0] if 'model' in df.columns else "Unknown Model"
    st.header(f"Analisis: {model_name}")
    
    col1, col2, col3, col4 = st.columns(4)
    
    avg_usage = df['avg_weekly_usage'].iloc[0] if 'avg_weekly_usage' in df.columns else 0
    best_time = df['best_time_to_charge'].iloc[0] if 'best_time_to_charge' in df.columns else "N/A"
    temp_status = df['temp_status'].iloc[0] if 'temp_status' in df.columns else "N/A"
    est_charge = df['charge_estimation_20_80'].iloc[0] if 'charge_estimation_20_80' in df.columns else "N/A"
    
    col1.metric("Rata-rata Penggunaan", f"{avg_usage:.1f}% /hari")
    col2.metric("Waktu Charge Terbaik", best_time)
    
    # Menampilkan Suhu Banjarmasin di metrik utama (Murni dari API)
    outdoor_temp = df['outdoor_temp'].iloc[0] if 'outdoor_temp' in df.columns else "N/A"
    location = df['location'].iloc[0] if 'location' in df.columns else "Banjarmasin"
    col3.metric(f"Suhu {location}", outdoor_temp)
    
    col4.metric("Estimasi Charge (20-80%)", est_charge)

    st.subheader("Rekomendasi Preskriptif")
    rec_text = df['recommendation'].iloc[0] if 'recommendation' in df.columns else "N/A"
    st.info(rec_text)

    st.subheader(f"Tren Penggunaan Baterai Harian - {model_name}")
    if 'date' in df.columns and 'daily_usage' in df.columns:
        chart_df = df.copy()
        chart_df['date_label'] = chart_df['date'].dt.strftime('%d %b')
        st.bar_chart(chart_df.set_index('date_label')['daily_usage'])

        st.write("**Riwayat Penggunaan Per Hari:**")
        summary_df = df[['date', 'daily_usage']].copy()
        summary_df['date'] = summary_df['date'].dt.strftime('%A, %d %B %Y')
        summary_df.columns = ['Tanggal', 'Penggunaan Baterai (%)']
        st.table(summary_df)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyshark
import pandas as pd
import requests
from googletrans import Translator
import time

# CSV 파일 저장 경로
PACKET_CSV_PATH = "/usr/local/airflow/dags/wireshark_packets.csv"
PULSE_CSV_PATH = "/usr/local/airflow/dags/otx_pulses.csv"
INDICATOR_CSV_PATH = "/usr/local/airflow/dags/otx_indicators.csv"

# OTX API 설정
OTX_API_KEY = "d2a046c5876934c7b6d0675f5bf568db5010840f3fcf1c60f651c29ddffbebbe" 
BASE_URL = "https://otx.alienvault.com/api/v1/pulses/subscribed"
HEADERS = {"X-OTX-API-KEY": OTX_API_KEY}
translator = Translator()

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 5),
    "retries": 1
}

dag = DAG(
    dag_id="packet_capture_otx_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

# Wireshark 패킷 캡처 및 CSV 저장
def capture_packets():
    columns = ["timestamp", "packet_length", "protocol", "src_port", "dst_port", "additional_info"]
    df = pd.DataFrame(columns=columns)
    df.to_csv(PACKET_CSV_PATH, index=False, mode="w", encoding="utf-8")

    capture = pyshark.LiveCapture(interface="Wi-Fi 2", display_filter="http")
    print("Wireshark 패킷 캡처 시작...")

    for packet in capture.sniff_continuously(packet_count=50):  # 50개 패킷 캡처
        try:
            data = {
                "timestamp": datetime.now(),
                "packet_length": int(packet.length),
                "protocol": packet.transport_layer,
                "src_port": getattr(packet[packet.transport_layer], "srcport", None) if packet.transport_layer else None,
                "dst_port": getattr(packet[packet.transport_layer], "dstport", None) if packet.transport_layer else None,
                "additional_info": str(packet.http) if "http" in packet else None,
            }
            df = pd.DataFrame([data])
            df.to_csv(PACKET_CSV_PATH, index=False, mode="a", header=False, encoding="utf-8")

        except Exception as e:
            print(f"패킷 저장 오류: {e}")

# OTX 데이터 수집 및 CSV 저장
def fetch_otx_data():
    response = requests.get(BASE_URL, headers=HEADERS)
    if response.status_code != 200:
        print(f"OTX API 요청 실패: {response.status_code}")
        return

    data = response.json()
    pulses = []
    indicators = []

    for pulse in data.get("results", []):
        pulse_id = pulse["id"]
        pulse_name = translator.translate(pulse["name"], src="en", dest="ko").text
        description = translator.translate(pulse["description"], src="en", dest="ko").text

        pulses.append({"pulse_id": pulse_id, "pulse_name": pulse_name, "description": description, "label": 1})

        for indicator in pulse.get("indicators", []):
            indicator_value = indicator["indicator"]
            indicator_type = indicator["type"]
            if indicator_type in ["url", "hostname", "domain", "ipv4"]:
                indicators.append({"pulse_id": pulse_id, "indicator_type": indicator_type, "indicator_value": indicator_value})

    pd.DataFrame(pulses).to_csv(PULSE_CSV_PATH, index=False, mode="w", encoding="utf-8")
    pd.DataFrame(indicators).to_csv(INDICATOR_CSV_PATH, index=False, mode="w", encoding="utf-8")
    print("OTX 데이터 CSV 저장 완료")

# Airflow DAG Task 정의
task1 = PythonOperator(
    task_id="capture_packets",
    python_callable=capture_packets,
    dag=dag
)

task2 = PythonOperator(
    task_id="fetch_otx_data",
    python_callable=fetch_otx_data,
    dag=dag
)

# Task 실행 순서 정의
task1 >> task2

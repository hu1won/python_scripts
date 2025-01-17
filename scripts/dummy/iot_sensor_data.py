from dotenv import load_dotenv
import os
import psycopg2
from datetime import datetime, timedelta
import random
import string
import hashlib

load_dotenv()

db_name = os.getenv('LOCAL_DB_CONFIG_DBNAME')
db_user = os.getenv('LOCAL_DB_CONFIG_USER')
db_password = os.getenv('LOCAL_DB_CONFIG_PASSWORD')
db_host = os.getenv('LOCAL_DB_CONFIG_HOST')
db_port = os.getenv('LOCAL_DB_CONFIG_PORT')

db_config = {
    'dbname': db_name,
    'user': db_user,
    'password': db_password,
    'host': db_host,
    'port': db_port,
}

def connect_db():
    conn = psycopg2.connect(**db_config)
    return conn

def get_sensor_equipment():
    conn = connect_db()
    cur = conn.cursor()
    
    # 센서 장비 정보 조회
    cur.execute("""
        SELECT snsr_eqpmnt_uid, snsr_type 
        FROM jadxdb2.snsr_eqpmnt
    """)
    
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def generate_dummy_rtu_data(snsr_eqpmnt_uid, count=100):
    data = []
    base_date = datetime.now()
    
    for i in range(count):
        log_date = base_date - timedelta(hours=i)
        data.append({
            'snsr_eqpmnt_uid': snsr_eqpmnt_uid,
            'mode': '1',
            'pressure': round(random.uniform(900, 1100), 3),
            'tp': round(random.uniform(-10, 40), 3),
            'soil_potential': round(random.uniform(-100, 0), 3),
            'teros21_tp': round(random.uniform(-5, 30), 3),
            'soil_moisture': round(random.uniform(0, 100), 3),
            'teros12_tp': round(random.uniform(-5, 30), 3),
            'ec': round(random.uniform(0, 5), 3),
            'rsrp': random.randint(-140, -40),
            'rssi': random.randint(-100, -40),
            'rsrq': random.randint(-20, 0),
            'telecom_cd': random.choice(['SKT', 'KT', 'LGU']),
            'channel': str(random.randint(1, 100)),
            'ver': 'T.08',
            'imsi': ''.join(random.choices(string.digits, k=15)),
            'snsr_log_dt': log_date.strftime('%Y%m%d'),
            'snsr_log_tm': log_date.strftime('%H%M%S'),
            'err_cd': str(random.randint(100, 999))
        })
    return data

def generate_dummy_aws_data(snsr_eqpmnt_uid, count=100):
    data = []
    base_date = datetime.now()
    sensor_codes = ['TMP', 'HUM', 'RAI', 'WIN', 'DIR']  # 온도, 습도, 강우, 풍속, 풍향
    
    for i in range(count):
        log_date = base_date - timedelta(hours=i)
        for snsr_cd in sensor_codes:
            value = {
                'TMP': random.uniform(-10, 40),  # 온도
                'HUM': random.uniform(0, 100),   # 습도
                'RAI': random.uniform(0, 50),    # 강우
                'WIN': random.uniform(0, 20),    # 풍속
                'DIR': random.uniform(0, 360),   # 풍향
            }[snsr_cd]
            
            data.append({
                'snsr_eqpmnt_uid': snsr_eqpmnt_uid,
                'sn': f'AWS{random.randint(1000, 9999)}',
                'snsr_log_dt': log_date.strftime('%Y%m%d'),
                'snsr_log_tm': log_date.strftime('%H%M%S'),
                'snsr_cd': snsr_cd,
                'snsr_vl': round(value, 3)
            })
    return data

def generate_dummy_ad_data(snsr_eqpmnt_uid, count=100):
    data = []
    base_date = datetime.now()
    
    for i in range(count):
        log_date = base_date - timedelta(hours=i)
        data.append({
            'snsr_eqpmnt_uid': snsr_eqpmnt_uid,
            'sht_time': log_date,
            'sender': f'DT{random.randint(1000, 9999)}',
            'insd_tp': round(random.uniform(15, 35), 3),
            'insd_hum': round(random.uniform(30, 80), 3),
            'otsd_tp': round(random.uniform(-10, 40), 3),
            'otsd_hum': round(random.uniform(20, 90), 3),
            'btry_volt': round(random.uniform(2.5, 4.2), 3),
            'img': f'image_{random.randint(1000, 9999)}.jpg'
        })
    return data

def generate_unique_uid(table_name):
    parts = table_name.split('_')[1:6]
    uid_prefix = ''.join(part[0].upper() for part in parts if part)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"
    return unique_uid

def insert_dummy_data():
    conn = connect_db()
    cur = conn.cursor()
    
    sensors = get_sensor_equipment()
    print(f"총 {len(sensors)}개의 센서 장비를 찾았습니다.")
    
    for snsr_eqpmnt_uid, snsr_type in sensors:
        print(f"\n센서 {snsr_eqpmnt_uid} (타입: {snsr_type}) 처리 중...")
        
        if snsr_type == 'RTU':
            data = generate_dummy_rtu_data(snsr_eqpmnt_uid)
            print(f"RTU 센서 데이터 {len(data)}개 생성 완료")
            for i, item in enumerate(data, 1):
                cur.execute("""
                    INSERT INTO jadxdb2.rtu_snsr_log (
                        rtu_snsr_log_uid, snsr_eqpmnt_uid, mode, pressure, tp, soil_potential, 
                        teros21_tp, soil_moisture, teros12_tp, ec, rsrp, rssi, 
                        rsrq, telecom_cd, channel, ver, imsi, snsr_log_dt, 
                        snsr_log_tm, err_cd, reg_uid
                    ) VALUES (
                        %(uid)s, %(snsr_eqpmnt_uid)s, %(mode)s, %(pressure)s, %(tp)s, 
                        %(soil_potential)s, %(teros21_tp)s, %(soil_moisture)s, 
                        %(teros12_tp)s, %(ec)s, %(rsrp)s, %(rssi)s, %(rsrq)s, 
                        %(telecom_cd)s, %(channel)s, %(ver)s, %(imsi)s, 
                        %(snsr_log_dt)s, %(snsr_log_tm)s, %(err_cd)s, 'test'
                    )
                """, {**item, 'uid': generate_unique_uid('rtu_snsr_log')})
                if i % 10 == 0:  # 10개마다 진행상황 출력
                    print(f"RTU 데이터 {i}/{len(data)} 삽입 완료")
                
        elif snsr_type == 'AWS':
            data = generate_dummy_aws_data(snsr_eqpmnt_uid)
            print(f"AWS 센서 데이터 {len(data)}개 생성 완료")
            for i, item in enumerate(data, 1):
                cur.execute("""
                    INSERT INTO jadxdb2.agrclt_mlph_snsr_log (
                        agrclt_mlph_snsr_log_uid, snsr_eqpmnt_uid, sn, snsr_log_dt, snsr_log_tm, 
                        snsr_cd, snsr_vl, reg_uid
                    ) VALUES (
                        %(uid)s, %(snsr_eqpmnt_uid)s, %(sn)s, %(snsr_log_dt)s, 
                        %(snsr_log_tm)s, %(snsr_cd)s, %(snsr_vl)s, 'test'
                    )
                """, {**item, 'uid': generate_unique_uid('agrclt_mlph_snsr_log')})
                if i % 50 == 0:  # 50개마다 진행상황 출력
                    print(f"AWS 데이터 {i}/{len(data)} 삽입 완료")
                
        elif snsr_type == 'DT':
            data = generate_dummy_ad_data(snsr_eqpmnt_uid)
            print(f"DT 센서 데이터 {len(data)}개 생성 완료")
            for i, item in enumerate(data, 1):
                cur.execute("""
                    INSERT INTO jadxdb2.ad_snsr_log (
                        ad_snsr_log_uid, snsr_eqpmnt_uid, capture_time, sender, insd_tp, insd_hum,
                        otsd_tp, otsd_hum, btry_volt, img, reg_uid
                    ) VALUES (
                        %(uid)s, %(snsr_eqpmnt_uid)s, %(sht_time)s, %(sender)s, 
                        %(insd_tp)s, %(insd_hum)s, %(otsd_tp)s, %(otsd_hum)s,
                        %(btry_volt)s, %(img)s, 'test'
                    )
                """, {**item, 'uid': generate_unique_uid('ad_snsr_log')})
                if i % 10 == 0:  # 10개마다 진행상황 출력
                    print(f"DT 데이터 {i}/{len(data)} 삽입 완료")
        
        print(f"{snsr_type} 센서 {snsr_eqpmnt_uid}의 모든 데이터 삽입 완료")
    
    conn.commit()
    print("\n모든 데이터가 성공적으로 저장되었습니다.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("더미 데이터 생성 및 삽입 시작...")
    insert_dummy_data()
    print("프로그램 종료")


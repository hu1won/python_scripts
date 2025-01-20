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
    modes = [0, 1, 2, 6, 8]
    error_codes = ['100', '101', '102', '110', '205', '206', '207']
    
    for i in range(count):
        log_date = base_date - timedelta(hours=i)
        mode = random.choice(modes)
        
        # 기본 데이터 구조
        item = {
            'snsr_eqpmnt_uid': snsr_eqpmnt_uid,
            'mode': str(mode),
            'ver': 'T.08',
            'imsi': ''.join(random.choices(string.digits, k=15)),
            'snsr_log_dt': log_date.strftime('%Y%m%d'),
            'snsr_log_tm': log_date.strftime('%H%M%S'),
        }
        
        # 모드별 데이터 추가
        if mode in [0, 1, 2]:
            item.update({
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
                'err_cd': None
            })
        elif mode == 6:
            item.update({
                'pressure': None,
                'tp': None,
                'soil_potential': None,
                'teros21_tp': None,
                'soil_moisture': None,
                'teros12_tp': None,
                'ec': None,
                'rsrp': random.randint(-140, -40),
                'rssi': random.randint(-100, -40),
                'rsrq': random.randint(-20, 0),
                'telecom_cd': random.choice(['SKT', 'KT', 'LGU']),
                'channel': str(random.randint(1, 100)),
                'err_cd': random.choice(error_codes)
            })
        elif mode == 8:
            item.update({
                'pressure': 0,
                'tp': 0,
                'soil_potential': round(random.uniform(-7000, -6500), 3),
                'teros21_tp': round(random.uniform(20, 25), 3),
                'soil_moisture': round(random.uniform(1800, 2000), 3),
                'teros12_tp': round(random.uniform(20, 25), 3),
                'ec': 0,
                'rsrp': random.randint(-140, -40),
                'rssi': random.randint(-100, -40),
                'rsrq': random.randint(-20, 0),
                'telecom_cd': random.choice(['SKT', 'KT', 'LGU']),
                'channel': str(random.randint(1, 100)),
                'err_cd': random.choice(error_codes)
            })
        
        data.append(item)
    return data

def get_sensor_value_range(sensor_code):
    # 센서 코드별 값의 범위 정의
    ranges = {
        # 기온 관련 (섭씨)
        'G1': (-30, 50),  # 현재기온
        'G36': (-30, 50), # 최고기온
        'G37': (-30, 50), # 최저기온
        'G38': (-30, 50), # 최고기온 2
        'G39': (-30, 50), # 최저기온 2
        'G40': (-30, 50), # 최고기온 3
        'G41': (-30, 50), # 최저기온 3
        
        # 풍속 관련 (m/s)
        'G2': (0, 50),    # 기온 2
        'G3': (0, 50),    # 기온 3
        
        # 풍향 관련 (도)
        'G4': (0, 360),   # 풍향 1
        'G5': (0, 360),   # 풍향 2
        'G6': (0, 360),   # 풍향 3
        
        # 습도 관련 (%)
        'G7': (0, 100),   # 습도
        'G8': (0, 100),   # 풍속 2
        'G9': (0, 100),   # 풍속 3
        
        # 대기압 관련 (hPa)
        'G10': (900, 1100),  # 대기압도
        
        # 습도 관련 (%)
        'G11': (0, 100),  # 습도 2
        'G12': (0, 100),  # 습도 3
        
        # 강수량 관련 (mm)
        'G13': (0, 100),  # 강수량
        
        # 토양수분 관련 (%)
        'G14': (0, 100),  # 토양수분
        'G15': (0, 100),  # 토양수분 2
        'G16': (0, 100),  # 토양수분 3
        
        # 일사량 관련 (W/m²)
        'G17': (0, 1200), # 이슬지속시간
        'G18': (0, 1200), # 일사량
        'G19': (0, 1200), # 일조량
        
        # 지중온도 관련 (섭씨)
        'G20': (-10, 40), # 초상온도
        'G21': (-10, 40), # 토양온도,지중온도
        'G22': (-10, 40), # 지중온도 2
        'G23': (-10, 40), # 지중온도 3
    }
    
    # 기본값 범위 (0-100)
    default_range = (0, 100)
    
    return ranges.get(sensor_code, default_range)

def generate_dummy_aws_data(snsr_eqpmnt_uid, count=100):
    data = []
    base_date = datetime.now()
    sensor_codes = [f'G{i}' for i in range(1, 76)]  # G1부터 G75까지
    
    for i in range(count):
        log_date = base_date - timedelta(hours=i)
        sensor_code = random.choice(sensor_codes)
        min_val, max_val = get_sensor_value_range(sensor_code)
        
        data.append({
            'snsr_eqpmnt_uid': snsr_eqpmnt_uid,
            'sn': ''.join(random.choices(string.digits, k=10)),
            'snsr_log_dt': log_date.strftime('%Y%m%d'),
            'snsr_log_tm': log_date.strftime('%H%M%S'),
            'snsr_cd': sensor_code,
            'snsr_vl': round(random.uniform(min_val, max_val), 3)
        })
    return data

def generate_dummy_ad_data(snsr_eqpmnt_uid, count=100):
    data = []
    base_date = datetime.now()
    
    for i in range(count):
        log_date = base_date - timedelta(hours=i)
        
        # 50% 확률로 이미지 포함 여부 결정
        has_image = random.choice([True, False])
        
        if has_image:
            # 이미지가 있는 경우: img, sender, capture_time만 포함
            data.append({
                'snsr_eqpmnt_uid': snsr_eqpmnt_uid,
                'sht_time': log_date,
                'sender': f'DT{random.randint(1000, 9999)}',
                'insd_tp': None,
                'insd_hum': None,
                'otsd_tp': None,
                'otsd_hum': None,
                'btry_volt': None,
                'img': f'image_{random.randint(1000, 9999)}.jpg'
            })
        else:
            # 이미지가 없는 경우: img를 제외한 모든 필드 포함
            data.append({
                'snsr_eqpmnt_uid': snsr_eqpmnt_uid,
                'sht_time': log_date,
                'sender': f'DT{random.randint(1000, 9999)}',
                'insd_tp': round(random.uniform(15, 35), 3),
                'insd_hum': round(random.uniform(30, 80), 3),
                'otsd_tp': round(random.uniform(-10, 40), 3),
                'otsd_hum': round(random.uniform(20, 90), 3),
                'btry_volt': round(random.uniform(0, 100), 3),
                'img': None
            })
    return data

def generate_unique_uid(table_name):
    parts = table_name.split('_')[1:6]
    uid_prefix = ''.join(part[0].upper() for part in parts if part)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"
    return unique_uid

def clear_existing_data():
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        print("기존 데이터 삭제 중...")
        
        # RTU 데이터 삭제
        cur.execute("DELETE FROM jadxdb2.rtu_snsr_log")
        print("RTU 센서 데이터 삭제 완료")
        
        # AWS 데이터 삭제
        cur.execute("DELETE FROM jadxdb2.agrclt_mlph_snsr_log")
        print("AWS 센서 데이터 삭제 완료")
        
        # DT 데이터 삭제
        cur.execute("DELETE FROM jadxdb2.ad_snsr_log")
        print("DT 센서 데이터 삭제 완료")
        
        conn.commit()
        print("모든 기존 데이터 삭제 완료")
        
    except Exception as e:
        conn.rollback()
        print(f"데이터 삭제 중 오류 발생: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def insert_dummy_data():
    # 기존 데이터 삭제
    clear_existing_data()
    
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
    try:
        insert_dummy_data()
        print("프로그램 정상 종료")
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        print("프로그램 비정상 종료")


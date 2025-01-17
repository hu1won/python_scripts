from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
import ssl
import certifi
import geopy.geocoders

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

# SSL 컨텍스트 설정 추가
ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx

def insert_iot_eqpment():
    pass

def get_coordinates(address):
    try:
        geolocator = Nominatim(user_agent="my_app")
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        return None, None
    except GeocoderTimedOut:
        time.sleep(1)
        return None, None

def aws_insert_iot_eqpment():
    try:
        # CSV 파일 읽기
        df = pd.read_csv('aws_20250108 (1).csv')
        print("\n=== CSV 파일 구조 ===")
        print(df.head())
        print("\n=== 컬럼 목록 ===")
        print(df.columns.tolist())
        
        # 데이터베이스 연결
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # INSERT 쿼리 수정 (lat, lng 추가)
        insert_query = """
            INSERT INTO jadxdb2.snsr_eqpmnt 
            (snsr_uid, snsr_type, snsr_eqpmnt_nm, addr, lat, lng, reg_uid) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        print("\n=== 삽입될 데이터 샘플 ===")
        inserted_count = 0
        
        # 데이터 삽입
        for _, row in df.iterrows():
            # 주소로부터 위도, 경도 구하기
            lat, lng = get_coordinates(row['주소2'])
            
            values = (
                str(row['우편번호']),  # snsr_uid
                'AWS',                # snsr_type
                row['표출명'],         # snsr_eqpmnt_nm
                row['주소2'],         # addr
                lat,                  # latitude
                lng,                  # longitude
                'administrator'       # reg_uid
            )
            
            # 처음 5개 행 출력
            if _ < 5:
                print(f"데이터: {values}")
            
            # DB에 데이터 삽입
            cur.execute(insert_query, values)
            inserted_count += 1
            
            # API 제한을 피하기 위한 잠깐의 대기
            time.sleep(1)
        
        # 변경사항 저장 및 연결 종료
        conn.commit()
        cur.close()
        conn.close()
        
        return {"message": f"AWS 데이터 삽입 완료. 총 {inserted_count}개 행이 삽입되었습니다."}
    
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        return {"error": f"데이터 삽입 중 오류 발생: {str(e)}"}

def ad_insert_iot_eqpment():
    try:
        # 엑셀 파일 읽기
        df = pd.read_excel('디지털트랩가공데이터.xlsx')
        print("\n=== 엑셀 파일 구조 ===")
        print(df.head())
        print("\n=== 컬럼 목록 ===")
        print(df.columns.tolist())
        
        # 데이터베이스 연결
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # INSERT 쿼리
        insert_query = """
            INSERT INTO jadxdb2.snsr_eqpmnt 
            (snsr_uid, snsr_type, snsr_eqpmnt_nm, addr, lat, lng, reg_uid) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        print("\n=== 삽입될 데이터 샘플 ===")
        inserted_count = 0
        
        # 데이터 삽입
        for _, row in df.iterrows():
            values = (
                str(row['uid']),                    # snsr_uid
                'DT',                               # snsr_type
                f"{row['표출명']}_{row['uid']}",    # snsr_eqpmnt_nm
                row['주소'],                        # addr
                float(row['위도']),                 # latitude
                float(row['경도']),                 # longitude
                'administrator'                     # reg_uid
            )
            
            # 처음 5개 행 출력
            if _ < 5:
                print(f"데이터: {values}")
            
            # DB에 데이터 삽입
            cur.execute(insert_query, values)
            inserted_count += 1
        
        # 변경사항 저장 및 연결 종료
        conn.commit()
        cur.close()
        conn.close()
        
        return {"message": f"디지털트랩 데이터 삽입 완료. 총 {inserted_count}개 행이 삽입되었습니다."}
    
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        return {"error": f"데이터 삽입 중 오류 발생: {str(e)}"}

# 함수 실행
if __name__ == "__main__":
    result = ad_insert_iot_eqpment()
    print("\n=== 실행 결과 ===")
    print(result)
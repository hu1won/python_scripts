import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import pandas as pd
import datetime
import uuid
import random
import hashlib

load_dotenv()

db_name = os.getenv('SURVER_DB_CONFIG_DBNAME')
db_user = os.getenv('SURVER_DB_CONFIG_USER')
db_password = os.getenv('SURVER_DB_CONFIG_PASSWORD')
db_host = os.getenv('SURVER_DB_CONFIG_HOST')
db_port = os.getenv('SURVER_DB_CONFIG_PORT')

# Database connection details
db_config = {
    'dbname': db_name,
    'user': db_user,
    'password': db_password,
    'host': db_host,
    'port': db_port
}

def generate_unique_uid():
    # UID 생성 로직
    tbl = "food_nutr_info"  # 테이블명 변경
    parts = tbl.split('_')
    uid_prefix = ''.join(part[0].upper() for part in parts if part)  # FNI
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"
    return unique_uid

def insert_nutrient_data():
    # Excel 파일 읽기 - 5행을 건너뛰고 6행부터 시작 (skiprows=5)
    df = pd.read_excel('data.xlsx', skiprows=5)
    
    # 데이터 확인을 위한 출력
    print("Excel 파일의 열 목록:", df.columns.tolist())
    print("첫 번째 행 데이터:", df.iloc[0])
    
    try:
        # 데이터베이스 연결
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        for index, row in df.iterrows():
            crop_query = """
                SELECT crop_sm_uid 
                FROM jadxdb2.crop_sm 
                WHERE dsply_nm = %s
            """
            # iloc를 사용하여 위치 기반 인덱싱
            dsply_nm = row.iloc[1]  # B열
            
            if pd.notna(dsply_nm) and str(dsply_nm).strip():
                cur.execute(crop_query, (dsply_nm,))
                result = cur.fetchone()
                
                if result:
                    crop_sm_uid = result[0]
                    food_nutr_info_uid = generate_unique_uid()
                    reg_uid = "administrator"
                    
                    insert_query = """
                        INSERT INTO jadxdb2.food_nutr_info 
                        (food_nutr_info_uid, crop_sm_uid, ntrgn, phsphrs, ptssm, reg_uid, reg_dt)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """
                    cur.execute(insert_query, (
                        food_nutr_info_uid,
                        crop_sm_uid,
                        row.iloc[22],  # W열
                        row.iloc[23],  # X열
                        row.iloc[24],  # Y열
                        reg_uid
                    ))
                else:
                    print(f"'{dsply_nm}' 에 해당하는 crop_sm_uid를 찾을 수 없습니다.")
            
        conn.commit()
        print("데이터 삽입이 완료되었습니다.")
        
    except Exception as e:
        print(f"에러가 발생했습니다: {str(e)}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()

def generate_fertilizer_uid():
    # UID 생성 로직
    tbl = "standard_fertilizer"  # 테이블명
    parts = tbl.split('_')
    uid_prefix = ''.join(part[0].upper() for part in parts if part)  # SF
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"
    return unique_uid

def insert_fertilizer_data():
    # Excel 파일 읽기 - 5행을 건너뛰고 6행부터 시작 (skiprows=5)
    df = pd.read_excel('data.xlsx', skiprows=5)
    
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        for index, row in df.iterrows():
            crop_query = """
                SELECT crop_sm_uid 
                FROM jadxdb2.crop_sm 
                WHERE dsply_nm = %s
            """
            dsply_nm = row.iloc[1]  # B열
            
            if pd.notna(dsply_nm) and str(dsply_nm).strip():
                cur.execute(crop_query, (dsply_nm,))
                result = cur.fetchone()
                
                if result:
                    crop_sm_uid = result[0]
                    standard_fertilizer_uid = generate_fertilizer_uid()
                    reg_uid = "administrator"
                    
                    # 각 컬럼의 값을 가져오되, None이 아닌 값만 포함
                    columns = []
                    values = []
                    params = []
                    
                    # 컬럼과 값 매핑
                    field_mappings = {
                        'n_bgrm': row.iloc[13],    # N열
                        'p_bgrm': row.iloc[14],    # O열
                        'k_bgrm': row.iloc[15],    # P열
                        'lime_bgrm': row.iloc[16], # Q열
                        'n_ygrm': row.iloc[17],    # R열
                        'p_ygrm': row.iloc[18],    # S열
                        'k_ygrm': row.iloc[19],    # T열
                        'lime_ygrm': row.iloc[20]  # U열
                    }
                    
                    # None이 아닌 값만 추가
                    for field, value in field_mappings.items():
                        if pd.notna(value):
                            columns.append(field)
                            values.append('%s')
                            params.append(value)
                    
                    # 기본 필드 추가
                    columns.extend(['standard_fertilizer_uid', 'crop_sm_uid', 'reg_uid'])
                    values.extend(['%s', '%s', '%s'])
                    params.extend([standard_fertilizer_uid, crop_sm_uid, reg_uid])
                    
                    # 동적 쿼리 생성
                    insert_query = f"""
                        INSERT INTO jadxdb2.standard_fertilizer 
                        ({', '.join(columns)}, reg_dt)
                        VALUES ({', '.join(values)}, CURRENT_TIMESTAMP)
                    """
                    
                    cur.execute(insert_query, params)
                else:
                    print(f"'{dsply_nm}' 에 해당하는 crop_sm_uid를 찾을 수 없습니다.")
            
        conn.commit()
        print("비료 데이터 삽입이 완료되었습니다.")
        
    except Exception as e:
        print(f"에러가 발생했습니다: {str(e)}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()

def generate_soil_uid():
    # UID 생성 로직
    tbl = "soil_chemical"  # 테이블명 변경
    parts = tbl.split('_')
    uid_prefix = ''.join(part[0].upper() for part in parts if part)  # SC
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"
    return unique_uid

def parse_range_value(value):
    """문자열에서 최소값과 최대값을 추출"""
    if pd.isna(value) or not str(value).strip():
        return None, None
    
    try:
        # '~' 또는 '-'로 분리
        parts = str(value).replace(' ', '').split('~')
        if len(parts) != 2:
            parts = str(value).replace(' ', '').split('-')
        
        if len(parts) == 2:
            return float(parts[0]), float(parts[1])
        return None, None
    except:
        return None, None

def insert_soil_data():
    # Excel 파일 읽기 - 5행을 건너뛰고 6행부터 시작 (skiprows=5)
    df = pd.read_excel('data.xlsx', skiprows=5)
    
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        for index, row in df.iterrows():
            crop_query = """
                SELECT crop_sm_uid 
                FROM jadxdb2.crop_sm 
                WHERE dsply_nm = %s
            """
            dsply_nm = row.iloc[1]  # B열
            
            if pd.notna(dsply_nm) and str(dsply_nm).strip():
                cur.execute(crop_query, (dsply_nm,))
                result = cur.fetchone()
                
                if result:
                    crop_sm_uid = result[0]
                    soil_chemical_uid = generate_soil_uid()
                    reg_uid = "administrator"
                    
                    # 각 컬럼의 값을 가져오되, None이 아닌 값만 포함
                    columns = []
                    values = []
                    params = []
                    
                    # 컬럼과 값 매핑 (D~L열)
                    field_pairs = [
                        ('ph_min', 'ph_max', row.iloc[3]),      # D열: pH
                        ('ec_min', 'ec_max', row.iloc[4]),      # E열: EC
                        ('no3n_min', 'no3n_max', row.iloc[5]),  # F열: NO3-N
                        ('om_min', 'om_max', row.iloc[6]),      # G열: 유기물
                        ('avp2o5_min', 'avp2o5_max', row.iloc[7]), # H열: P2O5
                        ('k_min', 'k_max', row.iloc[8]),        # I열: K
                        ('ca_min', 'ca_max', row.iloc[9]),      # J열: Ca
                        ('mg_min', 'mg_max', row.iloc[10]),     # K열: Mg
                        ('cec_min', 'cec_max', row.iloc[11])    # L열: CEC
                    ]
                    
                    # 각 필드 쌍에 대해 min, max 값 추출
                    for min_field, max_field, value in field_pairs:
                        min_val, max_val = parse_range_value(value)
                        if min_val is not None:
                            columns.extend([min_field, max_field])
                            values.extend(['%s', '%s'])
                            params.extend([min_val, max_val])
                    
                    # 기본 필드 추가
                    columns.extend(['soil_chemical_uid', 'crop_sm_uid', 'reg_uid'])
                    values.extend(['%s', '%s', '%s'])
                    params.extend([soil_chemical_uid, crop_sm_uid, reg_uid])
                    
                    # 동적 쿼리 생성
                    insert_query = f"""
                        INSERT INTO jadxdb2.soil_chemical 
                        ({', '.join(columns)}, reg_dt)
                        VALUES ({', '.join(values)}, CURRENT_TIMESTAMP)
                    """
                    
                    cur.execute(insert_query, params)
                else:
                    print(f"'{dsply_nm}' 에 해당하는 crop_sm_uid를 찾을 수 없습니다.")
            
        conn.commit()
        print("토양 데이터 삽입이 완료되었습니다.")
        
    except Exception as e:
        print(f"에러가 발생했습니다: {str(e)}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # insert_nutrient_data()
    # insert_fertilizer_data()
    insert_soil_data()

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
    tbl = "frtlzr_lst"
    parts = tbl.split('_')
    uid_prefix = ''.join(part[0].upper() for part in parts if part)  # FNI
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"
    return unique_uid

def insert_fertilizer_data():
    # 엑셀 파일 읽기
    df = pd.read_excel('비료DB통합작업.xlsx')
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # CASCADE 옵션을 사용하여 참조 테이블의 데이터도 함께 삭제
        truncate_query = sql.SQL("TRUNCATE TABLE jadxdb2.frtlzr_lst CASCADE")
        cursor.execute(truncate_query)
        print("기존 데이터가 삭제되었습니다.")
        
        for _, row in df.iterrows():
            uid = generate_unique_uid()
            current_time = datetime.datetime.now()
            
            # source1, source2, source3 값 설정
            # source 컬럼의 값을 source1에 설정
            source1 = row['source'] if pd.notna(row['source']) else None
            # jadx 컬럼의 값을 source2에 설정
            source2 = row['jadx'] if pd.notna(row['jadx']) else None
            # 농협 컬럼의 값을 source3에 설정
            source3 = row['농협'] if pd.notna(row['농협']) else None
            
            # SQL 쿼리 작성
            insert_query = sql.SQL("""
                INSERT INTO jadxdb2.frtlzr_lst (
                    frtlzr_lst_uid, frtlzr_nm, ntrgn, phsphrs, ptssm, 
                    kg_per_sack, source1, source2, source3, reg_uid,
                    reg_dt
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            # 데이터 삽입
            cursor.execute(insert_query, (
                uid,  # frtlzr_lst_uid
                row['frtlzr_nm'],
                row['ntrgn'],
                row['phsphrs'],
                row['ptssm'],
                row['kg_per_sack'],
                source1,
                source2,
                source3,
                'administrator',  # reg_uid
                current_time  # reg_dt
            ))
        
        conn.commit()
        print("새로운 데이터 삽입이 완료되었습니다.")
        
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()

def process_nh_fertilizer_data():
    # 엑셀 파일 읽기 - NH 시트
    df = pd.read_excel('비료DB통합작업.xlsx', sheet_name='NH')
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            product_name = row['상품명']
            
            # 기존 데이터 확인
            check_query = sql.SQL("""
                SELECT frtlzr_lst_uid FROM jadxdb2.frtlzr_lst 
                WHERE frtlzr_nm = %s
            """)
            cursor.execute(check_query, (product_name,))
            existing_record = cursor.fetchone()
            
            # 성분 파싱 함수
            def parse_components(component_str):
                # 불필요한 문자 제거 (괄호류)
                clean_str = component_str.replace('[', '').replace(']', '')\
                                      .replace('(', '').replace(')', '')\
                                      .replace('<', '').replace('>', '')
                
                # 하이픈으로 분리
                parts = clean_str.split('-')
                
                # 첫 세 개의 숫자만 추출
                numbers = []
                for part in parts:
                    # + 기호가 있으면 그 전까지만 사용
                    if '+' in part:
                        part = part.split('+')[0]
                    
                    # 숫자만 추출 (소수점 포함)
                    num = ''.join(c for c in part if c.isdigit() or c == '.')
                    if num:
                        try:
                            numbers.append(float(num))
                            if len(numbers) == 3:  # 첫 세 개의 숫자만 사용
                                break
                        except ValueError:
                            continue
                
                # 세 개의 값이 없는 경우 None으로 채우기
                while len(numbers) < 3:
                    numbers.append(None)
                
                return numbers[0], numbers[1], numbers[2]
            
            if existing_record:
                # 기존 레코드 업데이트
                update_query = sql.SQL("""
                    UPDATE jadxdb2.frtlzr_lst 
                    SET price = %s 
                    WHERE frtlzr_nm = %s
                """)
                cursor.execute(update_query, (row['과세'], product_name))
                print(f"가격 업데이트 완료: {product_name}")
            
            else:
                # 새 레코드 추가
                uid = generate_unique_uid()
                current_time = datetime.datetime.now()
                
                # 성분 파싱
                ntrgn, phsphrs, ptssm = parse_components(row['성분'])
                
                insert_query = sql.SQL("""
                    INSERT INTO jadxdb2.frtlzr_lst (
                        frtlzr_lst_uid, frtlzr_nm, ntrgn, phsphrs, ptssm,
                        kg_per_sack, source3, price, reg_uid, reg_dt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """)
                
                cursor.execute(insert_query, (
                    uid,
                    product_name,
                    ntrgn,
                    phsphrs,
                    ptssm,
                    row['단량(kg)'],
                    'NH',
                    row['과세'],
                    'administrator',
                    current_time
                ))
                print(f"새로운 데이터 추가 완료: {product_name}")
        
        conn.commit()
        print("NH 데이터 처리가 완료되었습니다.")
        
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # insert_fertilizer_data()
    process_nh_fertilizer_data()
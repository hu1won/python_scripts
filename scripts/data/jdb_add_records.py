# jdb_add_records.py
import itertools
import time
from dotenv import load_dotenv
import os
import psycopg2

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

table_name = os.getenv('TEMP_TABLE_NAME')

def print_db_contents():
    try:
        # 데이터베이스에 연결
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # 데이터베이스의 내용을 가져오는 쿼리 실행
        cursor.execute(f"SELECT * FROM {table_name};")
        records = cursor.fetchall()
        
        # 결과 출력
        for record in records:
            print(record)
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # 연결 종료
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            
def insert_unique_records():
    try:
        # 데이터베이스에 연결
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # 조합 생성
        years = range(2020, 2024)
        regions = ['JEJU', 'SEOGWIPO']
        group_numbers = range(2, 9) 
        job_cycles = [1, 2, 3]
        
        # 중복 없는 조합 생성
        combinations = itertools.product(years, regions, group_numbers, job_cycles)
        
        # 데이터 삽입
        for year, region, group_no, job_cycl in combinations:
            cursor.execute(
                f"INSERT INTO {table_name} (crtr_yr, obsrvn_group_rgn_cd, obsrvn_group_no, job_cycl, reg_uid) VALUES (%s, %s, %s, %s, %s)",
                (year, region, group_no, job_cycl, 'administrator')
            )
        
        # 변경 사항 커밋
        connection.commit()
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # 연결 종료
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            
def insert_records_with_examiners():
    try:
        # 데이터베이스에 연결
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # 조합 생성
        years = range(2018, 2024)  # 2018부터 2023까지
        job_cycles = [1, 2, 3]  # job_cycl에 1, 2, 3만 포함
        exmnr_names = [f'조사원{i}' for i in range(1, 101)]
        
        # 중복 없는 조합 생성
        combinations = itertools.product(years, job_cycles, exmnr_names)
        
        # 데이터 삽입
        for year, job_cycl, exmnr_nm in combinations:
            try:
                # 중복 검사 (crtr_yr를 문자열로 변환)
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE crtr_yr = %s AND job_cycl = %s AND exmnr_nm = %s",
                    (str(year), job_cycl, exmnr_nm)  # year를 문자열로 변환
                )
                count = cursor.fetchone()[0]
                
                if count == 0:  # 중복이 없을 경우에만 삽입
                    cursor.execute(
                        f"INSERT INTO {table_name} (crtr_yr, job_cycl, exmnr_nm, reg_uid) VALUES (%s, %s, %s, %s)",
                        (str(year), job_cycl, exmnr_nm, 'administrator')  # year를 문자열로 변환
                    )
                    connection.commit()  # 각 데이터 삽입 후 즉시 커밋
                    time.sleep(0.01)
                    print(f"Inserted {year}, {job_cycl}, {exmnr_nm}")
                else:
                    print(f"Duplicate entry for {year}, {job_cycl}, {exmnr_nm} not inserted.")
                    
            except Exception as e:
                print(f"Error during insert for {year}, {job_cycl}, {exmnr_nm}: {e}")
                connection.rollback()  # 오류가 발생하면 롤백
    
    except Exception as e:
        print(f"Error: {e}")
        if connection:
            connection.rollback()  # 전체 트랜잭션 실패 시 롤백
    
    finally:
        # 연결 종료
        if cursor:
            cursor.close()
        if connection:
            connection.close()

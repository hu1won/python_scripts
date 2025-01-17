import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
import hashlib
import random

load_dotenv()

db_name = os.getenv('LOCAL_DB_CONFIG_DBNAME')
db_user = os.getenv('LOCAL_DB_CONFIG_USER')
db_password = os.getenv('LOCAL_DB_CONFIG_PASSWORD')
db_host = os.getenv('LOCAL_DB_CONFIG_HOST')
db_port = os.getenv('LOCAL_DB_CONFIG_PORT')

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
    tbl = "obsrvn_exmn_lst"  # 고정된 tbl 값
    parts = tbl.split('_')[1:6]  # tbl의 두 번째 요소부터 다섯 번째 요소까지 가져옴
    uid_prefix = ''.join(part[0].upper() for part in parts if part)  # 각 부분의 첫 글자를 대문자로 변환
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]  # 현재 시간을 포맷팅
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]  # 랜덤 값의 MD5 해시의 앞 4자리
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"  # 최종 UID 생성
    return unique_uid

# Excel 파일 읽기
def insert_data_from_excel(excel_file_path):
    conn = None
    cursor = None
    try:
        # Excel 파일 읽기 - skiprows=1 옵션을 추가하여 첫 번째 행을 건너뜁니다
        df = pd.read_excel(excel_file_path, skiprows=1)
        
        # 빈 값을 '알수없음'으로 대체
        df = df.fillna('알수없음')
        
        # DB 연결
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 데이터 삽입 쿼리
        insert_query = """
            INSERT INTO jadxdb2.obsrvn_exmn_lst 
            (obsrvn_exmn_lst_uid, crtr_yr, frmhs_addr, brdt, mbl_telno, frmhs_nm, exmn_psblty_yn, del_yn, reg_uid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # DataFrame의 각 행을 순회하며 데이터 삽입
        for index, row in df.iterrows():
            values = (
                generate_unique_uid(),  # obsrvn_exmn_lst_uid
                '2024',                # crtr_yr
                row.iloc[4],           # frmhs_addr (E열 = 5번째 열)
                row.iloc[5],           # brdt (F열 = 6번째 열)
                row.iloc[6],           # mbl_telno (G열 = 7번째 열)
                row.iloc[2],           # frmhs_nm (C열 = 3번째 열)
                'Y',                   # exmn_psblty_yn
                'N',                   # del_yn
                'administrator'        # reg_uid
            )
            
            cursor.execute(insert_query, values)
        
        # 변경사항 저장
        conn.commit()
        print("데이터 삽입이 완료되었습니다.")
        
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        if conn:
            conn.rollback()
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 함수 실행
if __name__ == "__main__":
    # Mac의 문서 폴더에 있는 list2.xlsx 파일 경로 지정
    excel_file_path = os.path.expanduser("~/Documents/list2024.xlsx")
    insert_data_from_excel(excel_file_path)


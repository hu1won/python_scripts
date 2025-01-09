import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import pandas as pd

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

# Excel 파일 읽기
def insert_data_from_excel(excel_file_path):
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
            INSERT INTO obsrvn_exmn_lst 
            (crtr_yr, frmhs_addr, brdt, mbl_telno, frmhs_nm, exmn_psblty_yn, del_yn, reg_uid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # DataFrame의 각 행을 순회하며 데이터 삽입
        for index, row in df.iterrows():
            values = (
                '2025',                # crtr_yr
                row['E'],             # frmhs_addr (E열)
                row['F'],             # brdt (F열)
                row['G'],             # mbl_telno (G열)
                row['C'],             # frmhs_nm (C열)
                'Y',                  # exmn_psblty_yn
                'N',                  # del_yn
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
    excel_file_path = "your_excel_file.xlsx"  # Excel 파일 경로를 지정해주세요
    insert_data_from_excel(excel_file_path)


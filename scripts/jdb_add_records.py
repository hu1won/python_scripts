# jdb_add_records.py
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
            
print_db_contents()
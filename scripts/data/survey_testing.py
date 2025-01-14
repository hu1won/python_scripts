import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

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

def insert_test_data():
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # obsrvn_exmn_lst 테이블에 50개의 데이터 삽입
        inserted_uids = []
        for i in range(1, 51):
            cur.execute("""
                INSERT INTO jadxdb2.obsrvn_exmn_lst 
                (crtr_yr, brdt, mbl_telno, exmn_psblty_yn, del_yn, reg_uid, frmhs_nm)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING obsrvn_exmn_lst_uid
            """, ('2024', '20250113', '01012345678', 'Y', 'N', 'test', f'테스트{i}'))
            
            uid = cur.fetchone()[0]
            inserted_uids.append(uid)
        
        # obsrvn_exmn 테이블에 데이터 삽입
        for uid in inserted_uids:
            cur.execute("""
                INSERT INTO jadxdb2.obsrvn_exmn 
                (obsrvn_exmn_lst_uid, obsrvn_scnd_qlty_exmn_hrvst_yn, 
                obsrvn_trd_qlty_exmn_hrvst_yn, scnd_qlty_exmn_yn, 
                trd_qlty_exmn_yn, reg_uid)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (uid, 'N', 'N', 'N', 'N', 'test'))
        
        conn.commit()
        print("데이터 삽입이 완료되었습니다.")
        
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    insert_test_data()


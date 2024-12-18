from datetime import datetime
import hashlib
import itertools
import random
import time
from dotenv import load_dotenv
import os
from psycopg2 import sql
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

# 데이터베이스 연결 설정
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# obsrvn_exmn_lst 테이블에서 모든 pk 값을 가져오기
cur.execute("SELECT obsrvn_exmn_lst_uid FROM jadxdb2.obsrvn_exmn_lst")
obsrvn_exmn_lst_uids = cur.fetchall()

def generate_unique_uid():
    # UID 생성 로직
    tbl = "obsrvn_exmn_lst"  # 고정된 tbl 값
    parts = tbl.split('_')[1:6]  # tbl의 두 번째 요소부터 다섯 번째 요소까지 가져옴
    uid_prefix = ''.join(part[0].upper() for part in parts if part)  # 각 부분의 첫 글자를 대문자로 변환
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]  # 현재 시간을 포맷팅
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]  # 랜덤 값의 MD5 해시의 앞 4자리
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"  # 최종 UID 생성
    return unique_uid

# obsrvn_exmn 테이블에 데이터 삽입
insert_query = sql.SQL("""
    INSERT INTO jadxdb2.obsrvn_exmn (
        obsrvn_exmn_uid,
        obsrvn_exmn_lst_uid, 
        obsrvn_scnd_qlty_exmn_hrvst_yn, 
        obsrvn_trd_qlty_exmn_hrvst_yn, 
        scnd_qlty_exmn_yn, 
        trd_qlty_exmn_yn, 
        reg_uid
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

for uid in obsrvn_exmn_lst_uids:
    cur.execute(insert_query, (generate_unique_uid(), uid[0], 'Y', 'Y', 'Y', 'Y', 'administrator'))

# 변경사항 커밋
conn.commit()

# 연결 종료
cur.close()
conn.close()
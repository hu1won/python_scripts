import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
from sqlalchemy.exc import OperationalError
import hashlib
import random
from datetime import datetime

# .env 파일에서 환경 변수 로드
load_dotenv()

# 데이터베이스 연결 정보 설정
source_db_config = {
    'dbname': os.getenv('SOURCE_DB_CONFIG_DBNAME'),
    'user': os.getenv('SOURCE_DB_CONFIG_USER'),
    'password': os.getenv('SOURCE_DB_CONFIG_PASSWORD'),
    'host': os.getenv('SOURCE_DB_CONFIG_HOST'),
    'port': os.getenv('SOURCE_DB_CONFIG_PORT'),
}

target_db_config = {
    'dbname': os.getenv('TARGET_DB_CONFIG_DBNAME'),
    'user': os.getenv('TARGET_DB_CONFIG_USER'),
    'password': os.getenv('TARGET_DB_CONFIG_PASSWORD'),
    'host': os.getenv('TARGET_DB_CONFIG_HOST'),
    'port': os.getenv('TARGET_DB_CONFIG_PORT'),
}

# 데이터베이스 URL 생성 (PostgreSQL)
source_db_url = f"postgresql+psycopg2://{source_db_config['user']}:{source_db_config['password']}@{source_db_config['host']}:{source_db_config['port']}/{source_db_config['dbname']}"
target_db_url = f"postgresql+psycopg2://{target_db_config['user']}:{target_db_config['password']}@{target_db_config['host']}:{target_db_config['port']}/{target_db_config['dbname']}"

# 데이터베이스 엔진 생성
print("Creating database engines...")
source_engine = create_engine(source_db_url, pool_pre_ping=True, connect_args={'connect_timeout': 10})
target_engine = create_engine(target_db_url, pool_pre_ping=True, connect_args={'connect_timeout': 10})
print("Database engines created successfully.")

# 데이터 읽기 및 재시도 로직
max_retries = 3
for attempt in range(max_retries):
    try:
        print("Reading data from tobsrvn_exmn_lst table...")
        tobsrvn_df = pd.read_sql('SELECT crtr_yr, frmlnd_addr, brdt, mbl_telno, home_telno, dpst_actno, dlng_bank_nm, cert_frmhs_nm, exmn_psblty_yn FROM tobsrvn_exmn_lst', source_engine)
        print(f"Data read successfully. Number of records: {len(tobsrvn_df)}")
        break  # 성공적으로 읽으면 루프 종료
    except OperationalError as e:
        print(f"Attempt {attempt + 1} failed: {e}")
        if attempt < max_retries - 1:
            print("Retrying...")
            time.sleep(2)  # 2초 대기 후 재시도
        else:
            raise  # 마지막 시도에서 실패하면 예외를 다시 발생시킴
        
def generate_unique_uid():
    # UID 생성 로직
    tbl = "obsrvn_exmn_lst"  # 고정된 tbl 값
    parts = tbl.split('_')[1:6]  # tbl의 두 번째 요소부터 다섯 번째 요소까지 가져옴
    uid_prefix = ''.join(part[0].upper() for part in parts if part)  # 각 부분의 첫 글자를 대문자로 변환
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]  # 현재 시간을 포맷팅
    random_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:4]  # 랜덤 값의 MD5 해시의 앞 4자리
    unique_uid = f"{uid_prefix}_{timestamp}{random_suffix}"  # 최종 UID 생성
    return unique_uid

# 필요한 열만 선택하고 매핑
print("Mapping data to jadxdb2.obsrvn_exmn_lst format...")
obsrvn_df = pd.DataFrame({
    'obsrvn_exmn_lst_uid': generate_unique_uid(),  # UID 생성 로직 추가
    'crtr_yr': tobsrvn_df['crtr_yr'],
    'frmhs_addr': tobsrvn_df['frmlnd_addr'].fillna('Unknown'),  # frmhs_addr에 frmlnd_addr 매핑
    'brdt': tobsrvn_df['brdt'],
    'mbl_telno': tobsrvn_df['mbl_telno'],
    'home_telno': tobsrvn_df['home_telno'],
    'actno': tobsrvn_df['dpst_actno'],  # actno에 dpst_actno 매핑
    'bank_cd': tobsrvn_df['dlng_bank_nm'],  # bank_cd에 dlng_bank_nm 매핑
    'frmhs_nm': tobsrvn_df['cert_frmhs_nm'].fillna('Unknown'),  # frmhs_nm에 korn_flnm 매핑, None인 경우 "Unknown"으로 대체
    'exmn_psblty_yn': tobsrvn_df['exmn_psblty_yn'],
    'vrty_nm': None,  # vrty_nm은 None으로 설정 (필요시 수정)
    'tree_age': None,  # tree_age는 None으로 설정 (필요시 수정)
    'del_yn': 'N',  # del_yn에 "N" 설정
    'sgg': None,  # sgg는 None으로 설정 (필요시 수정)
    'emd': None,  # emd는 None으로 설정 (필요시 수정)
    'stli': None,  # stli는 None으로 설정 (필요시 수정)
    'reg_uid': 'administrator'  # reg_uid에 "administrator" 설정
})

print("Data mapping completed.")
print(obsrvn_df.head())  # 데이터프레임의 첫 5개 행 출력

# 중복 확인 후 UID 생성
def check_and_generate_uid(existing_uids):
    uid = generate_unique_uid()
    while uid in existing_uids:
        uid = generate_unique_uid()  # 중복된 UID가 있다면 다시 생성
    return uid

# 데이터프레임에 UID 생성
existing_uids = set()  # 기존에 있는 UID 목록을 저장
for idx, row in obsrvn_df.iterrows():
    new_uid = check_and_generate_uid(existing_uids)
    obsrvn_df.at[idx, 'obsrvn_exmn_lst_uid'] = new_uid
    existing_uids.add(new_uid)

# 데이터를 테이블에 삽입
obsrvn_df.to_sql('obsrvn_exmn_lst', target_engine, schema='jadxdb2', if_exists='append', index=False)

# obsrvn_df에 UID 생성 로직 추가
# obsrvn_df['obsrvn_exmn_lst_uid'] = [generate_unique_uid() for _ in range(len(obsrvn_df))]

# obsrvn_exmn_lst 테이블에 데이터 쓰기
print("Writing data to jadxdb2.obsrvn_exmn_lst table...")
# batch_size = 4  # 배치 크기를 4로 설정
# num_batches = len(obsrvn_df) // batch_size + (1 if len(obsrvn_df) % batch_size > 0 else 0)

# with target_engine.begin() as connection:  # 트랜잭션 시작
#     for i in range(num_batches):
#         batch_df = obsrvn_df[i * batch_size:(i + 1) * batch_size]  # 배치 데이터 선택
#         try:
#             batch_df.to_sql('obsrvn_exmn_lst', connection, schema='jadxdb2', if_exists='append', index=False)
#             print(f"Batch {i + 1}/{num_batches} written successfully.")
#         except Exception as e:
#             print(f"Error writing batch {i + 1}: {e}")
#             time.sleep(5)  # 대기 시간 증가
#             continue  # 다음 배치로 넘어감
#         time.sleep(2)  # 2초 대기 후 다음 배치 처리

print("Data write completed successfully.")

print("Data dump completed successfully.")

# crtr_yr
# group_id
# group_sn
# cert_frmhs_nm
# frmhs_bsc_addr
# frmlnd_addr
# prcl_unq_no
# frtr_cltvar
# brdt
# mbl_telno
# dlng_bank_nm
# dpst_actno
# home_telno
# korn_flnm
# bgng_yr
# end_yr
# agre_ymd
# exmn_psblty_yn


# crtr_yr
# frmhs_addr
# brdt
# mbl_telno
# home_telno
# euse_telno
# actno
# bank_cd
# frmhs_nm
# exmn_psblty_yn
# vrty_nm
# tree_age
# del_yn
# sgg
# emd
# stli
# reg_uid
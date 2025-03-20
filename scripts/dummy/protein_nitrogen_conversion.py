import os
from dotenv import load_dotenv
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 환경 변수 로드
load_dotenv()

# 데이터베이스 연결 설정
db_name = os.getenv('SURVER_DB_CONFIG_DBNAME')
db_user = os.getenv('SURVER_DB_CONFIG_USER')
db_password = os.getenv('SURVER_DB_CONFIG_PASSWORD')
db_host = os.getenv('SURVER_DB_CONFIG_HOST')
db_port = os.getenv('SURVER_DB_CONFIG_PORT')

# 데이터베이스 연진 문자열 생성
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# 데이터베이스 엔진 생성
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()

try:
    # food_category 테이블에서 특정 crop_sm_uid를 가진 데이터 조회 및 업데이트
    query = """
    UPDATE food_category 
    SET ntrgn = (ntrgn / 1000 / 6.25)  -- g를 kg으로 변환(1000으로 나눔)하고 6.25로 나눔
    WHERE crop_sm_uid = 'CROPSM_6CA61FFC'
    """
    
    session.execute(query)
    session.commit()
    print("데이터 업데이트가 완료되었습니다.")

except Exception as e:
    session.rollback()
    print(f"오류가 발생했습니다: {str(e)}")

finally:
    session.close()


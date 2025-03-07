import sys
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

DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# 현재 디렉토리 설정
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, "data")

def generate_unique_uid(prefix):
    """고유 ID 생성 함수"""
    return f"{prefix}_{int(time.time() * 1000)}"

class FoodNutrInfo:
    """식품 영양 정보 모델"""
    def __init__(self, food_nutr_info_uid, food_nm, ntrgn, phsphrs, ptssm):
        self.food_nutr_info_uid = food_nutr_info_uid
        self.food_nm = food_nm
        self.ntrgn = ntrgn
        self.phsphrs = phsphrs
        self.ptssm = ptssm

def process_food_data():
    """식품 영양 정보 데이터 처리 및 저장"""
    file_name = "국가표준식품성분 Database 10.2 - 공개용_VF.xlsx"
    
    # 엑셀 파일 읽기
    df = pd.read_excel(
        os.path.join(DATA_DIR, file_name), 
        sheet_name="국가표준식품성분 Database 10.2", 
        header=2, 
        usecols="D, H, Y, Z"
    )

    # 컬럼명 매핑
    column_mapping = {
        "Unnamed: 3": "food_nm",
        "g.1": "ntrgn",
        "mg.3": "phsphrs",
        "mg.4": "ptssm",
    }
    df = df.rename(columns=column_mapping)

    # 데이터 전처리
    for col in ["phsphrs", "ptssm", "ntrgn"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["phsphrs", "ptssm", "ntrgn"])

    # 단위 변환
    df["ntrgn"] = df["ntrgn"] / 1000
    df["phsphrs"] = df["phsphrs"] / (1000 * 1000)
    df["ptssm"] = df["ptssm"] / (1000 * 1000)

    # 데이터베이스 저장
    batch_size = 1000
    food_nutr_info_list = []
    n_params = {
        # 필요한 n_params 딕셔너리 정의
    }

    for i, row in df.iterrows():
        food_nm = row["food_nm"]
        phsphrs = row["phsphrs"]
        ptssm = row["ptssm"]

        # n_param 계산
        n_param = 6.25  # 기본값
        name_split = [name.strip() for name in food_nm.split(",")]
        for name in name_split:
            if name in n_params:
                n_param = n_params[name]
                break
        
        ntrgn = row["ntrgn"] / n_param

        food_nutr_info = FoodNutrInfo(
            food_nutr_info_uid=generate_unique_uid("food_nutr_info"),
            food_nm=food_nm,
            ntrgn=ntrgn,
            phsphrs=phsphrs,
            ptssm=ptssm,
        )
        food_nutr_info_list.append(food_nutr_info)

        if len(food_nutr_info_list) >= batch_size:
            print(f"Inserting {i} rows / {len(df)} rows")
            session.add_all(food_nutr_info_list)
            session.commit()
            food_nutr_info_list = []

    # 남은 데이터 처리
    if food_nutr_info_list:
        session.add_all(food_nutr_info_list)
        session.commit()

if __name__ == "__main__":
    process_food_data()

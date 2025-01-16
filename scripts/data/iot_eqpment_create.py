from dotenv import load_dotenv
import os
import pandas as pd
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

def insert_iot_eqpment():
    pass

def aws_insert_iot_eqpment():
    try:
        # CSV 파일 읽기
        df = pd.read_csv('aws_20250108 (1).csv')
        print("\n=== CSV 파일 구조 ===")
        print(df.head())
        print("\n=== 컬럼 목록 ===")
        print(df.columns.tolist())
        
        # 데이터베이스 연결
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # INSERT 쿼리 수정 (reg_uid 추가)
        insert_query = """
            INSERT INTO jadxdb2.snsr_eqpmnt 
            (snsr_uid, snsr_type, snsr_eqpmnt_nm, addr, reg_uid) 
            VALUES (%s, %s, %s, %s, %s)
        """
        
        print("\n=== 삽입될 데이터 샘플 ===")
        inserted_count = 0
        
        # 데이터 삽입
        for _, row in df.iterrows():
            values = (
                str(row['우편번호']),  # snsr_uid
                'AWS',                # snsr_type
                row['표출명'],         # snsr_eqpmnt_nm
                row['주소2'],         # addr
                'administrator'       # reg_uid
            )
            
            # 처음 5개 행 출력
            if _ < 5:
                print(f"데이터: {values}")
            
            # DB에 데이터 삽입
            cur.execute(insert_query, values)
            inserted_count += 1
        
        # 변경사항 저장 및 연결 종료
        conn.commit()
        cur.close()
        conn.close()
        
        return {"message": f"AWS 데이터 삽입 완료. 총 {inserted_count}개 행이 삽입되었습니다."}
    
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        return {"error": f"데이터 삽입 중 오류 발생: {str(e)}"}

# 함수 실행
if __name__ == "__main__":
    result = aws_insert_iot_eqpment()
    print("\n=== 실행 결과 ===")
    print(result)
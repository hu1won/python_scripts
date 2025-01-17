import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# .env 파일에서 환경 변수 로드
load_dotenv()

# 데이터베이스 연결 정보 설정
source_db_config = {
    'dbname': os.getenv('LOCAL_DB_CONFIG_DBNAME'),
    'user': os.getenv('LOCAL_DB_CONFIG_USER'),
    'password': os.getenv('LOCAL_DB_CONFIG_PASSWORD'),
    'host': os.getenv('LOCAL_DB_CONFIG_HOST'),
    'port': os.getenv('LOCAL_DB_CONFIG_PORT'),
}

target_db_config = {
    'dbname': os.getenv('SURVER_DB_CONFIG_DBNAME'),
    'user': os.getenv('SURVER_DB_CONFIG_USER'),
    'password': os.getenv('SURVER_DB_CONFIG_PASSWORD'),
    'host': os.getenv('SURVER_DB_CONFIG_HOST'),
    'port': os.getenv('SURVER_DB_CONFIG_PORT'),
}

def transfer_table_data():
    try:
        # 소스 DB 연결
        source_conn = psycopg2.connect(**source_db_config)
        source_cur = source_conn.cursor()
        
        # 타겟 DB 연결
        target_conn = psycopg2.connect(**target_db_config)
        target_cur = target_conn.cursor()
        
        print("1. DB 연결 성공")
        
        try:
            # CASCADE 옵션으로 한 번에 처리
            target_cur.execute("TRUNCATE TABLE jadxdb2.snsr_eqpmnt CASCADE")
            print("2. 테이블들 비우기 성공")
            
            # 소스 테이블에서 데이터 가져오기
            source_cur.execute("SELECT * FROM jadxdb2.snsr_eqpmnt")
            rows = source_cur.fetchall()
            print(f"4. 소스에서 {len(rows)}개의 데이터 조회 성공")
            
            # 컬럼 정보 가져오기
            source_cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'jadxdb2' AND table_name = 'snsr_eqpmnt' ORDER BY ordinal_position")
            columns = [col[0] for col in source_cur.fetchall()]
            print(f"5. 컬럼 정보 가져오기 성공: {columns}")
            
            # 데이터 삽입을 위한 SQL 문 생성
            insert_sql = sql.SQL("INSERT INTO jadxdb2.snsr_eqpmnt ({}) VALUES ({})").format(
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            print("6. INSERT SQL 생성 성공")
            
            # 데이터 삽입
            target_cur.executemany(insert_sql, rows)
            print("7. 데이터 삽입 성공")
            
            # 변경사항 커밋
            target_conn.commit()
            print(f"8. 커밋 성공 - 총 {len(rows)}개의 레코드를 전송했습니다.")
            
        except Exception as e:
            print(f"상세 에러 발생 위치 확인: {str(e)}")
            raise e
            
    except Exception as e:
        print(f"최종 에러 발생: {str(e)}")
        if 'target_conn' in locals():
            target_conn.rollback()
            
    finally:
        # 연결 종료
        if 'source_cur' in locals():
            source_cur.close()
        if 'target_cur' in locals():
            target_cur.close()
        if 'source_conn' in locals():
            source_conn.close()
        if 'target_conn' in locals():
            target_conn.close()

if __name__ == "__main__":
    transfer_table_data()


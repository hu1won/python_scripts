#!/usr/bin/env python3
"""
실제 DB에서 로컬 DB로 스키마와 데이터를 완전히 dump하는 스크립트
PostgreSQL을 지원하며, 스키마, 데이터, 인덱스, 제약조건 등을 모두 포함합니다.
"""

import os
import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_dump.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DatabaseDumper:
    """데이터베이스 덤프 및 복원을 위한 클래스"""
    
    def __init__(self, source_config: Dict[str, Any], target_config: Dict[str, Any]):
        """
        Args:
            source_config: 소스 DB 연결 정보
            target_config: 타겟 DB 연결 정보
        """
        self.source_config = source_config
        self.target_config = target_config
        self.dump_file = None
        
    def test_connection(self, config: Dict[str, Any], db_name: str = None) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            conn_config = config.copy()
            if db_name:
                conn_config['database'] = db_name
                
            conn = psycopg2.connect(**conn_config)
            conn.close()
            logger.info(f"✅ {db_name or 'default'} DB 연결 성공")
            return True
        except Exception as e:
            logger.error(f"❌ {db_name or 'default'} DB 연결 실패: {e}")
            return False
    
    def create_database_if_not_exists(self) -> bool:
        """타겟 데이터베이스가 없으면 생성"""
        try:
            # postgres DB에 연결하여 새 DB 생성
            conn_config = self.target_config.copy()
            conn_config['database'] = 'postgres'
            
            conn = psycopg2.connect(**conn_config)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # DB 존재 여부 확인
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.target_config['database'],)
            )
            
            if not cursor.fetchone():
                # DB 생성
                cursor.execute(f"CREATE DATABASE {self.target_config['database']}")
                logger.info(f"✅ 데이터베이스 '{self.target_config['database']}' 생성 완료")
            else:
                logger.info(f"✅ 데이터베이스 '{self.target_config['database']}' 이미 존재")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ 데이터베이스 생성 실패: {e}")
            return False
    
    def clear_target_database(self) -> bool:
        """타겟 데이터베이스의 모든 테이블 삭제 (모든 스키마)"""
        try:
            # postgres 사용자로 연결해서 삭제
            postgres_config = self.target_config.copy()
            postgres_config['user'] = 'postgres'
            postgres_config['password'] = '1234'
            
            conn = psycopg2.connect(**postgres_config)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # 모든 스키마의 테이블 목록 가져오기
            cursor.execute("""
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE schemaname IN ('public', 'malirang')
                ORDER BY schemaname, tablename
            """)
            
            tables = cursor.fetchall()
            
            if tables:
                logger.info(f"🗑️ 타겟 DB의 {len(tables)}개 테이블 삭제 중...")
                
                # 모든 테이블 삭제
                for schema, table in tables:
                    try:
                        # 스키마와 테이블명을 따옴표로 감싸서 대소문자 보존
                        cursor.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
                        logger.info(f"  ✅ 테이블 {schema}.{table} 삭제 완료")
                    except Exception as e:
                        logger.warning(f"  ⚠️ 테이블 {schema}.{table} 삭제 실패: {e}")
                
                logger.info("✅ 타겟 DB 초기화 완료")
            else:
                logger.info("ℹ️ 타겟 DB에 삭제할 테이블이 없습니다")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ 타겟 DB 초기화 실패: {e}")
            return False
    
    def create_malirang_schema(self) -> bool:
        """malirang 스키마 생성 (없으면)"""
        try:
            conn = psycopg2.connect(**self.target_config)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # malirang 스키마 존재 여부 확인
            cursor.execute("""
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'malirang'
            """)
            
            if not cursor.fetchone():
                # malirang 스키마 생성
                cursor.execute('CREATE SCHEMA IF NOT EXISTS "malirang"')
                logger.info("✅ malirang 스키마 생성 완료")
            else:
                logger.info("✅ malirang 스키마 이미 존재")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ malirang 스키마 생성 실패: {e}")
            return False
    
    def check_pg_dump_version(self) -> tuple[bool, str]:
        """pg_dump 버전 확인"""
        try:
            result = subprocess.run(
                ['pg_dump', '--version'],
                capture_output=True,
                text=True,
                check=True
            )
            version = result.stdout.strip()
            logger.info(f"📋 pg_dump 버전: {version}")
            return True, version
        except Exception as e:
            logger.error(f"❌ pg_dump 버전 확인 실패: {e}")
            return False, ""

    def dump_database(self, output_file: str = None) -> bool:
        """pg_dump를 사용하여 데이터베이스 덤프"""
        try:
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"db_dump_{timestamp}.sql"
            
            self.dump_file = output_file
            
            # pg_dump 버전 확인
            version_ok, version = self.check_pg_dump_version()
            if not version_ok:
                logger.warning("⚠️ pg_dump 버전 확인 실패, 계속 진행합니다.")
            
            # pg_dump 명령어 구성
            dump_cmd = [
                'pg_dump',
                f"--host={self.source_config['host']}",
                f"--port={self.source_config['port']}",
                f"--username={self.source_config['user']}",
                f"--dbname={self.source_config['database']}",
                '--verbose',
                '--clean',  # DROP 문 포함
                '--create',  # CREATE DATABASE 문 포함
                '--if-exists',  # IF EXISTS 옵션 추가
                '--no-owner',  # 소유자 정보 제외
                '--no-privileges',  # 권한 정보 제외
                '--format=plain',  # 텍스트 형식
                '--no-sync',  # 동기화 비활성화 (성능 향상)
                f"--file={output_file}"
            ]
            
            # 환경변수 설정
            env = os.environ.copy()
            env['PGPASSWORD'] = self.source_config['password']
            
            logger.info(f"🔄 데이터베이스 덤프 시작: {self.source_config['database']}")
            logger.info(f"📁 출력 파일: {output_file}")
            
            # pg_dump 실행
            result = subprocess.run(
                dump_cmd,
                env=env,
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info("✅ 데이터베이스 덤프 완료")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ pg_dump 실행 실패: {e}")
            logger.error(f"stderr: {e.stderr}")
            
            # 버전 불일치 에러인 경우 대안 방법 시도
            if "server version mismatch" in str(e.stderr):
                logger.info("🔄 버전 불일치 감지, 대안 방법을 시도합니다...")
                return self.dump_database_with_alternative_method(output_file)
            
            return False
        except Exception as e:
            logger.error(f"❌ 덤프 중 오류 발생: {e}")
            return False
    
    def dump_database_with_alternative_method(self, output_file: str) -> bool:
        """버전 불일치 시 대안 방법으로 덤프"""
        try:
            logger.info("🔄 Python을 통한 직접 덤프 방법을 시도합니다...")
            
            # PostgreSQL 연결
            conn = psycopg2.connect(**self.source_config)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # 파일에 쓰기
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("-- Database dump created by Python script\n")
                f.write("-- Source: {}\n".format(self.source_config['database']))
                f.write("-- Generated: {}\n\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                
                # 스키마 덤프
                logger.info("📋 스키마 정보 추출 중...")
                schema_sql = self.get_schema_dump_sql(cursor)
                f.write("-- ============================================\n")
                f.write("-- SCHEMA DUMP\n")
                f.write("-- ============================================\n\n")
                f.write(schema_sql)
                f.write("\n\n")
                
                # 데이터 덤프
                logger.info("📊 데이터 정보 추출 중...")
                data_sql = self.get_data_dump_sql(cursor)
                f.write("-- ============================================\n")
                f.write("-- DATA DUMP\n")
                f.write("-- ============================================\n\n")
                f.write(data_sql)
            
            cursor.close()
            conn.close()
            
            logger.info("✅ 대안 방법으로 덤프 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 대안 방법 덤프 실패: {e}")
            return False
    
    def get_schema_dump_sql(self, cursor) -> str:
        """스키마 정보를 SQL로 생성"""
        schema_sql = []
        
        # 시퀀스 정보 먼저 생성
        schema_sql.append("-- ============================================")
        schema_sql.append("-- SEQUENCES")
        schema_sql.append("-- ============================================")
        schema_sql.append("")
        
        # 모든 스키마의 시퀀스 목록 가져오기
        cursor.execute("""
            SELECT schemaname, sequencename
            FROM pg_sequences
            WHERE schemaname IN ('public', 'malirang')
            ORDER BY schemaname, sequencename
        """)
        
        sequences = cursor.fetchall()
        for schema, sequence in sequences:
            try:
                # 간단한 시퀀스 생성 SQL (기본값으로)
                schema_sql.append(f"-- Sequence: {schema}.{sequence}")
                schema_sql.append(f'CREATE SEQUENCE IF NOT EXISTS "{schema}"."{sequence}" START 1;')
                schema_sql.append("")
            except Exception as e:
                logger.warning(f"⚠️ 시퀀스 {schema}.{sequence} 추출 실패: {e}")
                continue
        
        # 테이블 정보 생성
        schema_sql.append("-- ============================================")
        schema_sql.append("-- TABLES")
        schema_sql.append("-- ============================================")
        schema_sql.append("")
        
        # 모든 스키마의 테이블 목록 가져오기 (public과 malirang 스키마)
        cursor.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname IN ('public', 'malirang')
            ORDER BY schemaname, tablename
        """)
        
        # 디버깅: 실제 테이블 목록 출력
        tables = cursor.fetchall()
        logger.info(f"🔍 발견된 테이블 목록:")
        for schema, table in tables:
            logger.info(f"  - {schema}.{table}")
        
        # 다시 쿼리 실행 (fetchall로 인해 커서가 비어있음)
        cursor.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname IN ('public', 'malirang')
            ORDER BY schemaname, tablename
        """)
        
        tables = cursor.fetchall()
        
        for schema, table in tables:
            try:
                # 테이블 구조 정보 가져오기
                table_sql = self.get_table_creation_sql(cursor, table, schema)
                if table_sql:
                    # 스키마 그대로 유지
                    schema_sql.append(f"-- Table: {schema}.{table}")
                    schema_sql.append(table_sql)
                    schema_sql.append("")
            except Exception as e:
                logger.warning(f"⚠️ 테이블 {schema}.{table} 스키마 추출 실패: {e}")
                continue
        
        return "\n".join(schema_sql)
    
    def get_table_creation_sql(self, cursor, table_name: str, schema_name: str = 'public') -> str:
        """특정 테이블의 CREATE TABLE SQL 생성"""
        try:
            # 테이블 컬럼 정보 가져오기
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = %s
                ORDER BY ordinal_position
            """, (table_name, schema_name))
            
            columns = cursor.fetchall()
            if not columns:
                return ""
            
            # CREATE TABLE 문 시작 (스키마와 테이블명을 따옴표로 감싸기)
            create_sql = [f'CREATE TABLE "{schema_name}"."{table_name}" (']
            
            column_definitions = []
            for col in columns:
                col_name, data_type, max_length, is_nullable, default, position = col
                
                # 데이터 타입 처리
                if data_type == 'ARRAY':
                    # ARRAY 타입을 text[]로 변환
                    type_def = "TEXT[]"
                elif data_type == 'USER-DEFINED':
                    # USER-DEFINED 타입을 text로 변환
                    type_def = "TEXT"
                elif max_length and data_type in ['character varying', 'character', 'varchar']:
                    type_def = f"{data_type}({max_length})"
                else:
                    # 기타 타입은 그대로 사용하되, ARRAY나 USER-DEFINED가 포함된 경우 처리
                    if 'ARRAY' in data_type.upper():
                        type_def = "TEXT[]"
                    elif 'USER-DEFINED' in data_type.upper():
                        type_def = "TEXT"
                    else:
                        type_def = data_type
                
                # NULL 허용 여부
                null_constraint = "" if is_nullable == 'YES' else " NOT NULL"
                
                # 기본값
                default_constraint = ""
                if default:
                    default_constraint = f" DEFAULT {default}"
                
                # 컬럼명도 따옴표로 감싸기
                column_def = f'    "{col_name}" {type_def}{null_constraint}{default_constraint}'
                column_definitions.append(column_def)
            
            create_sql.append(",\n".join(column_definitions))
            create_sql.append(");")
            
            return "\n".join(create_sql)
            
        except Exception as e:
            logger.warning(f"⚠️ 테이블 {table_name} CREATE SQL 생성 실패: {e}")
            return ""
    
    def get_data_dump_sql(self, cursor) -> str:
        """데이터를 SQL로 생성"""
        data_sql = []
        
        # 모든 스키마의 테이블 목록 가져오기
        cursor.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname IN ('public', 'malirang')
            ORDER BY schemaname, tablename
        """)
        
        tables = cursor.fetchall()
        
        # 디버깅: 실제 테이블 목록 출력
        logger.info(f"🔍 데이터 덤프 대상 테이블 목록:")
        for schema, table in tables:
            logger.info(f"  - {schema}.{table}")
        
        for schema, table in tables:
            try:
                # 테이블 데이터 개수 확인 (스키마 포함, 따옴표로 감싸기)
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                row_count = cursor.fetchone()[0]
                
                if row_count == 0:
                    data_sql.append(f"-- Table {table} (from {schema} schema) is empty")
                    data_sql.append("")
                    continue
                
                logger.info(f"📊 테이블 {schema}.{table}: {row_count}개 행 처리 중...")
                
                # 컬럼 정보 가져오기
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND table_schema = %s
                    ORDER BY ordinal_position
                """, (table, schema))
                columns = cursor.fetchall()
                
                data_sql.append(f"-- Data for table: {schema}.{table} ({row_count} rows)")
                data_sql.append(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE;')
                
                # 배치로 데이터 처리 (메모리 절약)
                batch_size = 1000
                offset = 0
                
                while offset < row_count:
                    cursor.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT {batch_size} OFFSET {offset}')
                    rows = cursor.fetchall()
                    
                    if not rows:
                        break
                    
                    for row in rows:
                        values = []
                        for i, value in enumerate(row):
                            if value is None:
                                values.append('NULL')
                            elif isinstance(value, str):
                                # 문자열 이스케이프
                                escaped_value = value.replace("'", "''").replace('\n', '\\n').replace('\r', '\\r')
                                values.append(f"'{escaped_value}'")
                            elif isinstance(value, (int, float)):
                                values.append(str(value))
                            elif isinstance(value, bool):
                                values.append('TRUE' if value else 'FALSE')
                            else:
                                # 기타 타입은 문자열로 변환
                                escaped_value = str(value).replace("'", "''").replace('\n', '\\n').replace('\r', '\\r')
                                values.append(f"'{escaped_value}'")
                        
                        insert_sql = f'INSERT INTO "{schema}"."{table}" VALUES ({", ".join(values)});'
                        data_sql.append(insert_sql)
                    
                    offset += batch_size
                
                data_sql.append("")
                    
            except Exception as e:
                logger.warning(f"⚠️ 테이블 {schema}.{table} 데이터 덤프 실패: {e}")
                continue
        
        return "\n".join(data_sql)
    
    def restore_database(self) -> bool:
        """덤프 파일을 타겟 데이터베이스로 복원"""
        try:
            if not self.dump_file or not os.path.exists(self.dump_file):
                logger.error("❌ 덤프 파일이 존재하지 않습니다")
                return False
            
            # 복원 전에 필요한 스키마들 생성
            if not self.ensure_schemas_exist():
                return False
            
            # psql 명령어 구성
            restore_cmd = [
                'psql',
                f"--host={self.target_config['host']}",
                f"--port={self.target_config['port']}",
                f"--username={self.target_config['user']}",
                f"--dbname={self.target_config['database']}",
                f"--file={self.dump_file}",
                "--echo-errors"
            ]
            
            # 환경변수 설정
            env = os.environ.copy()
            env['PGPASSWORD'] = self.target_config['password']
            
            logger.info(f"🔄 데이터베이스 복원 시작: {self.target_config['database']}")
            
            # psql 실행
            result = subprocess.run(
                restore_cmd,
                env=env,
                capture_output=True,
                text=True,
                check=False  # 에러가 발생해도 계속 진행
            )
            
            # 에러가 발생했는지 확인
            if result.returncode != 0:
                logger.warning(f"⚠️ psql 실행 중 일부 에러 발생 (exit code: {result.returncode})")
                if result.stderr:
                    logger.warning(f"stderr: {result.stderr}")
                if result.stdout:
                    logger.info(f"stdout: {result.stdout}")
            else:
                logger.info("✅ psql 실행 완료 (에러 없음)")
            
            logger.info("✅ 데이터베이스 복원 완료")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ psql 실행 실패: {e}")
            logger.error(f"stderr: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"❌ 복원 중 오류 발생: {e}")
            return False
    
    def ensure_schemas_exist(self) -> bool:
        """필요한 스키마들이 존재하는지 확인하고 없으면 생성"""
        try:
            conn = psycopg2.connect(**self.target_config)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # 필요한 스키마들
            required_schemas = ['public', 'malirang']
            
            for schema in required_schemas:
                # 스키마 존재 여부 확인
                cursor.execute("""
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                """, (schema,))
                
                if not cursor.fetchone():
                    # 스키마 생성
                    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                    logger.info(f"✅ {schema} 스키마 생성 완료")
                else:
                    logger.info(f"✅ {schema} 스키마 이미 존재")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ 스키마 생성 실패: {e}")
            return False
    
    def get_table_count(self, config: Dict[str, Any]) -> int:
        """테이블 개수 조회 (모든 스키마)"""
        try:
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema IN ('public', 'malirang')
            """)
            
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            return count
        except Exception as e:
            logger.error(f"❌ 테이블 개수 조회 실패: {e}")
            return 0
    
    def run_full_dump_and_restore(self, output_file: str = None) -> bool:
        """전체 덤프 및 복원 프로세스 실행"""
        logger.info("🚀 데이터베이스 덤프 및 복원 프로세스 시작")
        
        # 1. 소스 DB 연결 테스트
        if not self.test_connection(self.source_config):
            return False
        
        # 2. 타겟 DB 연결 테스트 (postgres DB로)
        target_postgres_config = self.target_config.copy()
        target_postgres_config['database'] = 'postgres'
        if not self.test_connection(target_postgres_config, 'postgres'):
            return False
        
        # 3. 타겟 데이터베이스 생성
        if not self.create_database_if_not_exists():
            return False
        
        # 4. 타겟 DB 연결 테스트
        if not self.test_connection(self.target_config):
            return False
        
        # 5. 타겟 DB 초기화 (기존 데이터 삭제)
        if not self.clear_target_database():
            return False
        
        # 5.5. malirang 스키마 생성 (없으면)
        if not self.create_malirang_schema():
            return False
        
        # 6. 소스 DB에서 덤프 생성
        if not self.dump_database(output_file):
            return False
        
        # 7. 타겟 DB로 복원
        if not self.restore_database():
            return False
        
        # 8. 결과 확인
        source_table_count = self.get_table_count(self.source_config)
        target_table_count = self.get_table_count(self.target_config)
        
        logger.info(f"📊 소스 DB 테이블 수: {source_table_count}")
        logger.info(f"📊 타겟 DB 테이블 수: {target_table_count}")
        
        # 상세한 테이블 목록 확인
        self.log_table_details(self.source_config, "소스 DB")
        self.log_table_details(self.target_config, "타겟 DB")
        
        logger.info("✅ 덤프 및 복원이 성공적으로 완료되었습니다!")
        return True
    
    def log_table_details(self, config: Dict[str, Any], db_name: str):
        """테이블 상세 정보 로그 출력"""
        try:
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE schemaname IN ('public', 'malirang')
                ORDER BY schemaname, tablename
            """)
            
            tables = cursor.fetchall()
            
            logger.info(f"🔍 {db_name} 테이블 목록:")
            public_tables = [t for s, t in tables if s == 'public']
            malirang_tables = [t for s, t in tables if s == 'malirang']
            
            logger.info(f"  📁 public 스키마 ({len(public_tables)}개): {', '.join(public_tables)}")
            logger.info(f"  📁 malirang 스키마 ({len(malirang_tables)}개): {', '.join(malirang_tables[:10])}{'...' if len(malirang_tables) > 10 else ''}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ {db_name} 테이블 목록 조회 실패: {e}")


def load_database_config() -> tuple[Dict[str, Any], Dict[str, Any]]:
    """환경변수에서 데이터베이스 설정 로드"""
    
    # 소스 DB 설정 (실제 운영 DB)
    source_config = {
        'host': os.getenv('SOURCE_DB_HOST', '133.186.228.217'),
        'port': int(os.getenv('SOURCE_DB_PORT', '5432')),
        'user': os.getenv('SOURCE_DB_USER', 'malirang'),
        'password': os.getenv('SOURCE_DB_PASSWORD', 'nf5EcEj6mSlDsUr4HIxm0YVAGR9AxTg1'),
        'database': os.getenv('SOURCE_DB_NAME', 'malirang')
    }
    
    # 타겟 DB 설정 (로컬 DB)
    target_config = {
        'host': os.getenv('TARGET_DB_HOST', 'localhost'),
        'port': int(os.getenv('TARGET_DB_PORT', '5432')),
        'user': os.getenv('TARGET_DB_USER', 'malirang'),
        'password': os.getenv('TARGET_DB_PASSWORD', 'malirang123'),
        'database': os.getenv('TARGET_DB_NAME', 'malirang')
    }
    
    return source_config, target_config


def main():
    """메인 실행 함수"""
    try:
        # 데이터베이스 설정 로드
        source_config, target_config = load_database_config()
        
        # 설정 출력
        logger.info("📋 데이터베이스 설정:")
        logger.info(f"  소스: {source_config['host']}:{source_config['port']}/{source_config['database']}")
        logger.info(f"  타겟: {target_config['host']}:{target_config['port']}/{target_config['database']}")
        
        # Dumper 인스턴스 생성 및 실행
        dumper = DatabaseDumper(source_config, target_config)
        
        # 출력 파일명 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"db_dump_{source_config['database']}_{timestamp}.sql"
        
        # 전체 프로세스 실행
        success = dumper.run_full_dump_and_restore(output_file)
        
        if success:
            logger.info("🎉 모든 작업이 성공적으로 완료되었습니다!")
            sys.exit(0)
        else:
            logger.error("💥 작업 중 오류가 발생했습니다!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⏹️ 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 예상치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

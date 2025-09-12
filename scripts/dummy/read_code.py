import os
import psycopg2
from dotenv import load_dotenv
from urllib.parse import urlparse

# .env 파일에서 환경 변수 로드
load_dotenv()

# DATABASE_URL에서 데이터베이스 연결 정보 가져오기
DATABASE_URL = os.getenv('DATABASE_URL')

def clean_database_url(url):
    """
    psycopg2에서 지원하지 않는 쿼리 파라미터를 제거하는 함수
    """
    if not url:
        return url
    
    # URL 파싱
    parsed = urlparse(url)
    
    # schema 파라미터가 있으면 제거
    if '?schema=' in url:
        # schema 파라미터 제거
        clean_url = url.split('?schema=')[0]
        return clean_url
    
    return url

def connect_db():
    """데이터베이스에 연결하는 함수"""
    try:
        # schema 파라미터 제거
        clean_url = clean_database_url(DATABASE_URL)
        conn = psycopg2.connect(clean_url)
        return conn
    except Exception as e:
        print(f"데이터베이스 연결 오류: {e}")
        return None

def get_child_id_by_name(name):
    """
    Child 테이블에서 name으로 검색하여 해당하는 id 값을 반환하는 함수
    
    Args:
        name (str): 검색할 name 값
        
    Returns:
        int or None: 찾은 id 값, 없으면 None
    """
    conn = None
    cursor = None
    
    try:
        # 데이터베이스 연결
        conn = connect_db()
        if not conn:
            return None
            
        cursor = conn.cursor()
        
        # Child 테이블에서 name으로 id 검색 (따옴표로 테이블명 감싸기)
        query = 'SELECT id FROM "Child" WHERE name = %s;'
        cursor.execute(query, (name,))
        
        result = cursor.fetchone()
        
        if result:
            child_id = result[0]
            print(f"찾은 Child ID: {child_id} (name: {name})")
            return child_id
        else:
            print(f"'{name}'에 해당하는 Child를 찾을 수 없습니다.")
            return None
            
    except Exception as e:
        print(f"데이터베이스 조회 오류: {e}")
        return None
        
    finally:
        # 연결 종료
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_child_info_by_name(name):
    """
    Child 테이블에서 name으로 검색하여 id와 contact 정보를 반환하는 함수
    
    Args:
        name (str): 검색할 name 값
        
    Returns:
        dict or None: {id, name, contact} 정보, 없으면 None
    """
    conn = None
    cursor = None
    
    try:
        # 데이터베이스 연결
        conn = connect_db()
        if not conn:
            return None
            
        cursor = conn.cursor()
        
        # Child 테이블에서 name으로 id, name, contact 검색
        query = 'SELECT id, name, contact FROM "Child" WHERE name = %s;'
        cursor.execute(query, (name,))
        
        result = cursor.fetchone()
        
        if result:
            child_info = {
                'id': result[0],
                'name': result[1],
                'contact': result[2]
            }
            print(f"찾은 Child 정보: ID {child_info['id']}, Name: {child_info['name']}, Contact: {child_info['contact']}")
            return child_info
        else:
            print(f"'{name}'에 해당하는 Child를 찾을 수 없습니다.")
            return None
            
    except Exception as e:
        print(f"데이터베이스 조회 오류: {e}")
        return None
        
    finally:
        # 연결 종료
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_all_children():
    """
    Child 테이블의 모든 데이터를 조회하는 함수 (디버깅용)
    
    Returns:
        list: 모든 Child 데이터 리스트
    """
    conn = None
    cursor = None
    
    try:
        conn = connect_db()
        if not conn:
            return []
            
        cursor = conn.cursor()
        
        # Child 테이블의 모든 데이터 조회 (따옴표로 테이블명 감싸기)
        query = 'SELECT id, name, contact FROM "Child" ORDER BY id;'
        cursor.execute(query)
        
        results = cursor.fetchall()
        
        print("Child 테이블의 모든 데이터:")
        for row in results:
            print(f"ID: {row[0]}, Name: {row[1]}, Contact: {row[2]}")
            
        return results
        
    except Exception as e:
        print(f"데이터베이스 조회 오류: {e}")
        return []
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def search_children_by_name_pattern(pattern):
    """
    Child 테이블에서 name 패턴으로 검색하는 함수
    
    Args:
        pattern (str): 검색할 name 패턴 (LIKE 검색)
        
    Returns:
        list: 검색된 Child 데이터 리스트
    """
    conn = None
    cursor = None
    
    try:
        conn = connect_db()
        if not conn:
            return []
            
        cursor = conn.cursor()
        
        # Child 테이블에서 name 패턴으로 검색 (따옴표로 테이블명 감싸기)
        query = 'SELECT id, name, contact FROM "Child" WHERE name LIKE %s ORDER BY id;'
        cursor.execute(query, (f"%{pattern}%",))
        
        results = cursor.fetchall()
        
        if results:
            print(f"'{pattern}' 패턴으로 검색된 결과:")
            for row in results:
                print(f"ID: {row[0]}, Name: {row[1]}, Contact: {row[2]}")
        else:
            print(f"'{pattern}' 패턴에 해당하는 Child를 찾을 수 없습니다.")
            
        return results
        
    except Exception as e:
        print(f"데이터베이스 조회 오류: {e}")
        return []
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def search_multiple_names(names_text):
    """
    여러 이름을 한 번에 검색하는 함수
    
    Args:
        names_text (str): 여러 이름이 포함된 텍스트 (줄바꿈으로 구분)
        
    Returns:
        dict: {name: {id, contact}} 형태의 딕셔너리
    """
    # 텍스트를 줄바꿈으로 분리하고 빈 줄 제거
    names = [name.strip() for name in names_text.strip().split('\n') if name.strip()]
    
    results = {}
    
    print(f"총 {len(names)}개의 이름을 검색합니다...")
    print("=" * 50)
    
    for i, name in enumerate(names, 1):
        print(f"[{i}/{len(names)}] '{name}' 검색 중...")
        child_info = get_child_info_by_name(name)
        if child_info:
            results[name] = child_info
        print("-" * 30)
    
    return results

def search_multiple_names_id_only(names_text):
    """
    여러 이름을 한 번에 검색하여 ID만 반환하는 함수
    
    Args:
        names_text (str): 여러 이름이 포함된 텍스트 (줄바꿈으로 구분)
        
    Returns:
        dict: {name: id} 형태의 딕셔너리
    """
    # 텍스트를 줄바꿈으로 분리하고 빈 줄 제거
    names = [name.strip() for name in names_text.strip().split('\n') if name.strip()]
    
    results = {}
    
    print(f"총 {len(names)}개의 이름을 검색합니다...")
    print("=" * 50)
    
    for i, name in enumerate(names, 1):
        print(f"[{i}/{len(names)}] '{name}' 검색 중...")
        child_id = get_child_id_by_name(name)
        if child_id:
            results[name] = child_id
        print("-" * 30)
    
    return results

def get_names_from_text():
    """
    사용자로부터 텍스트를 입력받아 이름들을 추출하는 함수
    """
    print("이름 목록을 입력하세요 (한 줄에 하나씩, 빈 줄로 입력 완료):")
    print("예시:")
    print("이도윤")
    print("김라온")
    print("김리원")
    print("(빈 줄 입력)")
    print()
    
    lines = []
    while True:
        line = input().strip()
        if line == "":
            break
        lines.append(line)
    
    return '\n'.join(lines)

def check_table_exists():
    """
    Child 테이블이 존재하는지 확인하는 함수
    """
    conn = None
    cursor = None
    
    try:
        conn = connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        # 테이블 존재 여부 확인
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'Child'
        );
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result and result[0]:
            print("✓ 'Child' 테이블이 존재합니다.")
            return True
        else:
            print("✗ 'Child' 테이블이 존재하지 않습니다.")
            
            # 사용 가능한 테이블 목록 조회
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            
            if tables:
                print("사용 가능한 테이블 목록:")
                for table in tables:
                    print(f"  - {table[0]}")
            else:
                print("사용 가능한 테이블이 없습니다.")
            
            return False
        
    except Exception as e:
        print(f"테이블 확인 오류: {e}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def print_excel_format(results):
    """
    엑셀에 바로 붙여넣을 수 있는 형식으로 결과를 출력하는 함수
    """
    print("\n" + "="*50)
    print("엑셀 붙여넣기용 결과:")
    print("="*50)
    if results:
        for name, info in results.items():
            # None 값을 빈 문자열로 변환
            contact = info['contact'] if info['contact'] is not None else ""
            print(f"{name} {info['id']} {contact}")
    else:
        print("검색된 결과가 없습니다.")

def print_id_only_format(results):
    """
    ID만 출력하는 엑셀 붙여넣기용 형식으로 결과를 출력하는 함수
    """
    print("\n" + "="*50)
    print("ID만 출력 (엑셀 붙여넣기용):")
    print("="*50)
    if results:
        for name, child_id in results.items():
            print(f"{child_id}")
    else:
        print("검색된 결과가 없습니다.")

if __name__ == "__main__":
    # 사용 예시
    print("=== Child 테이블 조회 테스트 ===")
    print(f"연결된 데이터베이스: {DATABASE_URL.split('@')[1].split('/')[0] if DATABASE_URL else 'None'}")
    
    # 테이블 존재 여부 확인
    print("\n테이블 존재 여부 확인:")
    table_exists = check_table_exists()
    
    if not table_exists:
        print("\n⚠️  Child 테이블이 존재하지 않습니다. 프로그램을 종료합니다.")
        exit()
    
    while True:
        print("\n" + "="*60)
        print("1. 모든 Child 데이터 조회")
        print("2. 특정 name으로 id 검색")
        print("3. 특정 name으로 전체 정보 검색 (id, name, contact)")
        print("4. 패턴 검색")
        print("5. 여러 이름 한 번에 검색 (전체 정보)")
        print("6. 여러 이름 한 번에 검색 (ID만)")
        print("7. 종료")
        print("="*60)
        
        choice = input("선택하세요 (1-7): ").strip()
        
        if choice == "1":
            print("\n1. 모든 Child 데이터 조회:")
            all_children = get_all_children()
            
        elif choice == "2":
            print("\n2. 특정 name으로 id 검색:")
            search_name = input("검색할 name을 입력하세요: ").strip()
            if search_name:
                child_id = get_child_id_by_name(search_name)
                if child_id:
                    print(f"결과: name '{search_name}'의 id는 {child_id}입니다.")
                else:
                    print(f"결과: name '{search_name}'에 해당하는 데이터가 없습니다.")
        
        elif choice == "3":
            print("\n3. 특정 name으로 전체 정보 검색:")
            search_name = input("검색할 name을 입력하세요: ").strip()
            if search_name:
                child_info = get_child_info_by_name(search_name)
                if child_info:
                    print(f"결과: {child_info}")
                else:
                    print(f"결과: name '{search_name}'에 해당하는 데이터가 없습니다.")
        
        elif choice == "4":
            print("\n4. 패턴 검색:")
            pattern = input("검색할 패턴을 입력하세요 (부분 일치): ").strip()
            if pattern:
                search_children_by_name_pattern(pattern)
        
        elif choice == "5":
            print("\n5. 여러 이름 한 번에 검색 (전체 정보):")
            names_text = get_names_from_text()
            if names_text.strip():
                results = search_multiple_names(names_text)
                print_excel_format(results)
            else:
                print("입력된 이름이 없습니다.")
        
        elif choice == "6":
            print("\n6. 여러 이름 한 번에 검색 (ID만):")
            names_text = get_names_from_text()
            if names_text.strip():
                results = search_multiple_names_id_only(names_text)
                print_id_only_format(results)
            else:
                print("입력된 이름이 없습니다.")
        
        elif choice == "7":
            print("프로그램을 종료합니다.")
            break
        
        else:
            print("잘못된 선택입니다. 1-7 중에서 선택해주세요.")

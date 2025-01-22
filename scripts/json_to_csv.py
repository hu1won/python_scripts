import json
import csv
import sys
import os

def json_to_csv(json_file_path, csv_file_path):
    try:
        # JSON 파일 읽기
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        # 데이터가 리스트가 아닌 경우 리스트로 변환
        if not isinstance(data, list):
            data = [data]
        
        # CSV 파일 작성
        if data:
            headers = data[0].keys()
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data)
            print(f"변환 완료: {csv_file_path}")
        else:
            print("데이터가 비어있습니다.")
            
    except FileNotFoundError:
        print(f"오류: {json_file_path} 파일을 찾을 수 없습니다.")
    except json.JSONDecodeError:
        print("오류: JSON 파일 형식이 올바르지 않습니다.")
    except Exception as e:
        print(f"오류 발생: {str(e)}")

def main():
    if len(sys.argv) != 3:
        print("사용법: python json_to_csv.py <입력_json_파일> <출력_csv_파일>")
        sys.exit(1)
    
    json_file_path = sys.argv[1]
    csv_file_path = sys.argv[2]
    
    json_to_csv(json_file_path, csv_file_path)

if __name__ == "__main__":
    main()

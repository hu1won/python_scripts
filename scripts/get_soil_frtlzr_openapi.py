import requests
import json

url = 'http://apis.data.go.kr/1390802/SoilEnviron/FrtlzrUse/getSoilFrtlzrExamInfo'
params = {
    'serviceKey': '',
    'PNU_Code': '5011025628110850001',
    'crop_Code': '09001'
}

try:
    response = requests.get(url, params=params)
    response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
    
    # XML 응답인 경우 (대부분의 공공데이터 API는 XML 형식입니다)
    if 'xml' in response.headers.get('Content-Type', '').lower():
        print("XML 응답:")
        print(response.text)
    # JSON 응답인 경우
    else:
        data = response.json()
        print("JSON 응답:")
        print(json.dumps(data, ensure_ascii=False, indent=2))

except requests.exceptions.RequestException as e:
    print(f"API 호출 중 오류 발생: {e}")


import requests
import os
# 서버 엔드포인트 URL
url = os.getenv('API_URL')

# 요청에 필요한 데이터 (GET, POST에 따라 다름)
params = {"key1": "value1", "key2": "value2"}  # GET 요청 시 사용
data = {"key1": "value1", "key2": "value2"}   # POST 요청 시 사용

# GET 요청 예제
response = requests.get(url, params=params)

# POST 요청 예제
response = requests.post(url, json=data)

# 응답 확인
if response.status_code == 200:
    print("Success:", response.json())  # JSON 응답 파싱
else:
    print("Error:", response.status_code, response.text)

print(response.json())
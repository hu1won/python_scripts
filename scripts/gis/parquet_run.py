import os
import geopandas as gpd
from shapely.geometry import Point
from dotenv import load_dotenv
from geopy.geocoders import Nominatim

def geocode(address):
    """주소를 위경도 좌표로 변환하는 함수"""
    geolocator = Nominatim(user_agent="soil_finder")
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None

def get_soil_info_from_parquet(address):
    """Parquet 파일을 사용하여 주소 위치의 토양 정보를 조회하는 함수"""
    # 환경 변수에서 Parquet 파일 경로 가져오기
    load_dotenv()
    parquet_path = os.getenv("PARQUET_PATH", "soil_data.parquet")
    
    if not os.path.exists(parquet_path):
        return "Parquet 파일을 찾을 수 없습니다."
    
    # 위경도 좌표 얻기
    lat, lon = geocode(address)
    if lat is None or lon is None:
        return f"주소를 찾을 수 없습니다: {address}"
    
    # Parquet 파일 로드
    gdf = gpd.read_parquet(parquet_path)
    
    # 좌표를 Point 형식으로 변환
    point = gpd.GeoDataFrame([{'geometry': Point(lon, lat)}], crs="EPSG:4326")
    
    # 공간 조인 수행
    point_in_polygon = gpd.sjoin(point, gdf, how="left", predicate="within")
    
    # 결과 반환
    if point_in_polygon.empty or point_in_polygon.iloc[0].isna().all():
        return f"주소 {address}의 위치는 데이터 영역에 없습니다."
    else:
        soil_code = point_in_polygon.iloc[0]['토양부호']
        soil_name = point_in_polygon.iloc[0]['한글명']
        return f"주소 {address}의 토양정보:\n- 토양부호: {soil_code}\n- 한글명: {soil_name}"

if __name__ == "__main__":
    # 테스트할 주소 목록
    test_addresses = [
        "제주특별자치도 제주시 오라동 100",
        "제주특별자치도 제주시 건입동 100"
    ]
    
    print("토양 정보 조회 시작...\n")
    for address in test_addresses:
        result = get_soil_info_from_parquet(address)
        print(f"{result}\n")

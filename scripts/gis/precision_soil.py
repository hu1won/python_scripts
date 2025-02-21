import os
import geopandas as gpd
from shapely.geometry import Point
from dotenv import load_dotenv
from geopy.geocoders import Nominatim

# 1️⃣ .env 파일 로드 (SHP 파일 경로 불러오기)
load_dotenv()
shp_path = os.getenv("SHP_PATH")

# 2️⃣ SHP 파일 불러오기
if not shp_path or not os.path.exists(shp_path):
    raise FileNotFoundError(f"SHP 파일을 찾을 수 없습니다: {shp_path}")

gdf = gpd.read_file(shp_path)

# 좌표계 확인 후 EPSG:4326 (위도/경도)로 변환
if gdf.crs is None:
    raise ValueError("SHP 파일에 좌표계(CRS)가 지정되지 않았습니다.")
elif gdf.crs.to_string() != "EPSG:4326":
    gdf = gdf.to_crs(epsg=4326)

# 3️⃣ 주소 → 좌표 변환 (Geocoding)
geolocator = Nominatim(user_agent="geo_app")

def geocode(address):
    """주소를 입력하면 위도, 경도를 반환하는 함수"""
    location = geolocator.geocode(address)
    if location:
        return location.latitude, location.longitude
    else:
        return None, None

# 4️⃣ 특정 주소의 좌표가 속한 SHP 데이터 찾기
def get_soil_info(address):
    """주소를 입력하면 해당 위치의 토양부호와 한글명을 반환하는 함수"""
    lat, lon = geocode(address)
    
    if lat is None or lon is None:
        return f"주소를 찾을 수 없습니다: {address}"
    
    # 좌표를 Point 형식으로 변환
    point = gpd.GeoDataFrame([{'address': address, 'geometry': Point(lon, lat)}], crs="EPSG:4326")

    # 공간 조인 (해당 좌표가 포함된 폴리곤 찾기)
    point_in_polygon = gpd.sjoin(point, gdf, how="left", predicate="within")

    # 결과 반환
    if point_in_polygon.empty:
        return f"주소 {address}의 위치는 SHP 데이터 영역에 없습니다."
    else:
        soil_code = point_in_polygon.iloc[0]['토양부호']
        soil_sr = point_in_polygon.iloc[0]['한글명']  # 한글명 컬럼 추가
        return f"주소 {address}의 토양부호: {soil_code}, 토양통: {soil_sr}"

# 5️⃣ 실행 예제
address = "제주특별자치도 제주시 오라동 100"
soil_info = get_soil_info(address)
print(soil_info)

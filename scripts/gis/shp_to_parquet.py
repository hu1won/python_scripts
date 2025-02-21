import os
import geopandas as gpd
from dotenv import load_dotenv

def convert_shp_to_parquet():
    """SHP 파일을 Parquet 파일로 변환하는 함수"""
    # .env 파일에서 환경 변수 로드
    load_dotenv()
    shp_path = os.getenv("SHP_PATH")
    parquet_path = os.getenv("PARQUET_PATH", "soil_data.parquet")

    if not shp_path or not os.path.exists(shp_path):
        raise FileNotFoundError(f"SHP 파일을 찾을 수 없습니다: {shp_path}")

    # SHP 파일 로드
    print("SHP 파일 로딩 중...")
    gdf = gpd.read_file(shp_path)
    
    # 좌표계 확인 및 변환
    if gdf.crs is None:
        raise ValueError("SHP 파일에 좌표계(CRS)가 지정되지 않았습니다.")
    elif gdf.crs.to_string() != "EPSG:4326":
        print("좌표계를 EPSG:4326으로 변환 중...")
        gdf = gdf.to_crs(epsg=4326)
    
    # Parquet 파일로 저장
    print(f"Parquet 파일로 저장 중: {parquet_path}")
    gdf.to_parquet(parquet_path)
    print("변환 완료!")
    
    # 파일 크기 비교
    shp_size = os.path.getsize(shp_path) / (1024 * 1024)  # MB
    parquet_size = os.path.getsize(parquet_path) / (1024 * 1024)  # MB
    print(f"\n파일 크기 비교:")
    print(f"SHP 파일: {shp_size:.2f} MB")
    print(f"Parquet 파일: {parquet_size:.2f} MB")

if __name__ == "__main__":
    convert_shp_to_parquet() 
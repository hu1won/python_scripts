import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()

db_name = os.getenv('SURVER_DB_CONFIG_DBNAME')
db_user = os.getenv('SURVER_DB_CONFIG_USER')
db_password = os.getenv('SURVER_DB_CONFIG_PASSWORD')
db_host = os.getenv('SURVER_DB_CONFIG_HOST')
db_port = os.getenv('SURVER_DB_CONFIG_PORT')

# Database connection details
db_config = {
    'dbname': db_name,
    'user': db_user,
    'password': db_password,
    'host': db_host,
    'port': db_port
}

fertilizer_data = [
  {
    "name": "default",
    "nh_pre_fert_n": "21.0",
    "nh_pre_fert_p": "17.0",
    "nh_pre_fert_k": "17.0",
    "nh_pre_fert_qy": "0"
  },
  {
    "name": "NK+인(엔케이플러스인)",
    "nh_pre_fert_n": "18",
    "nh_pre_fert_p": "1",
    "nh_pre_fert_k": "15",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "감귤달콤-2호",
    "nh_pre_fert_n": "7",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "감귤달콤-특호",
    "nh_pre_fert_n": "6",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "감귤천하",
    "nh_pre_fert_n": "7",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "5",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "감자왕알통",
    "nh_pre_fert_n": "8",
    "nh_pre_fert_p": "9",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "고구마전용",
    "nh_pre_fert_n": "7",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "18",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "고추고형비료",
    "nh_pre_fert_n": "15",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "고추전용",
    "nh_pre_fert_n": "11",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "고추특호",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "5",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "골드고형비료",
    "nh_pre_fert_n": "15",
    "nh_pre_fert_p": "4",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "과일채소나라(고추,마늘,사과",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "5",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "논편한 원타임 골드",
    "nh_pre_fert_n": "20",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "논편한-올코팅",
    "nh_pre_fert_n": "31",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "논편한-원타임",
    "nh_pre_fert_n": "29",
    "nh_pre_fert_p": "4",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "단한번",
    "nh_pre_fert_n": "18",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "달콤한라",
    "nh_pre_fert_n": "6",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "4",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "달콤한라-특호",
    "nh_pre_fert_n": "5",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "4",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "대풍PNS15(원예)",
    "nh_pre_fert_n": "13",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "15"
  },
  {
    "name": "더블윈",
    "nh_pre_fert_n": "7",
    "nh_pre_fert_p": "2",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "더존(원예)",
    "nh_pre_fert_n": "13",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "도우미골드",
    "nh_pre_fert_n": "16",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "땅심30",
    "nh_pre_fert_n": "30",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "땅심골드(원예)",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "롱런모든작물",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "5",
    "nh_pre_fert_k": "5",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "롱스타플러스(완효성)",
    "nh_pre_fert_n": "21",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "맞춤16호",
    "nh_pre_fert_n": "22",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "맞춤18호",
    "nh_pre_fert_n": "21",
    "nh_pre_fert_p": "13",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "맞춤19호",
    "nh_pre_fert_n": "20",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "11",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "맞춤20호",
    "nh_pre_fert_n": "19",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "맞춤23호",
    "nh_pre_fert_n": "16",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "맞춤24호",
    "nh_pre_fert_n": "13",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "맞춤25호",
    "nh_pre_fert_n": "10",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "명품300",
    "nh_pre_fert_n": "30",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "명품유비롱",
    "nh_pre_fert_n": "18",
    "nh_pre_fert_p": "9",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "명품유비롱",
    "nh_pre_fert_n": "18",
    "nh_pre_fert_p": "9",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "미생물고형비료",
    "nh_pre_fert_n": "15",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "미생물논비료",
    "nh_pre_fert_n": "28",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "미생물밭비료",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "5",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "미생물올코팅",
    "nh_pre_fert_n": "28",
    "nh_pre_fert_p": "5",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "미생물완효성",
    "nh_pre_fert_n": "21",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "배추고형비료",
    "nh_pre_fert_n": "15",
    "nh_pre_fert_p": "4",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "보카시 3호",
    "nh_pre_fert_n": "3",
    "nh_pre_fert_p": "3.5",
    "nh_pre_fert_k": "0.6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "보카시1호",
    "nh_pre_fert_n": "3",
    "nh_pre_fert_p": "4",
    "nh_pre_fert_k": "1",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "복합비료(21-17-17)",
    "nh_pre_fert_n": "21",
    "nh_pre_fert_p": "17",
    "nh_pre_fert_k": "17",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "빠른원예추비(질산태,유황함유",
    "nh_pre_fert_n": "14",
    "nh_pre_fert_p": "2",
    "nh_pre_fert_k": "12",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "뿌리왕왕PN(고추,참외,호박",
    "nh_pre_fert_n": "13",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "수퍼고추/감자/마늘양파",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "슈퍼50",
    "nh_pre_fert_n": "6",
    "nh_pre_fert_p": "4",
    "nh_pre_fert_k": "4",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "슈퍼고추",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "슈퍼듀얼요소",
    "nh_pre_fert_n": "40",
    "nh_pre_fert_p": "4",
    "nh_pre_fert_k": "4",
    "nh_pre_fert_qy": "15"
  },
  {
    "name": "슈퍼오래가",
    "nh_pre_fert_n": "22",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "스피드NK",
    "nh_pre_fert_n": "18",
    "nh_pre_fert_p": "2",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "신세대22",
    "nh_pre_fert_n": "22",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "썰파원예(감자,마늘,생강,유",
    "nh_pre_fert_n": "11",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "알찬 (마늘. 당근)",
    "nh_pre_fert_n": "8",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "알찬(감자.더덕)",
    "nh_pre_fert_n": "7",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "야라밀라",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "11",
    "nh_pre_fert_k": "18",
    "nh_pre_fert_qy": "0"
  },
  {
    "name": "야라밀라",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "11",
    "nh_pre_fert_k": "18",
    "nh_pre_fert_qy": "0"
  },
  {
    "name": "오래가",
    "nh_pre_fert_n": "22",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "왕왕감자(감자,마늘)",
    "nh_pre_fert_n": "11",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원샷 15",
    "nh_pre_fert_n": "15",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원샷 감자/고구마",
    "nh_pre_fert_n": "11",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원샷 마늘/양파",
    "nh_pre_fert_n": "13",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원샷고추",
    "nh_pre_fert_n": "13",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원샷올인원(완효성)",
    "nh_pre_fert_n": "20",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원샷채소(대파/배추/양배추)",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원샷특호",
    "nh_pre_fert_n": "13",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원예1호복합(유황)",
    "nh_pre_fert_n": "11",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원예과수풍년",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "원예추비",
    "nh_pre_fert_n": "14",
    "nh_pre_fert_p": "2",
    "nh_pre_fert_k": "12",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "웰빙유기농",
    "nh_pre_fert_n": "3",
    "nh_pre_fert_p": "3.5",
    "nh_pre_fert_k": "0.5",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "유기농 특호",
    "nh_pre_fert_n": "4",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "1",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "유기농특호",
    "nh_pre_fert_n": "45",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "5",
    "nh_pre_fert_qy": "0"
  },
  {
    "name": "유황감자비료",
    "nh_pre_fert_n": "11",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "으뜸왕감자",
    "nh_pre_fert_n": "10",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "인산칼리 맞춤2호",
    "nh_pre_fert_n": "17",
    "nh_pre_fert_p": "19",
    "nh_pre_fert_k": "15",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "인산칼리맞춤1호",
    "nh_pre_fert_n": "20",
    "nh_pre_fert_p": "18",
    "nh_pre_fert_k": "15",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "일회만290",
    "nh_pre_fert_n": "22",
    "nh_pre_fert_p": "9",
    "nh_pre_fert_k": "10",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "잡곡(콩,땅콩,깨,팥)",
    "nh_pre_fert_n": "8",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "잡곡(콩/깨/땅콩)",
    "nh_pre_fert_n": "8",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "제주원예 복비 (무.양배추)",
    "nh_pre_fert_n": "10",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "7",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "조비21복합(원예)",
    "nh_pre_fert_n": "21",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "참세대22(22-7-9)",
    "nh_pre_fert_n": "22",
    "nh_pre_fert_p": "7",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "콩비료",
    "nh_pre_fert_n": "5",
    "nh_pre_fert_p": "20",
    "nh_pre_fert_k": "15",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "토토그린",
    "nh_pre_fert_n": "4",
    "nh_pre_fert_p": "2",
    "nh_pre_fert_k": "1",
    "nh_pre_fert_qy": "0"
  },
  {
    "name": "토토유박",
    "nh_pre_fert_n": "4",
    "nh_pre_fert_p": "2",
    "nh_pre_fert_k": "1",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "파워감자",
    "nh_pre_fert_n": "11",
    "nh_pre_fert_p": "8",
    "nh_pre_fert_k": "9",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "파워원예복합",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "6",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "폴리피드(19-19-19)",
    "nh_pre_fert_n": "19",
    "nh_pre_fert_p": "19",
    "nh_pre_fert_k": "19",
    "nh_pre_fert_qy": "25"
  },
  {
    "name": "한번에아리커",
    "nh_pre_fert_n": "21",
    "nh_pre_fert_p": "10",
    "nh_pre_fert_k": "11",
    "nh_pre_fert_qy": "20"
  },
  {
    "name": "휴믹황원예",
    "nh_pre_fert_n": "12",
    "nh_pre_fert_p": "6",
    "nh_pre_fert_k": "8",
    "nh_pre_fert_qy": "20"
  }
]

def insert_fertilizerlist(data):
    conn = None
    try:
        # 데이터베이스 연결
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # 데이터 삽입
        for fertilizer in data:
            frtlzr_nm = fertilizer["name"]
            ntrgn = fertilizer["nh_pre_fert_n"]
            phsphrs = fertilizer["nh_pre_fert_p"]
            ptssm = fertilizer["nh_pre_fert_k"]

            # SQL 쿼리
            insert_query = """
            INSERT INTO jadxdb2.frtlzr_lst (frtlzr_nm, ntrgn, phsphrs, ptssm, reg_uid)
            VALUES (%s, %s, %s, %s, 'system')
            ON CONFLICT (frtlzr_nm) DO NOTHING
            """
            cursor.execute(insert_query, (frtlzr_nm, ntrgn, phsphrs, ptssm))

        # 변경 사항 커밋
        conn.commit()
        print("Data inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    insert_fertilizerlist(fertilizer_data)
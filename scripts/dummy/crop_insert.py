import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import uuid

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

# crop_lg_uid 매핑 (임시 UID 사용)
crop_lg_mapping = {
    "곡류(벼)": "CROPLG_0001",
    "곡류(기타)": "CROPLG_0002",
    "유지류": "CROPLG_0003",
    "서류": "CROPLG_0004",
    "과채류": "CROPLG_0005",
    "근채류": "CROPLG_0006",
    "인경채류": "CROPLG_0007",
    "경엽채류": "CROPLG_0008",
    "산채류": "CROPLG_0009",
    "과수": "CROPLG_0010",
    "약용작물": "CROPLG_0011",
    "화훼류": "CROPLG_0012",
    "사료작물": "CROPLG_0013",
    "기타": "CROPLG_0014",
    "미분류": "CROPLG_0015"
}

# JSON 데이터 (여기서는 예시로 변수에 직접 할당)
data = {
  "곡류(벼)": [
    "[00165] 밭벼(비화산회토)",
    "[00164] 밭벼(화산회토)",
    "[00430] 벼(염해답-미숙답)",
    "[00305] 벼(염해답-숙답)",
    "[00000] 벼(일반답)"
  ],
  "곡류(기타)": [
    "[00311] 강낭콩",
    "[00773] 귀리",
    "[00775] 귀리(비화산회토)",
    "[00776] 귀리(화산회토)",
    "[00344] 기장",
    "[00415] 녹두",
    "[00006] 단옥수수",
    "[00159] 대두(비화산회토)",
    "[00158] 대두(화산회토)",
    "[00530] 동부콩",
    "[00219] 맥주맥",
    "[00002] 맥주보리",
    "[00313] 메밀",
    "[00691] 메밀(비화산회토)",
    "[00690] 메밀(화산회토)",
    "[00414] 밀",
    "[00307] 보리(남부)",
    "[00157] 보리(비화산회토)",
    "[00001] 보리(중북부)",
    "[00156] 보리(화산회토)",
    "[00221] 보통옥수수",
    "[00544] 보통옥수수(사양질)",
    "[00545] 보통옥수수(식양질)",
    "[00424] 서리태콩",
    "[00356] 수수",
    "[00589] 아마란스",
    "[00005] 옥수수",
    "[00777] 완두콩",
    "[00588] 작두콩",
    "[00367] 조",
    "[00631] 쥐눈이콩",
    "[00003] 콩",
    "[00004] 콩(개간지 노지)",
    "[00233] 콩(노지)",
    "[01051] 콩(논재배)",
    "[00778] 콩(잎수확용)",
    "[00371] 팥",
    "[01068] 호밀"
  ],
  "유지류": [
    "[00324] 들깨(종실용)",
    "[00011] 땅콩",
    "[00169] 땅콩(기경지,비화산회토)",
    "[00168] 땅콩(기경지,화산회토)",
    "[00131] 유채(1대잡종)점파",
    "[00130] 유채(고정품종)점파",
    "[00175] 유채(비화산회토)",
    "[00174] 유채(화산회토)",
    "[00009] 참깨",
    "[00167] 참깨(비화산회토)",
    "[00166] 참깨(화산회토)"
  ],
  "서류": [
    "[00163] 감자(가을재배 비화산회토)",
    "[00171] 감자(가을재배 화산회토)",
    "[00018] 감자(남부,가을재배)",
    "[00017] 감자(남부,봄재배)",
    "[00170] 감자(봄겨울재배 비화산회토)",
    "[00162] 감자(봄겨울재배 화산회토)",
    "[00207] 감자(준고랭지,고랭지)",
    "[00007] 고구마",
    "[00161] 고구마(비화산회토)",
    "[00160] 고구마(화산회토)",
    "[00466] 아피오스(인디언감자, 콩감자)"
  ],
  "과채류": [
    "[00046] 가지(노지)",
    "[00047] 가지(시설)",
    "[00019] 고추(노지)",
    "[00020] 고추(시설)",
    "[00182] 꽈리고추(시설)",
    "[00405] 단호박",
    "[00031] 딸기(노지)",
    "[00032] 딸기(시설)",
    "[00779] 맷돌호박",
    "[00317] 멜론(시설재배)",
    "[00183] 방울토마토(시설)",
    "[00780] 복수박",
    "[00781] 송이토마토",
    "[00043] 수박(노지)",
    "[00044] 수박(시설)",
    "[00635] 수세미",
    "[00591] 애플수박",
    "[00359] 애호박",
    "[00513] 여주",
    "[00027] 오이(노지)",
    "[00028] 오이(시설)",
    "[00782] 울외",
    "[00637] 주키니호박",
    "[00041] 참외(노지)",
    "[00042] 참외(시설)",
    "[00025] 토마토(노지)",
    "[00026] 토마토(시설)",
    "[00783] 파프리카",
    "[00181] 피망(시설)",
    "[00050] 호박(노지)",
    "[00051] 호박(시설)"
  ],
  "근채류": [
    "[00646] 고추냉이(근경)",
    "[00033] 당근",
    "[00210] 당근(비화산회토)",
    "[00209] 당근(화산회토)",
    "[00035] 무(노지)",
    "[00212] 무(비화산회토)",
    "[00036] 무(시설)",
    "[00211] 무(화산회토)",
    "[00188] 비트(시설)",
    "[00769] 비트(화산,비화산-가을)",
    "[00768] 비트(화산,비화산-봄)",
    "[00023] 생강(노지)",
    "[00024] 생강(시설)",
    "[00450] 순무",
    "[01065] 시래기용무",
    "[00758] 알타리무",
    "[00360] 야콘",
    "[00521] 얌빈",
    "[00433] 연근",
    "[00645] 열무(노지)",
    "[00187] 열무(시설)",
    "[00420] 우엉",
    "[00493] 콜라비",
    "[00770] 콜라비(비화산회토)",
    "[01038] 콜라비(화산회토)",
    "[00323] 토란"
  ],
  "인경채류": [
    "[00021] 마늘",
    "[00177] 마늘(비화산회토)",
    "[01074] 마늘(비화산회토,전량밑거름)",
    "[00176] 마늘(화산회토)",
    "[01075] 마늘(화산회토,전량밑거름)",
    "[00013] 양파",
    "[00216] 양파(가을재배, 비화산회토)",
    "[00215] 양파(가을재배, 화산회토)",
    "[00214] 양파(봄재배, 비화산회토)",
    "[00213] 양파(봄재배, 화산회토)"
  ],
  "경엽채류": [
    "[00339] 갓",
    "[00784] 갯방풍(해방풍)",
    "[00754] 고구마순",
    "[00343] 근대",
    "[00553] 대파(노지)",
    "[00555] 대파(시설)",
    "[00787] 방울양배추",
    "[00772] 방울양배추(비화산회토)",
    "[00771] 방울양배추(화산회토)",
    "[00191] 밭미나리(시설)",
    "[00178] 배추(고냉지 노지)",
    "[00037] 배추(노지)",
    "[00038] 배추(시설)",
    "[00678] 배추(화산회토)",
    "[00056] 부추(노지)",
    "[00057] 부추(시설)",
    "[00194] 브로콜리",
    "[00694] 브로콜리(비화산회토)",
    "[00693] 브로콜리(화산회토)",
    "[00196] 삼엽채(시설)",
    "[00029] 상추(노지)",
    "[00030] 상추(시설)",
    "[00138] 서양냉이",
    "[00054] 셀러리(노지)",
    "[00055] 셀러리(시설)",
    "[00060] 스위트팬넬",
    "[00048] 시금치(노지)",
    "[00049] 시금치(시설)",
    "[00208] 신선초(시설)",
    "[00052] 쑥갓(노지)",
    "[00053] 쑥갓(시설)",
    "[00357] 아스파라거스(1년,정식)",
    "[00755] 아스파라거스(2년)",
    "[00756] 아스파라거스(3년)",
    "[00757] 아스파라거스(4년이상)",
    "[00512] 아욱",
    "[00039] 양배추(노지)",
    "[00040] 양배추(시설)",
    "[00788] 양배추(적채, 노지)",
    "[00789] 양배추(적채, 시설)",
    "[00679] 양배추(화산회토)",
    "[00189] 양상추(시설 고랭지)",
    "[00186] 양상추(시설 평야지)",
    "[00785] 얼갈이배추(노지)",
    "[00786] 얼갈이배추(시설)",
    "[00058] 엔다이브",
    "[00059] 오너멘탈케일",
    "[00153] 잎들깨(노지)",
    "[00190] 잎들깨(시설)",
    "[00380] 쪽파(노지)",
    "[00556] 쪽파(시설)",
    "[00432] 청경채",
    "[00192] 치커리(시설)",
    "[00193] 케일(시설)",
    "[00195] 콜리플라워(시설)",
    "[00015] 파(노지)"
  ],
  "산채류": [
    "[00793] 갯기름나물",
    "[00467] 고들빼기",
    "[00602] 고려엉겅퀴(곤드레)",
    "[00340] 고사리(1-2년)",
    "[00522] 고사리(3년이상)",
    "[00180] 곤달비",
    "[00155] 곰취",
    "[00608] 냉이",
    "[00205] 누룩치",
    "[00519] 눈개승마(삼나물)",
    "[00325] 달래",
    "[00346] 돌나물",
    "[00790] 두메부추(노지)",
    "[00791] 두메부추(시설)",
    "[00185] 머위",
    "[00234] 모시대",
    "[00554] 모시풀",
    "[00331] 미역취(시설재배)",
    "[00618] 민들레",
    "[00496] 방풍",
    "[00718] 병풍취(병풍쌈)",
    "[00792] 비름나물",
    "[00236] 산마늘",
    "[00532] 섬쑥부쟁이",
    "[00759] 수리취(1년)",
    "[00760] 수리취(2년)",
    "[00487] 씀바귀",
    "[00362] 어수리",
    "[00235] 영아자",
    "[00232] 참나물",
    "[00154] 참취",
    "[00445] 흰민들레"
  ],
  "과수": [
    "[00796] 감(대봉)",
    "[00230] 감귤(13~17년생, 비화산회토)",
    "[00226] 감귤(13~17년생, 화산회토)",
    "[00309] 감귤(18년생 이상, 비화산회토)",
    "[00308] 감귤(18년생 이상, 화산회토)",
    "[00227] 감귤(1~2년생, 비화산회토)",
    "[00223] 감귤(1~2년생, 화산회토)",
    "[00228] 감귤(3~7년생, 비화산회토)",
    "[00224] 감귤(3~7년생, 화산회토)",
    "[00229] 감귤(8~12년생, 비화산회토)",
    "[00225] 감귤(8~12년생, 화산회토)",
    "[00105] 감귤만감(비화)11~19년",
    "[00103] 감귤만감(비화)1~5년",
    "[00106] 감귤만감(비화)20년이상",
    "[00104] 감귤만감(비화)6~10년",
    "[00097] 감귤만감(화산)11~19년",
    "[00095] 감귤만감(화산)1~5년",
    "[00098] 감귤만감(화산)20년이상",
    "[00096] 감귤만감(화산)6~10년",
    "[00102] 감귤온주(비화)13~17년",
    "[00742] 감귤온주(비화)18년이상",
    "[00099] 감귤온주(비화)1~2년",
    "[00100] 감귤온주(비화)3~7년",
    "[00101] 감귤온주(비화)8~12년",
    "[00094] 감귤온주(화산)13~17년",
    "[00741] 감귤온주(화산)18년이상",
    "[00091] 감귤온주(화산)1~2년",
    "[00092] 감귤온주(화산)3~7년",
    "[00093] 감귤온주(화산)8~12년",
    "[00419] 꾸지뽕나무",
    "[00352] 나무딸기(산딸기)",
    "[00345] 다래(토종다래)",
    "[00197] 단감(1-2년생)",
    "[00202] 단감(11년생 이상)",
    "[00198] 단감(3-4년생)",
    "[00199] 단감(5-6년생)",
    "[00200] 단감(7-8년생)",
    "[00201] 단감(9-10년생)",
    "[00314] 대추(1년생)",
    "[00436] 대추(2년생)",
    "[00437] 대추(3년생)",
    "[00439] 대추(4년생)",
    "[00440] 대추(5년생)",
    "[00441] 대추(6년이상)",
    "[00453] 돌배",
    "[00733] 떫은감(1-2년생)",
    "[00738] 떫은감(11년생 이상)",
    "[00734] 떫은감(3-4년생)",
    "[00735] 떫은감(5-6년생)",
    "[00736] 떫은감(7-8년생)",
    "[00737] 떫은감(9-10년생)",
    "[00333] 매실(1~2년)",
    "[00334] 매실(3~4년)",
    "[00335] 매실(5~6년)",
    "[00336] 매실(7~8년)",
    "[00337] 매실(9년 이상)",
    "[00473] 무화과(1-2년)",
    "[00477] 무화과(10년이상)",
    "[00474] 무화과(3-4년)",
    "[00475] 무화과(5-6년)",
    "[00476] 무화과(7-9년)",
    "[00113] 밤나무(10-14년생)",
    "[00114] 밤나무(15-19년생)",
    "[00107] 밤나무(1년생)",
    "[00115] 밤나무(20-25년생)",
    "[00108] 밤나무(2년생)",
    "[00109] 밤나무(3년생)",
    "[00110] 밤나무(4년생)",
    "[00111] 밤나무(5-6년생)",
    "[00112] 밤나무(7-9년생)",
    "[00066] 배(1-4년생)",
    "[00068] 배(10-14년생)",
    "[00069] 배(15-19년생)",
    "[00070] 배(20년생 이상)",
    "[00067] 배(5-9년생)",
    "[00075] 복숭아(1-2년생)",
    "[00078] 복숭아(11년생 이상)",
    "[00076] 복숭아(3-4년생)",
    "[00077] 복숭아(5-10년생)",
    "[01011] 부지화(비화)11~15년",
    "[01012] 부지화(비화)16~20년",
    "[01009] 부지화(비화)1~5년",
    "[01013] 부지화(비화)21년이상",
    "[01010] 부지화(비화)6~10년",
    "[01017] 부지화(화산)11~15년",
    "[01018] 부지화(화산)16~20년",
    "[01015] 부지화(화산)1~5년",
    "[01019] 부지화(화산)21년이상",
    "[01016] 부지화(화산)6~10년",
    "[00551] 블랙베리",
    "[00507] 블랙커런트(1~2년)",
    "[00764] 블랙커런트(3년이상)",
    "[00351] 블루베리(1-2년)",
    "[00523] 블루베리(3-4년)",
    "[00524] 블루베리(5-6년)",
    "[00525] 블루베리(7년)",
    "[00526] 블루베리(8년이상)",
    "[00795] 뽕나무(오디)",
    "[00061] 사과(1-4년생)",
    "[00063] 사과(10-14년생)",
    "[00064] 사과(15-19년생)",
    "[00065] 사과(20년생 이상)",
    "[00062] 사과(5-9년생)",
    "[00354] 산수유(성목)",
    "[00500] 살구(1-2년생)",
    "[00504] 살구(11년이상)",
    "[00501] 살구(3-4년생)",
    "[00502] 살구(5-7년생)",
    "[00503] 살구(8-10년생)",
    "[00794] 서양자두(푸룬)",
    "[00492] 아로니아(1~2년)",
    "[00761] 아로니아(3년이상)",
    "[00361] 앵두",
    "[00412] 유자(11~19년)",
    "[00410] 유자(1~5년)",
    "[00413] 유자(20년이상)",
    "[00411] 유자(6~10년)",
    "[00243] 유자(비화산,11~19년)",
    "[00241] 유자(비화산,1~5년)",
    "[00244] 유자(비화산,20년이상)",
    "[00242] 유자(비화산,6~10년)",
    "[00239] 유자(화산,11~19년)",
    "[00237] 유자(화산,1~5년)",
    "[00240] 유자(화산,20년이상)",
    "[00238] 유자(화산,6~10년)",
    "[00478] 자두(1-2년)",
    "[00479] 자두(3-4년)",
    "[00480] 자두(5-6년)",
    "[00481] 자두(7-8년)",
    "[00482] 자두(9년이상)",
    "[00381] 참다래(1년생,비화산회토)",
    "[00695] 참다래(1년생,비화산회토)",
    "[00682] 참다래(1년생,화산회토)",
    "[00382] 참다래(2년생,비화산회토)",
    "[00696] 참다래(2년생,비화산회토)",
    "[00683] 참다래(2년생,화산회토)",
    "[00383] 참다래(3년생,비화산회토)",
    "[00697] 참다래(3년생,비화산회토)",
    "[00684] 참다래(3년생,화산회토)",
    "[00384] 참다래(4년생,비화산회토)",
    "[00698] 참다래(4년생,비화산회토)",
    "[00685] 참다래(4년생,화산회토)",
    "[00385] 참다래(5년생,비화산회토)",
    "[00699] 참다래(5년생,비화산회토)",
    "[00686] 참다래(5년생,화산회토)",
    "[00386] 참다래(6년생,비화산회토)",
    "[00700] 참다래(6년생,비화산회토)",
    "[00687] 참다래(6년생,화산회토)",
    "[00387] 참다래(7년생,비화산회토)",
    "[00701] 참다래(7년생,비화산회토)",
    "[00688] 참다래(7년생,화산회토)",
    "[00388] 참다래(8년이상,비화산회토)",
    "[00702] 참다래(8년이상,비화산회토)",
    "[00689] 참다래(8년이상,화산회토)",
    "[00071] 포도(1-2년생)",
    "[00074] 포도(11년생 이상)",
    "[00072] 포도(3-4년생)",
    "[00073] 포도(5-10년생)",
    "[00743] 포도(샤인머스캣, 5년이상)",
    "[01000] 포도(샤인머스캣,1~2년)",
    "[01001] 포도(샤인머스캣,3~4년)",
    "[00615] 플럼코트",
    "[00762] 허니베리(1~2년)",
    "[00763] 허니베리(3년이상)"
  ],
  "약용작물": [
    "[00457] 감초",
    "[00528] 강활",
    "[00460] 강황(개간지)",
    "[01035] 강황(기경지)",
    "[00557] 결명자",
    "[00137] 구기자",
    "[00136] 구약감자",
    "[00134] 길경(도라지)",
    "[00600] 단삼",
    "[00249] 당귀",
    "[00148] 더덕",
    "[00692] 더덕(화산회토)",
    "[00604] 더위지기(인진쑥)",
    "[00315] 땅두릅(독활,뿌리)",
    "[00396] 마(장마,단마)",
    "[00145] 맥문동",
    "[00586] 모링가",
    "[00144] 박하",
    "[00135] 반하",
    "[00663] 배암차즈기(곰보배추)",
    "[00141] 백지",
    "[00132] 백하수오",
    "[00391] 복분자(1년생)",
    "[00392] 복분자(2년생 이상)",
    "[00514] 삼백초",
    "[00456] 삽주",
    "[00143] 스테비아",
    "[00619] 식방풍",
    "[00428] 어성초",
    "[00463] 엉겅퀴",
    "[00364] 오갈피",
    "[00365] 오미자(1년)",
    "[00434] 오미자(2년)",
    "[00435] 오미자(3년이상)",
    "[00146] 율무",
    "[00576] 인삼(예정지)-논",
    "[00338] 인삼(예정지)-밭",
    "[00250] 일천궁",
    "[00245] 작약(1년)",
    "[00246] 작약(2년)",
    "[00247] 작약(3년)",
    "[00454] 잔대(뿌리)",
    "[00139] 적하수오",
    "[00251] 지황(신품종)",
    "[00133] 지황(재래종)",
    "[00147] 향부자",
    "[00416] 헛개나무",
    "[00248] 홍화",
    "[00332] 황금",
    "[00140] 황기",
    "[00142] 황련",
    "[00520] 황칠나무"
  ],
  "화훼류": [
    "[00127] 1년초(노지)",
    "[00126] 1년초(절화)",
    "[00125] 구근류(시설재배)",
    "[00123] 국화(노지재배)",
    "[00122] 국화(온실절화)",
    "[00330] 나리",
    "[00597] 리시안서스",
    "[00666] 안개초",
    "[00767] 알스트로메리아",
    "[00765] 용담",
    "[00121] 장미",
    "[00124] 카네이션(시설재배)",
    "[00766] 코스모스",
    "[00560] 프리지아",
    "[00547] 해바라기"
  ],
  "사료작물": [
    "[00486] 귀리(사료, 하파용)",
    "[00150] 목초(관리용)",
    "[00675] 목초(관리용, 두과목초)",
    "[00676] 목초(관리용, 방목초지)",
    "[00149] 목초(조성용)",
    "[01077] 목초(화본과,추비용)",
    "[00798] 수단그라스",
    "[00677] 유채(사료용)",
    "[00394] 이탈리안라이그라스",
    "[01043] 이탈리안라이그라스(화산회토)",
    "[00509] 청보리",
    "[00151] 청예옥수수",
    "[00152] 청예옥수수(화산회)",
    "[00483] 청예용수수류",
    "[00640] 초지(방목용)",
    "[00575] 케나프",
    "[00727] 케나프(간척지)",
    "[00797] 트리티케일",
    "[00716] 피(사료용)",
    "[00508] 호밀(사료용)"
  ],
  "기타": [
  ],
  "미분류": [
    "[00455] 가죽나무",
    "[00747] 가죽나물",
    "[00449] 감",
    "[00803] 개나리",
    "[00421] 개똥쑥",
    "[00660] 개복숭아",
    "[00558] 개암나무",
    "[00804] 개양귀비",
    "[00527] 거베라",
    "[00659] 게걸무",
    "[01024] 겨자채",
    "[00451] 고로쇠나무",
    "[00806] 고마리",
    "[00529] 고비",
    "[00574] 고수(방아풀)",
    "[00341] 곤드레",
    "[00807] 골담초",
    "[01050] 골드키위",
    "[00705] 공심채",
    "[01022] 곽향",
    "[00595] 관음죽",
    "[00568] 광나무",
    "[00572] 구상나무",
    "[00731] 구실잣밤나무",
    "[00342] 구아바",
    "[00490] 구절초",
    "[00542] 글로리오샤",
    "[00808] 금계국",
    "[00658] 금목서",
    "[00728] 금화규",
    "[00606] 기린초",
    "[00537] 기타",
    "[00617] 까마중",
    "[00579] 꽃사과",
    "[01052] 꽃잔디",
    "[00612] 낙엽송",
    "[00819] 난지형 잔디",
    "[01014] 남바람꽃",
    "[01057] 네마장황",
    "[00548] 노나무",
    "[01005] 노니",
    "[00726] 녹보수",
    "[00417] 느릅나무",
    "[01076] 느타리버섯",
    "[00328] 느티나무",
    "[01034] 능이버섯",
    "[00321] 단무지",
    "[00573] 단풍나무",
    "[01067] 달뿌리풀(용상초)",
    "[01021] 당아욱",
    "[00681] 대나무",
    "[00326] 대마",
    "[00725] 델피니움",
    "[00470] 도꼬마리",
    "[00318] 도라지",
    "[00739] 독활",
    "[00570] 동과",
    "[01059] 동국화",
    "[00628] 동백나무",
    "[00642] 동설목(해피엔젤)",
    "[00418] 돼지감자",
    "[00347] 두릅",
    "[00327] 두충나무",
    "[00348] 둥글레",
    "[01031] 때죽나무",
    "[00650] 라런큘러스",
    "[00592] 라벤더",
    "[00809] 라일락",
    "[00752] 라즈베리",
    "[00652] 레드향",
    "[00720] 레몬",
    "[01030] 레몬밤",
    "[00712] 로즈마리",
    "[00564] 롱빈",
    "[00661] 루꼴라",
    "[00715] 마가목",
    "[00611] 마카",
    "[00610] 마키베리",
    "[01072] 만삼",
    "[00540] 망고",
    "[01069] 망초대",
    "[00583] 맨드라미",
    "[00643] 멀구슬나무",
    "[00753] 메리골드",
    "[01060] 메타세쿼이아",
    "[00598] 멜라초",
    "[00561] 명월초",
    "[00471] 모과",
    "[00567] 모로헤이야",
    "[00751] 목련",
    "[00722] 목이버섯",
    "[00511] 목화",
    "[00750] 묘삼",
    "[01008] 무궁화",
    "[00746] 물레나물",
    "[00655] 물푸레나무",
    "[00310] 미나리",
    "[00534] 미역취(노지)",
    "[00711] 민트",
    "[00649] 바나나",
    "[00603] 바질",
    "[00729] 박달나무",
    "[00516] 밤호박",
    "[00799] 배롱나무",
    "[00569] 백년초",
    "[00664] 백련화",
    "[00817] 백리향",
    "[00805] 백산차",
    "[00582] 백일홍",
    "[00495] 백합",
    "[00614] 백합나무",
    "[00549] 백향과",
    "[00636] 번행초",
    "[00587] 벌나무",
    "[00745] 범부채",
    "[01004] 벚나무",
    "[01070] 베고니아",
    "[00656] 병꽃나무",
    "[00472] 보리수",
    "[01033] 복령",
    "[01054] 부지갱이 나물",
    "[00517] 부지화",
    "[01037] 블루아이스",
    "[00818] 비비추",
    "[00624] 비자나무",
    "[01025] 비타민",
    "[00397] 비타민나무",
    "[00406] 비파",
    "[01026] 사철나무",
    "[00403] 사탕수수",
    "[00510] 사향나무",
    "[00800] 산겨릅나무",
    "[00605] 산딸나무",
    "[00353] 산머루",
    "[00535] 산사나무",
    "[00462] 산약",
    "[00404] 산양삼",
    "[00447] 산초나무",
    "[01041] 삼잎국화",
    "[00609] 삼지구엽초",
    "[00497] 삼채",
    "[00613] 상수리나무",
    "[00562] 상황버섯",
    "[00518] 새싹채소",
    "[00810] 생강나무",
    "[00355] 석류",
    "[00459] 석잠풀(초석잠)",
    "[00488] 석창포",
    "[00407] 선인장",
    "[00531] 섬바디",
    "[00398] 세발나물",
    "[00427] 소나무",
    "[00458] 소엽",
    "[00724] 송로버섯",
    "[00647] 송악",
    "[00425] 송이",
    "[00469] 쇠비름",
    "[00444] 수국",
    "[00571] 수수꽃다리",
    "[01032] 스타티스",
    "[00515] 스토크",
    "[00812] 스피아민트",
    "[00811] 시로미",
    "[00461] 시호",
    "[01042] 싸리버섯",
    "[01023] 쌈추",
    "[00395] 쑥",
    "[01007] 아까시나무",
    "[00721] 아네모네",
    "[00626] 아마",
    "[01029] 아마릴리스",
    "[00704] 아보카도",
    "[00634] 아사이베리",
    "[01073] 아스틸베",
    "[00505] 아왜나무",
    "[01027] 아이스플랜트",
    "[00585] 아주까리",
    "[00653] 아카시아나무",
    "[00552] 아티초크",
    "[00448] 안개꽃",
    "[00358] 알로에",
    "[01062] 알팔파",
    "[00594] 야관문",
    "[00559] 양송이",
    "[00668] 양하",
    "[00442] 엄나무",
    "[01046] 에키네시아",
    "[00801] 엘더베리",
    "[00363] 연",
    "[00446] 연산홍",
    "[00740] 열매마(하늘마)",
    "[00730] 영지버섯",
    "[00563] 오크라",
    "[00680] 올리브",
    "[00422] 옻나무",
    "[00409] 와송",
    "[00719] 왕골",
    "[00565] 왕벗나무",
    "[00538] 우슬",
    "[00399] 울금",
    "[00813] 원추리",
    "[01064] 월계수나무",
    "[00641] 유칼립투스",
    "[01006] 으름",
    "[00429] 은행",
    "[00581] 이팝나무",
    "[00533] 익모초",
    "[00814] 인동",
    "[01028] 자소엽",
    "[01063] 자운영",
    "[00452] 자작나무",
    "[00566] 잣나무",
    "[00366] 장군차",
    "[00426] 전나무",
    "[01071] 제라늄",
    "[00633] 조팝나무",
    "[00657] 주목나무",
    "[00546] 죽순",
    "[00644] 줄사철나무",
    "[00498] 쥐똥나무",
    "[00400] 지초",
    "[00802] 진달래",
    "[01055] 진주조",
    "[00621] 질경이",
    "[00584] 쪽",
    "[00815] 찔레",
    "[00714] 차요테",
    "[00607] 차조",
    "[00599] 차조기",
    "[00669] 참가시나무",
    "[01053] 참나무",
    "[00713] 참박",
    "[00443] 참죽",
    "[00408] 천년초",
    "[00379] 천마",
    "[00465] 천문동",
    "[00601] 천일초",
    "[01039] 천혜향",
    "[00369] 철쭉",
    "[00506] 체리",
    "[00816] 초롱꽃",
    "[00484] 초코베리",
    "[00468] 초피나무",
    "[00329] 취나물",
    "[00184] 취나물(시설)",
    "[00630] 측백나무",
    "[00667] 치자",
    "[01061] 칠자화",
    "[00625] 칡",
    "[00654] 카넬리니빈",
    "[01044] 카라향",
    "[00491] 칼라",
    "[00710] 캐모마일",
    "[00616] 커피나무",
    "[01049] 케일",
    "[00627] 퀴노아",
    "[01048] 키위베리",
    "[00732] 탱자",
    "[00651] 튤립",
    "[00706] 티머시(큰조아재비)",
    "[00709] 파슬리",
    "[00370] 파인애플",
    "[01056] 파인애플구아바(훼이조아)",
    "[00423] 파파야",
    "[01045] 팬지",
    "[00580] 팽나무",
    "[00665] 페칸나무",
    "[00723] 페퍼민트",
    "[00494] 편백나무",
    "[00620] 포포나무",
    "[00485] 표고버섯",
    "[00820] 피칸",
    "[01036] 필로덴드론셀로움",
    "[00703] 핑크뮬리그라스",
    "[00622] 하니베리",
    "[00638] 하스카프",
    "[01058] 하우스솔고",
    "[00543] 한라봉",
    "[00593] 한련초",
    "[00717] 한련화",
    "[00499] 한지형 잔디",
    "[00539] 함초",
    "[00590] 해당화",
    "[00744] 향나무",
    "[00402] 허브류",
    "[00707] 헤어리베치",
    "[00393] 호두",
    "[01047] 호라산밀",
    "[00748] 호리병박",
    "[01020] 호프",
    "[00662] 홉",
    "[01003] 홍가시나무",
    "[01002] 홍콩야자",
    "[00648] 화백나무",
    "[00749] 화살나무",
    "[01066] 환삼덩굴",
    "[00401] 황금소나무",
    "[01040] 황금향",
    "[00489] 회양목",
    "[00629] 후박나무",
    "[00550] 흑임자",
    "[00632] 히카마"
  ]
}

def generate_crop_sm_uid():
    return f"CROPSM_{str(uuid.uuid4())[:8].upper()}"

def insert_crop_lg():
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # 기존 데이터 삭제
        cursor.execute("TRUNCATE TABLE jadxdb2.crop_sm CASCADE")
        cursor.execute("TRUNCATE TABLE jadxdb2.crop_lg CASCADE")

        # crop_lg 테이블에 대분류 데이터 삽입
        for crop_lg_nm, crop_lg_uid in crop_lg_mapping.items():
            insert_query = """
            INSERT INTO jadxdb2.crop_lg (
                crop_lg_uid, crop_lg_nm, reg_uid
            )
            VALUES (%s, %s, 'SYSTEM')
            ON CONFLICT (crop_lg_uid) DO NOTHING
            """
            cursor.execute(insert_query, (crop_lg_uid, crop_lg_nm))

        conn.commit()
        print("Crop large categories inserted successfully.")

    except Exception as e:
        print(f"An error occurred while inserting crop_lg: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def insert_crop_sm(data):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # 데이터 삽입
        for crop_lg, crops in data.items():
            crop_lg_uid = crop_lg_mapping[crop_lg]
            for crop in crops:
                code, name = crop.split('] ')
                code = code[1:]  # 대괄호 제거
                display_name = f"[{code}] {name}"  # dsply_nm 형식
                crop_sm_uid = generate_crop_sm_uid()

                insert_query = """
                INSERT INTO jadxdb2.crop_sm (
                    crop_sm_uid, crop_lg_uid, crop_sm_nm, cd, dsply_nm, reg_uid
                )
                VALUES (%s, %s, %s, %s, %s, 'SYSTEM')
                ON CONFLICT (cd) DO UPDATE 
                SET 
                    crop_lg_uid = EXCLUDED.crop_lg_uid,
                    crop_sm_nm = EXCLUDED.crop_sm_nm,
                    dsply_nm = EXCLUDED.dsply_nm,
                    mdfcn_uid = 'SYSTEM',
                    mdfcn_dt = CURRENT_TIMESTAMP
                """
                cursor.execute(insert_query, (crop_sm_uid, crop_lg_uid, name.strip(), code, display_name))

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
    # 먼저 대분류 데이터 삽입
    insert_crop_lg()
    # 그 다음 소분류 데이터 삽입
    insert_crop_sm(data)
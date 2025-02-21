import requests
import random
import time
from fake_useragent import UserAgent
from rotating_proxies import ProxyRotator
from typing import Optional, List

class NaverTrafficGenerator:
    def __init__(self):
        self.ua = UserAgent()
        self.proxy_rotator = ProxyRotator()
        self.keywords = []
        self.place_urls = []
        
    def add_keywords(self, keywords):
        """키워드 리스트를 추가합니다."""
        if isinstance(keywords, str):
            self.keywords.append(keywords)
        elif isinstance(keywords, list):
            self.keywords.extend(keywords)
            
    def add_place_urls(self, urls):
        """장소 URL 리스트를 추가합니다."""
        if isinstance(urls, str):
            self.place_urls.append(urls)
        elif isinstance(urls, list):
            self.place_urls.extend(urls)
            
    def run_traffic_campaign(self, visits_per_url=5):
        """트래픽 생성 캠페인을 실행합니다."""
        for url in self.place_urls:
            for _ in range(visits_per_url):
                success = self.visit_page(url)
                if success:
                    print(f"성공적으로 방문완료: {url}")
                time.sleep(random.uniform(10, 30))  # 방문 사이의 대기 시간
        
    def generate_headers(self):
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'DNT': '1'
        }
    
    def simulate_human_behavior(self):
        # 실제 사용자처럼 페이지에서 머무는 시간
        time.sleep(random.uniform(30, 180))
        
    def visit_page(self, place_url):
        try:
            proxy = self.proxy_rotator.get_proxy()
            headers = self.generate_headers()
            
            response = requests.get(
                place_url,
                headers=headers,
                proxies={'http': proxy, 'https': proxy},
                timeout=30
            )
            
            self.simulate_human_behavior()
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"방문 실패: {str(e)}")
            return False
        
    def manage_cookies(self):
        return {
            'NID_AUT': self.generate_random_cookie(),
            'NID_SES': self.generate_random_cookie(),
            # 기타 필요한 쿠키들
        }

    def generate_device_info(self):
        devices = [
            'iPhone', 'Android', 'iPad',
            'Windows NT 10.0', 'Macintosh'
        ]
        return random.choice(devices)

class ProxyRotator:
    def __init__(self):
        self.proxies: List[str] = []
        self.current_index: int = 0
        self.initialize_proxy_list()
    
    def initialize_proxy_list(self) -> None:
        """프록시 목록을 초기화합니다."""
        # 방법 1: 무료 프록시 API에서 프록시 목록 가져오기
        try:
            response = requests.get('https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all')
            if response.status_code == 200:
                self.proxies = [proxy.strip() for proxy in response.text.split('\n') if proxy.strip()]
        except:
            # API 호출 실패시 기본 프록시 사용
            self.use_fallback_proxies()
    
    def use_fallback_proxies(self) -> None:
        """기본 프록시 목록을 설정합니다."""
        # 실제 운영시에는 신뢰할 수 있는 프록시 서버 목록을 사용해야 합니다
        self.proxies = [
            "http://proxy1.example.com:8080",
            "http://proxy2.example.com:8080",
            "http://proxy3.example.com:8080"
        ]
    
    def get_proxy(self) -> Optional[str]:
        """다음 프록시를 반환합니다."""
        if not self.proxies:
            return None
            
        # 라운드 로빈 방식으로 프록시 선택
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy
    
    def remove_proxy(self, proxy: str) -> None:
        """작동하지 않는 프록시를 제거합니다."""
        if proxy in self.proxies:
            self.proxies.remove(proxy)
    
    def test_proxy(self, proxy: str) -> bool:
        """프록시가 정상 작동하는지 테스트합니다."""
        try:
            response = requests.get(
                'http://www.google.com',
                proxies={'http': proxy, 'https': proxy},
                timeout=5
            )
            return response.status_code == 200
        except:
            return False
        
# generator = NaverTrafficGenerator()
# generator.add_keywords(['맛집', '카페'])
# generator.add_place_urls([
#     'https://place.naver.com/restaurant/1234567890',
#     'https://place.naver.com/restaurant/0987654321'
# ])
# generator.run_traffic_campaign(visits_per_url=5)
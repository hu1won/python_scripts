import requests
import time
from typing import Optional, List, Dict
from datetime import datetime, timedelta

class ProxyRotator:
    def __init__(self):
        self.proxies: List[Dict] = []
        self.current_index: int = 0
        self.last_update: datetime = datetime.min
        self.update_interval: timedelta = timedelta(hours=1)
        self.initialize_proxy_list()
    
    def initialize_proxy_list(self) -> None:
        """프록시 목록을 초기화하고 테스트합니다."""
        self._fetch_proxies()
        self._test_all_proxies()
        
    def _fetch_proxies(self) -> None:
        """여러 소스에서 프록시를 가져옵니다."""
        proxy_apis = [
            'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all',
            'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
            'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt'
        ]
        
        temp_proxies = set()
        for api in proxy_apis:
            try:
                response = requests.get(api, timeout=10)
                if response.status_code == 200:
                    proxies = [proxy.strip() for proxy in response.text.split('\n') if proxy.strip()]
                    temp_proxies.update(proxies)
            except Exception as e:
                print(f"프록시 API 호출 실패: {api} - {str(e)}")
                
        if not temp_proxies:
            self.use_fallback_proxies()
        else:
            self.proxies = [
                {
                    'address': proxy,
                    'fails': 0,
                    'last_used': datetime.min,
                    'average_response': 0
                } 
                for proxy in temp_proxies
            ]
    
    def use_fallback_proxies(self) -> None:
        """기본 프록시 목록을 설정합니다."""
        fallback_proxies = [
            "http://proxy1.example.com:8080",
            "http://proxy2.example.com:8080",
            "http://proxy3.example.com:8080"
        ]
        
        self.proxies = [
            {
                'address': proxy,
                'fails': 0,
                'last_used': datetime.min,
                'average_response': 0
            } 
            for proxy in fallback_proxies
        ]
    
    def _test_all_proxies(self) -> None:
        """모든 프록시를 테스트하고 작동하지 않는 것을 제거합니다."""
        working_proxies = []
        for proxy in self.proxies:
            if self.test_proxy(proxy['address']):
                working_proxies.append(proxy)
        self.proxies = working_proxies
    
    def get_proxy(self) -> Optional[str]:
        """다음 프록시를 반환합니다."""
        if not self.proxies:
            self.initialize_proxy_list()
            if not self.proxies:
                return None
                
        # 프록시 목록 업데이트가 필요한지 확인
        if datetime.now() - self.last_update > self.update_interval:
            self.initialize_proxy_list()
            
        # 가장 적게 실패하고 오래 사용하지 않은 프록시 선택
        self.proxies.sort(key=lambda x: (x['fails'], x['last_used']))
        selected_proxy = self.proxies[0]
        
        # 프록시 사용 정보 업데이트
        selected_proxy['last_used'] = datetime.now()
        
        return selected_proxy['address']
    
    def remove_proxy(self, proxy: str) -> None:
        """작동하지 않는 프록시를 제거하거나 실패 횟수를 증가시킵니다."""
        for p in self.proxies:
            if p['address'] == proxy:
                p['fails'] += 1
                if p['fails'] >= 3:  # 3번 이상 실패하면 제거
                    self.proxies.remove(p)
                break
    
    def test_proxy(self, proxy: str) -> bool:
        """프록시가 정상 작동하는지 테스트합니다."""
        test_urls = [
            'http://www.google.com',
            'http://www.example.com'
        ]
        
        for url in test_urls:
            try:
                start_time = time.time()
                response = requests.get(
                    url,
                    proxies={'http': proxy, 'https': proxy},
                    timeout=5
                )
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    # 프록시 응답 시간 업데이트
                    for p in self.proxies:
                        if p['address'] == proxy:
                            if p['average_response'] == 0:
                                p['average_response'] = response_time
                            else:
                                p['average_response'] = (p['average_response'] + response_time) / 2
                            break
                    return True
            except:
                continue
        
        return False
    
    def get_stats(self) -> Dict:
        """프록시 통계를 반환합니다."""
        return {
            'total_proxies': len(self.proxies),
            'average_response_time': sum(p['average_response'] for p in self.proxies) / len(self.proxies) if self.proxies else 0,
            'failed_proxies': sum(1 for p in self.proxies if p['fails'] > 0),
            'last_update': self.last_update
        } 
    

# proxy_rotator = ProxyRotator()
# proxy = proxy_rotator.get_proxy()
# if proxy:
#     try:
#         # 프록시 사용
#         response = requests.get('http://example.com', proxies={'http': proxy, 'https': proxy})
#     except:
#         proxy_rotator.remove_proxy(proxy)

# # 통계 확인
# stats = proxy_rotator.get_stats()
# print(stats)
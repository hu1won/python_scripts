import random
from typing import List, Dict

class UserAgent:
    def __init__(self):
        self.browsers: Dict[str, List[str]] = {
            'chrome': [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ],
            'firefox': [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0',
                'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0'
            ],
            'safari': [
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
            ],
            'edge': [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
            ]
        }
        
        self.mobile_devices: List[str] = [
            'iPhone',
            'iPad',
            'Samsung Galaxy S23',
            'Google Pixel 8',
            'OnePlus 11'
        ]
        
        self.os_versions: Dict[str, List[str]] = {
            'Windows': ['10.0', '11.0'],
            'MacOS': ['10_15_7', '11_0', '12_0'],
            'iOS': ['16.0', '17.0'],
            'Android': ['13.0.0', '14.0.0']
        }

    @property
    def random(self) -> str:
        """무작위 User-Agent 문자열을 반환합니다."""
        browser = random.choice(list(self.browsers.keys()))
        return random.choice(self.browsers[browser])
    
    def chrome(self) -> str:
        """Chrome User-Agent를 반환합니다."""
        return random.choice(self.browsers['chrome'])
    
    def firefox(self) -> str:
        """Firefox User-Agent를 반환합니다."""
        return random.choice(self.browsers['firefox'])
    
    def safari(self) -> str:
        """Safari User-Agent를 반환합니다."""
        return random.choice(self.browsers['safari'])
    
    def edge(self) -> str:
        """Edge User-Agent를 반환합니다."""
        return random.choice(self.browsers['edge'])
    
    def mobile(self) -> str:
        """모바일 User-Agent를 생성합니다."""
        device = random.choice(self.mobile_devices)
        os = 'iOS' if device in ['iPhone', 'iPad'] else 'Android'
        version = random.choice(self.os_versions[os])
        
        if os == 'iOS':
            return f'Mozilla/5.0 ({device}; CPU iPhone OS {version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
        else:
            return f'Mozilla/5.0 (Linux; Android {version}; {device}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36' 
        
# ua = UserAgent()
# print(ua.random)  # 무작위 User-Agent
# print(ua.chrome())  # Chrome User-Agent
# print(ua.mobile())  # 모바일 User-Agent
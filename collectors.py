# file name : collectors.py
# pwd : /dal9/app/ex_app/collectors.py
# 미국 증시 급등주 예측 앱 - 데이터 수집 모듈

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import re

# yfinance는 선택적 (설치되어 있으면 사용)
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    print("[EX_APP] yfinance not installed. Some features will be limited.")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


class FinvizCollector:
    """Finviz에서 스몰캡 뉴스 수집"""
    
    BASE_URL = "https://finviz.com"
    NEWS_URL = "https://finviz.com/news.ashx"
    SCREENER_URL = "https://finviz.com/screener.ashx"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def get_news_for_symbol(self, symbol):
        """특정 종목의 뉴스 수집"""
        try:
            url = f"{self.BASE_URL}/quote.ashx?t={symbol}"
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            news_table = soup.find('table', {'id': 'news-table'})
            if not news_table:
                return []
            
            news_items = []
            rows = news_table.find_all('tr')
            
            current_date = None
            for row in rows[:20]:  # 최근 20개만
                cols = row.find_all('td')
                if len(cols) >= 2:
                    date_cell = cols[0].text.strip()
                    link = cols[1].find('a')
                    
                    if link:
                        # 날짜 파싱
                        if len(date_cell) > 10:
                            current_date = date_cell.split()[0]
                        
                        news_items.append({
                            'symbol': symbol,
                            'headline': link.text.strip(),
                            'url': link.get('href', ''),
                            'source': 'finviz',
                            'published_at': current_date
                        })
            
            return news_items
        except Exception as e:
            print(f"[Finviz] Error fetching news for {symbol}: {e}")
            return []
    
    def get_top_gainers(self, premarket=False):
        """오늘의 급등주 목록"""
        try:
            if premarket:
                # 프리마켓 급등주 (Finviz에서 직접 제공 안함, 다른 소스 필요)
                return []
            
            # 정규장 급등주
            url = f"{self.SCREENER_URL}?v=111&s=ta_topgainers&f=cap_smallover"
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            table = soup.find('table', {'class': 'table-light'})
            if not table:
                return []
            
            gainers = []
            rows = table.find_all('tr')[1:]  # 헤더 제외
            
            for row in rows[:20]:
                cols = row.find_all('td')
                if len(cols) >= 10:
                    symbol = cols[1].text.strip()
                    change = cols[9].text.strip()
                    
                    gainers.append({
                        'symbol': symbol,
                        'change_pct': change,
                        'source': 'finviz'
                    })
            
            return gainers
        except Exception as e:
            print(f"[Finviz] Error fetching top gainers: {e}")
            return []
    
    def get_smallcap_news(self):
        """스몰캡 관련 뉴스 전체"""
        try:
            url = f"{self.SCREENER_URL}?v=111&f=cap_smallover,sh_curvol_o1000&ta=1"
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 스몰캡 종목 리스트 수집
            table = soup.find('table', {'class': 'table-light'})
            if not table:
                return []
            
            symbols = []
            rows = table.find_all('tr')[1:21]  # 상위 20개
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    symbol = cols[1].text.strip()
                    symbols.append(symbol)
            
            # 각 종목의 뉴스 수집
            all_news = []
            for symbol in symbols[:10]:  # API 제한 고려, 10개만
                news = self.get_news_for_symbol(symbol)
                all_news.extend(news)
                time.sleep(0.5)  # Rate limiting
            
            return all_news
        except Exception as e:
            print(f"[Finviz] Error fetching smallcap news: {e}")
            return []


class YahooFinanceCollector:
    """Yahoo Finance에서 주가/거래량 데이터 수집"""
    
    def __init__(self):
        if not HAS_YFINANCE:
            raise ImportError("yfinance is required for YahooFinanceCollector")
    
    def get_stock_info(self, symbol):
        """종목 기본 정보"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            return {
                'symbol': symbol.upper(),
                'name': info.get('shortName', ''),
                'sector': info.get('sector', ''),
                'market_cap': info.get('marketCap', 0),
                'float_shares': info.get('floatShares', 0),
                'is_smallcap': info.get('marketCap', 0) < 2_000_000_000,  # $2B 이하
                'is_penny': info.get('currentPrice', 0) < 5  # $5 이하
            }
        except Exception as e:
            print(f"[Yahoo] Error fetching info for {symbol}: {e}")
            return None
    
    def get_premarket_data(self, symbol):
        """프리마켓 데이터"""
        try:
            ticker = yf.Ticker(symbol)
            
            # 프리마켓 가격 (제한적)
            info = ticker.info
            prev_close = info.get('previousClose', 0)
            premarket_price = info.get('preMarketPrice', prev_close)
            
            if prev_close and premarket_price:
                change_pct = ((premarket_price - prev_close) / prev_close) * 100
            else:
                change_pct = 0
            
            return {
                'symbol': symbol.upper(),
                'prev_close': prev_close,
                'premarket_price': premarket_price,
                'premarket_change_pct': round(change_pct, 2)
            }
        except Exception as e:
            print(f"[Yahoo] Error fetching premarket for {symbol}: {e}")
            return None
    
    def get_volume_spike_candidates(self, symbols):
        """거래량 급증 종목 필터링"""
        candidates = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="1mo")
                
                if len(hist) < 5:
                    continue
                
                avg_volume = hist['Volume'].mean()
                current_volume = hist['Volume'].iloc[-1]
                
                if current_volume > avg_volume * 2:  # 평균의 2배 이상
                    candidates.append({
                        'symbol': symbol,
                        'avg_volume': int(avg_volume),
                        'current_volume': int(current_volume),
                        'volume_ratio': round(current_volume / avg_volume, 2)
                    })
                
                time.sleep(0.2)  # Rate limiting
            except Exception as e:
                print(f"[Yahoo] Error checking volume for {symbol}: {e}")
                continue
        
        return sorted(candidates, key=lambda x: x['volume_ratio'], reverse=True)
    
    def get_current_price(self, symbol):
        """현재가 조회"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1d')
            if len(data) > 0:
                return {
                    'symbol': symbol.upper(),
                    'price': data['Close'].iloc[-1],
                    'high': data['High'].iloc[-1],
                    'low': data['Low'].iloc[-1],
                    'volume': int(data['Volume'].iloc[-1])
                }
            return None
        except Exception as e:
            print(f"[Yahoo] Error fetching price for {symbol}: {e}")
            return None


class RedditCollector:
    """Reddit에서 핫한 종목 수집 (r/wallstreetbets, r/pennystocks)"""
    
    BASE_URL = "https://www.reddit.com"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            **HEADERS,
            'Accept': 'application/json'
        })
    
    def get_hot_posts(self, subreddit='wallstreetbets', limit=25):
        """핫 포스트 수집"""
        try:
            url = f"{self.BASE_URL}/r/{subreddit}/hot.json?limit={limit}"
            response = self.session.get(url, timeout=10)
            data = response.json()
            
            posts = []
            for item in data.get('data', {}).get('children', []):
                post = item.get('data', {})
                posts.append({
                    'title': post.get('title', ''),
                    'score': post.get('score', 0),
                    'num_comments': post.get('num_comments', 0),
                    'url': post.get('url', ''),
                    'created_utc': post.get('created_utc', 0),
                    'source': f'reddit/{subreddit}'
                })
            
            return posts
        except Exception as e:
            print(f"[Reddit] Error fetching from r/{subreddit}: {e}")
            return []
    
    def extract_symbols_from_posts(self, posts):
        """포스트에서 티커 심볼 추출"""
        # 일반적인 티커 패턴: $TSLA, TSLA, $tsla
        ticker_pattern = r'\$?[A-Z]{1,5}\b'
        
        symbol_counts = {}
        
        for post in posts:
            title = post.get('title', '').upper()
            matches = re.findall(ticker_pattern, title)
            
            for match in matches:
                symbol = match.replace('$', '')
                # 일반 영어 단어 필터링
                if symbol not in ['THE', 'AND', 'FOR', 'BUT', 'NOT', 'YOU', 'ALL', 
                                  'CAN', 'HER', 'WAS', 'ONE', 'OUR', 'OUT', 'ARE',
                                  'HAS', 'HIS', 'HOW', 'ITS', 'LET', 'MAY', 'NEW',
                                  'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'BOY', 'DID',
                                  'GET', 'PUT', 'SAY', 'SHE', 'TOO', 'USE', 'BUY',
                                  'SELL', 'HOLD', 'CALL', 'PUTS', 'YOLO', 'DD', 'WSB']:
                    symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        
        # 언급 횟수로 정렬
        sorted_symbols = sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'symbol': s, 'mentions': c, 'source': 'reddit'} for s, c in sorted_symbols[:20]]


class SECEdgarCollector:
    """SEC EDGAR에서 공시 수집"""
    
    RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=100&output=atom"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def get_recent_8k_filings(self):
        """최근 8-K 공시 (중요 이벤트 공시)"""
        try:
            response = self.session.get(self.RSS_URL, timeout=10)
            soup = BeautifulSoup(response.text, 'xml')
            
            filings = []
            entries = soup.find_all('entry')
            
            for entry in entries[:50]:
                title = entry.find('title')
                link = entry.find('link')
                updated = entry.find('updated')
                
                if title:
                    # 종목 티커 추출 시도
                    title_text = title.text
                    # "8-K - COMPANY NAME (0001234567) (Filer)" 형식
                    
                    filings.append({
                        'headline': title_text,
                        'url': link.get('href') if link else '',
                        'published_at': updated.text[:10] if updated else None,
                        'source': 'sec',
                        'catalyst_type': '8k_filing'
                    })
            
            return filings
        except Exception as e:
            print(f"[SEC] Error fetching 8-K filings: {e}")
            return []


# 메인 수집 함수
def collect_all_data(session_id=None):
    """모든 소스에서 데이터 수집"""
    results = {
        'finviz_news': [],
        'top_gainers': [],
        'reddit_mentions': [],
        'sec_filings': [],
        'errors': []
    }
    
    # 1. Finviz 뉴스
    try:
        finviz = FinvizCollector()
        results['finviz_news'] = finviz.get_smallcap_news()
        results['top_gainers'] = finviz.get_top_gainers()
    except Exception as e:
        results['errors'].append(f"Finviz: {e}")
    
    # 2. Reddit
    try:
        reddit = RedditCollector()
        wsb_posts = reddit.get_hot_posts('wallstreetbets')
        penny_posts = reddit.get_hot_posts('pennystocks')
        all_posts = wsb_posts + penny_posts
        results['reddit_mentions'] = reddit.extract_symbols_from_posts(all_posts)
    except Exception as e:
        results['errors'].append(f"Reddit: {e}")
    
    # 3. SEC EDGAR
    try:
        sec = SECEdgarCollector()
        results['sec_filings'] = sec.get_recent_8k_filings()
    except Exception as e:
        results['errors'].append(f"SEC: {e}")
    
    return results


if __name__ == "__main__":
    # 테스트
    print("=== Finviz Test ===")
    finviz = FinvizCollector()
    news = finviz.get_news_for_symbol("AAPL")
    print(f"Found {len(news)} news items for AAPL")
    
    print("\n=== Reddit Test ===")
    reddit = RedditCollector()
    mentions = reddit.extract_symbols_from_posts(reddit.get_hot_posts())
    print(f"Top mentions: {mentions[:5]}")
    
    if HAS_YFINANCE:
        print("\n=== Yahoo Finance Test ===")
        yahoo = YahooFinanceCollector()
        info = yahoo.get_stock_info("AAPL")
        print(f"AAPL info: {info}")

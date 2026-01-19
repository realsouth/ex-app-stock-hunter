# file name : analyzer.py
# pwd : /dal9/app/ex_app/analyzer.py
# 미국 증시 급등주 예측 앱 - 분석 및 예측 모듈

from datetime import datetime
import re

# 뉴스 촉매 키워드 및 가중치
CATALYST_KEYWORDS = {
    # FDA 관련 (최고 가중치)
    'fda approval': {'score': 100, 'type': 'fda'},
    'fda approved': {'score': 100, 'type': 'fda'},
    'fda clears': {'score': 90, 'type': 'fda'},
    'fda accepts': {'score': 80, 'type': 'fda'},
    'breakthrough therapy': {'score': 85, 'type': 'fda'},
    'fast track': {'score': 75, 'type': 'fda'},
    
    # 계약/파트너십
    'contract': {'score': 70, 'type': 'contract'},
    'partnership': {'score': 65, 'type': 'contract'},
    'agreement': {'score': 60, 'type': 'contract'},
    'collaboration': {'score': 55, 'type': 'contract'},
    'deal': {'score': 50, 'type': 'contract'},
    
    # M&A
    'merger': {'score': 85, 'type': 'merger'},
    'acquisition': {'score': 85, 'type': 'merger'},
    'buyout': {'score': 90, 'type': 'merger'},
    'takeover': {'score': 85, 'type': 'merger'},
    
    # 실적
    'beats': {'score': 60, 'type': 'earnings'},
    'exceeds': {'score': 60, 'type': 'earnings'},
    'raises guidance': {'score': 70, 'type': 'earnings'},
    'record revenue': {'score': 65, 'type': 'earnings'},
    
    # 숏스퀴즈
    'short squeeze': {'score': 75, 'type': 'short_squeeze'},
    'short interest': {'score': 50, 'type': 'short_squeeze'},
    'heavily shorted': {'score': 60, 'type': 'short_squeeze'},
    
    # 일반 긍정
    'surges': {'score': 40, 'type': 'momentum'},
    'soars': {'score': 40, 'type': 'momentum'},
    'jumps': {'score': 35, 'type': 'momentum'},
    'rallies': {'score': 35, 'type': 'momentum'},
    'spikes': {'score': 35, 'type': 'momentum'},
}

# 부정적 키워드 (점수 감점)
NEGATIVE_KEYWORDS = {
    'lawsuit': -30,
    'investigation': -25,
    'downgrade': -40,
    'misses': -35,
    'fails': -40,
    'declined': -20,
    'drops': -20,
    'falls': -20,
    'bankruptcy': -50,
    'delisting': -60,
    'sec investigation': -45,
    'fraud': -50,
}


class NewsAnalyzer:
    """뉴스 분석 및 점수화"""
    
    def analyze_headline(self, headline):
        """헤드라인 분석하여 점수와 촉매 유형 반환"""
        if not headline:
            return {'score': 0, 'catalyst_type': 'other', 'sentiment': 0}
        
        headline_lower = headline.lower()
        total_score = 0
        catalyst_type = 'other'
        max_catalyst_score = 0
        
        # 긍정 키워드 확인
        for keyword, data in CATALYST_KEYWORDS.items():
            if keyword in headline_lower:
                total_score += data['score']
                if data['score'] > max_catalyst_score:
                    max_catalyst_score = data['score']
                    catalyst_type = data['type']
        
        # 부정 키워드 확인
        for keyword, penalty in NEGATIVE_KEYWORDS.items():
            if keyword in headline_lower:
                total_score += penalty  # 음수값 더하기
        
        # 점수 범위 제한 (0-100)
        total_score = max(0, min(100, total_score))
        
        # 센티멘트 계산 (-1 ~ 1)
        if total_score >= 70:
            sentiment = 0.8
        elif total_score >= 50:
            sentiment = 0.5
        elif total_score >= 30:
            sentiment = 0.2
        elif total_score > 0:
            sentiment = 0.1
        else:
            sentiment = -0.3
        
        return {
            'score': total_score,
            'catalyst_type': catalyst_type,
            'sentiment': sentiment
        }
    
    def analyze_news_batch(self, news_items):
        """뉴스 배치 분석"""
        analyzed = []
        for item in news_items:
            analysis = self.analyze_headline(item.get('headline', ''))
            analyzed.append({
                **item,
                'importance_score': analysis['score'],
                'catalyst_type': analysis['catalyst_type'],
                'sentiment_score': analysis['sentiment']
            })
        
        # 점수순 정렬
        return sorted(analyzed, key=lambda x: x['importance_score'], reverse=True)


class StockScorer:
    """종목 종합 점수 계산"""
    
    def __init__(self):
        self.news_analyzer = NewsAnalyzer()
    
    def calculate_news_score(self, news_items):
        """뉴스 기반 점수 (0-100)"""
        if not news_items:
            return 0
        
        # 최근 뉴스 중 가장 높은 점수
        max_score = 0
        for item in news_items:
            analysis = self.news_analyzer.analyze_headline(item.get('headline', ''))
            max_score = max(max_score, analysis['score'])
        
        return max_score
    
    def calculate_momentum_score(self, premarket_change_pct, volume_ratio=1):
        """모멘텀 점수 (0-100)"""
        score = 0
        
        # 프리마켓 변동률 기반
        if premarket_change_pct >= 20:
            score += 50
        elif premarket_change_pct >= 10:
            score += 40
        elif premarket_change_pct >= 5:
            score += 30
        elif premarket_change_pct >= 2:
            score += 15
        
        # 거래량 비율 기반
        if volume_ratio >= 5:
            score += 50
        elif volume_ratio >= 3:
            score += 35
        elif volume_ratio >= 2:
            score += 20
        elif volume_ratio >= 1.5:
            score += 10
        
        return min(100, score)
    
    def calculate_social_score(self, reddit_mentions=0, stocktwits_mentions=0):
        """소셜 미디어 점수 (0-100)"""
        total_mentions = reddit_mentions + stocktwits_mentions
        
        if total_mentions >= 50:
            return 100
        elif total_mentions >= 30:
            return 75
        elif total_mentions >= 15:
            return 50
        elif total_mentions >= 5:
            return 25
        elif total_mentions >= 1:
            return 10
        return 0
    
    def calculate_total_score(self, news_score, momentum_score, social_score):
        """종합 점수 계산 (가중 평균)"""
        # 가중치: 뉴스 40%, 모멘텀 35%, 소셜 25%
        total = (news_score * 0.40) + (momentum_score * 0.35) + (social_score * 0.25)
        return round(total, 2)


class PredictionEngine:
    """최종 예측 생성 엔진"""
    
    def __init__(self):
        self.scorer = StockScorer()
        self.news_analyzer = NewsAnalyzer()
    
    def generate_predictions(self, collected_data, top_n=5):
        """
        수집된 데이터를 기반으로 Top N 예측 생성
        
        Args:
            collected_data: collectors.collect_all_data()의 결과
            top_n: 반환할 상위 종목 수
        
        Returns:
            list: 예측 종목 리스트
        """
        symbol_scores = {}
        symbol_data = {}
        
        # 1. 뉴스 데이터 처리
        for news in collected_data.get('finviz_news', []):
            symbol = news.get('symbol', '').upper()
            if not symbol:
                continue
            
            if symbol not in symbol_scores:
                symbol_scores[symbol] = {'news': 0, 'momentum': 0, 'social': 0}
                symbol_data[symbol] = {'news': [], 'mentions': 0}
            
            analysis = self.news_analyzer.analyze_headline(news.get('headline', ''))
            symbol_data[symbol]['news'].append({
                **news,
                **analysis
            })
            symbol_scores[symbol]['news'] = max(
                symbol_scores[symbol]['news'],
                analysis['score']
            )
        
        # 2. 탑 게이너 데이터 처리
        for gainer in collected_data.get('top_gainers', []):
            symbol = gainer.get('symbol', '').upper()
            if not symbol:
                continue
            
            if symbol not in symbol_scores:
                symbol_scores[symbol] = {'news': 0, 'momentum': 0, 'social': 0}
                symbol_data[symbol] = {'news': [], 'mentions': 0}
            
            # 변동률 파싱 (예: "15.32%")
            change_str = gainer.get('change_pct', '0%')
            try:
                change = float(change_str.replace('%', ''))
            except:
                change = 0
            
            symbol_scores[symbol]['momentum'] = self.scorer.calculate_momentum_score(change)
        
        # 3. Reddit 멘션 처리
        for mention in collected_data.get('reddit_mentions', []):
            symbol = mention.get('symbol', '').upper()
            if not symbol:
                continue
            
            if symbol not in symbol_scores:
                symbol_scores[symbol] = {'news': 0, 'momentum': 0, 'social': 0}
                symbol_data[symbol] = {'news': [], 'mentions': 0}
            
            mentions = mention.get('mentions', 1)
            symbol_data[symbol]['mentions'] = mentions
            symbol_scores[symbol]['social'] = self.scorer.calculate_social_score(mentions)
        
        # 4. 종합 점수 계산
        predictions = []
        for symbol, scores in symbol_scores.items():
            total_score = self.scorer.calculate_total_score(
                scores['news'],
                scores['momentum'],
                scores['social']
            )
            
            # 가장 높은 점수의 뉴스 찾기
            best_news = None
            catalyst_type = 'other'
            if symbol_data[symbol]['news']:
                sorted_news = sorted(
                    symbol_data[symbol]['news'],
                    key=lambda x: x.get('score', 0),
                    reverse=True
                )
                if sorted_news:
                    best_news = sorted_news[0]
                    catalyst_type = best_news.get('catalyst_type', 'other')
            
            # 카테고리 결정
            if scores['news'] >= 50:
                category = 'news_catalyst'
            elif scores['momentum'] >= 50:
                category = 'premarket_gainer'
            elif scores['social'] >= 50:
                category = 'penny_runner'
            else:
                category = 'volume_explosion'
            
            predictions.append({
                'symbol': symbol,
                'confidence_score': total_score,
                'category': category,
                'catalyst_type': catalyst_type,
                'news_score': scores['news'],
                'momentum_score': scores['momentum'],
                'social_score': scores['social'],
                'reasoning': self._generate_reasoning(symbol, scores, best_news),
                'top_news': best_news.get('headline') if best_news else None
            })
        
        # 5. 점수순 정렬 후 상위 N개 반환
        predictions.sort(key=lambda x: x['confidence_score'], reverse=True)
        
        # 순위 부여
        for i, pred in enumerate(predictions[:top_n]):
            pred['pick_rank'] = i + 1
        
        return predictions[:top_n]
    
    def _generate_reasoning(self, symbol, scores, best_news):
        """예측 근거 생성"""
        reasons = []
        
        if scores['news'] >= 70:
            reasons.append(f"강력한 뉴스 촉매 (점수: {scores['news']})")
        elif scores['news'] >= 40:
            reasons.append(f"긍정적 뉴스 (점수: {scores['news']})")
        
        if scores['momentum'] >= 50:
            reasons.append(f"프리마켓 강세 (점수: {scores['momentum']})")
        
        if scores['social'] >= 50:
            reasons.append(f"소셜 미디어 주목 (점수: {scores['social']})")
        
        if best_news:
            reasons.append(f"주요 뉴스: {best_news.get('headline', '')[:50]}...")
        
        return " | ".join(reasons) if reasons else "종합적 분석 기반"


def run_analysis(collected_data):
    """분석 실행 메인 함수"""
    engine = PredictionEngine()
    predictions = engine.generate_predictions(collected_data, top_n=5)
    
    return {
        'predictions': predictions,
        'analyzed_at': datetime.now().isoformat(),
        'total_symbols_analyzed': len(set(
            [n.get('symbol') for n in collected_data.get('finviz_news', [])] +
            [g.get('symbol') for g in collected_data.get('top_gainers', [])] +
            [m.get('symbol') for m in collected_data.get('reddit_mentions', [])]
        ))
    }


if __name__ == "__main__":
    # 테스트
    analyzer = NewsAnalyzer()
    
    test_headlines = [
        "FDA Approves XYZ's New Drug for Cancer Treatment",
        "Company ABC Signs $500M Contract with Government",
        "DEF Stock Surges on Short Squeeze",
        "GHI Faces SEC Investigation",
        "JKL Reports Record Revenue, Raises Guidance"
    ]
    
    print("=== Headline Analysis Test ===")
    for headline in test_headlines:
        result = analyzer.analyze_headline(headline)
        print(f"\n{headline}")
        print(f"  Score: {result['score']}, Type: {result['catalyst_type']}, Sentiment: {result['sentiment']}")

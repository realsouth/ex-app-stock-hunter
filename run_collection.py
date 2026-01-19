# file name : run_collection.py
# pwd : /dal9/app/ex_app/run_collection.py
# 데이터 수집 및 예측 실행 스크립트

import sys
import os

# 프로젝트 루트 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 환경 변수 로드 (.env 파일이 있으면)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# DB 모듈 import (PostgreSQL/MySQL 자동 선택)
if os.environ.get('DATABASE_URL'):
    # Railway/Render 등 배포 환경
    from module.dbModule_ex_pg import Database
else:
    # 로컬 개발 환경
    try:
        from app.module.dbModule_ex import Database
    except ImportError:
        from module.dbModule_ex_pg import Database

# 수집기 및 분석기 import
try:
    from app.ex_app.collectors import collect_all_data, FinvizCollector, RedditCollector
    from app.ex_app.analyzer import run_analysis, NewsAnalyzer
except ImportError:
    from collectors import collect_all_data, FinvizCollector, RedditCollector
    from analyzer import run_analysis, NewsAnalyzer

from datetime import datetime

def run_full_collection():
    print('=' * 60)
    print('🚀 Stock Hunter - 실시간 데이터 수집 시작')
    print(f'⏰ 현재 시간: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)

    # 1. 세션 생성
    print('\n📋 Step 1: 세션 생성...')
    db = Database()
    today = datetime.now().strftime('%Y-%m-%d')

    existing = db.executeOne('SELECT * FROM collection_sessions WHERE session_date = %s', (today,))
    if existing:
        session_id = existing['id']
        print(f'   기존 세션 사용: #{session_id}')
    else:
        db.execute('INSERT INTO collection_sessions (session_date, status) VALUES (%s, %s)', (today, 'collecting'))
        db.commit()
        session_id = db.lid()
        print(f'   새 세션 생성: #{session_id}')
    db.close()

    # 2. 데이터 수집
    print('\n📊 Step 2: 데이터 수집 중...')
    data = collect_all_data(session_id)

    print(f'   ✓ Finviz 뉴스: {len(data.get("finviz_news", []))}건')
    print(f'   ✓ Top 게이너: {len(data.get("top_gainers", []))}건')
    print(f'   ✓ Reddit 멘션: {len(data.get("reddit_mentions", []))}건')
    print(f'   ✓ SEC 공시: {len(data.get("sec_filings", []))}건')

    if data.get('errors'):
        print(f'   ⚠ 에러: {data["errors"]}')

    # 3. 분석 및 예측
    print('\n🎯 Step 3: 분석 및 예측 생성...')
    result = run_analysis(data)
    predictions = result.get('predictions', [])

    print(f'   분석된 심볼 수: {result.get("total_symbols_analyzed", 0)}')
    print(f'   생성된 예측 수: {len(predictions)}')

    # 4. DB에 저장
    print('\n💾 Step 4: DB에 저장...')
    db = Database()

    # 뉴스 저장
    news_count = 0
    analyzer = NewsAnalyzer()
    for news in data.get('finviz_news', [])[:30]:
        try:
            analysis = analyzer.analyze_headline(news.get('headline', ''))
            
            db.execute('''
                INSERT INTO news_events (symbol, headline, source, url, importance_score, catalyst_type, sentiment_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                news.get('symbol'),
                news.get('headline', '')[:500],
                news.get('source', 'finviz'),
                news.get('url', '')[:500],
                analysis['score'],
                analysis['catalyst_type'],
                analysis['sentiment']
            ))
            news_count += 1
        except Exception as e:
            pass

    db.commit()
    print(f'   ✓ 뉴스 저장: {news_count}건')

    # 예측 저장
    pick_count = 0
    for pred in predictions:
        try:
            db.execute('''
                INSERT INTO daily_picks (session_id, symbol, pick_rank, category, confidence_score, reasoning)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                session_id,
                pred['symbol'],
                pred['pick_rank'],
                pred['category'],
                pred['confidence_score'],
                pred.get('reasoning', '')[:500]
            ))
            pick_count += 1
        except Exception as e:
            print(f'   에러: {e}')

    # 세션 상태 업데이트
    db.execute('UPDATE collection_sessions SET status = %s WHERE id = %s', ('predicted', session_id))
    db.commit()
    db.close()
    print(f'   ✓ 예측 저장: {pick_count}건')

    # 5. 결과 출력
    print('\n' + '=' * 60)
    print('🎯 오늘의 Top Picks')
    print('=' * 60)

    for pred in predictions:
        print(f'''
#{pred['pick_rank']} {pred['symbol']}
   신뢰도: {pred['confidence_score']:.1f}%
   카테고리: {pred['category']}
   뉴스 점수: {pred['news_score']} | 모멘텀: {pred['momentum_score']} | 소셜: {pred['social_score']}
   근거: {pred.get('reasoning', 'N/A')[:100]}
''')

    print('=' * 60)
    print('✅ 완료!')
    
    return predictions

if __name__ == "__main__":
    run_full_collection()

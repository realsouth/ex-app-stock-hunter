# file name : index.py
# pwd : /dal9/app/ex_app/index.py
# 미국 증시 급등주 예측 앱 - 메인 라우트

from flask import Blueprint, request, jsonify, render_template, make_response
from datetime import datetime, timedelta
import json

# DB 모듈 import (PostgreSQL/MySQL 자동 선택)
import os
import sys

# 배포 환경에서는 상대 import 사용
if os.environ.get('DATABASE_URL'):
    # Railway/Render 등 배포 환경
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from module import dbModule_ex_pg as dbModule_ex
else:
    # 로컬 개발 환경 (기존 방식)
    try:
        from app.module import dbModule_ex
    except ImportError:
        from module import dbModule_ex_pg as dbModule_ex

ex_app = Blueprint('ex_app', __name__, url_prefix='/ex_app')

@ex_app.before_request
def handle_preflight():
    """CORS preflight 처리"""
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PATCH, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response, 200


# ============================================
# 페이지 라우트
# ============================================

@ex_app.route('/')
def dashboard():
    """메인 대시보드"""
    db = dbModule_ex.Database()
    try:
        # 오늘의 세션 확인
        today = datetime.now().strftime('%Y-%m-%d')
        session = db.executeOne(
            "SELECT * FROM collection_sessions WHERE session_date = %s ORDER BY id DESC LIMIT 1",
            (today,)
        )
        
        # 최근 Picks
        recent_picks = db.executeAll("""
            SELECT dp.*, s.name as stock_name 
            FROM daily_picks dp
            LEFT JOIN stocks s ON dp.symbol = s.symbol
            ORDER BY dp.created_at DESC LIMIT 10
        """)
        
        # 성과 통계
        stats = db.executeOne("""
            SELECT 
                COUNT(*) as total_picks,
                SUM(CASE WHEN pr.is_successful = 1 THEN 1 ELSE 0 END) as successful,
                AVG(pr.gain_pct_eod) as avg_gain
            FROM daily_picks dp
            LEFT JOIN prediction_results pr ON dp.id = pr.pick_id
            WHERE dp.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        """)
        
        # 최근 뉴스
        recent_news = db.executeAll("""
            SELECT * FROM news_events 
            ORDER BY collected_at DESC LIMIT 20
        """)
        
        return render_template('ex_app/dashboard.html', 
                             session=session,
                             recent_picks=recent_picks,
                             stats=stats,
                             recent_news=recent_news)
    except Exception as e:
        print(f"[EX_APP] Dashboard Error: {e}")
        return render_template('ex_app/dashboard.html', 
                             session=None, recent_picks=[], stats=None, recent_news=[])
    finally:
        db.close()


@ex_app.route('/predictions')
def predictions():
    """예측 목록 페이지"""
    db = dbModule_ex.Database()
    try:
        picks = db.executeAll("""
            SELECT 
                dp.*,
                s.name as stock_name,
                s.market_cap,
                pr.price_at_open,
                pr.gain_pct_eod,
                pr.is_successful
            FROM daily_picks dp
            LEFT JOIN stocks s ON dp.symbol = s.symbol
            LEFT JOIN prediction_results pr ON dp.id = pr.pick_id
            ORDER BY dp.created_at DESC
            LIMIT 50
        """)
        return render_template('ex_app/predictions.html', picks=picks)
    except Exception as e:
        print(f"[EX_APP] Predictions Error: {e}")
        return render_template('ex_app/predictions.html', picks=[])
    finally:
        db.close()


@ex_app.route('/history')
def history():
    """과거 기록 및 적중률 페이지"""
    db = dbModule_ex.Database()
    try:
        # 일별 통계
        daily_stats = db.executeAll("""
            SELECT * FROM performance_stats
            ORDER BY stat_date DESC
            LIMIT 30
        """)
        return render_template('ex_app/history.html', daily_stats=daily_stats)
    except Exception as e:
        print(f"[EX_APP] History Error: {e}")
        return render_template('ex_app/history.html', daily_stats=[])
    finally:
        db.close()


# ============================================
# API 엔드포인트
# ============================================

@ex_app.route('/api/session/start', methods=['POST'])
def start_session():
    """새 수집 세션 시작"""
    db = dbModule_ex.Database()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 기존 세션 확인
        existing = db.executeOne(
            "SELECT * FROM collection_sessions WHERE session_date = %s",
            (today,)
        )
        
        if existing:
            return jsonify({
                "status": "exists",
                "session_id": existing['id'],
                "message": "오늘 세션이 이미 존재합니다."
            })
        
        # 새 세션 생성
        db.execute("""
            INSERT INTO collection_sessions (session_date, status)
            VALUES (%s, 'collecting')
        """, (today,))
        db.commit()
        session_id = db.lid()
        
        return jsonify({
            "status": "created",
            "session_id": session_id,
            "message": "새 수집 세션이 시작되었습니다."
        })
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/news', methods=['GET'])
def get_news():
    """수집된 뉴스 목록 조회"""
    db = dbModule_ex.Database()
    try:
        limit = request.args.get('limit', 50, type=int)
        symbol = request.args.get('symbol', None)
        
        if symbol:
            news = db.executeAll("""
                SELECT * FROM news_events 
                WHERE symbol = %s
                ORDER BY collected_at DESC LIMIT %s
            """, (symbol, limit))
        else:
            news = db.executeAll("""
                SELECT * FROM news_events 
                ORDER BY collected_at DESC LIMIT %s
            """, (limit,))
        
        return jsonify({"status": "success", "news": news})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/news', methods=['POST'])
def add_news():
    """뉴스 추가 (수집기에서 호출)"""
    db = dbModule_ex.Database()
    try:
        data = request.json
        
        db.execute("""
            INSERT INTO news_events 
            (symbol, headline, source, url, sentiment_score, catalyst_type, importance_score, published_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get('symbol'),
            data.get('headline'),
            data.get('source'),
            data.get('url'),
            data.get('sentiment_score', 0),
            data.get('catalyst_type', 'other'),
            data.get('importance_score', 0),
            data.get('published_at')
        ))
        db.commit()
        
        return jsonify({"status": "success", "id": db.lid()})
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/picks', methods=['GET'])
def get_picks():
    """오늘의 예측 목록"""
    db = dbModule_ex.Database()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        
        picks = db.executeAll("""
            SELECT 
                dp.*,
                s.name as stock_name,
                s.market_cap,
                s.sector
            FROM daily_picks dp
            LEFT JOIN stocks s ON dp.symbol = s.symbol
            LEFT JOIN collection_sessions cs ON dp.session_id = cs.id
            WHERE cs.session_date = %s
            ORDER BY dp.pick_rank ASC
        """, (today,))
        
        return jsonify({"status": "success", "picks": picks})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/picks', methods=['POST'])
def add_pick():
    """예측 추가"""
    db = dbModule_ex.Database()
    try:
        data = request.json
        
        db.execute("""
            INSERT INTO daily_picks 
            (session_id, symbol, pick_rank, category, confidence_score, entry_price, predicted_target, reasoning)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get('session_id'),
            data.get('symbol'),
            data.get('pick_rank', 1),
            data.get('category', 'news_catalyst'),
            data.get('confidence_score', 50),
            data.get('entry_price'),
            data.get('predicted_target'),
            data.get('reasoning')
        ))
        db.commit()
        
        return jsonify({"status": "success", "id": db.lid()})
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/stocks', methods=['GET'])
def get_stocks():
    """종목 목록 조회"""
    db = dbModule_ex.Database()
    try:
        stocks = db.executeAll("SELECT * FROM stocks ORDER BY symbol")
        return jsonify({"status": "success", "stocks": stocks})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/stocks', methods=['POST'])
def add_stock():
    """종목 추가/업데이트"""
    db = dbModule_ex.Database()
    try:
        data = request.json
        symbol = data.get('symbol', '').upper()
        
        # 기존 종목 확인
        existing = db.executeOne("SELECT id FROM stocks WHERE symbol = %s", (symbol,))
        
        if existing:
            # 업데이트
            db.execute("""
                UPDATE stocks SET 
                    name = %s, sector = %s, market_cap = %s, 
                    float_shares = %s, is_smallcap = %s, is_penny = %s
                WHERE symbol = %s
            """, (
                data.get('name'),
                data.get('sector'),
                data.get('market_cap'),
                data.get('float_shares'),
                data.get('is_smallcap', False),
                data.get('is_penny', False),
                symbol
            ))
        else:
            # 삽입
            db.execute("""
                INSERT INTO stocks 
                (symbol, name, sector, market_cap, float_shares, is_smallcap, is_penny)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol,
                data.get('name'),
                data.get('sector'),
                data.get('market_cap'),
                data.get('float_shares'),
                data.get('is_smallcap', False),
                data.get('is_penny', False)
            ))
        
        db.commit()
        return jsonify({"status": "success", "symbol": symbol})
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/results/<int:pick_id>', methods=['POST'])
def update_result():
    """예측 결과 업데이트"""
    db = dbModule_ex.Database()
    try:
        data = request.json
        pick_id = request.view_args['pick_id']
        
        # 기존 결과 확인
        existing = db.executeOne("SELECT id FROM prediction_results WHERE pick_id = %s", (pick_id,))
        
        if existing:
            db.execute("""
                UPDATE prediction_results SET
                    price_at_open = %s, price_1h = %s, price_2h = %s,
                    price_eod = %s, high_of_day = %s, low_of_day = %s,
                    volume_day = %s, gain_pct_1h = %s, gain_pct_eod = %s,
                    is_successful = %s
                WHERE pick_id = %s
            """, (
                data.get('price_at_open'),
                data.get('price_1h'),
                data.get('price_2h'),
                data.get('price_eod'),
                data.get('high_of_day'),
                data.get('low_of_day'),
                data.get('volume_day'),
                data.get('gain_pct_1h'),
                data.get('gain_pct_eod'),
                data.get('is_successful'),
                pick_id
            ))
        else:
            db.execute("""
                INSERT INTO prediction_results 
                (pick_id, price_at_open, price_1h, price_2h, price_eod, 
                 high_of_day, low_of_day, volume_day, gain_pct_1h, gain_pct_eod, is_successful)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                pick_id,
                data.get('price_at_open'),
                data.get('price_1h'),
                data.get('price_2h'),
                data.get('price_eod'),
                data.get('high_of_day'),
                data.get('low_of_day'),
                data.get('volume_day'),
                data.get('gain_pct_1h'),
                data.get('gain_pct_eod'),
                data.get('is_successful')
            ))
        
        db.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/stats/summary', methods=['GET'])
def get_stats_summary():
    """통계 요약"""
    db = dbModule_ex.Database()
    try:
        stats = db.executeOne("""
            SELECT 
                COUNT(DISTINCT dp.id) as total_picks,
                SUM(CASE WHEN pr.is_successful = 1 THEN 1 ELSE 0 END) as successful_picks,
                ROUND(AVG(pr.gain_pct_eod), 2) as avg_gain,
                MAX(pr.gain_pct_eod) as best_gain,
                MIN(pr.gain_pct_eod) as worst_gain
            FROM daily_picks dp
            LEFT JOIN prediction_results pr ON dp.id = pr.pick_id
        """)
        
        return jsonify({"status": "success", "stats": stats})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


# ============================================
# AI 분석 API (Claude가 사용)
# ============================================

@ex_app.route('/ai')
def ai_analysis_page():
    """AI 분석 결과 페이지"""
    db = dbModule_ex.Database()
    try:
        # 최근 AI 분석 결과
        analyses = db.executeAll("""
            SELECT * FROM ai_analysis 
            ORDER BY created_at DESC LIMIT 20
        """)
        
        # 오늘의 추천 종목
        today_picks = db.executeAll("""
            SELECT * FROM ai_analysis 
            WHERE analysis_type = 'stock_pick' 
            AND DATE(created_at) = CURDATE()
            ORDER BY confidence_score DESC
        """)
        
        # 일일 요약
        daily_summary = db.executeOne("""
            SELECT * FROM ai_analysis 
            WHERE analysis_type = 'daily_summary' 
            ORDER BY created_at DESC LIMIT 1
        """)
        
        return render_template('ex_app/ai_analysis.html', 
                             analyses=analyses,
                             today_picks=today_picks,
                             daily_summary=daily_summary)
    except Exception as e:
        print(f"[EX_APP] AI Analysis Page Error: {e}")
        return render_template('ex_app/ai_analysis.html', 
                             analyses=[], today_picks=[], daily_summary=None)
    finally:
        db.close()


@ex_app.route('/api/ai/analysis', methods=['GET'])
def get_ai_analysis():
    """AI 분석 결과 조회"""
    db = dbModule_ex.Database()
    try:
        analysis_type = request.args.get('type', None)
        limit = request.args.get('limit', 20, type=int)
        
        if analysis_type:
            analyses = db.executeAll("""
                SELECT * FROM ai_analysis 
                WHERE analysis_type = %s
                ORDER BY created_at DESC LIMIT %s
            """, (analysis_type, limit))
        else:
            analyses = db.executeAll("""
                SELECT * FROM ai_analysis 
                ORDER BY created_at DESC LIMIT %s
            """, (limit,))
        
        return jsonify({"status": "success", "analyses": analyses})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/ai/analysis', methods=['POST'])
def add_ai_analysis():
    """AI 분석 저장 (Claude가 호출)"""
    db = dbModule_ex.Database()
    try:
        data = request.json
        
        # 오늘의 세션 ID 가져오기
        today = datetime.now().strftime('%Y-%m-%d')
        session = db.executeOne(
            "SELECT id FROM collection_sessions WHERE session_date = %s",
            (today,)
        )
        session_id = session['id'] if session else None
        
        db.execute("""
            INSERT INTO ai_analysis 
            (session_id, analysis_type, symbol, title, content, confidence_score, recommendation)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            session_id,
            data.get('analysis_type', 'daily_summary'),
            data.get('symbol'),
            data.get('title'),
            data.get('content'),
            data.get('confidence_score', 50),
            data.get('recommendation', 'watch')
        ))
        db.commit()
        
        return jsonify({
            "status": "success", 
            "id": db.lid(),
            "message": "분석이 저장되었습니다."
        })
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/ai/daily-summary', methods=['POST'])
def add_daily_summary():
    """일일 시장 요약 저장 (Claude 전용)"""
    db = dbModule_ex.Database()
    try:
        data = request.json
        
        today = datetime.now().strftime('%Y-%m-%d')
        session = db.executeOne(
            "SELECT id FROM collection_sessions WHERE session_date = %s",
            (today,)
        )
        session_id = session['id'] if session else None
        
        db.execute("""
            INSERT INTO ai_analysis 
            (session_id, analysis_type, title, content)
            VALUES (%s, 'daily_summary', %s, %s)
        """, (
            session_id,
            data.get('title', f'{today} 시장 분석'),
            data.get('content')
        ))
        db.commit()
        
        return jsonify({
            "status": "success",
            "message": "일일 요약이 저장되었습니다."
        })
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/ai/stock-pick', methods=['POST'])
def add_stock_pick():
    """종목 추천 저장 (Claude 전용)"""
    db = dbModule_ex.Database()
    try:
        data = request.json
        
        today = datetime.now().strftime('%Y-%m-%d')
        session = db.executeOne(
            "SELECT id FROM collection_sessions WHERE session_date = %s",
            (today,)
        )
        session_id = session['id'] if session else None
        
        db.execute("""
            INSERT INTO ai_analysis 
            (session_id, analysis_type, symbol, title, content, confidence_score, recommendation)
            VALUES (%s, 'stock_pick', %s, %s, %s, %s, %s)
        """, (
            session_id,
            data.get('symbol', '').upper(),
            data.get('title'),
            data.get('content'),
            data.get('confidence_score', 50),
            data.get('recommendation', 'watch')
        ))
        db.commit()
        
        return jsonify({
            "status": "success",
            "message": f"{data.get('symbol')} 추천이 저장되었습니다."
        })
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


# ============================================
# AI 운용 전용 API (Claude/GPT가 호출)
# ============================================

@ex_app.route('/api/ai/dashboard', methods=['GET'])
def ai_dashboard():
    """
    AI 운용 전용 - 오늘의 종합 데이터 조회
    Claude가 이 API를 호출하여 분석에 필요한 모든 데이터를 한번에 가져옴
    """
    db = dbModule_ex.Database()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 오늘의 세션
        session = db.executeOne(
            "SELECT * FROM collection_sessions WHERE session_date = %s ORDER BY id DESC LIMIT 1",
            (today,)
        )
        
        # 오늘의 예측
        picks = db.executeAll("""
            SELECT 
                dp.*,
                s.name as stock_name,
                s.sector,
                s.market_cap
            FROM daily_picks dp
            LEFT JOIN stocks s ON dp.symbol = s.symbol
            LEFT JOIN collection_sessions cs ON dp.session_id = cs.id
            WHERE cs.session_date = %s
            ORDER BY dp.pick_rank ASC
        """, (today,))
        
        # 최근 고점수 뉴스 (importance_score >= 50)
        high_impact_news = db.executeAll("""
            SELECT * FROM news_events 
            WHERE importance_score >= 50
            AND DATE(collected_at) = %s
            ORDER BY importance_score DESC
            LIMIT 20
        """, (today,))
        
        # 최근 7일 성과 통계
        recent_performance = db.executeAll("""
            SELECT 
                DATE(dp.created_at) as pick_date,
                COUNT(*) as total_picks,
                SUM(CASE WHEN pr.is_successful = 1 THEN 1 ELSE 0 END) as successful,
                AVG(pr.gain_pct_eod) as avg_gain
            FROM daily_picks dp
            LEFT JOIN prediction_results pr ON dp.id = pr.pick_id
            WHERE dp.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY DATE(dp.created_at)
            ORDER BY pick_date DESC
        """)
        
        # 소셜 트렌드 (Reddit 멘션 기반 - 있다면)
        # 이 데이터는 collectors.py에서 수집 시 저장되어야 함
        
        return jsonify({
            "status": "success",
            "date": today,
            "session": session,
            "picks": picks,
            "high_impact_news": high_impact_news,
            "recent_performance": recent_performance,
            "message": "AI 분석용 데이터 조회 완료"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/ai/collect', methods=['POST'])
def ai_trigger_collection():
    """
    AI 운용 전용 - 데이터 수집 트리거
    Claude가 이 API를 호출하여 즉시 수집 실행
    주의: 서버 리소스를 고려하여 속도 제한 필요
    """
    db = dbModule_ex.Database()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 오늘 이미 수집 완료되었는지 확인
        existing = db.executeOne(
            "SELECT * FROM collection_sessions WHERE session_date = %s AND status = 'predicted'",
            (today,)
        )
        
        if existing:
            return jsonify({
                "status": "already_done",
                "session_id": existing['id'],
                "message": "오늘 수집이 이미 완료되었습니다. /api/ai/dashboard에서 결과를 확인하세요."
            })
        
        # 새 세션 생성 또는 기존 세션 조회
        session = db.executeOne(
            "SELECT * FROM collection_sessions WHERE session_date = %s",
            (today,)
        )
        
        if not session:
            db.execute(
                "INSERT INTO collection_sessions (session_date, status) VALUES (%s, 'collecting')",
                (today,)
            )
            db.commit()
            session_id = db.lid()
        else:
            session_id = session['id']
            db.execute(
                "UPDATE collection_sessions SET status = 'collecting' WHERE id = %s",
                (session_id,)
            )
            db.commit()
        
        # 실제 수집은 비동기로 처리해야 하지만,
        # 간단한 구현을 위해 동기 처리 (시간이 오래 걸릴 수 있음)
        # 프로덕션에서는 Celery 등 비동기 작업 큐 사용 권장
        
        try:
            # collectors와 analyzer import
            from collectors import collect_all_data
            from analyzer import run_analysis, NewsAnalyzer
            
            # 데이터 수집
            data = collect_all_data(session_id)
            
            # 분석 실행
            result = run_analysis(data)
            predictions = result.get('predictions', [])
            
            # 뉴스 저장
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
                except:
                    pass
            
            # 예측 저장
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
                except:
                    pass
            
            # 세션 상태 업데이트
            db.execute(
                "UPDATE collection_sessions SET status = 'predicted' WHERE id = %s",
                (session_id,)
            )
            db.commit()
            
            return jsonify({
                "status": "success",
                "session_id": session_id,
                "predictions_count": len(predictions),
                "news_count": len(data.get('finviz_news', [])),
                "top_picks": [p['symbol'] for p in predictions[:5]],
                "message": "수집 및 분석 완료"
            })
            
        except ImportError as e:
            # collectors/analyzer import 실패 시
            db.execute(
                "UPDATE collection_sessions SET status = 'error' WHERE id = %s",
                (session_id,)
            )
            db.commit()
            return jsonify({
                "status": "error",
                "message": f"수집 모듈 로드 실패: {str(e)}. GitHub Actions에서 실행하세요."
            }), 500
            
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


@ex_app.route('/api/ai/report', methods=['POST'])
def ai_save_report():
    """
    AI 운용 전용 - 분석 보고서 저장
    Claude가 분석 완료 후 보고서를 DB에 저장
    """
    db = dbModule_ex.Database()
    try:
        data = request.json
        
        today = datetime.now().strftime('%Y-%m-%d')
        session = db.executeOne(
            "SELECT id FROM collection_sessions WHERE session_date = %s",
            (today,)
        )
        session_id = session['id'] if session else None
        
        db.execute("""
            INSERT INTO ai_analysis 
            (session_id, analysis_type, symbol, title, content, confidence_score, recommendation)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            session_id,
            data.get('analysis_type', 'daily_summary'),
            data.get('symbol'),
            data.get('title', f'{today} AI 분석 보고서'),
            data.get('content'),
            data.get('confidence_score', 50),
            data.get('recommendation', 'watch')
        ))
        db.commit()
        
        return jsonify({
            "status": "success",
            "id": db.lid(),
            "message": "분석 보고서가 저장되었습니다."
        })
    except Exception as e:
        db.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        db.close()


# file name : dbModule_ex_pg.py
# pwd : /project_name/app/module/dbModule_ex_pg.py
# ex_app 데이터베이스(미국 증시 급등주 예측 앱)에 연결하는 모듈
# PostgreSQL 버전 - Railway/Render 무료 호스팅용

import os

# PostgreSQL 드라이버 (psycopg2 또는 psycopg)
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# MySQL/MariaDB 드라이버 (폴백용)
try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False


class Database:
    """
    ex_app 데이터베이스에 연결하는 Database 클래스.
    미국 증시 급등주 예측 앱 전용.
    환경 변수로 PostgreSQL 또는 MySQL 자동 선택.
    """
    
    def __init__(self):
        # DATABASE_URL 환경 변수 확인 (Railway/Heroku 스타일)
        database_url = os.environ.get('DATABASE_URL')
        
        if database_url:
            self._connect_from_url(database_url)
        else:
            # 로컬 개발용 기본값 (환경 변수 개별 설정)
            self._connect_from_env()
    
    def _connect_from_url(self, url):
        """DATABASE_URL에서 연결 (Railway/Heroku 형식)"""
        if url.startswith('postgres://') or url.startswith('postgresql://'):
            # PostgreSQL
            if not HAS_PSYCOPG2:
                raise ImportError("psycopg2 is required for PostgreSQL connection")
            
            # postgres:// -> postgresql:// 변환 (psycopg2 호환)
            if url.startswith('postgres://'):
                url = url.replace('postgres://', 'postgresql://', 1)
            
            self.db = psycopg2.connect(url, cursor_factory=RealDictCursor)
            self.cursor = self.db.cursor()
            self.db_type = 'postgresql'
            print("[EX_APP DB] Connected to PostgreSQL via DATABASE_URL")
        
        elif url.startswith('mysql://'):
            # MySQL (PyMySQL은 URL 직접 파싱 필요)
            self._parse_mysql_url(url)
        
        else:
            raise ValueError(f"Unsupported database URL scheme: {url[:20]}...")
    
    def _parse_mysql_url(self, url):
        """MySQL URL 파싱 및 연결"""
        if not HAS_PYMYSQL:
            raise ImportError("pymysql is required for MySQL connection")
        
        # mysql://user:pass@host:port/dbname 파싱
        import re
        pattern = r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
        match = re.match(pattern, url)
        
        if match:
            user, password, host, port, db = match.groups()
            self.db = pymysql.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                db=db,
                charset='utf8mb4'
            )
            self.cursor = self.db.cursor(pymysql.cursors.DictCursor)
            self.db_type = 'mysql'
            print("[EX_APP DB] Connected to MySQL via DATABASE_URL")
        else:
            raise ValueError("Invalid MySQL URL format")
    
    def _connect_from_env(self):
        """개별 환경 변수에서 연결 설정 읽기"""
        db_type = os.environ.get('DB_TYPE', 'postgresql').lower()
        
        host = os.environ.get('DB_HOST', 'localhost')
        port = int(os.environ.get('DB_PORT', 5432 if db_type == 'postgresql' else 3306))
        user = os.environ.get('DB_USER', 'postgres' if db_type == 'postgresql' else 'root')
        password = os.environ.get('DB_PASSWORD', '')
        dbname = os.environ.get('DB_NAME', 'ex_app')
        
        if db_type == 'postgresql':
            if not HAS_PSYCOPG2:
                raise ImportError("psycopg2 is required for PostgreSQL connection")
            
            self.db = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname
            )
            self.cursor = self.db.cursor(cursor_factory=RealDictCursor)
            self.db_type = 'postgresql'
            print(f"[EX_APP DB] Connected to PostgreSQL at {host}:{port}")
        
        else:  # mysql/mariadb
            if not HAS_PYMYSQL:
                raise ImportError("pymysql is required for MySQL connection")
            
            self.db = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                db=dbname,
                charset='utf8mb4'
            )
            self.cursor = self.db.cursor(pymysql.cursors.DictCursor)
            self.db_type = 'mysql'
            print(f"[EX_APP DB] Connected to MySQL at {host}:{port}")

    def execute(self, query, args=None):
        """쿼리 실행 (INSERT, UPDATE, DELETE)"""
        try:
            # PostgreSQL의 경우 %s 플레이스홀더 사용
            affected_rows = self.cursor.execute(query, args)
            return affected_rows
        except Exception as e:
            print(f"[EX_APP DB] Execute Error: {e}")
            raise

    def executeOne(self, query, args=None):
        """단일 행 조회"""
        try:
            self.cursor.execute(query, args)
            row = self.cursor.fetchone()
            # PostgreSQL RealDictCursor는 이미 dict 반환
            return dict(row) if row else None
        except Exception as e:
            print(f"[EX_APP DB] ExecuteOne Error: {e}")
            raise

    def executeAll(self, query, args=None):
        """전체 행 조회"""
        try:
            self.cursor.execute(query, args)
            rows = self.cursor.fetchall()
            # PostgreSQL RealDictCursor는 이미 dict 반환
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            print(f"[EX_APP DB] ExecuteAll Error: {e}")
            raise

    def lid(self):
        """마지막 삽입된 행의 ID"""
        if self.db_type == 'postgresql':
            # PostgreSQL은 RETURNING id 또는 별도 쿼리 필요
            self.cursor.execute("SELECT lastval()")
            result = self.cursor.fetchone()
            return result['lastval'] if result else None
        else:
            return self.cursor.lastrowid

    def commit(self):
        """트랜잭션 커밋"""
        try:
            self.db.commit()
        except Exception as e:
            print(f"[EX_APP DB] Commit Error: {e}")
            raise

    def rollback(self):
        """트랜잭션 롤백"""
        try:
            self.db.rollback()
        except Exception as e:
            print(f"[EX_APP DB] Rollback Error: {e}")
            raise

    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.db:
            self.db.close()
            self.db = None


# 기존 코드와의 호환성을 위한 별칭
# from module.dbModule_ex_pg import Database 로 사용 가능

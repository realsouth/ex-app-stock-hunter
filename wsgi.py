import os
import sys

project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, redirect, render_template

def create_app():
    app = Flask(__name__, 
                template_folder='.',
                static_folder='static')
    app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-key')
    app.debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    
    @app.route('/')
    def root():
        return render_template('dashboard.html')
    
    @app.route('/health')
    def health():
        return {'status': 'healthy'}, 200
    
    return app

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)

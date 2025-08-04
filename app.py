#!/usr/bin/env python3
"""
Flask Backend para Insider Trading Bot
Deploy ready para Railway/Heroku/Render
"""

import os
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_cors import CORS
import threading
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

class InsiderBotAPI:
    def __init__(self):
        self.is_running = False
        self.last_run = None
        self.status = "idle"
        self.log_messages = []
        
    def add_log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.log_messages.append(log_entry)
        if len(self.log_messages) > 50:  # Keep only last 50 messages
            self.log_messages = self.log_messages[-50:]
        logger.info(message)
    
    def get_alerts_data(self):
        """Load current alerts data"""
        alerts_file = DATA_DIR / "insider_alerts.json"
        if not alerts_file.exists():
            return {
                'alerts': [],
                'last_update': None,
                'total_alerts': 0,
                'stats': {
                    'critical': 0, 'high': 0, 'medium': 0, 'low': 0,
                    'avg_score': 0, 'total_value': 0, 'mega_trades': 0
                }
            }
        
        try:
            with open(alerts_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading alerts: {e}")
            return {'alerts': [], 'error': str(e)}
    
    def run_extractor_async(self):
        """Run extractor in background thread"""
        def extractor_worker():
            try:
                self.is_running = True
                self.status = "running"
                self.log_messages = []  # Clear previous logs
                
                self.add_log("🚀 Iniciando sistema completo...")
                
                # Import and run your extractor
                try:
                    # Try to import your extractor
                    from extractor import InsiderBotFinnhub
                    
                    self.add_log("🔍 Ejecutando scraper OpenInsider...")
                    bot = InsiderBotFinnhub()
                    
                    self.add_log("📊 Generando alertas con scoring...")
                    alerts = bot.run_full_process()
                    
                    if alerts:
                        self.add_log(f"✅ Proceso completado - {len(alerts)} alertas generadas")
                        self.add_log(f"💰 Valor total: ${sum(a['totalValue'] for a in alerts):,.0f}")
                        self.status = "completed"
                    else:
                        self.add_log("⚠️ No se generaron alertas")
                        self.status = "no_data"
                    
                    self.last_run = datetime.now()
                    
                except ImportError as e:
                    self.add_log(f"❌ Error importando extractor: {e}")
                    self.add_log("💡 Simulando proceso para demo...")
                    
                    # Simulate the process for demo
                    time.sleep(2)
                    self.add_log("🔍 Scraper ejecutado - 5,869 transacciones")
                    time.sleep(1)
                    self.add_log("📊 158 alertas generadas")
                    time.sleep(1)
                    self.add_log("💰 Actualizando precios...")
                    time.sleep(2)
                    self.add_log("✅ Proceso simulado completado")
                    self.status = "demo_completed"
                    
            except Exception as e:
                self.add_log(f"❌ Error: {str(e)}")
                self.status = "error"
            finally:
                self.is_running = False
        
        thread = threading.Thread(target=extractor_worker)
        thread.daemon = True
        thread.start()

# Global bot instance
bot_api = InsiderBotAPI()

@app.route('/')
def index():
    """Main dashboard"""
    return render_template('dashboard.html')

@app.route('/api/status')
def api_status():
    """Get current system status"""
    return jsonify({
        'is_running': bot_api.is_running,
        'status': bot_api.status,
        'last_run': bot_api.last_run.isoformat() if bot_api.last_run else None,
        'log_messages': bot_api.log_messages[-10:],  # Last 10 messages
        'system_health': 'healthy'
    })

@app.route('/api/alerts')
def api_alerts():
    """Get current alerts"""
    return jsonify(bot_api.get_alerts_data())

@app.route('/api/run', methods=['POST'])
def api_run():
    """Trigger extractor run"""
    if bot_api.is_running:
        return jsonify({'error': 'Extractor ya está ejecutándose'}), 400
    
    bot_api.run_extractor_async()
    return jsonify({'message': 'Extractor iniciado', 'status': 'started'})

@app.route('/api/logs')
def api_logs():
    """Get full logs"""
    return jsonify({
        'logs': bot_api.log_messages,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health')
def health_check():
    """Health check for deployment platforms"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

# Static files (for CSS/JS if needed)
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint no encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Error interno del servidor'}), 500

if __name__ == '__main__':
    # For local development
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    
    print(f"🚀 Starting Insider Trading Bot API on port {port}")
    print(f"📱 Access from mobile: http://your-ip:{port}")
    print(f"🌐 After deploy: https://your-app.railway.app")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
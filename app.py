#!/usr/bin/env python3
"""
INSIDER TRADING RESEARCH APP
Integra scraper + research assistant en una app web
"""
from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import subprocess
import sys
import json
from datetime import datetime
from pathlib import Path
import threading
import time

app = Flask(__name__)

class InsiderTradingApp:
    def __init__(self):
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Estados del sistema
        self.is_scraping = False
        self.last_scrape_time = None
        self.scrape_progress = ""
        
        # Archivos de datos
        self.opportunities_file = self.data_dir / "insider_opportunities.csv"
        self.research_file = self.data_dir / "weekly_research_report.json"
        
    def get_system_status(self):
        """Estado actual del sistema"""
        return {
            'is_scraping': self.is_scraping,
            'last_scrape': self.last_scrape_time,
            'has_opportunities': self.opportunities_file.exists(),
            'has_research': self.research_file.exists(),
            'progress': self.scrape_progress
        }
    
    def run_full_pipeline(self):
        """Ejecuta scraper + research assistant completo"""
        self.is_scraping = True
        self.scrape_progress = "Iniciando scraping..."
        
        try:
            # Paso 1: Ejecutar scraper
            self.scrape_progress = "Ejecutando scraper inteligente..."
            result1 = subprocess.run([
                sys.executable, "scraper.py"
            ], capture_output=True, text=True, timeout=300)
            
            if result1.returncode != 0:
                raise Exception(f"Error en scraper: {result1.stderr}")
            
            # Paso 2: Ejecutar research assistant
            self.scrape_progress = "Enriqueciendo con datos de mercado..."
            result2 = subprocess.run([
                sys.executable, "asistente.py"
            ], capture_output=True, text=True, timeout=300)
            
            if result2.returncode != 0:
                raise Exception(f"Error en research assistant: {result2.stderr}")
            
            self.last_scrape_time = datetime.now().isoformat()
            self.scrape_progress = "Completado exitosamente"
            
            return True, "Pipeline ejecutado exitosamente"
            
        except Exception as e:
            self.scrape_progress = f"Error: {str(e)}"
            return False, str(e)
        finally:
            self.is_scraping = False
    
    def get_opportunities(self):
        """Obtiene oportunidades del scraper"""
        if not self.opportunities_file.exists():
            return []
        
        df = pd.read_csv(self.opportunities_file)
        return df.to_dict('records')
    
    def get_research_data(self):
        """Obtiene datos de research enriquecidos"""
        if not self.research_file.exists():
            return None
        
        with open(self.research_file, 'r') as f:
            return json.load(f)

# Instancia global
insider_app = InsiderTradingApp()

@app.route('/')
def dashboard():
    """Dashboard principal"""
    return render_template('dashboard.html')

@app.route('/api/status')
def api_status():
    """API: Estado del sistema"""
    return jsonify(insider_app.get_system_status())

@app.route('/api/run-pipeline', methods=['POST'])
def api_run_pipeline():
    """API: Ejecutar pipeline completo"""
    if insider_app.is_scraping:
        return jsonify({'success': False, 'message': 'Ya hay un proceso ejecutándose'})
    
    # Ejecutar en thread separado para no bloquear
    def run_async():
        insider_app.run_full_pipeline()
    
    thread = threading.Thread(target=run_async)
    thread.start()
    
    return jsonify({'success': True, 'message': 'Pipeline iniciado'})

@app.route('/api/opportunities')
def api_opportunities():
    """API: Lista de oportunidades básicas"""
    opportunities = insider_app.get_opportunities()
    return jsonify(opportunities)

@app.route('/api/research-data')
def api_research_data():
    """API: Datos de research completos"""
    research_data = insider_app.get_research_data()
    return jsonify(research_data)

@app.route('/api/research-targets')
def api_research_targets():
    """API: Top targets para research manual"""
    research_data = insider_app.get_research_data()
    
    if not research_data:
        return jsonify([])
    
    # Solo top targets con datos de mercado
    targets = research_data.get('top_research_targets', [])[:10]
    
    # Filtrar solo los que tienen precio actual
    targets_with_data = [
        target for target in targets 
        if target.get('current_price') is not None
    ]
    
    return jsonify(targets_with_data)

@app.route('/download/opportunities')
def download_opportunities():
    """Descarga CSV de oportunidades"""
    if insider_app.opportunities_file.exists():
        return send_file(insider_app.opportunities_file, as_attachment=True)
    return "No hay datos disponibles", 404

@app.route('/download/research')
def download_research():
    """Descarga CSV de research"""
    research_csv = insider_app.data_dir / "weekly_research_report.csv"
    if research_csv.exists():
        return send_file(research_csv, as_attachment=True)
    return "No hay datos de research disponibles", 404

@app.route('/health')
def health():
    """Health check para Railway"""
    return {'status': 'ok', 'timestamp': datetime.now().isoformat()}

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
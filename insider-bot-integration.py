#!/usr/bin/env python3
"""
Insider Trading Bot - Integración OpenInsider + Alpha Vantage
Ejecuta scraper y procesa datos para el dashboard web
"""

import os
import sys
import json
import subprocess
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from pathlib import Path

class InsiderBotIntegration:
    def __init__(self, alpha_vantage_key=None):
        self.alpha_key = alpha_vantage_key
        self.base_dir = Path(__file__).parent
        self.scraper_dir = self.base_dir / "openinsiderData"
        self.data_dir = self.base_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        
    def run_scraper(self):
        """Ejecuta el scraper de OpenInsider"""
        print("🔍 Ejecutando scraper de OpenInsider...")
        
        original_dir = os.getcwd()
        try:
            os.chdir(self.scraper_dir)
            result = subprocess.run([
                sys.executable, "openinsider_scraper.py"
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                print("✅ Scraper ejecutado exitosamente")
                return True
            else:
                print(f"❌ Error en scraper: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("⏰ Timeout del scraper")
            return False
        except Exception as e:
            print(f"❌ Error ejecutando scraper: {e}")
            return False
        finally:
            os.chdir(original_dir)
    
    def process_insider_data(self):
        """Procesa datos del scraper y añade scoring"""
        csv_path = self.scraper_dir / "data" / "insider_trades.csv"
        
        if not csv_path.exists():
            print("❌ No se encontró archivo de datos del scraper")
            return []
        
        print("📊 Procesando datos de insider trading...")
        
        try:
            df = pd.read_csv(csv_path)
            print(f"📈 Encontradas {len(df)} transacciones")
            
            alerts = []
            for _, row in df.iterrows():
                alert = self.create_alert_from_row(row)
                if alert and alert['score'] >= 60:  # Filtro mínimo
                    alerts.append(alert)
            
            # Ordenar por score
            alerts.sort(key=lambda x: x['score'], reverse=True)
            
            print(f"🚨 Generadas {len(alerts)} alertas")
            return alerts
            
        except Exception as e:
            print(f"❌ Error procesando datos: {e}")
            return []
    
    def create_alert_from_row(self, row):
        """Convierte fila CSV en alerta con scoring"""
        try:
            # Calcular valor total estimado
            shares = float(row.get('shares', 0))
            price = float(row.get('price', 0))
            total_value = shares * price
            
            if total_value < 50000:  # Filtrar trades pequeños
                return None
            
            # Scoring básico
            score = self.calculate_score(row, total_value)
            
            alert = {
                'id': f"insider_{hash(f'{row.get("ticker")}_{row.get("date")}_{row.get("insider")}')}",
                'ticker': row.get('ticker', '').upper(),
                'company': row.get('company', ''),
                'insider': row.get('insider', ''),
                'role': row.get('title', ''),
                'transaction': 'Purchase',
                'totalValue': int(total_value),
                'shares': int(shares),
                'pricePerShare': round(price, 2),
                'currentPrice': round(price, 2),  # Se actualizará con Alpha Vantage
                'priceChange': 0,
                'priceChangePercent': 0,
                'traded': row.get('date', ''),
                'filed': row.get('filing_date', row.get('date', '')),
                'score': score,
                'level': self.get_alert_level(score),
                'factors': self.get_factors(row, total_value),
                'source': 'OpenInsider',
                'timestamp': datetime.now().isoformat()
            }
            
            return alert
            
        except Exception as e:
            print(f"⚠️ Error procesando fila: {e}")
            return None
    
    def calculate_score(self, row, total_value):
        """Calcula score de alerta"""
        score = 0
        
        # Tamaño de transacción (0-30 puntos)
        if total_value >= 10000000:  # $10M+
            score += 30
        elif total_value >= 5000000:  # $5M+
            score += 25
        elif total_value >= 1000000:  # $1M+
            score += 20
        else:
            score += 10
        
        # Rol del insider (0-25 puntos)
        title = str(row.get('title', '')).lower()
        if any(word in title for word in ['ceo', 'president', 'chief']):
            score += 25
        elif any(word in title for word in ['director', 'chairman']):
            score += 20
        elif any(word in title for word in ['officer', 'vp', 'vice']):
            score += 15
        else:
            score += 10
        
        # Timing (0-15 puntos) - asumimos filing rápido es bueno
        score += 15  # Por defecto, se puede mejorar con datos reales
        
        # Sector bonus (0-10 puntos)
        ticker = str(row.get('ticker', '')).upper()
        tech_stocks = ['NVDA', 'TSLA', 'AAPL', 'MSFT', 'GOOGL', 'META', 'PLTR']
        if ticker in tech_stocks:
            score += 10
        
        return min(score, 100)
    
    def get_alert_level(self, score):
        """Determina nivel de alerta"""
        if score >= 80:
            return 'critical'
        elif score >= 70:
            return 'high'
        elif score >= 60:
            return 'medium'
        else:
            return 'low'
    
    def get_factors(self, row, total_value):
        """Genera factores de alerta"""
        factors = []
        
        if total_value >= 5000000:
            factors.append('Large trade')
        if total_value >= 10000000:
            factors.append('Mega trade')
        
        title = str(row.get('title', '')).lower()
        if 'ceo' in title:
            factors.append('CEO purchase')
        elif 'director' in title:
            factors.append('Director purchase')
        
        ticker = str(row.get('ticker', '')).upper()
        if ticker in ['NVDA', 'TSLA', 'AAPL', 'MSFT']:
            factors.append('Tech leader')
        
        factors.append('Recent filing')
        
        return factors
    
    def update_prices_with_alpha_vantage(self, alerts):
        """Actualiza precios con Alpha Vantage"""
        if not self.alpha_key:
            print("⚠️ Sin API key de Alpha Vantage, usando precios del scraper")
            return alerts
        
        print("💰 Actualizando precios con Alpha Vantage...")
        
        updated_alerts = []
        for alert in alerts[:5]:  # Máximo 5 para no gastar API
            try:
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={alert['ticker']}&apikey={self.alpha_key}"
                response = requests.get(url, timeout=10)
                data = response.json()
                
                if 'Global Quote' in data:
                    quote = data['Global Quote']
                    current_price = float(quote['05. price'])
                    change = float(quote['09. change'])
                    change_percent = float(quote['10. change percent'].replace('%', ''))
                    
                    alert['currentPrice'] = round(current_price, 2)
                    alert['priceChange'] = round(change, 2)
                    alert['priceChangePercent'] = round(change_percent, 2)
                    
                    print(f"✅ {alert['ticker']}: ${current_price} ({change_percent:+.2f}%)")
                
                updated_alerts.append(alert)
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"⚠️ Error obteniendo precio de {alert['ticker']}: {e}")
                updated_alerts.append(alert)
        
        return updated_alerts
    
    def save_alerts_for_web(self, alerts):
        """Guarda alertas en formato JSON para la app web"""
        output_file = self.data_dir / "insider_alerts.json"
        
        web_data = {
            'alerts': alerts,
            'last_update': datetime.now().isoformat(),
            'total_alerts': len(alerts),
            'stats': {
                'critical': len([a for a in alerts if a['level'] == 'critical']),
                'high': len([a for a in alerts if a['level'] == 'high']),
                'medium': len([a for a in alerts if a['level'] == 'medium']),
                'avg_score': round(sum(a['score'] for a in alerts) / len(alerts), 1) if alerts else 0
            }
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(web_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Datos guardados en: {output_file}")
        return output_file
    
    def run_full_process(self):
        """Ejecuta el proceso completo"""
        print("🚀 Iniciando proceso completo de insider trading bot...")
        
        # 1. Ejecutar scraper
        if not self.run_scraper():
            print("❌ Falló el scraping, usando datos de ejemplo")
            alerts = self.get_example_alerts()
        else:
            # 2. Procesar datos
            alerts = self.process_insider_data()
        
        if not alerts:
            print("⚠️ No se encontraron alertas, usando datos de ejemplo")
            alerts = self.get_example_alerts()
        
        # 3. Actualizar precios
        alerts = self.update_prices_with_alpha_vantage(alerts)
        
        # 4. Guardar para web
        self.save_alerts_for_web(alerts)
        
        print(f"✅ Proceso completado. {len(alerts)} alertas generadas.")
        return alerts
    
    def get_example_alerts(self):
        """Datos de ejemplo si falla el scraper"""
        return [
            {
                'id': 'example_1',
                'ticker': 'RDDT',
                'company': 'Reddit Inc',
                'insider': 'Steve Huffman',
                'role': 'CEO',
                'transaction': 'Purchase',
                'totalValue': 987000,
                'shares': 15000,
                'pricePerShare': 65.80,
                'currentPrice': 67.45,
                'priceChange': 1.65,
                'priceChangePercent': 2.51,
                'traded': '2025-08-02',
                'filed': '2025-08-02',
                'score': 78,
                'level': 'high',
                'factors': ['CEO purchase', 'Recent filing', 'Tech sector'],
                'source': 'OpenInsider',
                'timestamp': datetime.now().isoformat()
            }
        ]

def main():
    # Cargar API key desde archivo o variable de entorno
    api_key = None
    
    # Intentar cargar desde archivo
    try:
        with open('alpha_vantage_key.txt', 'r') as f:
            api_key = f.read().strip()
    except:
        pass
    
    # O desde input del usuario
    if not api_key:
        api_key = input("Ingresa tu API key de Alpha Vantage (o Enter para saltar): ").strip()
        if api_key:
            with open('alpha_vantage_key.txt', 'w') as f:
                f.write(api_key)
    
    # Ejecutar bot
    bot = InsiderBotIntegration(api_key)
    alerts = bot.run_full_process()
    
    print(f"\n📊 Resumen:")
    print(f"Total alertas: {len(alerts)}")
    if alerts:
        print(f"Score promedio: {sum(a['score'] for a in alerts) / len(alerts):.1f}")
        print(f"Mejor alerta: {alerts[0]['ticker']} (Score: {alerts[0]['score']})")

if __name__ == "__main__":
    main()
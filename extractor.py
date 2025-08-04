#!/usr/bin/env python3
"""
Insider Trading Bot - Tu scraper modificado con Finnhub
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

class InsiderBotFinnhub:
    def __init__(self):
        self.finnhub_key = "d28176pr01qr2iau5o4gd28176pr01qr2iau5o50"
        self.base_dir = Path(__file__).parent
        self.scraper_dir = self.base_dir / "openinsiderData"
        self.data_dir = self.base_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        
    def run_existing_scraper(self):
        """Ejecuta tu scraper OpenInsider existente"""
        print("Ejecutando tu scraper OpenInsider...")

        scraper_path = self.scraper_dir / "openinsider_scraper.py"
        if not scraper_path.exists():
            print(f"ERROR: No se encontró el archivo {scraper_path}")
            return False

        try:
            result = subprocess.run([
                sys.executable, str(scraper_path)
            ], capture_output=True, text=True, timeout=300)

            print(result.stdout)  # Muestra logs del scraper
            if result.returncode == 0:
                print("SUCCESS: Scraper ejecutado exitosamente")

                # Verificación del archivo CSV generado
                csv_check_path = self.scraper_dir / "data" / "insider_trades.csv"
                print(f"Verificando archivo CSV en: {csv_check_path}")
                csv_exists = csv_check_path.exists()
                print(f"¿Existe insider_trades.csv?: {csv_exists}")
                
                if csv_exists:
                    print(f"SUCCESS: Archivo CSV encontrado con {csv_check_path.stat().st_size} bytes")
                    return True
                else:
                    print("ERROR: Archivo CSV no encontrado - scraper falló")
                    return False
            else:
                print(f"ERROR en scraper:\n{result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print("TIMEOUT del scraper")
            return False
        except Exception as e:
            print(f"ERROR ejecutando scraper: {e}")
            return False


    
    def process_scraped_data(self):
        """Procesa datos de tu scraper existente"""
        csv_path = self.scraper_dir / "data" / "insider_trades.csv"
        
        if not csv_path.exists():
            print("ERROR: No se encontró archivo de datos del scraper")
            return []
        
        print("Procesando datos del scraper...")
        
        try:
            df = pd.read_csv(csv_path)
            print(f"Encontradas {len(df)} transacciones")
            
            alerts = []
            for _, row in df.iterrows():
                alert = self.create_enhanced_alert(row)
                if alert and alert['score'] >= 40:  # Filtro más bajo
                    alerts.append(alert)
            
            alerts.sort(key=lambda x: x['score'], reverse=True)
            print(f"Generadas {len(alerts)} alertas")
            return alerts
            
        except Exception as e:
            print(f"ERROR procesando datos: {e}")
            return []
    
    def clean_numeric(self, value):
        """Limpia valores numéricos removiendo símbolos"""
        if not value or pd.isna(value):
            return 0.0

        # Convertir a string y limpiar símbolos
        clean = str(value).replace('+', '').replace(',', '').replace('$', '').replace('"', '').replace("'", '').strip()
        
        # Remover espacios adicionales
        clean = ''.join(clean.split())
        
        try:
            return float(clean) if clean else 0.0
        except ValueError as e:
            if len(clean) > 0:  # Solo para casos no vacíos
                print(f"DEBUG: No se pudo convertir '{value}' -> '{clean}': {e}")
            return 0.0
    
    def create_enhanced_alert(self, row):
        """Convierte fila CSV en alerta con scoring mejorado"""
        try:
            shares = self.clean_numeric(row.get('Qty', 0))
            price = self.clean_numeric(row.get('last_price', 0))
            total_value = shares * price
            
            if total_value < 25000:  # Filtro más bajo
                return None
            
            score = self.calculate_enhanced_score(row, total_value)
            
            # Crear ID único
            ticker = row.get('ticker', '')
            trade_date = row.get('trade_date', '')
            owner_name = row.get('owner_name', '')
            alert_id = f"insider_{hash(f'{ticker}_{trade_date}_{owner_name}')}"
            
            alert = {
                'id': alert_id,
                'ticker': str(row.get('ticker', '')).upper(),
                'company': str(row.get('company_name', '')),
                'insider': str(row.get('owner_name', '')),
                'role': str(row.get('Title', '')),
                'transaction': 'Purchase',
                'totalValue': int(total_value),
                'shares': int(shares),
                'pricePerShare': round(price, 2),
                'currentPrice': round(price, 2),
                'priceChange': 0,
                'priceChangePercent': 0,
                'traded': str(row.get('trade_date', '')),
                'filed': str(row.get('transaction_date', row.get('trade_date', ''))),
                'score': score,
                'level': self.get_alert_level(score),
                'factors': self.get_enhanced_factors(row, total_value),
                'source': 'Tu OpenInsider Scraper',
                'timestamp': datetime.now().isoformat()
            }
            
            return alert
            
        except Exception as e:
            print(f"WARNING: Error procesando fila: {e}")
            return None
    
    def calculate_enhanced_score(self, row, total_value):
        """Sistema calibrado para mega trades"""
        score = 0
        
        # Replace value scoring (0-60 points):
        if total_value >= 500000000:    # $500M+
            score += 60
        elif total_value >= 200000000:  # $200M+
            score += 55  
        elif total_value >= 100000000:  # $100M+
            score += 50
        elif total_value >= 50000000:   # $50M+
            score += 40
        elif total_value >= 10000000:   # $10M+
            score += 30
        elif total_value >= 1000000:    # $1M+
            score += 20
        
        # 2. ROL DEL INSIDER (0-30 puntos)
        title = str(row.get('Title', '')).lower()
        insider_name = str(row.get('owner_name', '')).lower()
        
        # Special cases - founders get max points
        if 'moskovitz' in insider_name:  # Asana founder
            score += 30
        elif any(word in title for word in ['ceo', 'chief executive']):
            score += 28
        elif any(word in title for word in ['cfo', 'chief financial']):
            score += 25
        elif any(word in title for word in ['founder', 'co-founder']):
            score += 30
        elif any(word in title for word in ['president', 'pres']):
            score += 23
        elif any(word in title for word in ['chairman', 'chair']):
            score += 20
        elif '10%' in title:
            score += 25  # Major shareholders
        elif any(word in title for word in ['director', 'dir']):
            score += 18
        elif any(word in title for word in ['officer', 'vp', 'vice', 'evp']):
            score += 15
        else:
            score += 8
        
        # 3. TICKER PREMIUM (0-15 puntos)
        ticker = str(row.get('ticker', '')).upper()
        
        # Mega caps
        if ticker in ['AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN']:
            score += 15
        elif ticker in ['TSLA', 'NVDA', 'META', 'UPS', 'CHTR']:
            score += 12
        elif ticker in ['ASAN', 'PLTR']:  # High-growth
            score += 10
        elif ticker in ['RDDT', 'COIN']:
            score += 8
        else:
            score += 3
        
        # 4. VOLUMEN BONUS (0-5 puntos)
        shares = self.clean_numeric(row.get('Qty', 0))
        if shares >= 10000000:  # 10M+ shares
            score += 5
        elif shares >= 1000000:   # 1M+ shares
            score += 3
        elif shares >= 100000:    # 100K+ shares
            score += 2
        
        return min(score, 100)

    def get_enhanced_factors(self, row, total_value):
        """Factores específicos para mega trades"""
        factors = []
        
        # Valor específico
        if total_value >= 100000000:
            factors.append(f'MEGA (${total_value/1000000:.0f}M)')
        elif total_value >= 10000000:
            factors.append(f'MAJOR (${total_value/1000000:.0f}M)')
        elif total_value >= 1000000:
            factors.append(f'Large (${total_value/1000000:.1f}M)')
        
        # Rol específico
        title = str(row.get('Title', '')).lower()
        insider_name = str(row.get('owner_name', '')).lower()
        
        if 'moskovitz' in insider_name:
            factors.append('FOUNDER (Asana)')
        elif 'ceo' in title:
            factors.append('CEO')
        elif 'cfo' in title:
            factors.append('CFO')
        elif '10%' in title:
            factors.append('MAJOR OWNER')
        elif 'director' in title:
            factors.append('DIRECTOR')
        
        # Ticker
        ticker = str(row.get('ticker', '')).upper()
        if ticker in ['UPS', 'CHTR', 'ASAN']:
            factors.append(f'{ticker}')
        
        factors.append('Recent')
        return factors
    
    def get_alert_level(self, score):
        """Determina nivel de alerta"""
        if score >= 80:
            return 'critical'
        elif score >= 65:
            return 'high'
        elif score >= 50:
            return 'medium'
        else:
            return 'low'
    
    def update_prices_with_finnhub(self, alerts):
        """Actualiza precios con Finnhub"""
        print(f"Preparando actualización de precios...")
        
        # Obtener tickers únicos
        unique_tickers = list(set([alert['ticker'] for alert in alerts[:15]]))
        
        print(f"Se realizarán {len(unique_tickers)} requests a Finnhub")
        print(f"Tickers: {', '.join(unique_tickers)}")
        
        print(f"Procediendo automáticamente con {len(unique_tickers)} API calls de Finnhub")
        
        # Mapear precios
        ticker_prices = {}
        successful_updates = 0
        
        for ticker in unique_tickers:
            try:
                url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.finnhub_key}"
                response = requests.get(url, timeout=10)
                data = response.json()
                
                if 'c' in data and data['c'] > 0:
                    current_price = data['c']
                    prev_close = data.get('pc', current_price)
                    change = current_price - prev_close
                    change_percent = (change / prev_close) * 100 if prev_close > 0 else 0
                    
                    ticker_prices[ticker] = {
                        'current': current_price,
                        'change': change,
                        'change_percent': change_percent
                    }
                    
                    print(f"SUCCESS {ticker}: ${current_price:.2f} ({change_percent:+.2f}%)")
                    successful_updates += 1
                else:
                    print(f"WARNING: No data for {ticker}")
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"ERROR with {ticker}: {e}")
        
        # Actualizar alertas
        updated_alerts = []
        for alert in alerts:
            ticker = alert['ticker']
            if ticker in ticker_prices:
                price_data = ticker_prices[ticker]
                alert['currentPrice'] = round(price_data['current'], 2)
                alert['priceChange'] = round(price_data['change'], 2)
                alert['priceChangePercent'] = round(price_data['change_percent'], 2)
            
            updated_alerts.append(alert)
        
        print(f"SUCCESS: {successful_updates}/{len(unique_tickers)} precios actualizados")
        return updated_alerts
    
    def save_alerts_for_web(self, alerts):
        """Guarda alertas para la web"""
        output_file = self.data_dir / "insider_alerts.json"
        
        web_data = {
            'alerts': alerts,
            'last_update': datetime.now().isoformat(),
            'total_alerts': len(alerts),
            'stats': {
                'critical': len([a for a in alerts if a['level'] == 'critical']),
                'high': len([a for a in alerts if a['level'] == 'high']),
                'medium': len([a for a in alerts if a['level'] == 'medium']),
                'low': len([a for a in alerts if a['level'] == 'low']),
                'avg_score': round(sum(a['score'] for a in alerts) / len(alerts), 1) if alerts else 0,
                'total_value': sum(a['totalValue'] for a in alerts),
                'mega_trades': len([a for a in alerts if a['totalValue'] >= 1000000])
            }
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(web_data, f, indent=2, ensure_ascii=False)
        
        print(f"{len(alerts)} alertas guardadas")
        
        if alerts:
            print(f"\nRESUMEN:")
            print(f"Total: {len(alerts)}")
            print(f"Valor total: ${sum(a['totalValue'] for a in alerts):,.0f}")
            print(f"Mega trades ($1M+): {len([a for a in alerts if a['totalValue'] >= 1000000])}")
            
            print(f"\nTop 5:")
            for i, alert in enumerate(alerts[:5], 1):
                print(f"{i}. {alert['ticker']}: {alert['insider']} - ${alert['totalValue']:,.0f} (Score: {alert['score']})")
        
        return output_file
    
# En tu función run_full_process(), reemplaza esta parte:

    def run_full_process(self):
        """Proceso completo con tu scraper + Finnhub"""
        print("Iniciando con tu scraper existente + Finnhub...")
        
        # 1. Ejecutar tu scraper
        if not self.run_existing_scraper():
            print("ERROR: Falló el scraping")
            return []
        
        # 2. Procesar datos
        alerts = self.process_scraped_data()
        
        if not alerts:
            print("WARNING: No se generaron alertas")
            return []
        
        # 3. Actualizar precios con Finnhub - ACTIVADO
        print(f"Tienes {len(alerts)} alertas. Actualizando precios...")
        alerts = self.update_prices_with_finnhub(alerts)
        
        # 4. Guardar
        self.save_alerts_for_web(alerts)
        
        return alerts

def main():
    bot = InsiderBotFinnhub()
    alerts = bot.run_full_process()

if __name__ == "__main__":
    main()


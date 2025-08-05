#!/usr/bin/env python3
"""
INTELLIGENT INSIDER SCRAPER
Detecta patrones de acumulación con filtros inteligentes
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import re
from collections import defaultdict
import sys

class IntelligentInsiderScraper:
    def __init__(self, output_dir="data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Ubicaciones fijas
        self.raw_csv = self.output_dir / "insider_trades_raw.csv"
        self.filtered_csv = self.output_dir / "insider_opportunities.csv"
        
        # Configuración inteligente
        self.config = {
            'days_back': 30,           # Últimos 30 días para patrones
            'min_purchase_value': 500000,  # $500K mínimo individual
            'min_cluster_value': 1500000,  # $1.5M mínimo cluster
            'max_pe_ratio': 25,        # No overvalued stocks
            'min_market_cap': 500000000,   # $500M mínimo
            'max_market_cap': 50000000000, # $50B máximo
            'min_daily_volume': 100000,    # 100K shares liquidity
        }
        
        # Insiders relevantes (C-suite only)
        self.relevant_insiders = [
            'ceo', 'chief executive', 'founder', 'co-founder',
            'cfo', 'chief financial', 'president', 'chairman', 
            'chair', '10%'  # 10% shareholders
        ]
        
    def scrape_recent_insider_data(self):
        """Scrape datos de últimos 30 días con filtros básicos"""
        print(f"🔍 Scraping últimos {self.config['days_back']} días...")
        
        # Fechas
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.config['days_back'])
        
        start_str = start_date.strftime('%m/%d/%Y')
        end_str = end_date.strftime('%m/%d/%Y')
        
        print(f"📅 Período: {start_str} a {end_str}")
        
        # URL con filtros para solo compras
        # xp=1 = exclude option exercises, xs=1 = exclude small trades
        url = f'http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=-1&fdr={start_str}+-+{end_str}&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=2000&page=1'
        
        try:
            print("📡 Descargando datos...")
            response = requests.get(url, timeout=45)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'class': 'tinytable'})
            
            if not table:
                raise Exception("No se encontró tabla de datos")
                
            tbody = table.find('tbody')
            rows = tbody.find_all('tr')
            
            print(f"📊 Filas raw encontradas: {len(rows)}")
            
            # Extraer y limpiar datos
            data = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 13:
                    continue
                    
                # Extraer datos limpios
                row_data = {}
                
                # Campos básicos
                row_data['filing_date'] = self._clean_text(cols[1])
                row_data['trade_date'] = self._clean_text(cols[2])
                row_data['ticker'] = self._clean_text(cols[3]).upper()
                row_data['company_name'] = self._clean_text(cols[4])
                row_data['insider_name'] = self._clean_text(cols[5])
                row_data['title'] = self._clean_text(cols[6])
                row_data['transaction_type'] = self._clean_text(cols[7])
                
                # Campos numéricos - LIMPIEZA CRÍTICA
                row_data['price'] = self._clean_numeric(cols[8])
                row_data['qty'] = self._clean_numeric(cols[9])
                row_data['shares_owned'] = self._clean_numeric(cols[10])
                row_data['ownership_change'] = self._clean_percent(cols[11])
                row_data['transaction_value'] = self._clean_numeric(cols[12])
                
                # Solo procesar si tenemos datos válidos
                if (row_data['ticker'] and 
                    row_data['transaction_value'] != 0 and
                    row_data['qty'] != 0):
                    data.append(row_data)
            
            print(f"✅ Datos limpios extraídos: {len(data)} transacciones")
            
            # Crear DataFrame
            df = pd.DataFrame(data)
            
            # Guardar datos raw
            df.to_csv(self.raw_csv, index=False)
            print(f"💾 Datos raw guardados: {self.raw_csv}")
            
            return df
            
        except Exception as e:
            print(f"❌ ERROR scraping: {e}")
            return None
    
    def _clean_text(self, cell):
        """Limpia texto de celdas HTML"""
        link = cell.find('a')
        text = link.text.strip() if link else cell.text.strip()
        return text
    
    def _clean_numeric(self, cell):
        """Limpia valores numéricos - MANEJA SÍMBOLOS"""
        text = self._clean_text(cell)
        if not text or text.lower() in ['n/a', 'new', '']:
            return 0.0
        
        # Remover símbolos comunes: $, +, -, ,, espacios
        clean = re.sub(r'[\$\,\s]', '', text)
        
        # Manejar signo negativo
        is_negative = clean.startswith('-')
        if is_negative:
            clean = clean[1:]
        elif clean.startswith('+'):
            clean = clean[1:]
            
        try:
            value = float(clean)
            return -value if is_negative else value
        except ValueError:
            return 0.0
    
    def _clean_percent(self, cell):
        """Limpia porcentajes"""
        text = self._clean_text(cell)
        if not text or text.lower() in ['n/a', 'new']:
            return 0.0
            
        # Remover % y otros símbolos
        clean = re.sub(r'[\%\,\s]', '', text)
        
        is_negative = clean.startswith('-')
        if is_negative:
            clean = clean[1:]
        elif clean.startswith('+'):
            clean = clean[1:]
            
        try:
            value = float(clean)
            return -value if is_negative else value
        except ValueError:
            return 0.0
    
    def apply_intelligent_filters(self, df):
        """Aplica filtros inteligentes basados en nuestra estrategia"""
        print(f"\n🧠 Aplicando filtros inteligentes...")
        print(f"📊 Datos iniciales: {len(df)} transacciones")
        
        original_count = len(df)
        
        # 1. Solo compras (P - Purchase)
        df = df[df['transaction_type'].str.contains('P - Purchase', na=False)]
        print(f"✅ Solo compras: {len(df)} ({len(df)/original_count*100:.1f}%)")
        
        # 2. Valor mínimo de transacción
        min_value = self.config['min_purchase_value']
        df = df[abs(df['transaction_value']) >= min_value]
        print(f"✅ Valor >${min_value/1000000:.1f}M+: {len(df)} transacciones")
        
        # 3. Solo insiders relevantes (C-suite)
        df = df[df['title'].str.lower().str.contains('|'.join(self.relevant_insiders), na=False)]
        print(f"✅ Solo C-suite: {len(df)} transacciones")
        
        # 4. Filtrar tickers válidos (3-5 caracteres)
        df = df[df['ticker'].str.len().between(1, 5)]
        print(f"✅ Tickers válidos: {len(df)} transacciones")
        
        # 5. Eliminar duplicados exactos
        df = df.drop_duplicates(subset=['ticker', 'insider_name', 'trade_date', 'transaction_value'])
        print(f"✅ Sin duplicados: {len(df)} transacciones")
        
        return df
    
    def detect_cluster_buying(self, df):
        """Detecta patrones de cluster buying (múltiples insiders)"""
        print(f"\n🎯 Detectando cluster buying patterns...")
        
        # Agrupar por ticker
        clusters = defaultdict(list)
        
        for _, row in df.iterrows():
            ticker = row['ticker']
            clusters[ticker].append({
                'insider': row['insider_name'],
                'title': row['title'],
                'value': abs(row['transaction_value']),
                'date': row['trade_date'],
                'qty': row['qty']
            })
        
        # Analizar clusters
        cluster_results = []
        
        for ticker, purchases in clusters.items():
            if len(purchases) >= 1:  # Al menos 1 compra (podemos cambiar a 2+ después)
                total_value = sum(p['value'] for p in purchases)
                insider_count = len(set(p['insider'] for p in purchases))
                
                # Calcular score básico
                score = self._calculate_cluster_score(purchases, total_value, insider_count)
                
                cluster_results.append({
                    'ticker': ticker,
                    'insider_count': insider_count,
                    'total_value': total_value,
                    'avg_value': total_value / len(purchases),
                    'score': score,
                    'purchases': purchases,
                    'latest_date': max(p['date'] for p in purchases)
                })
        
        # Ordenar por score
        cluster_results.sort(key=lambda x: x['score'], reverse=True)
        
        print(f"📈 Clusters detectados: {len(cluster_results)}")
        
        # Mostrar top clusters
        print(f"\n🏆 TOP CLUSTERS:")
        for i, cluster in enumerate(cluster_results[:5], 1):
            print(f"{i}. {cluster['ticker']}: {cluster['insider_count']} insiders, "
                  f"${cluster['total_value']/1000000:.1f}M total (Score: {cluster['score']:.1f})")
        
        return cluster_results
    
    def _calculate_cluster_score(self, purchases, total_value, insider_count):
        """Calcula score realista para cluster"""
        score = 0
        
        # 1. Valor total (0-30 puntos)
        if total_value >= 5000000:      # $5M+
            score += 30
        elif total_value >= 2000000:    # $2M+
            score += 25
        elif total_value >= 1000000:    # $1M+
            score += 20
        elif total_value >= 500000:     # $500K+
            score += 15
        
        # 2. Cluster effect (0-25 puntos)
        if insider_count >= 3:
            score += 25
        elif insider_count >= 2:
            score += 15
        else:
            score += 5  # Single insider
        
        # 3. Insider quality (0-30 puntos)
        max_insider_score = 0
        for purchase in purchases:
            title = purchase['title'].lower()
            if any(word in title for word in ['ceo', 'chief executive']):
                max_insider_score = max(max_insider_score, 30)
            elif any(word in title for word in ['cfo', 'chief financial']):
                max_insider_score = max(max_insider_score, 25)
            elif any(word in title for word in ['founder', 'co-founder']):
                max_insider_score = max(max_insider_score, 30)
            elif '10%' in title:
                max_insider_score = max(max_insider_score, 25)
            elif any(word in title for word in ['president', 'chairman']):
                max_insider_score = max(max_insider_score, 20)
            else:
                max_insider_score = max(max_insider_score, 10)
        
        score += max_insider_score
        
        # 4. Recency bonus (0-15 puntos)
        latest_date = max(p['date'] for p in purchases)
        try:
            days_ago = (datetime.now() - datetime.strptime(latest_date, '%Y-%m-%d')).days
            if days_ago <= 7:
                score += 15
            elif days_ago <= 14:
                score += 10
            elif days_ago <= 21:
                score += 5
        except:
            score += 0
        
        return min(score, 100)
    
    def save_opportunities(self, clusters):
        """Guarda oportunidades filtradas para análisis"""
        opportunities = []
        
        for cluster in clusters:
            if cluster['score'] >= 50:  # Threshold mínimo
                # Crear registro detallado
                opp = {
                    'ticker': cluster['ticker'],
                    'score': round(cluster['score'], 1),
                    'insider_count': cluster['insider_count'],
                    'total_value_usd': int(cluster['total_value']),
                    'total_value_millions': round(cluster['total_value']/1000000, 1),
                    'avg_purchase_value': int(cluster['avg_value']),
                    'latest_purchase': cluster['latest_date'],
                }
                
                # Agregar detalles de insiders
                insiders_detail = []
                for purchase in cluster['purchases']:
                    insiders_detail.append(f"{purchase['insider']} ({purchase['title']}: ${purchase['value']/1000000:.1f}M)")
                
                opp['insiders_detail'] = ' | '.join(insiders_detail)
                opportunities.append(opp)
        
        # Guardar CSV
        if opportunities:
            df_opp = pd.DataFrame(opportunities)
            df_opp.to_csv(self.filtered_csv, index=False)
            print(f"💾 Oportunidades guardadas: {self.filtered_csv}")
            print(f"🎯 Total oportunidades: {len(opportunities)}")
            
            # Mostrar resumen
            print(f"\n📋 RESUMEN DE OPORTUNIDADES:")
            print(f"{'Ticker':<8} {'Score':<6} {'Insiders':<9} {'Value ($M)':<12} {'Latest'}")
            print("-" * 60)
            for opp in opportunities[:10]:
                print(f"{opp['ticker']:<8} {opp['score']:<6} {opp['insider_count']:<9} "
                      f"{opp['total_value_millions']:<12} {opp['latest_purchase']}")
        else:
            print("⚠️  No se encontraron oportunidades que cumplan criterios")
        
        return opportunities

def main():
    """Función principal"""
    print("🚀 INTELLIGENT INSIDER SCRAPER")
    print("=" * 50)
    
    scraper = IntelligentInsiderScraper()
    
    # 1. Scraping básico
    df = scraper.scrape_recent_insider_data()
    if df is None:
        print("❌ SCRAPING FALLIDO")
        sys.exit(1)
    
    # 2. Filtros inteligentes
    df_filtered = scraper.apply_intelligent_filters(df)
    
    # 3. Detección de clusters
    clusters = scraper.detect_cluster_buying(df_filtered)
    
    # 4. Guardar oportunidades
    opportunities = scraper.save_opportunities(clusters)
    
    print(f"\n✅ PROCESO COMPLETADO")
    print(f"📁 Datos raw: {scraper.raw_csv}")
    print(f"📁 Oportunidades: {scraper.filtered_csv}")
    print(f"🎯 Siguiente paso: Analizar manualmente las oportunidades top")

if __name__ == "__main__":
    main()
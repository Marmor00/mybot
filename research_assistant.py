#!/usr/bin/env python3
"""
RESEARCH ASSISTANT
Toma oportunidades del scraper y las enriquece para research manual
"""
import pandas as pd
import requests
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

class ResearchAssistant:
    def __init__(self, data_dir="data"):
        self.data_dir = Path(data_dir)
        
        # Archivos de entrada y salida
        self.opportunities_file = self.data_dir / "insider_opportunities.csv"
        self.research_report = self.data_dir / "weekly_research_report.json"
        self.research_csv = self.data_dir / "weekly_research_report.csv"
        
        # API key - DEBE estar en archivo separado
        self.finnhub_key = self.load_api_key()
        
    def load_api_key(self):
        """Carga API key desde archivo o environment variable"""
        # Primero intenta environment variable (Railway)
        import os
        env_key = os.environ.get('FINNHUB_API_KEY')
        if env_key:
            return env_key
        
        # Fallback a archivo local
        key_file = self.data_dir.parent / "finnhub_key.txt"
        if key_file.exists():
            with open(key_file, 'r') as f:
                return f.read().strip()
        
        # Último fallback
        print("⚠️  No API key found - usando hardcoded")
        return "d28176pr01qr2iau5o4gd28176pr01qr2iau5o50"
    
    def load_opportunities(self):
        """Carga oportunidades del scraper"""
        if not self.opportunities_file.exists():
            print(f"❌ No se encontró archivo: {self.opportunities_file}")
            print(f"💡 Ejecuta primero: python intelligent_scraper.py")
            return None
        
        df = pd.read_csv(self.opportunities_file)
        print(f"📊 Cargadas {len(df)} oportunidades del scraper")
        return df
    
    def enrich_with_market_data(self, df):
        """Enriquece con datos de mercado actuales"""
        print(f"📈 Obteniendo precios actuales via Finnhub...")
        
        # Obtener tickers únicos (solo top 10 para evitar rate limits)
        top_tickers = df.head(10)['ticker'].unique()
        print(f"🎯 Actualizando {len(top_tickers)} tickers: {', '.join(top_tickers)}")
        
        market_data = {}
        successful_updates = 0
        
        for ticker in top_tickers:
            try:
                # Obtener quote actual
                quote_data = self.get_stock_quote(ticker)
                
                # Obtener info básica de la empresa
                profile_data = self.get_company_profile(ticker)
                
                if quote_data and profile_data:
                    market_data[ticker] = {
                        'current_price': quote_data.get('c', 0),
                        'prev_close': quote_data.get('pc', 0),
                        'day_change': quote_data.get('d', 0),
                        'day_change_percent': quote_data.get('dp', 0),
                        'market_cap': profile_data.get('marketCapitalization', 0),
                        'pe_ratio': profile_data.get('peNWA', 0),  # P/E Next Twelve Months
                        'industry': profile_data.get('finnhubIndustry', 'Unknown'),
                        'sector': profile_data.get('gind', 'Unknown'),
                        '52w_high': quote_data.get('h', 0),
                        '52w_low': quote_data.get('l', 0)
                    }
                    
                    print(f"✅ {ticker}: ${quote_data.get('c', 0):.2f} (PE: {profile_data.get('peNWA', 'N/A')})")
                    successful_updates += 1
                else:
                    print(f"⚠️  {ticker}: No data available")
                
                time.sleep(1.2)  # Rate limiting
                
            except Exception as e:
                print(f"❌ Error con {ticker}: {e}")
        
        print(f"📊 Actualizados: {successful_updates}/{len(top_tickers)} tickers")
        return market_data
    
    def get_stock_quote(self, ticker):
        """Obtiene quote actual"""
        url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.finnhub_key}"
        response = requests.get(url, timeout=10)
        return response.json() if response.status_code == 200 else None
    
    def get_company_profile(self, ticker):
        """Obtiene perfil de empresa"""
        url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={self.finnhub_key}"
        response = requests.get(url, timeout=10)
        return response.json() if response.status_code == 200 else None
    
    def analyze_insider_performance(self, opportunities_df, market_data):
        """Analiza performance desde compra del insider"""
        print(f"🧮 Analizando performance desde compras insider...")
        
        enriched_opportunities = []
        
        for _, opp in opportunities_df.iterrows():
            ticker = opp['ticker']
            
            # Crear record enriquecido
            enriched = opp.to_dict()
            
            # Agregar datos de mercado
            if ticker in market_data:
                market = market_data[ticker]
                current_price = market['current_price']
                
                # Calcular distancia desde 52W highs/lows
                if market['52w_high'] > 0:
                    pct_from_high = ((current_price - market['52w_high']) / market['52w_high']) * 100
                else:
                    pct_from_high = 0
                
                if market['52w_low'] > 0:
                    pct_from_low = ((current_price - market['52w_low']) / market['52w_low']) * 100
                else:
                    pct_from_low = 0
                
                # Agregar datos enriquecidos
                enriched.update({
                    'current_price': round(current_price, 2),
                    'day_change_pct': round(market['day_change_percent'], 2),
                    'market_cap_millions': round(market['market_cap'], 0),
                    'pe_ratio': round(market['pe_ratio'], 1) if market['pe_ratio'] else None,
                    'industry': market['industry'],
                    'pct_from_52w_high': round(pct_from_high, 1),
                    'pct_from_52w_low': round(pct_from_low, 1),
                    'analysis_date': datetime.now().strftime('%Y-%m-%d')
                })
                
                # Research signals
                signals = []
                
                # Valuation check
                if market['pe_ratio'] and market['pe_ratio'] < 15:
                    signals.append("Low PE")
                elif market['pe_ratio'] and market['pe_ratio'] > 30:
                    signals.append("High PE - Caution")
                
                # Position vs 52W range
                if pct_from_high > -20:
                    signals.append("Near 52W High")
                elif pct_from_high < -50:
                    signals.append("Deep Value Territory")
                
                # Market cap category
                if market['market_cap'] > 50000:  # >$50B
                    signals.append("Large Cap")
                elif market['market_cap'] > 2000:  # >$2B
                    signals.append("Mid Cap")
                else:
                    signals.append("Small Cap")
                
                enriched['research_signals'] = ' | '.join(signals)
            else:
                # Sin datos de mercado
                enriched.update({
                    'current_price': None,
                    'day_change_pct': None,
                    'market_cap_millions': None,
                    'pe_ratio': None,
                    'industry': 'Unknown',
                    'pct_from_52w_high': None,
                    'pct_from_52w_low': None,
                    'research_signals': 'No market data',
                    'analysis_date': datetime.now().strftime('%Y-%m-%d')
                })
            
            enriched_opportunities.append(enriched)
        
        return enriched_opportunities
    
    def generate_research_report(self, enriched_opportunities):
        """Genera reporte final para research manual"""
        print(f"📋 Generando reporte de research...")
        
        # Filtrar solo top opportunities con datos
        research_candidates = [
            opp for opp in enriched_opportunities 
            if opp.get('current_price') and opp['score'] >= 70
        ]
        
        # Ordenar por score
        research_candidates.sort(key=lambda x: x['score'], reverse=True)
        
        # Crear reporte estructurado
        report = {
            'generation_date': datetime.now().isoformat(),
            'summary': {
                'total_opportunities': len(enriched_opportunities),
                'research_candidates': len(research_candidates),
                'avg_score': round(sum(opp['score'] for opp in research_candidates) / len(research_candidates), 1) if research_candidates else 0,
                'total_insider_value_millions': sum(opp['total_value_millions'] for opp in research_candidates)
            },
            'top_research_targets': research_candidates[:10],
            'quick_stats': {
                'sectors': {},
                'market_caps': {'large': 0, 'mid': 0, 'small': 0},
                'valuation': {'cheap': 0, 'fair': 0, 'expensive': 0}
            }
        }
        
        # Quick stats
        for opp in research_candidates:
            # Sector count
            industry = opp.get('industry', 'Unknown')
            report['quick_stats']['sectors'][industry] = report['quick_stats']['sectors'].get(industry, 0) + 1
            
            # Market cap buckets
            market_cap = opp.get('market_cap_millions', 0)
            if market_cap > 50000:
                report['quick_stats']['market_caps']['large'] += 1
            elif market_cap > 2000:
                report['quick_stats']['market_caps']['mid'] += 1
            else:
                report['quick_stats']['market_caps']['small'] += 1
            
            # Valuation buckets
            pe = opp.get('pe_ratio')
            if pe and pe < 15:
                report['quick_stats']['valuation']['cheap'] += 1
            elif pe and pe > 25:
                report['quick_stats']['valuation']['expensive'] += 1
            else:
                report['quick_stats']['valuation']['fair'] += 1
        
        # Guardar JSON completo
        with open(self.research_report, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Guardar CSV simple para Excel
        df_research = pd.DataFrame(research_candidates)
        df_research.to_csv(self.research_csv, index=False)
        
        print(f"💾 Reporte guardado: {self.research_report}")
        print(f"💾 CSV research: {self.research_csv}")
        
        return report
    
    def print_research_summary(self, report):
        """Imprime resumen ejecutivo"""
        summary = report['summary']
        
        print(f"\n📊 REPORTE DE RESEARCH - {datetime.now().strftime('%Y-%m-%d')}")
        print("=" * 70)
        
        print(f"🎯 OVERVIEW:")
        print(f"   Research candidates: {summary['research_candidates']}")
        print(f"   Avg score: {summary['avg_score']}")
        print(f"   Total insider value: ${summary['total_insider_value_millions']:.1f}M")
        
        print(f"\n🏆 TOP 5 PARA RESEARCH:")
        for i, opp in enumerate(report['top_research_targets'][:5], 1):
            price = opp.get('current_price', 'N/A')
            pe = opp.get('pe_ratio', 'N/A')
            signals = opp.get('research_signals', '')
            
            print(f"{i}. {opp['ticker']} (Score: {opp['score']})")
            print(f"   Price: ${price} | PE: {pe} | Cap: ${opp.get('market_cap_millions', 0):.0f}M")
            print(f"   Insiders: {opp['insider_count']} | Value: ${opp['total_value_millions']:.1f}M")
            print(f"   Signals: {signals}")
            print()

def main():
    """Función principal"""
    print("🔬 RESEARCH ASSISTANT")
    print("=" * 50)
    
    assistant = ResearchAssistant()
    
    # 1. Cargar oportunidades del scraper
    opportunities_df = assistant.load_opportunities()
    if opportunities_df is None:
        sys.exit(1)
    
    # 2. Enriquecer con datos de mercado
    market_data = assistant.enrich_with_market_data(opportunities_df)
    
    # 3. Analizar performance
    enriched_opportunities = assistant.analyze_insider_performance(opportunities_df, market_data)
    
    # 4. Generar reporte
    report = assistant.generate_research_report(enriched_opportunities)
    
    # 5. Mostrar resumen
    assistant.print_research_summary(report)
    
    print(f"\n✅ RESEARCH ASSISTANT COMPLETADO")
    print(f"📁 Revisa: {assistant.research_csv}")
    print(f"🎯 Próximo paso: Investigar manualmente los top 5")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Setup Script para Insider Trading Bot Cloud
Configura automáticamente el proyecto para deploy
"""

import os
import sys
import subprocess
import json
from pathlib import Path

class InsiderBotSetup:
    def __init__(self):
        self.project_dir = Path.cwd()
        self.required_files = [
            'app.py', 'extractor.py', 'openinsider_scraper.py',
            'requirements.txt', 'config.yaml'
        ]
        
    def print_banner(self):
        print("\n" + "="*60)
        print("🚀 INSIDER TRADING BOT - CLOUD SETUP")
        print("="*60)
        print("Este script configurará tu bot para deploy en la nube")
        print("Plataformas soportadas: Railway, Heroku, Render, Vercel")
        print("="*60 + "\n")
    
    def check_requirements(self):
        print("📋 Verificando archivos requeridos...")
        missing_files = []
        
        for file in self.required_files:
            if not (self.project_dir / file).exists():
                missing_files.append(file)
                print(f"   ❌ {file} - FALTANTE")
            else:
                print(f"   ✅ {file} - OK")
        
        if missing_files:
            print(f"\n❌ Faltan archivos: {', '.join(missing_files)}")
            print("Por favor, copia todos los archivos necesarios antes de continuar.")
            return False
        
        print("\n✅ Todos los archivos requeridos están presentes\n")
        return True
    
    def setup_directories(self):
        print("📁 Creando estructura de directorios...")
        
        dirs_to_create = [
            'data', 'templates', 'static', '.cache',
            'openInsiderData'  # Para tu scraper existente
        ]
        
        for directory in dirs_to_create:
            dir_path = self.project_dir / directory
            dir_path.mkdir(exist_ok=True)
            print(f"   ✅ {directory}/")
        
        print()
    
    def create_flask_files(self):
        print("🌐 Configurando archivos Flask...")
        
        # Crear templates/dashboard.html si no existe
        templates_dir = self.project_dir / 'templates'
        dashboard_file = templates_dir / 'dashboard.html'
        
        if not dashboard_file.exists():
            print(f"   ⚠️  Copia el contenido de dashboard.html a {dashboard_file}")
        else:
            print(f"   ✅ templates/dashboard.html")
        
        # Crear archivos de deploy
        deploy_files = {
            'Procfile': 'web: gunicorn --bind 0.0.0.0:$PORT app:app --timeout 120 --workers 2',
            'runtime.txt': 'python-3.9.20',
            '.gitignore': self.get_gitignore_content()
        }
        
        for filename, content in deploy_files.items():
            file_path = self.project_dir / filename
            if not file_path.exists():
                with open(file_path, 'w') as f:
                    f.write(content)
                print(f"   ✅ {filename}")
            else:
                print(f"   ✅ {filename} (ya existe)")
        
        print()
    
    def get_gitignore_content(self):
        return """__pycache__/
*.py[cod]
*$py.class
.Python
build/
dist/
*.egg-info/
.env
.venv
venv/
data/*.json
data/*.csv
*.log
.cache/
.DS_Store
.idea/
.vscode/
"""
    
    def create_config_files(self):
        print("⚙️  Creando archivos de configuración...")
        
        # Railway config
        railway_config = {
            "build": {"builder": "nixpacks"},
            "deploy": {
                "startCommand": "gunicorn --bind 0.0.0.0:$PORT app:app --timeout 120",
                "healthcheckPath": "/health"
            }
        }
        
        with open(self.project_dir / 'railway.toml', 'w') as f:
            # Simple TOML writing
            f.write('[build]\nbuilder = "nixpacks"\n\n')
            f.write('[deploy]\n')
            f.write('startCommand = "gunicorn --bind 0.0.0.0:$PORT app:app --timeout 120"\n')
            f.write('healthcheckPath = "/health"\n')
        
        print("   ✅ railway.toml")
        
        # Vercel config
        vercel_config = {
            "version": 2,
            "builds": [{"src": "app.py", "use": "@vercel/python"}],
            "routes": [{"src": "/(.*)", "dest": "app.py"}]
        }
        
        with open(self.project_dir / 'vercel.json', 'w') as f:
            json.dump(vercel_config, f, indent=2)
        
        print("   ✅ vercel.json")
        print()
    
    def install_dependencies(self):
        print("📦 Instalando dependencias...")
        
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], 
                         check=True, capture_output=True)
            print("   ✅ Dependencias instaladas correctamente")
        except subprocess.CalledProcessError as e:
            print(f"   ⚠️  Error instalando dependencias: {e}")
            print("   💡 Instálalas manualmente: pip install -r requirements.txt")
        
        print()
    
    def test_local_server(self):
        print("🧪 Probando servidor local...")
        print("   💡 Ejecuta manualmente para probar:")
        print("   python app.py")
        print("   Luego abre: http://localhost:5000")
        print()
    
    def show_deploy_instructions(self):
        print("🚀 INSTRUCCIONES PARA DEPLOY")
        print("="*50)
        
        print("\n1️⃣  RAILWAY (RECOMENDADO - GRATIS)")
        print("   • Ve a https://railway.app")
        print("   • Conecta tu GitHub")
        print("   • Deploy from GitHub repo")
        print("   • URL: https://tu-app.railway.app")
        
        print("\n2️⃣  HEROKU (CLÁSICO)")
        print("   • heroku create tu-app-name")
        print("   • git push heroku main")
        print("   • heroku open")
        
        print("\n3️⃣  RENDER (FÁCIL)")
        print("   • Ve a https://render.com")
        print("   • Connect GitHub repo")
        print("   • Auto-deploy activado")
        
        print("\n4️⃣  VERCEL (RÁPIDO)")
        print("   • vercel --prod")
        print("   • URL instantánea")
        
        print("\n📱 ACCESO MÓVIL:")
        print("   • Cualquier celular/tablet")
        print("   • Misma URL desde cualquier lugar")
        print("   • Se ve como app nativa")
        print("   • Funciona offline (básico)")
        
        print("\n⚙️  DESPUÉS DEL DEPLOY:")
        print("   • Abre la URL en tu celular")
        print("   • Haz clic en 'Ejecutar Extractor'")
        print("   • ¡Listo! Datos en tiempo real")
        
        print("\n" + "="*50)
    
    def run_setup(self):
        self.print_banner()
        
        if not self.check_requirements():
            return False
        
        self.setup_directories()
        self.create_flask_files()
        self.create_config_files()
        self.install_dependencies()
        self.test_local_server()
        self.show_deploy_instructions()
        
        print("✅ SETUP COMPLETADO!")
        print("🎉 Tu Insider Trading Bot está listo para deploy!")
        print("\n💡 Próximos pasos:")
        print("   1. Prueba localmente: python app.py")
        print("   2. Sube a GitHub")
        print("   3. Deploy en Railway/Heroku")
        print("   4. ¡Accede desde tu celular!")
        
        return True

if __name__ == "__main__":
    setup = InsiderBotSetup()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--check':
        # Solo verificar archivos
        setup.check_requirements()
    else:
        # Setup completo
        setup.run_setup()
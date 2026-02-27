"""
Configurações do IntegraGov - Sistema de Integração de Dados Públicos.
Execução local nas primeiras fases; preparado para migração para ambiente hospedado.
"""
import os
from pathlib import Path

# Raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent

# Camadas Data Lakehouse (armazenamento local)
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"   # Dados brutos, imutáveis
SILVER_DIR = DATA_DIR / "silver"   # Dados tratados e padronizados
GOLD_DIR = DATA_DIR / "gold"       # Dados analíticos e agregados

# Banco local (SQLite para MVP; trocar por PostgreSQL em produção)
DB_PATH = DATA_DIR / "integragov.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Garantir que diretórios existam
for d in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
    d.mkdir(parents=True, exist_ok=True)

# APIs externas
IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v3"
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
# Tabela SIDRA 6579 = População residente estimada
IBGE_AGREGADO_POPULACAO = 6579
IBGE_VARIAVEL_POPULACAO = 9324

# DEMAS / OpenDataSUS (Ministério da Saúde)
DATASUS_API_BASE = "https://apidadosabertos.saude.gov.br/v1"

# Fase atual do MVP (1=Saúde+Demografia, 2=+Educação, 3=+Transparência)
FASE_MVP = int(os.getenv("INTEGRAGOV_FASE", "1"))

# Log e ambiente
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEBUG = os.getenv("INTEGRAGOV_DEBUG", "0").lower() in ("1", "true", "yes")

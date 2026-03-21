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

# INEP (Educação)
INEP_SINOPSES_BASE = "https://download.inep.gov.br/dados_abertos/sinopses_estatisticas"
INEP_SINOPSE_URL = f"{INEP_SINOPSES_BASE}/sinopses_estatisticas_censo_escolar_{{ano}}.zip"
INEP_MICRODADOS_BASE = "https://download.inep.gov.br/dados_abertos"
INEP_MICRODADOS_CENSO_URL = f"{INEP_MICRODADOS_BASE}/microdados_censo_escolar_{{ano}}.zip"

# Fase atual do MVP (1=Saúde+Demografia, 2=+Educação, 3=+Transparência)
FASE_MVP = int(os.getenv("INTEGRAGOV_FASE", "1"))

# Log e ambiente
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEBUG = os.getenv("INTEGRAGOV_DEBUG", "0").lower() in ("1", "true", "yes")

# IBGE - PIB Municipal (SIDRA tabela 5938; dados disponíveis até ~2021)
IBGE_AGREGADO_PIB = 5938
IBGE_VARIAVEL_PIB_TOTAL = 37       # PIB total, a preços correntes (R$ mil)
IBGE_VARIAVEL_PIB_PERCAPITA = 498  # PIB per capita, a preços correntes (R$ 1,00)

# Portal da Transparência (requer chave gratuita: portaldatransparencia.gov.br/api-de-dados)
TRANSPARENCIA_API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
TRANSPARENCIA_API_KEY = os.getenv("TRANSPARENCIA_API_KEY", "")

"""
Camada de persistência local (SQLite no MVP).
Preparado para troca por PostgreSQL em ambiente hospedado.
"""
import sqlite3
from pathlib import Path
from contextlib import contextmanager
from typing import Iterator

from config.settings import DB_PATH


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    """Context manager para conexão com o banco local."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema(conn: sqlite3.Connection) -> None:
    """Cria tabelas necessárias para as camadas Silver e Gold (MVP)."""
    conn.executescript("""
    -- Referência de municípios (código IBGE 7 dígitos = chave de cruzamento)
    CREATE TABLE IF NOT EXISTS dim_municipio (
        cod_mun_ibge_7 TEXT PRIMARY KEY,
        nome_municipio TEXT,
        sigla_uf TEXT,
        cod_uf INTEGER
    );

    -- Silver: população por município/ano (IBGE)
    CREATE TABLE IF NOT EXISTS silver_ibge_populacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cod_mun_ibge_7 TEXT NOT NULL,
        ano INTEGER NOT NULL,
        populacao INTEGER,
        data_carga TEXT,
        FOREIGN KEY (cod_mun_ibge_7) REFERENCES dim_municipio(cod_mun_ibge_7)
    );

    -- Silver: indicadores de saúde por município (DATASUS - agregados)
    CREATE TABLE IF NOT EXISTS silver_datasus_indicadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cod_mun_ibge_7 TEXT,
        ano INTEGER,
        mes INTEGER,
        indicador TEXT,
        valor REAL,
        unidade TEXT,
        data_carga TEXT,
        FOREIGN KEY (cod_mun_ibge_7) REFERENCES dim_municipio(cod_mun_ibge_7)
    );

    -- Gold: indicadores de saúde por município (por 100k hab, etc.)
    CREATE TABLE IF NOT EXISTS gold_indicadores_saude_municipio (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cod_mun_ibge_7 TEXT NOT NULL,
        ano INTEGER NOT NULL,
        populacao INTEGER,
        total_internacoes INTEGER,
        total_obitos INTEGER,
        nascidos_vivos INTEGER,
        taxa_internacao_100k REAL,
        taxa_obitos_100k REAL,
        data_carga TEXT,
        FOREIGN KEY (cod_mun_ibge_7) REFERENCES dim_municipio(cod_mun_ibge_7)
    );

    CREATE INDEX IF NOT EXISTS idx_silver_pop_cod_ano ON silver_ibge_populacao(cod_mun_ibge_7, ano);
    CREATE INDEX IF NOT EXISTS idx_silver_datasus_cod_ano ON silver_datasus_indicadores(cod_mun_ibge_7, ano);
    CREATE INDEX IF NOT EXISTS idx_gold_cod_ano ON gold_indicadores_saude_municipio(cod_mun_ibge_7, ano);
    """)


def ensure_schema() -> None:
    """Garante que o schema está criado."""
    with get_connection() as conn:
        init_schema(conn)

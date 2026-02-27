"""
Conector DATASUS / OpenDataSUS (API DEMAS - Ministério da Saúde).
Camada Bronze: extração de dados de saúde (SIM, Sinasc, vacinação, etc.).
"""
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

from config.settings import DATASUS_API_BASE, BRONZE_DIR

logger = logging.getLogger(__name__)


class DatasusConnector:
    """Extrai dados de saúde das APIs do Ministério da Saúde (OpenDataSUS/DEMAS)."""

    def __init__(self, bronze_dir: Path = BRONZE_DIR):
        self.bronze_dir = Path(bronze_dir)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, path: str, params: dict | None = None) -> list | dict:
        """Requisição GET à API DEMAS."""
        url = f"{DATASUS_API_BASE.rstrip('/')}/{path.lstrip('/')}"
        try:
            r = self.session.get(url, params=params or {}, timeout=120)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if e.response is None or e.response.status_code != 404:
                logger.exception("Erro ao acessar %s: %s", url, e)
            raise
        except requests.RequestException as e:
            logger.exception("Erro ao acessar %s: %s", url, e)
            raise

    def sim_obitos(self, ano: int | None = None, uf: str | None = None) -> pd.DataFrame:
        """
        Sistema de Informação sobre Mortalidade (SIM).
        Usa PySUS (FTP DATASUS). A API REST do Ministério da Saúde retorna 404 e está fora do ar.
        Requer: pip install pysus
        """
        from src.connectors.datasus_pysus_fallback import sim_obitos_por_municipio_pysus
        ufs = [uf] if uf else None
        return sim_obitos_por_municipio_pysus(ano=ano or 2024, ufs=ufs)

    def sinasc_nascidos_vivos(self, ano: int | None = None) -> pd.DataFrame:
        """
        Sistema de Informações sobre Nascidos Vivos (Sinasc).
        Usa PySUS (FTP DATASUS). A API REST do Ministério da Saúde retorna 404 e está fora do ar.
        Requer: pip install pysus
        """
        from src.connectors.datasus_pysus_fallback import sinasc_nascidos_por_municipio_pysus
        return sinasc_nascidos_por_municipio_pysus(ano=ano or 2024, ufs=None)

    def vacinacao_pni(self, ano: int) -> pd.DataFrame:
        """Doses aplicadas PNI por ano (ex: 2024)."""
        path = f"vacinacao/doses-aplicadas-pni-{ano}"
        data = self._get(path)
        if isinstance(data, dict) and "results" in data:
            records = data["results"]
        elif isinstance(data, list):
            records = data
        else:
            records = []
        return pd.DataFrame(records) if records else pd.DataFrame()

    def salvar_bronze(self, df: pd.DataFrame, nome_fonte: str) -> Path:
        """Salva DataFrame na camada Bronze (parquet + csv)."""
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = nome_fonte.replace(" ", "_").lower()
        path_parquet = self.bronze_dir / f"datasus_{safe_name}_{ts}.parquet"
        path_csv = self.bronze_dir / f"datasus_{safe_name}_{ts}.csv"
        df.to_parquet(path_parquet, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Bronze DATASUS salvo: %s", path_parquet)
        return path_parquet

    def carregar_ultimo_bronze(self, prefixo: str) -> pd.DataFrame | None:
        """Carrega o arquivo Bronze mais recente com o prefixo dado (ex: datasus_sim)."""
        files = list(self.bronze_dir.glob(f"{prefixo}_*.parquet"))
        if not files:
            return None
        latest = max(files, key=lambda p: p.stat().st_mtime)
        return pd.read_parquet(latest)

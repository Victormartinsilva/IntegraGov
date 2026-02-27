"""
Conector IBGE - API de Dados Agregados (SIDRA) e Localidades.
Camada Bronze: extração e gravação dos dados brutos.
"""
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

from config.settings import (
    IBGE_BASE_URL,
    IBGE_MUNICIPIOS_URL,
    IBGE_AGREGADO_POPULACAO,
    IBGE_VARIAVEL_POPULACAO,
    BRONZE_DIR,
)

logger = logging.getLogger(__name__)


class IBGEConnector:
    """Extrai dados do IBGE (municípios e população estimada)."""

    def __init__(self, bronze_dir: Path = BRONZE_DIR):
        self.bronze_dir = Path(bronze_dir)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def listar_municipios(self) -> pd.DataFrame:
        """Lista todos os municípios do Brasil (IBGE localidades). Retorna código IBGE e nome."""
        url = IBGE_MUNICIPIOS_URL
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.exception("Erro ao obter municípios do IBGE: %s", e)
            raise

        rows = []
        for item in data:
            # Código do município no IBGE é id (7 dígitos: 2 UF + 5 município)
            id_mun = item.get("id")
            nome = item.get("nome")
            microrregiao = item.get("microrregiao") or {}
            mesorregiao = (microrregiao.get("mesorregiao") or {}) if isinstance(microrregiao, dict) else {}
            uf = (mesorregiao.get("UF") or {}) if isinstance(mesorregiao, dict) else {}
            sigla_uf = uf.get("sigla", "") if isinstance(uf, dict) else ""
            cod_uf = uf.get("id") if isinstance(uf, dict) else None
            if id_mun and nome:
                rows.append({
                    "cod_mun_ibge_7": str(id_mun).zfill(7),
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "cod_uf": cod_uf,
                })
        df = pd.DataFrame(rows)
        logger.info("Listados %d municípios do IBGE.", len(df))
        return df

    def obter_populacao_municipios(self, ano: int, codigos_municipios: list[int] | None = None) -> pd.DataFrame:
        """
        Obtém população residente estimada por município para um ano.
        Usa API SIDRA v3 - agregado 6579, variável 9324.
        Se codigos_municipios for passado, consulta só esses (evita timeout em MVP).
        """
        if codigos_municipios:
            return self._obter_populacao_por_lista(codigos_municipios, ano)
        # Todos os municípios: N6all (pode ser lento/timeout em ambiente limitado)
        url = (
            f"{IBGE_BASE_URL}/agregados/{IBGE_AGREGADO_POPULACAO}/periodos/-1/variaveis/{IBGE_VARIAVEL_POPULACAO}"
            "?localidades=N6[N6all]&formato=json"
        )
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.exception("Erro ao obter população do IBGE: %s", e)
            raise
        rows = self._parse_resposta_populacao(data, ano)
        df = pd.DataFrame(rows)
        logger.info("População obtida para %d municípios (ano %d).", len(df), ano)
        return df

    def _obter_populacao_por_lista(self, codigos: list[int], ano: int) -> pd.DataFrame:
        """Obtém população para uma lista de códigos de município (MVP local)."""
        rows = []
        for cod in codigos[:500]:  # limite para não sobrecarregar
            url = (
                f"{IBGE_BASE_URL}/agregados/{IBGE_AGREGADO_POPULACAO}/periodos/-1/variaveis/{IBGE_VARIAVEL_POPULACAO}"
                f"?localidades=N6[{cod}]&formato=json"
            )
            try:
                r = requests.get(url, timeout=15)
                r.raise_for_status()
                data = r.json()
                parsed = self._parse_resposta_populacao(data, ano)
                rows.extend(parsed)
            except requests.RequestException as e:
                logger.warning("Falha município %s: %s", cod, e)
        return pd.DataFrame(rows)

    def _parse_resposta_populacao(self, data: list, ano: int) -> list:
        """Extrai população da resposta da API (formato: series[].localidade.id + serie.{ano})."""
        rows = []
        if not isinstance(data, list):
            return rows
        for item in data:
            for res in item.get("resultados", []):
                for serie in res.get("series", []):
                    loc = serie.get("localidade", {})
                    cod_mun = loc.get("id") if isinstance(loc, dict) else None
                    if not cod_mun:
                        continue
                    cod_mun = str(cod_mun).zfill(7)
                    serie_vals = serie.get("serie") or {}
                    val = serie_vals.get(str(ano))
                    # Se o ano pedido não existir, usa o ano mais recente disponível (mantém ano solicitado no resultado)
                    if val is None and serie_vals:
                        anos_disponiveis = [k for k in serie_vals if k.isdigit() and len(k) == 4]
                        if anos_disponiveis:
                            ano_mais_recente = max(anos_disponiveis, key=int)
                            val = serie_vals.get(ano_mais_recente)
                    if val is not None:
                        try:
                            rows.append({
                                "cod_mun_ibge_7": cod_mun,
                                "ano": int(ano),
                                "populacao": int(float(str(val).replace(".", "").replace(",", ".") or 0)),
                            })
                        except (ValueError, TypeError):
                            rows.append({"cod_mun_ibge_7": cod_mun, "ano": int(ano), "populacao": None})
        return rows

    def salvar_bronze_municipios(self, df: pd.DataFrame) -> Path:
        """Salva lista de municípios na camada Bronze (parquet + csv)."""
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_parquet = self.bronze_dir / f"ibge_municipios_{ts}.parquet"
        path_csv = self.bronze_dir / f"ibge_municipios_{ts}.csv"
        df.to_parquet(path_parquet, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Bronze IBGE municípios salvo: %s", path_parquet)
        return path_parquet

    def salvar_bronze_populacao(self, df: pd.DataFrame, ano: int) -> Path:
        """Salva população por município na camada Bronze."""
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_parquet = self.bronze_dir / f"ibge_populacao_{ano}_{ts}.parquet"
        path_csv = self.bronze_dir / f"ibge_populacao_{ano}_{ts}.csv"
        df.to_parquet(path_parquet, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Bronze IBGE população salvo: %s", path_parquet)
        return path_parquet

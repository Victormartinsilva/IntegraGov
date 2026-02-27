"""
Fallback para dados DATASUS via PySUS (FTP).
Usado quando a API DEMAS retorna 404. Requer: pip install pysus
Retorna DataFrames com cod_mun_ibge_7, ano e totais (óbitos ou nascidos vivos).
"""
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_PYSUS_AVAILABLE = False
try:
    from pysus.online_data import SIM as pysus_sim
    from pysus.online_data import SINASC as pysus_sinasc
    _PYSUS_AVAILABLE = True
except ImportError:
    pass


def _read_parquet_or_df(obj):
    """Lê parquet: obj pode ser path (str/Path) ou objeto com .to_dataframe()."""
    if hasattr(obj, "to_dataframe"):
        return obj.to_dataframe()
    return pd.read_parquet(obj)


def _codigo_7(valor) -> Optional[str]:
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    s = str(int(valor)) if isinstance(valor, (int, float)) else str(valor).strip()
    s = "".join(c for c in s if c.isdigit())
    if len(s) == 6:
        return s.zfill(7)
    return s if len(s) == 7 else None


def sim_obitos_por_municipio_pysus(ano: int, ufs: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Baixa SIM (óbitos) via FTP (PySUS) e agrega por município.
    ufs: lista de siglas (ex: ['SP','RJ']). Se None, usa todas as UFs (pode ser lento).
    """
    if not _PYSUS_AVAILABLE:
        logger.warning("PySUS não instalado. Instale com: pip install pysus")
        return pd.DataFrame()

    if ufs is None:
        ufs = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
            "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
        ]
    try:
        files = pysus_sim.download(groups="CID10", states=ufs, years=ano)
        if not files:
            return pd.DataFrame()
        all_dfs = []
        for f in files:
            try:
                df = _read_parquet_or_df(f)
                # Coluna de município de residência no SIM: CODMUNRES
                col_mun = "CODMUNRES" if "CODMUNRES" in df.columns else "codmunres"
                if col_mun not in df.columns:
                    for c in ["CODMUNRES", "codmunres", "CODESTAB"]:
                        if c in df.columns:
                            col_mun = c
                            break
                    else:
                        continue
                df["cod_mun_ibge_7"] = df[col_mun].apply(_codigo_7)
                df = df[df["cod_mun_ibge_7"].notna()]
                agg = df.groupby("cod_mun_ibge_7", as_index=False).size().rename(columns={"size": "total_obitos"})
                agg["ano"] = ano
                all_dfs.append(agg)
            except Exception as e:
                logger.debug("Falha ao processar arquivo SIM %s: %s", getattr(f, "name", f), e)
        if not all_dfs:
            return pd.DataFrame()
        out = pd.concat(all_dfs, ignore_index=True)
        out = out.groupby(["cod_mun_ibge_7", "ano"], as_index=False)["total_obitos"].sum()
        logger.info("SIM (óbitos) via PySUS: %d municípios, ano %d.", len(out), ano)
        return out
    except Exception as e:
        logger.warning("Fallback PySUS SIM falhou: %s", e)
        return pd.DataFrame()


def sinasc_nascidos_por_municipio_pysus(ano: int, ufs: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Baixa Sinasc (nascidos vivos) via FTP (PySUS) e agrega por município.
    """
    if not _PYSUS_AVAILABLE:
        logger.warning("PySUS não instalado. Instale com: pip install pysus")
        return pd.DataFrame()

    if ufs is None:
        ufs = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
            "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
        ]
    try:
        files = pysus_sinasc.download(groups="DN", states=ufs, years=ano)
        if not files:
            return pd.DataFrame()
        all_dfs = []
        for f in files:
            try:
                df = _read_parquet_or_df(f)
                col_mun = "CODMUNRES" if "CODMUNRES" in df.columns else "codmunres"
                if col_mun not in df.columns:
                    for c in ["CODMUNRES", "codmunres", "CODMUNNASC"]:
                        if c in df.columns:
                            col_mun = c
                            break
                    else:
                        continue
                df["cod_mun_ibge_7"] = df[col_mun].apply(_codigo_7)
                df = df[df["cod_mun_ibge_7"].notna()]
                agg = df.groupby("cod_mun_ibge_7", as_index=False).size().rename(columns={"size": "nascidos_vivos"})
                agg["ano"] = ano
                all_dfs.append(agg)
            except Exception as e:
                logger.debug("Falha ao processar arquivo Sinasc %s: %s", getattr(f, "name", f), e)
        if not all_dfs:
            return pd.DataFrame()
        out = pd.concat(all_dfs, ignore_index=True)
        out = out.groupby(["cod_mun_ibge_7", "ano"], as_index=False)["nascidos_vivos"].sum()
        logger.info("Sinasc (nascidos vivos) via PySUS: %d municípios, ano %d.", len(out), ano)
        return out
    except Exception as e:
        logger.warning("Fallback PySUS Sinasc falhou: %s", e)
        return pd.DataFrame()

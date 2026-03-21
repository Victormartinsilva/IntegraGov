"""
Conector INEP - Censo Escolar (matrículas, docentes, escolas por município).
Camada Bronze: extração via Sinopse Estatística (XLSX) ou microdados (CSV).
"""
import io
import zipfile
import logging
import re
from pathlib import Path
from datetime import datetime
import pandas as pd
import requests

from config.settings import (
    INEP_SINOPSE_URL,
    INEP_MICRODADOS_CENSO_URL,
    BRONZE_DIR,
)

logger = logging.getLogger(__name__)

# Colunas possíveis nos arquivos INEP para código de município (6 ou 7 dígitos)
COLS_COD_MUN = [
    "CO_MUNICIPIO",
    "CO_MUNICÍPIO",
    "Cod_Municipio",
    "Código do Município",
    "cod_mun",
    "cod_mun_ibge_7",
    "ID_MUNICIPIO",
    "codigo_ibge",
]
# Colunas para matrículas / totais
COLS_MATRICULAS = ["QT_MAT_BAS", "Matrículas", "Total de Matrículas", "matriculas", "QT_MAT"]
COLS_DOCENTES = ["QT_DOC", "Docentes", "Total de Docentes", "docentes"]
COLS_ESCOLAS = ["QT_ESC", "Escolas", "Total de Escolas", "QT_ESTAB", "escolas"]


def _normalizar_codigo_municipio(serie: pd.Series) -> pd.Series:
    """Garante código com 7 dígitos (padrão IBGE)."""
    def _norm(val):
        if pd.isna(val):
            return None
        s = str(int(float(val))) if isinstance(val, (int, float)) else str(val).strip()
        s = "".join(c for c in s if c.isdigit())
        if len(s) == 6:
            return s.zfill(7)
        if len(s) == 7:
            return s
        return None
    return serie.apply(_norm)


class InepConnector:
    """Extrai dados do INEP (Censo Escolar): matrículas, docentes e escolas por município."""

    def __init__(self, bronze_dir: Path | None = None):
        self.bronze_dir = Path(bronze_dir or BRONZE_DIR)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def _baixar_zip(self, url: str) -> bytes:
        """Baixa arquivo ZIP da URL."""
        logger.info("Baixando %s ...", url)
        r = requests.get(url, timeout=120, stream=True)
        r.raise_for_status()
        return r.content

    def censo_escolar_matriculas_microdados(self, ano: int) -> pd.DataFrame:
        """
        Obtém totais de matrículas por município a partir dos microdados do Censo Escolar.
        Baixa o ZIP, extrai CSVs de matrícula e agrega por CO_MUNICIPIO.
        Estrutura típica: ZIP contém pasta com arquivos MATRICULA*.csv ou matricula*.csv.
        """
        url = INEP_MICRODADOS_CENSO_URL.format(ano=ano)
        try:
            content = self._baixar_zip(url)
        except requests.RequestException as e:
            logger.warning("Falha ao baixar microdados INEP %s: %s", ano, e)
            return pd.DataFrame()

        all_dfs = []
        with zipfile.ZipFile(io.BytesIO(content), "r") as z:
            for name in z.namelist():
                if not name.lower().endswith(".csv"):
                    continue
                if "matricula" not in name.lower():
                    continue
                for sep, enc in [(";", "utf-8"), (";", "latin-1"), (",", "utf-8")]:
                    try:
                        with z.open(name) as f:
                            df = pd.read_csv(f, sep=sep, encoding=enc, low_memory=False, nrows=0)
                        break
                    except Exception:
                        continue
                else:
                    continue
                col_mun = None
                for c in df.columns:
                    if "CO_MUNICIPIO" in str(c).upper().replace(" ", ""):
                        col_mun = c
                        break
                if not col_mun:
                    continue
                for sep, enc in [(";", "utf-8"), (";", "latin-1"), (",", "utf-8")]:
                    try:
                        with z.open(name) as f:
                            df = pd.read_csv(f, sep=sep, encoding=enc, usecols=[col_mun], low_memory=False)
                        break
                    except Exception:
                        continue
                else:
                    continue
                df = df.rename(columns={col_mun: "cod_mun_ibge_7"})
                df["cod_mun_ibge_7"] = _normalizar_codigo_municipio(df["cod_mun_ibge_7"])
                df = df.dropna(subset=["cod_mun_ibge_7"])
                df = df[df["cod_mun_ibge_7"].astype(str).str.len() == 7]
                agg = df.groupby("cod_mun_ibge_7", as_index=False).size().rename(columns={"size": "matriculas"})
                agg["ano"] = ano
                all_dfs.append(agg)
        if not all_dfs:
            logger.warning("Nenhum CSV de matrícula encontrado no ZIP INEP %s.", ano)
            return pd.DataFrame()
        out = pd.concat(all_dfs, ignore_index=True)
        out = out.groupby(["cod_mun_ibge_7", "ano"], as_index=False)["matriculas"].sum()
        logger.info("INEP microdados: %d municípios com matrículas (ano %s).", len(out), ano)
        return out

    def censo_escolar_sinopse(self, ano: int) -> pd.DataFrame:
        """
        Obtém totais por município a partir da Sinopse Estatística (ZIP com XLSX).
        Procura planilhas com código de município e colunas de matrículas/docentes/escolas.
        """
        url = INEP_SINOPSE_URL.format(ano=ano)
        try:
            content = self._baixar_zip(url)
        except requests.RequestException as e:
            logger.warning("Falha ao baixar sinopse INEP %s: %s", ano, e)
            return pd.DataFrame()

        try:
            import openpyxl  # noqa: F401
        except ImportError:
            logger.warning("openpyxl não instalado. Use: pip install openpyxl. Tentando microdados.")
            return self.censo_escolar_matriculas_microdados(ano)

        all_sheets = []
        with zipfile.ZipFile(io.BytesIO(content), "r") as z:
            for name in z.namelist():
                if not (name.lower().endswith(".xlsx") or name.lower().endswith(".xls")):
                    continue
                try:
                    with z.open(name) as f:
                        xl = pd.ExcelFile(f, engine="openpyxl")
                        for sheet in xl.sheet_names:
                            df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
                            if df.empty or len(df) < 2:
                                continue
                            col_cod = None
                            for cand in COLS_COD_MUN:
                                if cand in df.columns:
                                    col_cod = cand
                                    break
                            if col_cod is None:
                                for c in df.columns:
                                    cstr = str(c).strip()
                                    if re.search(r"munic[ií]pio|cod.*mun|co_mun|id_mun", cstr, re.I):
                                        if pd.api.types.is_numeric_dtype(df[c]) or df[c].astype(str).str.match(r"^\d{6,7}$", na=False).any():
                                            col_cod = c
                                            break
                            if col_cod is None:
                                continue
                            df = df.rename(columns={col_cod: "cod_mun_ibge_7"})
                            df["cod_mun_ibge_7"] = _normalizar_codigo_municipio(df["cod_mun_ibge_7"])
                            df = df.dropna(subset=["cod_mun_ibge_7"])
                            df = df[df["cod_mun_ibge_7"].str.len() == 7]
                            col_mat = None
                            for c in COLS_MATRICULAS:
                                if c in df.columns:
                                    col_mat = c
                                    break
                            if col_mat is None:
                                for c in df.columns:
                                    if "matr" in str(c).lower() and pd.api.types.is_numeric_dtype(df[c]):
                                        col_mat = c
                                        break
                            if col_mat:
                                agg = df[["cod_mun_ibge_7", col_mat]].copy()
                                agg = agg.rename(columns={col_mat: "matriculas"})
                                for c in COLS_DOCENTES:
                                    if c in df.columns:
                                        agg["docentes"] = df[c]
                                        break
                                for c in COLS_ESCOLAS:
                                    if c in df.columns:
                                        agg["escolas"] = df[c]
                                        break
                                agg["ano"] = ano
                                all_sheets.append(agg)
                except Exception as e:
                    logger.debug("Erro ao ler %s: %s", name, e)
                    continue

        if not all_sheets:
            logger.warning("Nenhuma planilha por município encontrada na sinopse INEP %s.", ano)
            return pd.DataFrame()
        out = pd.concat(all_sheets, ignore_index=True)
        # Agrupar por município (pode ter duplicatas de várias abas)
        group_cols = ["cod_mun_ibge_7", "ano"]
        agg_dict = {"matriculas": "sum"}
        if "docentes" in out.columns:
            agg_dict["docentes"] = "sum"
        if "escolas" in out.columns:
            agg_dict["escolas"] = "sum"
        out = out.groupby(group_cols, as_index=False).agg(agg_dict)
        logger.info("INEP sinopse: %d municípios (ano %s).", len(out), ano)
        return out

    def obter_matriculas_por_municipio(self, ano: int, usar_microdados: bool = False) -> pd.DataFrame:
        """
        Retorna DataFrame com cod_mun_ibge_7, ano, matriculas (e opcionalmente docentes, escolas).
        Por padrão tenta Sinopse (mais leve). Se usar_microdados=True, usa microdados (mais completo, download maior).
        """
        if usar_microdados:
            return self.censo_escolar_matriculas_microdados(ano)
        df = self.censo_escolar_sinopse(ano)
        if df.empty:
            logger.info("Sinopse vazia ou indisponível; tentando microdados.")
            df = self.censo_escolar_matriculas_microdados(ano)
        return df

    def salvar_bronze(self, df: pd.DataFrame, nome: str) -> Path:
        """Salva DataFrame na camada Bronze (parquet + csv)."""
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_parquet = self.bronze_dir / f"inep_{nome}_{ts}.parquet"
        path_csv = self.bronze_dir / f"inep_{nome}_{ts}.csv"
        df.to_parquet(path_parquet, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Bronze INEP salvo: %s", path_parquet)
        return path_parquet

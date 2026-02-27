"""
Camada Silver: limpeza, padronização e enriquecimento.
Padronização da chave de cruzamento: código de município IBGE 7 dígitos.
"""
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

from config.settings import BRONZE_DIR, SILVER_DIR
from src.db import get_connection, init_schema

logger = logging.getLogger(__name__)


def codigo_municipio_7_digitos(valor) -> str | None:
    """
    Garante código de município com 7 dígitos (padrão IBGE).
    Aceita int, str com 6 ou 7 dígitos.
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    s = str(int(valor)) if isinstance(valor, (int, float)) else str(valor).strip()
    s = "".join(c for c in s if c.isdigit())
    if len(s) == 6:
        return s.zfill(7)
    if len(s) == 7:
        return s
    return None


class SilverTransform:
    """Transformações Silver: Bronze -> Silver (e persistência em SQLite local)."""

    def __init__(self, bronze_dir: Path = BRONZE_DIR, silver_dir: Path = SILVER_DIR):
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.silver_dir.mkdir(parents=True, exist_ok=True)

    def padronizar_codigo_municipio(self, df: pd.DataFrame, coluna: str = "cod_mun_ibge_7") -> pd.DataFrame:
        """Aplica código município 7 dígitos na coluna informada (ou cria a partir de outras)."""
        df = df.copy()
        if coluna not in df.columns:
            # Tentar inferir de colunas comuns (codigo_ibge, id_municipio, CODMUN etc.)
            for c in ["codigo_ibge", "id_municipio", "CODMUN", "cod_mun", "codmun"]:
                if c in df.columns:
                    df[coluna] = df[c].apply(codigo_municipio_7_digitos)
                    break
            if coluna not in df.columns:
                logger.warning("Coluna de código município não encontrada; mantendo DataFrame inalterado.")
                return df
        else:
            df[coluna] = df[coluna].apply(codigo_municipio_7_digitos)
        # Remover linhas sem código válido
        df = df[df[coluna].notna() & (df[coluna].str.len() == 7)]
        return df

    def bronze_ibge_municipios_para_silver(self) -> pd.DataFrame:
        """Lê o último arquivo Bronze de municípios IBGE e retorna DataFrame Silver."""
        files = list(self.bronze_dir.glob("ibge_municipios_*.parquet"))
        if not files:
            logger.warning("Nenhum arquivo Bronze de municípios IBGE encontrado.")
            return pd.DataFrame()
        latest = max(files, key=lambda p: p.stat().st_mtime)
        df = pd.read_parquet(latest)
        df = self.padronizar_codigo_municipio(df)
        return df

    def bronze_ibge_populacao_para_silver(self, ano: int | None = None) -> pd.DataFrame:
        """Lê o último Bronze de população IBGE (opcionalmente filtrado por ano) e retorna Silver."""
        files = list(self.bronze_dir.glob("ibge_populacao_*.parquet"))
        if not files:
            logger.warning("Nenhum arquivo Bronze de população IBGE encontrado.")
            return pd.DataFrame()
        latest = max(files, key=lambda p: p.stat().st_mtime)
        df = pd.read_parquet(latest)
        df = self.padronizar_codigo_municipio(df)
        if ano is not None and "ano" in df.columns:
            df = df[df["ano"] == ano]
        return df

    def persistir_silver_no_banco(
        self,
        df_municipios: pd.DataFrame,
        df_populacao: pd.DataFrame,
        df_datasus: pd.DataFrame | None = None,
    ) -> None:
        """Persiste dados Silver no SQLite local (dim_municipio, silver_ibge_populacao, silver_datasus)."""
        with get_connection() as conn:
            init_schema(conn)
            data_carga = datetime.now().isoformat()

            if not df_municipios.empty:
                cols = ["cod_mun_ibge_7", "nome_municipio", "sigla_uf", "cod_uf"]
                available = [c for c in cols if c in df_municipios.columns]
                if "cod_uf" not in available:
                    df_municipios = df_municipios.copy()
                    df_municipios["cod_uf"] = None
                    available = ["cod_mun_ibge_7", "nome_municipio", "sigla_uf", "cod_uf"]
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO dim_municipio (cod_mun_ibge_7, nome_municipio, sigla_uf, cod_uf)
                    VALUES (?, ?, ?, ?)
                    """,
                    [tuple(row[c] for c in ["cod_mun_ibge_7", "nome_municipio", "sigla_uf", "cod_uf"]) for _, row in df_municipios.iterrows()],
                )
                logger.info("dim_municipio: %d registros.", len(df_municipios))

            if not df_populacao.empty:
                conn.executemany(
                    """
                    INSERT INTO silver_ibge_populacao (cod_mun_ibge_7, ano, populacao, data_carga)
                    VALUES (?, ?, ?, ?)
                    """,
                    [
                        (r.cod_mun_ibge_7, r.ano, r.populacao, data_carga)
                        for r in df_populacao[["cod_mun_ibge_7", "ano", "populacao"]].itertuples(index=False)
                    ],
                )
                logger.info("silver_ibge_populacao: %d registros.", len(df_populacao))

            if df_datasus is not None and not df_datasus.empty and "cod_mun_ibge_7" in df_datasus.columns:
                # Esperado: cod_mun_ibge_7, ano, mes (opc), indicador, valor, unidade
                cols = [c for c in ["cod_mun_ibge_7", "ano", "mes", "indicador", "valor", "unidade"] if c in df_datasus.columns]
                if cols:
                    for _, row in df_datasus.iterrows():
                        conn.execute(
                            """
                            INSERT INTO silver_datasus_indicadores (cod_mun_ibge_7, ano, mes, indicador, valor, unidade, data_carga)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                row.get("cod_mun_ibge_7"),
                                row.get("ano"),
                                row.get("mes"),
                                row.get("indicador", "raw"),
                                row.get("valor"),
                                row.get("unidade"),
                                data_carga,
                            ),
                        )
                    logger.info("silver_datasus_indicadores: %d registros.", len(df_datasus))

    def salvar_silver_parquet(self, df: pd.DataFrame, nome: str) -> Path:
        """Salva DataFrame Silver em parquet no diretório silver."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.silver_dir / f"silver_{nome}_{ts}.parquet"
        df.to_parquet(path, index=False)
        logger.info("Silver salvo: %s", path)
        return path

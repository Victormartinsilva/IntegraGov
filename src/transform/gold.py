"""
Camada Gold: dados agregados e modelados para análise e BI.
Indicadores de saúde por município (ex.: taxas por 100 mil habitantes).
"""
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

from config.settings import GOLD_DIR
from src.db import get_connection, init_schema

logger = logging.getLogger(__name__)


class GoldTransform:
    """Agregações e métricas para consumo analítico."""

    def __init__(self, gold_dir: Path = GOLD_DIR):
        self.gold_dir = Path(gold_dir)
        self.gold_dir.mkdir(parents=True, exist_ok=True)

    def indicadores_saude_por_municipio(
        self,
        df_populacao: pd.DataFrame,
        df_obitos: pd.DataFrame | None = None,
        df_internacoes: pd.DataFrame | None = None,
        df_nascidos: pd.DataFrame | None = None,
        ano: int | None = None,
    ) -> pd.DataFrame:
        """
        Calcula indicadores de saúde por município (ex.: taxas por 100k hab).
        Requer cod_mun_ibge_7 em todos os DataFrames.
        """
        if df_populacao.empty:
            return pd.DataFrame()

        pop = df_populacao.copy()
        if ano and "ano" in pop.columns:
            pop = pop[pop["ano"] == ano]
        pop = pop.groupby("cod_mun_ibge_7", as_index=False).agg({"populacao": "max", "ano": "first"})

        gold = pop.rename(columns={"ano": "ano"})

        if df_obitos is not None and not df_obitos.empty and "cod_mun_ibge_7" in df_obitos.columns:
            ob = df_obitos.copy()
            if ano and "ano" in ob.columns:
                ob = ob[ob["ano"] == ano]
            if "total_obitos" in ob.columns:
                ob_agg = ob.groupby("cod_mun_ibge_7", as_index=False)["total_obitos"].sum()
            else:
                ob_agg = ob.groupby("cod_mun_ibge_7", as_index=False).size().rename(columns={"size": "total_obitos"})
            gold = gold.merge(ob_agg, on="cod_mun_ibge_7", how="left")
            gold["total_obitos"] = gold["total_obitos"].fillna(0).astype(int)
            gold["taxa_obitos_100k"] = (gold["total_obitos"] / gold["populacao"] * 100_000).round(2)
        else:
            gold["total_obitos"] = 0
            gold["taxa_obitos_100k"] = 0.0

        if df_internacoes is not None and not df_internacoes.empty and "cod_mun_ibge_7" in df_internacoes.columns:
            ih = df_internacoes.copy()
            if ano and "ano" in ih.columns:
                ih = ih[ih["ano"] == ano]
            ih_agg = ih.groupby("cod_mun_ibge_7", as_index=False).size().rename(columns={"size": "total_internacoes"})
            gold = gold.merge(ih_agg, on="cod_mun_ibge_7", how="left")
            gold["total_internacoes"] = gold["total_internacoes"].fillna(0).astype(int)
            gold["taxa_internacao_100k"] = (gold["total_internacoes"] / gold["populacao"] * 100_000).round(2)
        else:
            gold["total_internacoes"] = 0
            gold["taxa_internacao_100k"] = 0.0

        if df_nascidos is not None and not df_nascidos.empty and "cod_mun_ibge_7" in df_nascidos.columns:
            nv = df_nascidos.copy()
            if ano and "ano" in nv.columns:
                nv = nv[nv["ano"] == ano]
            if "nascidos_vivos" in nv.columns:
                nv_agg = nv.groupby("cod_mun_ibge_7", as_index=False)["nascidos_vivos"].sum()
            else:
                nv_agg = nv.groupby("cod_mun_ibge_7", as_index=False).size().rename(columns={"size": "nascidos_vivos"})
            gold = gold.merge(nv_agg, on="cod_mun_ibge_7", how="left")
            gold["nascidos_vivos"] = gold["nascidos_vivos"].fillna(0).astype(int)
        else:
            gold["nascidos_vivos"] = 0

        gold["data_carga"] = datetime.now().isoformat()
        return gold

    def persistir_gold_no_banco(self, df: pd.DataFrame) -> None:
        """Insere/atualiza tabela gold_indicadores_saude_municipio."""
        if df.empty:
            return
        with get_connection() as conn:
            init_schema(conn)
            data_carga = datetime.now().isoformat()
            anos = df["ano"].dropna().unique().tolist()
            for a in anos:
                conn.execute("DELETE FROM gold_indicadores_saude_municipio WHERE ano = ?", (int(a),))
            for _, row in df.iterrows():
                conn.execute(
                    """
                    INSERT INTO gold_indicadores_saude_municipio
                    (cod_mun_ibge_7, ano, populacao, total_internacoes, total_obitos, nascidos_vivos,
                     taxa_internacao_100k, taxa_obitos_100k, data_carga)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["cod_mun_ibge_7"],
                        int(row["ano"]),
                        int(row["populacao"]),
                        int(row.get("total_internacoes", 0)),
                        int(row.get("total_obitos", 0)),
                        int(row.get("nascidos_vivos", 0)),
                        float(row.get("taxa_internacao_100k", 0)),
                        float(row.get("taxa_obitos_100k", 0)),
                        data_carga,
                    ),
                )
            logger.info("Gold: %d registros persistidos.", len(df))

    def indicadores_educacao_por_municipio(
        self,
        df_educacao: pd.DataFrame,
        df_populacao: pd.DataFrame | None = None,
        ano: int | None = None,
    ) -> pd.DataFrame:
        """
        Monta Gold de educação por município (matrículas, docentes, escolas).
        Se df_populacao for passado, calcula taxa de matrículas por 1000 hab.
        """
        if df_educacao.empty or "cod_mun_ibge_7" not in df_educacao.columns:
            return pd.DataFrame()
        gold = df_educacao.copy()
        if ano and "ano" in gold.columns:
            gold = gold[gold["ano"] == ano]
        agg_dict = {"matriculas": "sum"}
        if "docentes" in gold.columns:
            agg_dict["docentes"] = "sum"
        if "escolas" in gold.columns:
            agg_dict["escolas"] = "sum"
        gold = gold.groupby(["cod_mun_ibge_7", "ano"], as_index=False).agg(agg_dict)
        if "docentes" not in gold.columns:
            gold["docentes"] = None
        if "escolas" not in gold.columns:
            gold["escolas"] = None
        if df_populacao is not None and not df_populacao.empty and "populacao" in df_populacao.columns:
            pop = df_populacao.copy()
            if ano and "ano" in pop.columns:
                pop = pop[pop["ano"] == ano]
            pop = pop.groupby("cod_mun_ibge_7", as_index=False)["populacao"].max()
            gold = gold.merge(pop, on="cod_mun_ibge_7", how="left")
            gold["taxa_matriculas_por_1000_hab"] = (
                gold["matriculas"] / gold["populacao"].replace(0, pd.NA) * 1000
            ).round(2)
            gold = gold.drop(columns=["populacao"], errors="ignore")
        else:
            gold["taxa_matriculas_por_1000_hab"] = None
        gold["data_carga"] = datetime.now().isoformat()
        return gold

    def persistir_gold_educacao_no_banco(self, df: pd.DataFrame) -> None:
        """Insere/atualiza tabela gold_indicadores_educacao_municipio (remove dados do ano antes)."""
        if df.empty:
            return
        with get_connection() as conn:
            init_schema(conn)
            data_carga = datetime.now().isoformat()
            anos = df["ano"].dropna().unique().tolist()
            for a in anos:
                conn.execute("DELETE FROM gold_indicadores_educacao_municipio WHERE ano = ?", (int(a),))
            for _, row in df.iterrows():
                conn.execute(
                    """
                    INSERT INTO gold_indicadores_educacao_municipio
                    (cod_mun_ibge_7, ano, matriculas, docentes, escolas, taxa_matriculas_por_1000_hab, data_carga)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["cod_mun_ibge_7"],
                        int(row["ano"]),
                        int(row.get("matriculas", 0) or 0),
                        int(row.get("docentes", 0) or 0) if pd.notna(row.get("docentes")) else None,
                        int(row.get("escolas", 0) or 0) if pd.notna(row.get("escolas")) else None,
                        float(row["taxa_matriculas_por_1000_hab"]) if pd.notna(row.get("taxa_matriculas_por_1000_hab")) else None,
                        data_carga,
                    ),
                )
            logger.info("Gold educação: %d registros persistidos.", len(df))

    def salvar_gold_parquet(self, df: pd.DataFrame, nome: str = "indicadores_saude_municipio") -> Path:
        """Salva Gold em parquet e CSV para exportação."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_pq = self.gold_dir / f"gold_{nome}_{ts}.parquet"
        path_csv = self.gold_dir / f"gold_{nome}_{ts}.csv"
        df.to_parquet(path_pq, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Gold salvo: %s", path_pq)
        return path_pq

    def indicadores_pib_por_municipio(
        self, df_pib: pd.DataFrame, ano: int | None = None
    ) -> pd.DataFrame:
        """
        Processa dados de PIB municipal (IBGE SIDRA 5938) para a camada Gold.
        Colunas esperadas: cod_mun_ibge_7, ano, pib_total_mil_reais, pib_per_capita.
        """
        if df_pib.empty or "cod_mun_ibge_7" not in df_pib.columns:
            return pd.DataFrame()
        gold = df_pib.copy()
        if ano and "ano" in gold.columns:
            gold = gold[gold["ano"] == ano]
        if "pib_total_mil_reais" not in gold.columns:
            gold["pib_total_mil_reais"] = None
        if "pib_per_capita" not in gold.columns:
            gold["pib_per_capita"] = None
        if "ano" not in gold.columns:
            gold["ano"] = ano
        gold = gold.groupby(["cod_mun_ibge_7", "ano"], as_index=False).agg(
            pib_total_mil_reais=("pib_total_mil_reais", "max"),
            pib_per_capita=("pib_per_capita", "max"),
        )
        gold["data_carga"] = datetime.now().isoformat()
        logger.info("Gold PIB: %d municípios.", len(gold))
        return gold

    def persistir_gold_pib_no_banco(self, df: pd.DataFrame) -> None:
        """Insere/atualiza tabela gold_pib_municipio."""
        if df.empty:
            return
        with get_connection() as conn:
            init_schema(conn)
            data_carga = datetime.now().isoformat()
            anos = df["ano"].dropna().unique().tolist()
            for a in anos:
                conn.execute("DELETE FROM gold_pib_municipio WHERE ano = ?", (int(a),))
            for _, row in df.iterrows():
                conn.execute(
                    """
                    INSERT INTO gold_pib_municipio
                    (cod_mun_ibge_7, ano, pib_total_mil_reais, pib_per_capita, data_carga)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        row["cod_mun_ibge_7"],
                        int(row["ano"]),
                        float(row["pib_total_mil_reais"]) if pd.notna(row.get("pib_total_mil_reais")) else None,
                        float(row["pib_per_capita"]) if pd.notna(row.get("pib_per_capita")) else None,
                        data_carga,
                    ),
                )
            logger.info("Gold PIB: %d registros persistidos.", len(df))

    def persistir_gold_transparencia_no_banco(self, df: pd.DataFrame) -> None:
        """Insere/atualiza tabela gold_transparencia_transferencias."""
        if df.empty or "cod_mun_ibge_7" not in df.columns:
            return
        with get_connection() as conn:
            init_schema(conn)
            data_carga = datetime.now().isoformat()
            anos = df["ano"].dropna().unique().tolist()
            for a in anos:
                conn.execute("DELETE FROM gold_transparencia_transferencias WHERE ano = ?", (int(a),))
            for _, row in df.iterrows():
                conn.execute(
                    """
                    INSERT INTO gold_transparencia_transferencias
                    (cod_mun_ibge_7, ano, total_transferencias_reais, data_carga)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        row["cod_mun_ibge_7"],
                        int(row["ano"]),
                        float(row.get("total_transferencias_reais", 0) or 0),
                        data_carga,
                    ),
                )
            logger.info("Gold Transparência: %d registros persistidos.", len(df))

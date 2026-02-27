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
            for _, row in df.iterrows():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO gold_indicadores_saude_municipio
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

    def salvar_gold_parquet(self, df: pd.DataFrame, nome: str = "indicadores_saude_municipio") -> Path:
        """Salva Gold em parquet e CSV para exportação."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_pq = self.gold_dir / f"gold_{nome}_{ts}.parquet"
        path_csv = self.gold_dir / f"gold_{nome}_{ts}.csv"
        df.to_parquet(path_pq, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Gold salvo: %s", path_pq)
        return path_pq

#!/usr/bin/env python3
"""
Pipeline principal do IntegraGov.
Coleta IBGE + PySUS/DATASUS + INEP + PIB + Transparência → Bronze → Silver → Gold.
"""
import sys
import logging
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import BRONZE_DIR, FASE_MVP
from src.connectors.ibge import IBGEConnector
from src.connectors.datasus import DatasusConnector
from src.connectors.inep import InepConnector
from src.connectors.transparencia import TransparenciaConnector
from src.connectors.cnes import CNESConnector
from src.transform.silver import SilverTransform
from src.transform.gold import GoldTransform
from src.db import ensure_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("integragov")


def run_fase1(
    ano: int = 2024,
    amostra_municipios: list[int] | None = None,
    todos_municipios: bool = False,
    amostra_n: int | None = None,
    incluir_pib: bool = False,
    incluir_transparencia: bool = False,
    incluir_cnes: bool = False,
):
    """
    Fase 1: MVP Saúde, Demografia, Educação.
    Opcional: --incluir-pib (IBGE SIDRA 5938), --incluir-transparencia (Portal da Transparência),
    --incluir-cnes (CNES infraestrutura de saúde).
    """
    logger.info(
        "Iniciando pipeline IntegraGov (ano=%s, todos_municipios=%s, pib=%s, transparencia=%s, cnes=%s)",
        ano, todos_municipios, incluir_pib, incluir_transparencia, incluir_cnes,
    )

    ensure_schema()

    # --- Bronze: IBGE municípios e população ---
    ibge = IBGEConnector(bronze_dir=BRONZE_DIR)
    df_mun = ibge.listar_municipios()
    if df_mun.empty:
        logger.error("Falha ao obter municípios do IBGE.")
        return
    ibge.salvar_bronze_municipios(df_mun)

    AMOSTRA_PADRAO = 100
    n_amostra = amostra_n if amostra_n is not None else AMOSTRA_PADRAO
    codigos = None
    if todos_municipios:
        codigos = None
    elif amostra_municipios is not None and len(amostra_municipios) > 0:
        codigos = [int(x) for x in amostra_municipios]
    elif len(df_mun) > n_amostra:
        por_uf = max(1, n_amostra // 27)
        df_amostra = df_mun.groupby("sigla_uf", group_keys=False).head(por_uf).head(n_amostra)
        codigos = df_amostra["cod_mun_ibge_7"].astype(int).tolist()
        logger.info(
            "Amostra: %d municípios (%d UFs). Use --todos-municipios para todos.",
            len(codigos), df_amostra["sigla_uf"].nunique(),
        )

    df_pop = ibge.obter_populacao_municipios(ano=ano, codigos_municipios=codigos)
    if df_pop.empty:
        logger.warning("Sem dados de população para %d; tentando %d.", ano, ano - 1)
        df_pop = ibge.obter_populacao_municipios(ano=ano - 1, codigos_municipios=codigos)
    if not df_pop.empty:
        ibge.salvar_bronze_populacao(df_pop, ano)

    # --- Bronze: DATASUS (PySUS) ---
    datasus = DatasusConnector(bronze_dir=BRONZE_DIR)
    df_obitos = datasus.sim_obitos(ano=ano)
    if not df_obitos.empty:
        datasus.salvar_bronze(df_obitos, "sim_obitos")
        logger.info("SIM (óbitos): %d municípios.", len(df_obitos))
    else:
        logger.info("SIM não disponível. Instale PySUS: pip install pysus")

    df_nascidos = datasus.sinasc_nascidos_vivos(ano=ano)
    if not df_nascidos.empty:
        datasus.salvar_bronze(df_nascidos, "sinasc_nascidos_vivos")
        logger.info("Sinasc (nascidos vivos): %d municípios.", len(df_nascidos))
    else:
        logger.info("Sinasc não disponível. Instale PySUS: pip install pysus")

    # --- Bronze: INEP Censo Escolar ---
    df_inep = pd.DataFrame()
    try:
        inep = InepConnector(bronze_dir=BRONZE_DIR)
        df_inep = inep.obter_matriculas_por_municipio(ano=ano, usar_microdados=False)
        if not df_inep.empty:
            inep.salvar_bronze(df_inep, "censo_escolar_matriculas")
            logger.info("INEP Censo Escolar: %d municípios.", len(df_inep))
        else:
            logger.info("INEP Censo Escolar sem dados para %d.", ano)
    except Exception as e:
        logger.warning("Falha INEP: %s", e)

    # --- Bronze: CNES (estabelecimentos de saúde) ---
    df_cnes = pd.DataFrame()
    if incluir_cnes:
        try:
            cnes = CNESConnector(bronze_dir=BRONZE_DIR)
            # Usa os mesmos códigos de município da amostra de população
            codigos_cnes = codigos if codigos else df_mun["cod_mun_ibge_7"].tolist()
            df_cnes = cnes.estabelecimentos_por_municipio(codigos_municipios=codigos_cnes, ano=ano)
            if not df_cnes.empty:
                cnes.salvar_bronze(df_cnes, ano)
                logger.info("CNES: %d municípios com dados de infraestrutura.", len(df_cnes))
            else:
                logger.info("CNES: API indisponível ou sem dados. Tente sem --incluir-cnes.")
        except Exception as e:
            logger.warning("Falha CNES: %s", e)

    # --- Bronze: PIB Municipal (IBGE SIDRA 5938) ---
    df_pib = pd.DataFrame()
    if incluir_pib:
        # PIB disponível com ~2-3 anos de defasagem; tenta ano informado, depois recua
        anos_pib = [ano - 2, ano - 3, ano - 1] if ano > 2021 else [ano]
        for ano_pib in anos_pib:
            try:
                df_pib = ibge.obter_pib_municipios(ano=ano_pib)
                if not df_pib.empty:
                    ibge.salvar_bronze_pib(df_pib, ano_pib)
                    logger.info("PIB municipal: %d municípios (ano %d).", len(df_pib), ano_pib)
                    break
            except Exception as e:
                logger.warning("PIB ano %d falhou: %s", ano_pib, e)
        if df_pib.empty:
            logger.warning("PIB municipal: sem dados disponíveis.")

    # --- Bronze: Portal da Transparência ---
    df_transf = pd.DataFrame()
    if incluir_transparencia:
        try:
            transp = TransparenciaConnector(bronze_dir=BRONZE_DIR)
            if not transp.configurado:
                logger.warning(
                    "Portal da Transparência: TRANSPARENCIA_API_KEY não configurada. "
                    "Obtenha em portaldatransparencia.gov.br/api-de-dados/cadastrar-email"
                )
            else:
                df_transf_raw = transp.transferencias_constitucionais(ano=ano)
                if not df_transf_raw.empty:
                    transp.salvar_bronze(df_transf_raw, "transferencias_constitucionais")
                    df_transf = transp.agregar_transferencias_por_municipio(df_transf_raw)
                    logger.info("Transferências constitucionais: %d municípios.", len(df_transf))
        except Exception as e:
            logger.warning("Falha Portal da Transparência: %s", e)

    # --- Silver: padronização ---
    silver = SilverTransform()
    df_mun_s = silver.bronze_ibge_municipios_para_silver()
    df_pop_s = silver.bronze_ibge_populacao_para_silver(ano=ano)
    if df_pop_s.empty and not df_pop.empty:
        df_pop_s = silver.padronizar_codigo_municipio(df_pop.copy())

    df_obitos_s = pd.DataFrame()
    if not df_obitos.empty:
        for c in ["CODMUN", "codigo_ibge", "cod_mun_ibge_7", "codmun"]:
            if c in df_obitos.columns:
                df_obitos = df_obitos.rename(columns={c: "cod_mun_ibge_7"})
                break
        df_obitos_s = silver.padronizar_codigo_municipio(df_obitos)

    df_nascidos_s = pd.DataFrame()
    if not df_nascidos.empty:
        for c in ["CODMUN", "codigo_ibge", "cod_mun_ibge_7", "codmun"]:
            if c in df_nascidos.columns:
                df_nascidos = df_nascidos.rename(columns={c: "cod_mun_ibge_7"})
                break
        df_nascidos_s = silver.padronizar_codigo_municipio(df_nascidos)

    silver.persistir_silver_no_banco(df_mun_s, df_pop_s, None, df_inep=df_inep if not df_inep.empty else None)
    silver.salvar_silver_parquet(df_mun_s, "municipios")
    if not df_pop_s.empty:
        silver.salvar_silver_parquet(df_pop_s, "populacao")

    # --- Gold: saúde ---
    gold_t = GoldTransform()
    df_gold = gold_t.indicadores_saude_por_municipio(
        df_populacao=df_pop_s,
        df_obitos=df_obitos_s if not df_obitos_s.empty else None,
        df_nascidos=df_nascidos_s if not df_nascidos_s.empty else None,
        ano=ano,
    )
    if not df_gold.empty:
        gold_t.persistir_gold_no_banco(df_gold)
        gold_t.salvar_gold_parquet(df_gold)
        logger.info("Gold saúde: %d municípios.", len(df_gold))

    # --- Gold: educação (INEP) ---
    if not df_inep.empty:
        df_gold_edu = gold_t.indicadores_educacao_por_municipio(
            df_educacao=df_inep,
            df_populacao=df_pop_s if not df_pop_s.empty else None,
            ano=ano,
        )
        if not df_gold_edu.empty:
            gold_t.persistir_gold_educacao_no_banco(df_gold_edu)
            gold_t.salvar_gold_parquet(df_gold_edu, "indicadores_educacao_municipio")
            logger.info("Gold educação: %d municípios.", len(df_gold_edu))

    # --- Gold: PIB Municipal ---
    if not df_pib.empty:
        df_gold_pib = gold_t.indicadores_pib_por_municipio(df_pib)
        if not df_gold_pib.empty:
            gold_t.persistir_gold_pib_no_banco(df_gold_pib)
            gold_t.salvar_gold_parquet(df_gold_pib, "pib_municipio")
            logger.info("Gold PIB: %d municípios.", len(df_gold_pib))

    # --- Gold: CNES ---
    if not df_cnes.empty:
        gold_t.persistir_gold_cnes_no_banco(df_cnes)
        gold_t.salvar_gold_parquet(df_cnes, "cnes_municipio")
        logger.info("Gold CNES: %d municípios.", len(df_cnes))

    # --- Gold: Transparência ---
    if not df_transf.empty:
        gold_t.persistir_gold_transparencia_no_banco(df_transf)
        gold_t.salvar_gold_parquet(df_transf, "transparencia_transferencias")
        logger.info("Gold transferências: %d municípios.", len(df_transf))

    logger.info("Pipeline concluído.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="IntegraGov – Pipeline de dados públicos")
    parser.add_argument("--ano", type=int, default=2024, help="Ano de referência (padrão: 2024)")
    parser.add_argument("--amostra", type=int, default=None, metavar="N", help="Número de municípios (padrão: 100)")
    parser.add_argument("--todos-municipios", action="store_true", help="Consultar todos os municípios")
    parser.add_argument("--incluir-pib", action="store_true", help="Incluir PIB municipal (IBGE SIDRA 5938)")
    parser.add_argument("--incluir-transparencia", action="store_true", help="Incluir transferências federais (Portal da Transparência, requer TRANSPARENCIA_API_KEY)")
    parser.add_argument("--incluir-cnes", action="store_true", help="Incluir infraestrutura de saúde (CNES) — requer API apidadosabertos.saude.gov.br")
    args = parser.parse_args()
    run_fase1(
        ano=args.ano,
        todos_municipios=args.todos_municipios,
        amostra_n=args.amostra,
        incluir_pib=args.incluir_pib,
        incluir_transparencia=args.incluir_transparencia,
        incluir_cnes=args.incluir_cnes,
    )

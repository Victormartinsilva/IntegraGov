"""
Conector CNES - Cadastro Nacional de Estabelecimentos de Saúde.
Fonte: apidadosabertos.saude.gov.br/v1/cnes
Infraestrutura de saúde por município: estabelecimentos, hospitais, UBS, leitos.
Não requer PySUS nem compilação C.
"""
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

from config.settings import BRONZE_DIR

logger = logging.getLogger(__name__)

CNES_API_BASE = "https://apidadosabertos.saude.gov.br/v1"


class CNESConnector:
    """Extrai dados do CNES (estabelecimentos e leitos de saúde) por município."""

    def __init__(self, bronze_dir: Path = BRONZE_DIR):
        self.bronze_dir = Path(bronze_dir)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, endpoint: str, params: dict | None = None, timeout: int = 30) -> list | dict | None:
        url = f"{CNES_API_BASE.rstrip('/')}/{endpoint.lstrip('/')}"
        try:
            r = self.session.get(url, params=params or {}, timeout=timeout)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.warning("CNES API error (%s): %s", url, e)
            return None

    def estabelecimentos_por_municipio(
        self,
        codigos_municipios: list[str],
        ano: int,
    ) -> pd.DataFrame:
        """
        Busca contagem de estabelecimentos de saúde ativos por município.
        codigos_municipios: lista de códigos IBGE 7 dígitos.
        Retorna DataFrame com colunas: cod_mun_ibge_7, ano, total_estabelecimentos,
        hospitais, ubs, leitos_totais, leitos_sus.
        """
        rows = []
        api_disponivel = True

        for i, cod7 in enumerate(codigos_municipios):
            if not api_disponivel:
                break
            estabelecimentos = []
            pagina = 1
            while True:
                data = self._get("cnes/estabelecimentos", params={
                    "co_municipio": str(cod7)[:7],
                    "st_ativo_in": "S",
                    "pagina": pagina,
                    "quantidade": 100,
                })
                if data is None:
                    logger.info("CNES API indisponível (404). Abortando.")
                    api_disponivel = False
                    break
                items = (
                    data if isinstance(data, list)
                    else data.get("estabelecimentos", data.get("data", data.get("items", [])))
                )
                if not items:
                    break
                estabelecimentos.extend(items)
                if len(items) < 100:
                    break
                pagina += 1

            if not api_disponivel:
                break

            if not estabelecimentos:
                continue

            df_est = pd.DataFrame(estabelecimentos)
            total = len(df_est)
            hospitais = 0
            ubs = 0
            leitos_totais = 0
            leitos_sus = 0

            tipo_col = next(
                (c for c in ["co_tipo_estabelecimento", "tp_estabelecimento", "ds_tipo_estabelecimento"] if c in df_est.columns),
                None,
            )
            if tipo_col:
                tipo_str = df_est[tipo_col].astype(str).str.upper()
                hospitais = int(tipo_str.str.contains(r"^05$|HOSPITAL", regex=True, na=False).sum())
                ubs = int(tipo_str.str.contains(r"^01$|^02$|BASICA|UBS", regex=True, na=False).sum())

            for leito_col, target in [("qt_leito", "leitos_totais"), ("qt_leito_sus", "leitos_sus")]:
                if leito_col in df_est.columns:
                    val = int(pd.to_numeric(df_est[leito_col], errors="coerce").fillna(0).sum())
                    if target == "leitos_totais":
                        leitos_totais = val
                    else:
                        leitos_sus = val

            rows.append({
                "cod_mun_ibge_7": str(cod7).zfill(7),
                "ano": ano,
                "total_estabelecimentos": total,
                "hospitais": hospitais,
                "ubs": ubs,
                "leitos_totais": leitos_totais,
                "leitos_sus": leitos_sus,
            })

            if i > 0 and i % 20 == 0:
                logger.info("CNES: %d/%d municípios processados.", i + 1, len(codigos_municipios))

        if not rows:
            return pd.DataFrame()

        logger.info("CNES: %d municípios com dados.", len(rows))
        return pd.DataFrame(rows)

    def salvar_bronze(self, df: pd.DataFrame, ano: int) -> Path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_pq = self.bronze_dir / f"cnes_estabelecimentos_{ano}_{ts}.parquet"
        path_csv = self.bronze_dir / f"cnes_estabelecimentos_{ano}_{ts}.csv"
        df.to_parquet(path_pq, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Bronze CNES salvo: %s", path_pq)
        return path_pq

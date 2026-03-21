"""
Conector Portal da Transparência - API de dados federais abertos.
Requer chave de API gratuita: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email

Configure via variável de ambiente: TRANSPARENCIA_API_KEY=<sua-chave>
"""
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

from config.settings import TRANSPARENCIA_API_BASE, TRANSPARENCIA_API_KEY, BRONZE_DIR

logger = logging.getLogger(__name__)


class TransparenciaConnector:
    """Extrai dados do Portal da Transparência (gastos, transferências constitucionais)."""

    def __init__(self, bronze_dir: Path = BRONZE_DIR, api_key: str = TRANSPARENCIA_API_KEY):
        self.bronze_dir = Path(bronze_dir)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = api_key
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({
                "chave-api": self.api_key,
                "Accept": "application/json",
            })

    @property
    def configurado(self) -> bool:
        """True se a chave de API está configurada."""
        return bool(self.api_key)

    def _get(self, endpoint: str, params: dict | None = None) -> list | dict:
        """Requisição GET à API."""
        if not self.configurado:
            raise RuntimeError(
                "Chave da API do Portal da Transparência não configurada. "
                "Obtenha gratuitamente em portaldatransparencia.gov.br/api-de-dados/cadastrar-email "
                "e configure: export TRANSPARENCIA_API_KEY=<sua-chave>"
            )
        url = f"{TRANSPARENCIA_API_BASE.rstrip('/')}/{endpoint.lstrip('/')}"
        try:
            r = self.session.get(url, params=params or {}, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            logger.error("Erro HTTP %s ao acessar %s: %s", e.response.status_code if e.response else "?", url, e)
            raise
        except requests.RequestException as e:
            logger.exception("Erro de rede ao acessar %s: %s", url, e)
            raise

    def transferencias_constitucionais(
        self, ano: int, paginas: int = 10
    ) -> pd.DataFrame:
        """
        Busca transferências constitucionais (FPM, ICMS, ITR, FUNDEB etc.) por município.
        Agrega o total por município/ano.
        """
        registros = []
        for pagina in range(1, paginas + 1):
            try:
                data = self._get(
                    "transferencias-constitucionais",
                    params={"ano": ano, "pagina": pagina, "quantidade": 500},
                )
                if not data:
                    break
                items = data if isinstance(data, list) else data.get("data", data.get("items", []))
                if not items:
                    break
                registros.extend(items)
                logger.debug("Página %d: %d registros.", pagina, len(items))
            except Exception as e:
                logger.warning("Falha na página %d: %s", pagina, e)
                break

        if not registros:
            logger.info("Transferências constitucionais: sem registros para ano %d.", ano)
            return pd.DataFrame()

        df = pd.DataFrame(registros)
        # Normaliza colunas comuns da API
        rename = {
            "codigoMunicipio": "cod_ibge_6",
            "nomeMunicipio": "nome_municipio",
            "siglaUf": "sigla_uf",
            "anoReferencia": "ano",
            "mesReferencia": "mes",
            "nomeTipoTransferencia": "tipo_transferencia",
            "valor": "valor",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        # Código IBGE 6 → 7 dígitos (acrescenta dígito verificador aproximado com zero)
        if "cod_ibge_6" in df.columns:
            df["cod_mun_ibge_7"] = df["cod_ibge_6"].astype(str).str.zfill(7)
        if "valor" in df.columns:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df["ano"] = ano
        logger.info("Transferências constitucionais: %d registros para ano %d.", len(df), ano)
        return df

    def agregar_transferencias_por_municipio(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrega transferências por município e ano."""
        if df.empty or "cod_mun_ibge_7" not in df.columns or "valor" not in df.columns:
            return pd.DataFrame()
        agg = df.groupby(["cod_mun_ibge_7", "ano"], as_index=False)["valor"].sum()
        agg = agg.rename(columns={"valor": "total_transferencias_reais"})
        return agg

    def salvar_bronze(self, df: pd.DataFrame, nome: str) -> Path:
        """Salva DataFrame na camada Bronze."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe = nome.replace(" ", "_").lower()
        path_pq = self.bronze_dir / f"transparencia_{safe}_{ts}.parquet"
        path_csv = self.bronze_dir / f"transparencia_{safe}_{ts}.csv"
        df.to_parquet(path_pq, index=False)
        df.to_csv(path_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Bronze Transparência salvo: %s", path_pq)
        return path_pq

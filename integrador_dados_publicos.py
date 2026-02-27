"""
Script legado de demonstração.
Para o pipeline completo (Data Lakehouse Bronze/Silver/Gold), use:
    python run_pipeline.py --ano 2024
"""
import requests
import pandas as pd


def get_ibge_data(municipio_id):
    """Obtém dados de população do IBGE para um município específico."""
    # Exemplo: População estimada para um município em um ano específico (2021)
    # A API do IBGE é complexa, este é um exemplo simplificado para demonstração
    # URL de exemplo para população de um município (ID 3550308 - São Paulo) em 2021
    url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2021/variaveis/9324?localidades=N6[{municipio_id}]"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Levanta um erro para códigos de status HTTP ruins
        data = response.json()
        if data and data[0]['resultados']:
            populacao = data[0]['resultados'][0]['series'][0]['serie']['2021']
            return int(populacao)
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter dados do IBGE para o município {municipio_id}: {e}")
    return None

def get_transparencia_data(municipio_nome):
    """Obtém dados de convênios do Portal da Transparência para um município específico."""
    # Exemplo: Convênios de um município (simplificado, a API real é mais complexa e exige autenticação)
    # Para fins de demonstração, vamos simular alguns dados
    data = {
        "São Paulo": [{"convenio_id": "123", "valor": 1000000, "objeto": "Construção de Escola"}],
        "Rio de Janeiro": [{"convenio_id": "456", "valor": 500000, "objeto": "Reforma de Praça"}],
        "Belo Horizonte": [{"convenio_id": "789", "valor": 750000, "objeto": "Saneamento Básico"}],
    }
    return data.get(municipio_nome, [])

def main():
    print("Iniciando integração de dados públicos...")

    # 1. Dados de Referência de Municípios (Exemplo simplificado)
    # Em um sistema real, isso viria de uma base padronizada do IBGE
    municipios_ref = pd.DataFrame({
        'municipio_id_ibge': [3550308, 3304557, 3106200],
        'municipio_nome': ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte']
    })
    print("Dados de referência de municípios carregados.")

    # 2. Coleta e Padronização de Dados do IBGE
    ibge_data = []
    for index, row in municipios_ref.iterrows():
        populacao = get_ibge_data(row['municipio_id_ibge'])
        if populacao is not None:
            ibge_data.append({
                'municipio_id_ibge': row['municipio_id_ibge'],
                'municipio_nome': row['municipio_nome'],
                'populacao_2021': populacao
            })
    df_ibge = pd.DataFrame(ibge_data)
    print("Dados do IBGE coletados e padronizados.")

    # 3. Coleta e Padronização de Dados do Portal da Transparência
    transparencia_data = []
    for index, row in municipios_ref.iterrows():
        convenios = get_transparencia_data(row['municipio_nome'])
        for convenio in convenios:
            transparencia_data.append({
                'municipio_nome': row['municipio_nome'],
                'convenio_id': convenio['convenio_id'],
                'valor_convenio': convenio['valor'],
                'objeto_convenio': convenio['objeto']
            })
    df_transparencia = pd.DataFrame(transparencia_data)
    print("Dados do Portal da Transparência coletados e padronizados.")

    # 4. Cruzamento das Bases
    # Usando 'municipio_nome' como chave para simplificar, mas 'municipio_id_ibge' seria o ideal
    # Em um cenário real, garantir que 'municipio_nome' seja único ou usar o ID do IBGE para o merge
    df_integrado = pd.merge(
        df_ibge,
        df_transparencia,
        on='municipio_nome',
        how='left'
    )
    print("Bases de dados cruzadas com sucesso.")

    # 5. Exibição e Salvamento do Resultado
    print("\nDados Integrados (amostra):\n")
    print(df_integrado.head())

    output_path = "/home/ubuntu/dados_publicos_integrados.csv"
    df_integrado.to_csv(output_path, index=False)
    print(f"\nDados integrados salvos em: {output_path}")

if __name__ == "__main__":
    main()

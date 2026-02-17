# ==========================================
# PIPELINE BRONZE → SILVER
# Execução via: ▶ Run Python File (VSCode)
# ==========================================

import json
import pandas as pd
import os
from datetime import datetime


def carregar_json(caminho_arquivo):
    """Carrega arquivo JSON bruto (Bronze)"""
    try:
        with open(caminho_arquivo, "r", encoding="utf-8") as file:
            dados = json.load(file)
        print(" JSON carregado com sucesso")
        return dados
    except Exception as erro:
        print(" Erro ao carregar JSON:", erro)
        return None


def criar_dataframe(dados):
    """Converte JSON em DataFrame"""
    if isinstance(dados, dict) and "issues" in dados:
        df = pd.json_normalize(dados["issues"])
    elif isinstance(dados, list):
        df = pd.json_normalize(dados)
    else:
        df = pd.json_normalize(dados)

    print(f" DataFrame criado | Linhas: {df.shape[0]} | Colunas: {df.shape[1]}")
    return df


def salvar_parquet(df, caminho_saida):
    """Salva DataFrame em Parquet (Silver)"""
    try:
        df.to_parquet(caminho_saida, index=False)
        print("✅ Arquivo salvo na camada Silver")
        print("📁 Local:", caminho_saida)
    except Exception as erro:
        print("❌ Erro ao salvar Parquet:", erro)


def main():

    print("🚀 Iniciando pipeline Bronze → Silver")
    print("⏱", datetime.now())

    # Caminhos
    caminho_bronze = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Bronze\jira_issues_raw.json"

    pasta_silver = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Silver"

    os.makedirs(pasta_silver, exist_ok=True)

    caminho_silver = os.path.join(pasta_silver, "jira_issues_silver.parquet")

    # Processo
    dados = carregar_json(caminho_bronze)

    if dados is not None:
        df = criar_dataframe(dados)

        # Cópia Bronze → Silver
        dataframe_saida = df.copy()

        salvar_parquet(dataframe_saida, caminho_silver)

    print(" Pipeline finalizado")


# Execução principal
if __name__ == "__main__":
    main()

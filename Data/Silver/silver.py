# ==========================================
# PIPELINE SILVER → GOLD
# ==========================================
import pandas as pd
from tabulate import tabulate
import os  # Import necessário para manipulação de pastas e caminhos

def main():
    arquivo_parquet = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Silver\jira_issues_silver.parquet"
    
    # Leitura do Parquet
    df = pd.read_parquet(arquivo_parquet)
    print("\n Arquivo Parquet carregado com sucesso!\n")
    
    # -------------------------------
    # Mostrar primeiras 10 linhas do DataFrame original
    # -------------------------------
    print("\n Primeiras 10 linhas do DataFrame original:")
    print(tabulate(df.head(10), headers='keys', tablefmt='grid', showindex=False))
    
    # -------------------------------
    # Curácia de dados: tratar coluna 'assignee'
    # Ajustar ordem das colunas, remover duplicados e ordenar por id
    # -------------------------------
    df_dados_equipe = df.explode('assignee')                                                                          # explode a lista 
    df_dados_equipe['assignee'] = df_dados_equipe['assignee'].apply(lambda x: x if isinstance(x, dict) else {})       # evita erro --> invalidos com {}
    df_dados_equipe = pd.json_normalize(df_dados_equipe['assignee'])                                                  # transforme em tabular
    df_dados_equipe = df_dados_equipe[['id', 'name', 'email']].drop_duplicates().sort_values('id').reset_index(drop=True)  # ordenação e limpeza
    
    print("\n Primeiras 10 linhas do DataFrame 'df_dados_equipe':")
    print(tabulate(df_dados_equipe.head(10), headers='keys', tablefmt='grid', showindex=False))
    
    # -------------------------------
    # Nova acurácia de dados: criar df_SLA
    # -------------------------------
    df_sla_temp = df.explode('assignee')                                                                                  # explode a lista
    df_sla_temp['assignee_id'] = df_sla_temp['assignee'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)    # extrai o assignee-id, se inválido retorna NaT
    df_SLA = df_sla_temp[['id', 'issue_type', 'status', 'priority', 'timestamps', 'assignee_id']]                         # deleciona as colunas
    
    print("\n Primeiras 10 linhas do DataFrame 'df_SLA':")
    print(tabulate(df_SLA.head(10), headers='keys', tablefmt='grid', showindex=False))
    
    # -------------------------------
    # Criar df_timestamps
    # Normalizar os dicionários da coluna timestamps
    # Criar df_timestamps final apenas com as colunas desejadas
    # Converter para datetime (datas de solução e de criação)
    # Ordenar as colunas e remover duplicados
    # -------------------------------
    df_timestamps_temp = df_SLA.explode('timestamps')                                                                                # explode a lista
    df_timestamps_temp['timestamps'] = df_timestamps_temp['timestamps'].apply(lambda x: x if isinstance(x, dict) else {})            # evita erro --> invalidos com {}
    df_timestamps_dict = pd.json_normalize(df_timestamps_temp['timestamps'])                                                         # transforme em tabular
    df_timestamps = pd.concat([df_timestamps_temp[['id', 'assignee_id']], df_timestamps_dict[['created_at','resolved_at']]], axis=1) #concatena as chaves
    df_timestamps['created_at'] = pd.to_datetime(df_timestamps['created_at'], errors='coerce', utc=True)                             # converte datetime --> errors='coerce' --> se data inválida converte para NaT
    df_timestamps['resolved_at'] = pd.to_datetime(df_timestamps['resolved_at'], errors='coerce', utc=True)                           # converte datetime --> errors='coerce' --> se data inválida converte para NaT
    df_timestamps = df_timestamps.sort_values(by=['id', 'assignee_id', 'created_at', 'resolved_at']).drop_duplicates().reset_index(drop=True) # ordenação e limpeza
    
    print("\n Primeiras 10 linhas do DataFrame 'df_timestamps' (com datetime):")
    print(tabulate(df_timestamps.head(10), headers='keys', tablefmt='grid', showindex=False))

    # ----------------------------------
    # Ajustes no Dataframe df_SLA.
    # Apenas depois df_timestamps pronto 
    # Remover a coluna 'timestamps'
    # Ordenar e remover duplicados
    # ----------------------------------
    df_SLA = df_SLA.drop(columns=['timestamps'])                                                                       #remove coluna timestamp (desnecessária)
    colunas_ordenadas = ['assignee_id', 'id'] + [col for col in df_SLA.columns if col not in ['assignee_id', 'id']]    # ordena as colunas
    df_SLA = df_SLA[colunas_ordenadas]
    df_SLA = df_SLA.sort_values(by=['assignee_id', 'id']).drop_duplicates().reset_index(drop=True)                     # ordenação e limpeza
    
    print("\n Primeiras 10 linhas do DataFrame 'df_SLA' (após remoção a coluna 'timestamps'):")
    print(tabulate(df_SLA.head(10), headers='keys', tablefmt='grid', showindex=False))
    
    # -------------------------------
    # Criar pasta Gold se não existir
    # -------------------------------
    pasta_gold = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Gold"
    os.makedirs(pasta_gold, exist_ok=True)
    
    # -------------------------------
    # Salvar DataFrames em Parquet
    # -------------------------------
    arquivo_timestamps = os.path.join(pasta_gold, "jira_issues_timestamp_gold.parquet")
    df_timestamps.to_parquet(arquivo_timestamps, index=False)
    
    arquivo_SLA = os.path.join(pasta_gold, "jira_issues_SLA_gold.parquet")
    df_SLA.to_parquet(arquivo_SLA, index=False)
    
    arquivo_equipe = os.path.join(pasta_gold, "jira_issues_equipe_gold.parquet")
    df_dados_equipe.to_parquet(arquivo_equipe, index=False)
    
    print("\n DataFrames salvos em Parquet na camada Gold com sucesso!")

if __name__ == "__main__":
    main()

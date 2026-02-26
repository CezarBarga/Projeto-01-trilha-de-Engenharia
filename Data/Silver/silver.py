# ==========================================
# PIPELINE BRONZE → SILVER
# ==========================================
import pandas as pd
from tabulate import tabulate
import os  # Import required for folder and path handling
from datetime import datetime  # Import required for timestamp logging


def main():

    # ------------------------------------------
    # Start ingestion pipeline to Silver layer
    # ------------------------------------------
    print("==========================================")
    print("INGESTION PIPELINE → SILVER")
    print("Start time:", datetime.now())
    print("==========================================")

    # Define Bronze input file (raw structured data)
    arquivo_parquet = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Bronze\jira_issues_raw.parquet"

    # Read Parquet file from Bronze layer
    df = pd.read_parquet(arquivo_parquet)
    # print("\n Parquet file successfully loaded from Bronze layer!\n")

    # -------------------------------
    # Data quality: process 'assignee' column
    # -------------------------------
    df_dados_equipe = df.explode('assignee')
    df_dados_equipe['assignee'] = df_dados_equipe['assignee'].apply(
        lambda x: x if isinstance(x, dict) else {}
    )

    df_dados_equipe = pd.json_normalize(df_dados_equipe['assignee'])
    df_dados_equipe = (
        df_dados_equipe[['id', 'name', 'email']]
        .drop_duplicates()
        .sort_values('id')
        .reset_index(drop=True)
    )

    # -------------------------------
    # Create df_SLA dataset
    # -------------------------------
    df_sla_temp = df.explode('assignee')
    df_sla_temp['assignee_id'] = df_sla_temp['assignee'].apply(
        lambda x: x.get('id') if isinstance(x, dict) else None
    )

    df_SLA = df_sla_temp[['id', 'issue_type', 'status', 'priority', 'timestamps', 'assignee_id']]

    # -------------------------------
    # Create df_timestamps dataset
    # -------------------------------
    df_timestamps_temp = df_SLA.explode('timestamps')
    df_timestamps_temp['timestamps'] = df_timestamps_temp['timestamps'].apply(
        lambda x: x if isinstance(x, dict) else {}
    )

    df_timestamps_dict = pd.json_normalize(df_timestamps_temp['timestamps'])

    df_timestamps = pd.concat(
        [
            df_timestamps_temp[['id', 'assignee_id']],
            df_timestamps_dict[['created_at', 'resolved_at']]
        ],
        axis=1
    )

    df_timestamps['created_at'] = pd.to_datetime(
        df_timestamps['created_at'], errors='coerce', utc=True
    )

    df_timestamps['resolved_at'] = pd.to_datetime(
        df_timestamps['resolved_at'], errors='coerce', utc=True
    )

    df_timestamps = (
        df_timestamps
        .sort_values(by=['id', 'assignee_id', 'created_at', 'resolved_at'])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # ----------------------------------
    # Adjust df_SLA after timestamps processing
    # ----------------------------------
    df_SLA = df_SLA.drop(columns=['timestamps'])

    colunas_ordenadas = ['assignee_id', 'id'] + [
        col for col in df_SLA.columns if col not in ['assignee_id', 'id']
    ]

    df_SLA = df_SLA[colunas_ordenadas]

    df_SLA = (
        df_SLA
        .sort_values(by=['assignee_id', 'id'])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # -------------------------------
    # Create Silver folder if it does not exist
    # -------------------------------
    pasta_silver = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Silver"
    os.makedirs(pasta_silver, exist_ok=True)

    # -------------------------------
    # Save DataFrames in Silver layer
    # -------------------------------
    arquivo_timestamps = os.path.join(pasta_silver, "jira_issues_timestamp_silver.parquet")
    df_timestamps.to_parquet(arquivo_timestamps, index=False)

    arquivo_SLA = os.path.join(pasta_silver, "jira_issues_SLA_silver.parquet")
    df_SLA.to_parquet(arquivo_SLA, index=False)

    arquivo_equipe = os.path.join(pasta_silver, "jira_issues_equipe_silver.parquet")
    df_dados_equipe.to_parquet(arquivo_equipe, index=False)

    print("\n DataFrames successfully saved in Silver layer!")    


if __name__ == "__main__":
    main()
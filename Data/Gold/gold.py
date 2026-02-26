# gold_process.py

import pandas as pd
import os
from tabulate import tabulate
from datetime import datetime
from sla_calculation import calcular_intervalo_uteis_em_horas


def main():

    # ------------------------------------------
    # Start ingestion pipeline to Gold layer
    # ------------------------------------------
    print("==========================================")
    print("INGESTION PIPELINE → GOLD")
    print("Start time:", datetime.now())
    print("==========================================")

    # ------------------------------------------------------------
    # Read Parquet files from Silver layer
    # ------------------------------------------------------------
    pasta_silver = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Silver"

    df_dados_equipe = pd.read_parquet(os.path.join(pasta_silver, "jira_issues_equipe_silver.parquet"))
    df_SLA = pd.read_parquet(os.path.join(pasta_silver, "jira_issues_SLA_silver.parquet"))
    df_timestamps = pd.read_parquet(os.path.join(pasta_silver, "jira_issues_timestamp_silver.parquet"))

    print("\n Silver files successfully loaded!\n")

    # ------------------------------------------------------------
    # SLA configuration list
    # ------------------------------------------------------------
    priority_sla = [
        {"priority": "High", "expected_sla": 24.0},
        {"priority": "Medium", "expected_sla": 72.0},
        {"priority": "Low", "expected_sla": 120.0}
    ]

    priority_sla_dict = {item["priority"]: item["expected_sla"] for item in priority_sla}

    # ------------------------------------------------------------
    # Final fact table structure
    # ------------------------------------------------------------
    df_tabela_final = pd.DataFrame(columns=[
        "id",
        "issue_type",
        "assignee_name",
        "priority",
        "created_at",
        "resolved_at",
        "resolution_hours",
        "sla_expected_hours",
        "sla_met_flag"
    ])

    df_timestamps = df_timestamps.sort_values(by=["id", "assignee_id"]).reset_index(drop=True)

    # ------------------------------------------------------------
    # Load final fact table
    # ------------------------------------------------------------
    for index, row in df_timestamps.iterrows():

        aux_total_hours = 0.0

        issue_id = row["id"]
        assignee_id = row["assignee_id"]
        created_at = row["created_at"]
        resolved_at = row["resolved_at"]

        sla_row = df_SLA[df_SLA["id"] == issue_id]

        if sla_row.empty:
            continue

        status = sla_row.iloc[0]["status"]
        issue_type = sla_row.iloc[0]["issue_type"]
        priority = sla_row.iloc[0]["priority"]

        if status != "Open" and pd.notnull(created_at) and pd.notnull(resolved_at):

            horas_uteis = calcular_intervalo_uteis_em_horas(created_at, resolved_at)
            aux_total_hours = aux_total_hours + horas_uteis

        sla_expected = priority_sla_dict.get(priority, 0.0)

        equipe_row = df_dados_equipe[df_dados_equipe["id"] == assignee_id]
        assignee_name = equipe_row.iloc[0]["name"] if not equipe_row.empty else None

        sla_flag = aux_total_hours <= sla_expected

        df_tabela_final.loc[len(df_tabela_final)] = [
            issue_id,
            issue_type,
            assignee_name,
            priority,
            created_at,
            resolved_at,
            aux_total_hours,
            sla_expected,
            sla_flag
        ]

    # ------------------------------------------------------------
    # Aggregate SLA by assignee
    # ------------------------------------------------------------
    df_tabela_final_temp = df_tabela_final.sort_values(
        by="assignee_name"
    ).reset_index(drop=True)

    df_sla_assignee = pd.DataFrame(columns=[
        "assignee_name",
        "total_id",
        "hour_avarage"
    ])

    first_time = True
    current_assignee_name = ""
    aux_sum = 0.0
    aux_count = 0

    for index, row in df_tabela_final_temp.iterrows():

        if first_time:
            first_time = False
            current_assignee_name = row["assignee_name"]
            aux_sum = 0.0
            aux_count = 0

        if current_assignee_name == row["assignee_name"]:
            aux_sum += row["resolution_hours"]
            aux_count += 1
        else:
            aux_avarage = aux_sum / aux_count

            df_sla_assignee.loc[len(df_sla_assignee)] = [
                current_assignee_name,
                aux_count,
                aux_avarage
            ]

            current_assignee_name = row["assignee_name"]
            aux_sum = row["resolution_hours"]
            aux_count = 1

    if aux_count > 0:
        aux_avarage = aux_sum / aux_count
        df_sla_assignee.loc[len(df_sla_assignee)] = [
            current_assignee_name,
            aux_count,
            aux_avarage
        ]

    # ------------------------------------------------------------
    # Aggregate SLA by issue type
    # ------------------------------------------------------------
    df_tabela_final_temp2 = df_tabela_final.sort_values(
        by="issue_type"
    ).reset_index(drop=True)

    df_sla_id = pd.DataFrame(columns=[
        "issue_type",
        "total_id",
        "hour_avarage"
    ])

    first_time = True
    current_issue_type = ""
    aux_sum = 0.0
    aux_count = 0

    for index, row in df_tabela_final_temp2.iterrows():

        if first_time:
            first_time = False
            current_issue_type = row["issue_type"]
            aux_sum = 0.0
            aux_count = 0

        if current_issue_type == row["issue_type"]:
            aux_sum += row["resolution_hours"]
            aux_count += 1
        else:
            aux_avarage = aux_sum / aux_count

            df_sla_id.loc[len(df_sla_id)] = [
                current_issue_type,
                aux_count,
                aux_avarage
            ]

            current_issue_type = row["issue_type"]
            aux_sum = row["resolution_hours"]
            aux_count = 1

    if aux_count > 0:
        aux_avarage = aux_sum / aux_count
        df_sla_id.loc[len(df_sla_id)] = [
            current_issue_type,
            aux_count,
            aux_avarage
        ]

   # ------------------------------------------------------------
    # Results
    # ------------------------------------------------------------
    # print("\n df_sla_assignee:\n")
    # print(tabulate(df_sla_assignee, headers="keys", tablefmt="grid", showindex=False))

    # print("\n df_sla_id:\n")
    # print(tabulate(df_sla_id, headers="keys", tablefmt="grid", showindex=False))

    # print("\n 10 primeiras linhas de df_tabela_final:\n")
    # print(tabulate(df_tabela_final.head(10), headers="keys", tablefmt="grid", showindex=False))

    # ------------------------------------------------------------
    # Undone SLAs
    # ------------------------------------------------------------
    # print("\n SLAs NÃO cumpridos (sla_met_flag = False):\n")

    # df_sla_nao_cumprido = df_tabela_final[
    #    df_tabela_final["sla_met_flag"] == False
    #]

    #if not df_sla_nao_cumprido.empty:
    #    print(tabulate(
    #        df_sla_nao_cumprido,
    #        headers="keys",
    #        tablefmt="grid",
    #        showindex=False
    #    ))
    #else:
    #    print("Nenhum SLA foi violado!")


    # ------------------------------------------------------------
    # Save output files in Gold layer
    # ------------------------------------------------------------
    pasta_gold = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Gold"
    os.makedirs(pasta_gold, exist_ok=True)

    caminho_parquet = os.path.join(pasta_gold, "jira_solution_final_table_gold.parquet")
    df_tabela_final.to_parquet(caminho_parquet, index=False)

    caminho_csv_assignee = os.path.join(pasta_gold, "jira_solution_report_assignee_gold.csv")
    df_sla_assignee.to_csv(caminho_csv_assignee, index=False, sep=";")

    caminho_csv_id = os.path.join(pasta_gold, "jira_solution_report_id_gold.csv")
    df_sla_id.to_csv(caminho_csv_id, index=False, sep=";")

    print("\n Gold files successfully generated!\n")


if __name__ == "__main__":
    main()
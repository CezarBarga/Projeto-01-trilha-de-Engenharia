# gold_process.py

import pandas as pd
import os
from tabulate import tabulate

def main():


    # ------------------------------------------------------------
    # Leitura dos arquivos Parquet
    # ------------------------------------------------------------
    pasta_gold = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Gold"
    df_dados_equipe = pd.read_parquet(os.path.join(pasta_gold, "jira_issues_equipe_gold.parquet"))
    df_SLA = pd.read_parquet(os.path.join(pasta_gold, "jira_issues_SLA_gold.parquet"))
    df_timestamps = pd.read_parquet(os.path.join(pasta_gold, "jira_issues_timestamp_gold.parquet"))

    print("\n Arquivos Gold carregados com sucesso!\n")

    # ------------------------------------------------------------
    # Variável auxiliar
    # ------------------------------------------------------------
    aux_total_hours = 0.0

    # ------------------------------------------------------------
    # Lista de SLA por prioridade
    # ------------------------------------------------------------
    priority_sla = [
        {"priority": "High", "expected_sla": 24.0},
        {"priority": "Medium", "expected_sla": 72.0},
        {"priority": "Low", "expected_sla": 120.0}
    ]

    priority_sla_dict = {item["priority"]: item["expected_sla"] for item in priority_sla}

    # ------------------------------------------------------------
    # Criar DataFrame final vazio
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

    # ------------------------------------------------------------
    # Ordenar df_timestamps
    # ------------------------------------------------------------
    df_timestamps = df_timestamps.sort_values(
        by=["id", "assignee_id"]
    ).reset_index(drop=True)

    # ------------------------------------------------------------
    # Loop principal
    # ------------------------------------------------------------
    for index, row in df_timestamps.iterrows():

        aux_total_hours = 0.0
        aux_status_sla = None

        issue_id = row["id"]
        assignee_id = row["assignee_id"]
        created_at = row["created_at"]
        resolved_at = row["resolved_at"]

        sla_row = df_SLA[df_SLA["id"] == issue_id]

        if sla_row.empty:
            continue

        aux_status_sla = sla_row.iloc[0]["status"]
        issue_type = sla_row.iloc[0]["issue_type"]
        priority = sla_row.iloc[0]["priority"]

        # Calcular horas somente se não estiver Open
        if aux_status_sla != "Open" and pd.notnull(resolved_at) and pd.notnull(created_at):
            diff = resolved_at - created_at
            aux_total_hours = diff.total_seconds() / 3600.0

        # Buscar SLA esperado
        sla_expected = priority_sla_dict.get(priority, 0.0)

        # Buscar nome do analista
        equipe_row = df_dados_equipe[df_dados_equipe["id"] == assignee_id]
        assignee_name = equipe_row.iloc[0]["name"] if not equipe_row.empty else None

        # Verificar SLA
        sla_flag = aux_total_hours <= sla_expected

        # Inserir linha no DataFrame final
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
    # Exibir apenas as 10 primeiras linhas
    # ------------------------------------------------------------
    print("\n df_tabela_final (10 primeiras linhas):\n")

    print(tabulate(
        df_tabela_final.head(10),
        headers="keys",
        tablefmt="grid",
        showindex=False
    ))

    # ------------------------------------------------------------
    # Salvar DataFrame final na camada Gold
    # ------------------------------------------------------------
    arquivo_saida = os.path.join(
        pasta_gold,
        "jira_solution_tabela_final_gold.parquet"
    )

    df_tabela_final.to_parquet(arquivo_saida, index=False)

    print("\n Arquivo Parquet gerado com sucesso:")
    print(arquivo_saida)


if __name__ == "__main__":
    main()

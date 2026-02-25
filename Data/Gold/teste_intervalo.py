# teste de Intervalo
from datetime import datetime
from sla_calculation import calcular_intervalo_uteis_em_horas


def executar_teste(descricao, data_inicio, data_fim):
    print("\n--------------------------------------------------")
    print(f"Teste: {descricao}")
    print(f"Data início : {data_inicio}")
    print(f"Data fim    : {data_fim}")

    resultado = calcular_intervalo_uteis_em_horas(data_inicio, data_fim)

    print(f"Horas úteis calculadas: {resultado}")
    print("--------------------------------------------------")


def main():

    # 1️⃣ Data início nula
    executar_teste(
        "Data início nula",
        None,
        datetime(2025, 2, 10, 10, 0)
    )

    # 2️⃣ Data fim nula
    executar_teste(
        "Data fim nula",
        datetime(2025, 2, 10, 10, 0),
        None
    )

    # 3️⃣ Início às 19h e fim às 9h do dia seguinte
    executar_teste(
        "Início às 19h e fim às 9h",
        datetime(2025, 2, 10, 19, 0),
        datetime(2025, 2, 11, 9, 0)
    )


if __name__ == "__main__":
    main()
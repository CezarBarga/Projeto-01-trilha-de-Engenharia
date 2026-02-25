# sla_calculation.py

from datetime import datetime, timedelta
import pandas as pd


def calcular_intervalo_uteis_em_horas(data_inicio: datetime, data_fim: datetime) -> float:
    
    # Calcula a quantidade de horas úteis (segunda a sexta) entre duas datas.

    # Regras:
    # - Se data_inicio ou data_fim forem nulas → retorna 0
    # - Se data_fim < data_inicio → retorna 0
    # - Ignora sábado e domingo
    # - Considera 24h por dia útil    

    # ------------------------------------------------------------
    # Validação de datas nulas (None ou NaT)
    # ------------------------------------------------------------
    if data_inicio is None or data_fim is None:
        return 0.0

    if pd.isna(data_inicio) or pd.isna(data_fim):
        return 0.0

    # ------------------------------------------------------------
    # Converter para datetime pandas (garante padronização)
    # ------------------------------------------------------------
    data_inicio = pd.to_datetime(data_inicio)
    data_fim = pd.to_datetime(data_fim)

    # ------------------------------------------------------------
    # Remover timezone (evita erro offset-naive vs offset-aware)
    # ------------------------------------------------------------
    if data_inicio.tzinfo is not None:
        data_inicio = data_inicio.tz_localize(None)

    if data_fim.tzinfo is not None:
        data_fim = data_fim.tz_localize(None)

    # ------------------------------------------------------------
    # Verificar ordem das datas
    # ------------------------------------------------------------
    if data_fim <= data_inicio:
        return 0.0

    # ------------------------------------------------------------
    # Loop dia a dia somando apenas dias úteis
    # ------------------------------------------------------------
    total_horas = 0.0
    atual = data_inicio

    while atual < data_fim:

        # Próximo dia
        proximo_dia = (atual + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # Limite do intervalo (não ultrapassa data_fim)
        limite = min(proximo_dia, data_fim)

        # Se for dia útil (0=segunda ... 4=sexta)
        if atual.weekday() < 5:
            diferenca = limite - atual
            total_horas += diferenca.total_seconds() / 3600.0

        atual = limite

    return round(total_horas, 2)
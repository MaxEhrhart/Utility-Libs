from math import log


def juros_composto(capital: float, taxa: float, tempo: float) -> float:
    montante = capital * (1 + taxa) ** tempo
    return float(montante)


def tempo_para_atingir_valor(montante: float, capital: float, taxa: float) -> float:
    tempo = (log(montante, 10) - log(capital, 10)) / log(1 + taxa, 10)
    return float(tempo)

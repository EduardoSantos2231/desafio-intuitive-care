import pandas as pd

def formatar_moeda_br(valor) -> str:
    if pd.isna(valor) or valor == "":
        return ""
    sinal = "-" if valor < 0 else ""
    valor_abs = abs(valor)
    return sinal + f"{valor_abs:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

from __future__ import annotations

import pandas as pd
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pandas import DataFrame

logger = logging.getLogger(__name__)


class CadopCleaner:
    """
    Cleans and deduplicates the healthcare providers registry (CADOP).
    """
    def clean(self, df_cadop: DataFrame) -> DataFrame:
        """
        Returns a DataFrame with:
        - One record per (CNPJ).
        - The most recent Company Name (Razão Social) based on ANS registration date.
        """        
        required_cols = ["REGISTRO_OPERADORA", "CNPJ", "Razao_Social", "Data_Registro_ANS"]
        df: DataFrame = df_cadop[required_cols].copy()

        # Converte data e remove inválidas
        df["Data_Registro_ANS"] = pd.to_datetime(
            df["Data_Registro_ANS"], format="mixed", errors="coerce"
        )
        df = df.dropna(subset=["Data_Registro_ANS"])

        # Ordena por data (mais recente primeiro)
        df = df.sort_values("Data_Registro_ANS", ascending=False)

        # Deduplica por CNPJ (não por REGISTRO_OPERADORA!)
        df_unique = df.drop_duplicates(subset=["CNPJ"], keep="first")

        logger.info(f"Cleaned CADOP: {len(df_unique)} unique operators by CNPJ")    
        return df_unique.set_index("REGISTRO_OPERADORA")

import pandas as pd
import logging

logger = logging.getLogger(__name__)


class ExpenseCalculator:
    """Calculates net expenses and consolidates by period."""
    def calculate_and_consolidate(
        self,
        df_contabil: pd.DataFrame,
        df_cadop_clean: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Steps:
        1. Merges accounting data with CADOP registry.
        2. Calculates expense = VL_SALDO_FINAL - VL_SALDO_INICIAL.
        3. Groups by Tax ID (CNPJ) + Period and sums values.
        """

        df_merged = self._merge_data(df_contabil, df_cadop_clean)

        df_with_expenses = self._calculate_expenses(df_merged)

        df_consolidated = self._consolidate_by_period(df_with_expenses)

        return df_consolidated

    def _merge_data(self, df_contabil: pd.DataFrame, df_cadop: pd.DataFrame) -> pd.DataFrame:
        """Realiza join e trata operadoras não encontradas."""
        df_contabil = df_contabil.copy()
        df_contabil["REG_ANS"] = df_contabil["REG_ANS"].astype(str).str.strip()
        df_cadop.index = df_cadop.index.astype(str).str.strip()

        merged = df_contabil.merge(
            df_cadop,
            left_on="REG_ANS",
            right_index=True,
            how="left"
        )

        missing = merged["CNPJ"].isna()
        if  missing.any():
            logger.warning(f"Operadoras not found: {missing.sum()}")
            merged.loc[missing, "CNPJ"] = "00.000.000/0000-00"
            merged.loc[missing, "Razao_Social"] = "NAO_ENCONTRADO"

        return merged

    def _calculate_expenses(self, df: pd.DataFrame): 
        """Calcula despesa líquida por registro."""
        df = df.copy()
        df["ValorDespesas"] = df["VL_SALDO_FINAL"] - df["VL_SALDO_INICIAL"]

        valid = df["ValorDespesas"].notna()
        logger.info(f"valid registers found: {valid.sum()}/{len(df)}")
        return df[valid].copy()

    def _consolidate_by_period(self, df: pd.DataFrame):
        """
        Agrupa por CNPJ + RazaoSocial + Ano + Trimestre e soma despesas.
        Garante que cada CNPJ tenha uma única Razão Social por período.
        """
        grouped = df.groupby(
            ["CNPJ", "Razao_Social", "Ano", "Trimestre"],
            as_index=False,
            dropna=False
        ).agg({
            "ValorDespesas": "sum"
        })

        result = grouped.rename(columns={"Razao_Social": "RazaoSocial"})
        result = result[["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]]
        
        logger.info(f" Total {len(result)} aggregated registers")
        return result

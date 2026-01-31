import pandas as pd
import logging

logger = logging.getLogger(__name__)


class AccountingProcessor:
    @staticmethod
    def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["DATA"] = pd.to_datetime(df["DATA"], format="mixed", errors="coerce")
        invalid = df["DATA"].isna().sum()
        if invalid:
            logger.warning(f"Invalid data formats was found: {invalid}")
            df = df.dropna(subset=["DATA"])
        return df

    @staticmethod
    def extract_period(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["Ano"] = df["DATA"].dt.year
        df["Trimestre"] = df["DATA"].dt.quarter
        return df

    @staticmethod
    def ensure_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in ["VL_SALDO_INICIAL", "VL_SALDO_FINAL"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

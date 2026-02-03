import re
import pandas as pd
from pathlib import Path
import logging
from src.utils.formatting import formatar_moeda_br
logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validates and enriches the consolidated expense dataset with quality flags.
    
    Adds three new columns:
    - RegistroCNPJValido: bool (True if CNPJ is valid)
    - RazaoSocial: str (ensures no empty values)
    - DespesaPositiva: bool (True if ValorDespesas > 0)
    """

    def validate_and_enrich(self, input_file: Path, output_file: Path) -> pd.DataFrame:
        logger.info("Starting data validation and enrichment...")

        df = pd.read_csv(
            input_file,
            sep=";",
            encoding="utf-8-sig",
            thousands=".",
            decimal=","
        )

        df = self._validate_cnpj(df)
        df = self._validate_razao_social(df)
        df = self._validate_despesa(df)  

        from src.utils.formatting import formatar_moeda_br
        df["ValorDespesas"] = df["ValorDespesas"].apply(formatar_moeda_br)

        # ✅ ESCRITA
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(
            output_file,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        # Log summary
        total = len(df)
        valid_cnpjs = df["RegistroCNPJValido"].sum()
        valid_names = (df["RazaoSocial"] != "NAO_ENCONTRADA").sum()
        positive_expenses = df["DespesaPositiva"].sum()

        logger.info(
            f"Validation completed:\n"
            f"- Total records: {total:,}\n"
            f"- Valid CNPJs: {valid_cnpjs:,} ({valid_cnpjs/total:.1%})\n"
            f"- Valid company names: {valid_names:,} ({valid_names/total:.1%})\n"
            f"- Positive expenses: {positive_expenses:,} ({positive_expenses/total:.1%})"
        )

        return df


    def _validate_cnpj(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds 'RegistroCNPJValido' column based on CNPJ validation."""
        df = df.copy()
        df["RegistroCNPJValido"] = df["CNPJ"].apply(self._is_valid_cnpj)
        return df

    def _validate_razao_social(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensures 'RazaoSocial' is never empty."""
        df = df.copy()
        mask_empty = (
            df["RazaoSocial"].isna() |
            (df["RazaoSocial"].astype(str).str.strip() == "") |
            (df["RazaoSocial"] == "NAO_ENCONTRADO")
        )
        df.loc[mask_empty, "RazaoSocial"] = "NAO_ENCONTRADA"
        return df

    def _validate_despesa(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds 'DespesaPositiva' flag (True if ValorDespesas > 0)."""
        df = df.copy()
        df["DespesaPositiva"] = df["ValorDespesas"] > 0
        return df

    @staticmethod
    def _is_valid_cnpj(cnpj: str) -> bool:
        """
        Validates CNPJ using official algorithm (digits + check digits).
        Accepts formatted (XX.XXX.XXX/XXXX-XX) or raw (XXXXXXXXXXXXXX) input.
        """
        if not isinstance(cnpj, str):
            return False

        # Remove non-digits
        cnpj = re.sub(r"[^0-9]", "", cnpj)

        # Must be 14 digits
        if len(cnpj) != 14:
            return False

        # Reject known invalid patterns
        if cnpj == cnpj[0] * 14:
            return False

        # Validate check digits
        def _calc_digit(number: str, weights: list[int]) -> int:
            total = sum(int(d) * w for d, w in zip(number, weights))
            remainder = total % 11
            return 0 if remainder < 2 else 11 - remainder

        # First check digit
        weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit1 = _calc_digit(cnpj[:12], weights1)

        # Second check digit
        weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit2 = _calc_digit(cnpj[:13], weights2)

        return digit1 == int(cnpj[12]) and digit2 == int(cnpj[13])

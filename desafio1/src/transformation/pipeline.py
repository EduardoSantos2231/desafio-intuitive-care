import pandas as pd
from pathlib import Path
import logging

from .cadop_cleaner import CadopCleaner
from .accounting_transformer import AccountingProcessor
from .expense_calculator import ExpenseCalculator
from .output_manager import OutputManager

logger = logging.getLogger(__name__)


class ExpenseConsolidationPipeline:
    
    def _apply_brazilian_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica formatação brasileira às colunas numéricas monetárias.
        Converte valores float para string no formato BR (ex: 1351.00 → "1.351,00")
        """
        df = df.copy()
        
        if "ValorDespesas" in df.columns:
            df["ValorDespesas"] = pd.to_numeric(df["ValorDespesas"], errors="coerce")
            
            df["ValorDespesas"] = df["ValorDespesas"].apply(
                lambda x: (
                    f"{abs(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    if pd.notna(x) else ""
                )
            ).apply(lambda x: f"-{x}" if x.startswith("-") else x)  
        return df

    def run(
        self,
        accounting_file: Path,
        cadop_file: Path,
        output_file: Path
    ) -> None:
        """Execute the complete consolidation pipeline."""
        logger.info("Starting consolidation...")

        # Load data
        df_contabil = pd.read_csv(
            accounting_file,
            sep=";",
            encoding="utf-8-sig",
            dtype=str
        )
        df_cadop = pd.read_csv(
            cadop_file,
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
            on_bad_lines="skip",
            skip_blank_lines=True
        )
        
        logger.info(f"Data loaded: {len(df_contabil)} accounting, {len(df_cadop)} CADOP")

        cadop_cleaner = CadopCleaner()
        df_cadop_clean = cadop_cleaner.clean(df_cadop)

        
        acc_processor = AccountingProcessor()
        df_contabil = acc_processor.parse_dates(df_contabil)
        df_contabil = acc_processor.extract_period(df_contabil)
        df_contabil = acc_processor.ensure_numeric_columns(df_contabil)

        
        calculator = ExpenseCalculator()
        df_final = calculator.calculate_and_consolidate(df_contabil, df_cadop_clean)

        df_final = self._apply_brazilian_formatting(df_final)

        output_file.parent.mkdir(exist_ok=True)
        output_manager = OutputManager(output_file)
        output_manager.export_to_csv(df_final)
        output_manager.compress_to_zip()

        logger.info("Consolidation completed successfully!")

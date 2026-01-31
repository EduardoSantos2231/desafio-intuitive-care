# src/transformation/pipeline.py
import pandas as pd
from pathlib import Path
import logging

from .cadop_cleaner import CadopCleaner
from .accounting_transformer import AccountingProcessor
from .expense_calculator import ExpenseCalculator
from .output_manager import OutputManager

logger = logging.getLogger(__name__)


class ExpenseConsolidationPipeline:
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

        # Clean CADOP
        cadop_cleaner = CadopCleaner()
        df_cadop_clean = cadop_cleaner.clean(df_cadop)

        # Process accounting data
        acc_processor = AccountingProcessor()
        df_contabil = acc_processor.parse_dates(df_contabil)
        df_contabil = acc_processor.extract_period(df_contabil)
        df_contabil = acc_processor.ensure_numeric_columns(df_contabil)

        # Calculate and consolidate
        calculator = ExpenseCalculator()
        df_final = calculator.calculate_and_consolidate(df_contabil, df_cadop_clean)

        # Output
        output_file.parent.mkdir(exist_ok=True)
        output_manager = OutputManager(output_file)
        output_manager.export_to_csv(df_final)
        output_manager.compress_to_zip()

        logger.info("Consolidation completed successfully!")

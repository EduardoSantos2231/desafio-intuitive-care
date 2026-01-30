import pandas as pd
import logging
from pathlib import Path
import zipfile

logger = logging.getLogger(__name__)


class CadopMapper:
    """Maps and cleans operator registry (CADOP) data."""

    def clean_and_map(self, df_cadop: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the CADOP DataFrame and resolves duplicates.
        
        Returns:
            A lookup DataFrame indexed by 'REGISTRO_OPERADORA' with unique entries,
            prioritizing the most recent registration date.
        """
        # Select only required columns
        df = df_cadop[['REGISTRO_OPERADORA', 'CNPJ', 'Razao_Social', 'Data_Registro_ANS']].copy()
        
        # Convert registration date to datetime
        df['Data_Registro_ANS'] = pd.to_datetime(
            df['Data_Registro_ANS'],
            format='mixed',
            errors='coerce'
        )
        
        # Drop rows with invalid dates
        df = df.dropna(subset=['Data_Registro_ANS'])
        
        # Sort by registration date (most recent first)
        df = df.sort_values('Data_Registro_ANS', ascending=False)
        
        # Keep only the first (most recent) entry per operator ID
        df = df.drop_duplicates(subset=['REGISTRO_OPERADORA'], keep='first')
        
        # Set index for efficient lookup
        df = df.set_index('REGISTRO_OPERADORA')
        
        logger.info(f"CADOP MAPPING: {len(df)} unique operators after cleaning")
        return df


class TemporalTransformer:
    """Transforms temporal fields in accounting data."""

    @staticmethod
    def parse_dates(df_contabil: pd.DataFrame) -> pd.DataFrame:
        """Converts 'DATA' column to datetime, supporting YYYY-MM-DD and DD/MM/YYYY formats."""
        df = df_contabil.copy()
        df['DATA'] = pd.to_datetime(df['DATA'], format='mixed', errors='coerce')
        
        invalid_count = df['DATA'].isna().sum()
        if invalid_count > 0:
            logger.warning(f"INVALID DATES: {invalid_count} records dropped")
            df = df.dropna(subset=['DATA'])
            
        return df

    @staticmethod
    def extract_period(df: pd.DataFrame) -> pd.DataFrame:
        """Extracts 'Ano' (Year) and 'Trimestre' (Quarter) from the 'DATA' column."""
        df = df.copy()
        df['Ano'] = df['DATA'].dt.year
        df['Trimestre'] = df['DATA'].dt.quarter
        return df



class ExpenseConsolidator:
    """Consolidates expense data with operator registry information."""

    def enrich_data(
        self,
        df_contabil: pd.DataFrame,
        cadop_lookup: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Enriches accounting data with CADOP registry info.
        
        Args:
            df_contabil: Processed accounting DataFrame (with Ano, Trimestre)
            cadop_lookup: Cleaned CADOP DataFrame indexed by REGISTRO_OPERADORA
            
        Returns:
            Final consolidated DataFrame with required columns.
        """
        # Perform left join on operator ID
        df_merged = df_contabil.merge(
            cadop_lookup,
            left_on='REG_ANS',
            right_index=True,
            how='left'
        )
        
        # Handle missing matches
        missing_mask = df_merged['CNPJ'].isna()
        df_merged.loc[missing_mask, 'CNPJ'] = "00.000.000/0000-00"
        df_merged.loc[missing_mask, 'Razao_Social'] = "NOT FOUND"
        
        # Rename columns to final specification
        df_merged = df_merged.rename(columns={
            'Razao_Social': 'RazaoSocial',
            'VL_SALDO_FINAL': 'ValorDespesas'
        })
        
        # Ensure numeric values
        df_merged['ValorDespesas'] = pd.to_numeric(df_merged['ValorDespesas'], errors='coerce')
        
        # Handle suspicious values (<= 0 or NaN)
        suspicious_mask = (df_merged['ValorDespesas'] <= 0) | (df_merged['ValorDespesas'].isna())
        suspicious_count = suspicious_mask.sum()
        if suspicious_count > 0:
            logger.warning(f"SUSPICIOUS VALUES: {suspicious_count} records with non-positive or invalid amounts")
            df_merged.loc[df_merged['ValorDespesas'].isna(), 'ValorDespesas'] = 0.0
        
        # Select and order final columns
        final_columns = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
        return df_merged[final_columns].reset_index(drop=True)


class OutputManager:
    """Manages final output generation and compression."""

    def __init__(self, output_path: Path = Path("consolidado_despesas.csv")):
        self.output_path = output_path

    def export_to_csv(self, df: pd.DataFrame) -> None:
        """Exports the final DataFrame to CSV."""
        df.to_csv(
            self.output_path,
            index=False,
            encoding='utf-8-sig',
            float_format='%.2f'
        )
        logger.info(f"CSV exported: {self.output_path}")

    def compress_to_zip(self) -> None:
        """Compresses the CSV file into a ZIP archive."""
        zip_path = self.output_path.with_suffix('.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(self.output_path, arcname=self.output_path.name)
        logger.info(f"ZIP created: {zip_path}")


def run_expense_consolidation_pipeline(
    df_contabil: pd.DataFrame,
    df_cadop: pd.DataFrame
) -> None:
    """
    Executes the full expense consolidation pipeline.
    
    Args:
        df_contabil: Accounting DataFrame filtered for code '41' (claims)
        df_cadop: Operator registry (CADOP) DataFrame
    """
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logger.info("Starting expense consolidation pipeline...")

    
    cadop_mapper = CadopMapper()
    cadop_lookup = cadop_mapper.clean_and_map(df_cadop)

    
    transformer = TemporalTransformer()
    df_contabil_clean = transformer.parse_dates(df_contabil)
    df_contabil_enriched = transformer.extract_period(df_contabil_clean)

    
    consolidator = ExpenseConsolidator()
    df_final = consolidator.enrich_data(df_contabil_enriched, cadop_lookup)

    logger.info(f"Consolidation complete: {len(df_final)} records processed")

    
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    output_manager = OutputManager(output_path=output_dir / "consolidado_despesas.csv")
    output_manager.export_to_csv(df_final)
    output_manager.compress_to_zip()
    logger.info("Pipeline completed successfully!")




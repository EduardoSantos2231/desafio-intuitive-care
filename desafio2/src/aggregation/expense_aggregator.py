import pandas as pd
from pathlib import Path
import logging
import zipfile

logger = logging.getLogger(__name__)


class ExpenseAggregator:
    """
    Aggregates expense data by RazaoSocial + UF to generate analytical metrics.
    
    Produces: despesas_agregadas.csv with:
    - Total expenses per operator/UF
    - Average expenses per quarter
    - Standard deviation of expenses
    - CNPJ validity flag
    """

    def aggregate_expenses(self, enriched_file: Path, output_file: Path) -> pd.DataFrame:
        """
        Creates aggregated expense metrics by operator and state.
        
        Args:
            enriched_file: Path to consolidado_enriquecido.csv
            output_file: Path for despesas_agregadas.csv
            
        Returns:
            Aggregated DataFrame
        """
        logger.info("Starting expense aggregation...")

        # Load enriched data
        df = pd.read_csv(
            enriched_file,
            sep=";",
            encoding="utf-8-sig",
            dtype=str
        )
        
        # Convert numeric columns
        df["ValorDespesas"] = pd.to_numeric(df["ValorDespesas"], errors="coerce")
        df["Trimestre"] = pd.to_numeric(df["Trimestre"], errors="coerce")
        df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
        df["RegistroCNPJValido"] = df["RegistroCNPJValido"].astype(bool)  # Ensure boolean

        # Remove invalid expense records
        valid_expenses = df["ValorDespesas"].notna()
        logger.info(f"Processing {valid_expenses.sum():,} valid expense records")
        df = df[valid_expenses].copy()

        # Group by operator + UF + CNPJ validity
        grouped = df.groupby(["RazaoSocial", "UF", "RegistroCNPJValido"], dropna=False)

        agg_df = grouped.agg({
            "ValorDespesas": ["sum", "mean", "std"],
            "Trimestre": "count"
        }).round(2)

        # Flatten column names
        agg_df.columns = [
            "TotalDespesas",
            "MediaDespesasTrimestral", 
            "DesvioPadraoDespesas",
            "NumeroTrimestres"
        ]

        # Reset index to make grouping columns regular columns
        agg_df = agg_df.reset_index()
        agg_df["DesvioPadraoDespesas"] = agg_df["DesvioPadraoDespesas"].fillna(0.0)

        # Reorder columns (INCLUDING RegistroCNPJValido)
        final_columns = [
            "RazaoSocial", "UF", "RegistroCNPJValido", 
            "TotalDespesas", "MediaDespesasTrimestral", 
            "DesvioPadraoDespesas", "NumeroTrimestres"
        ]
        agg_df = agg_df[final_columns]

        # Save CSV
        output_file.parent.mkdir(parents=True, exist_ok=True)
        agg_df.to_csv(
            output_file,
            index=False,
            sep=",",  # Note: saving with comma separator
            encoding="utf-8-sig",
            float_format="%.2f"
        )

        # Create ZIP
        self._create_zip(output_file)

        logger.info(f"Aggregation completed: {len(agg_df):,} operators/UF combinations")
        logger.info(f"Output saved: {output_file} and {output_file.parent}/Teste_Eduardo.zip")
        
        return agg_df

    def _create_zip(self, csv_file: Path) -> None:
        """Creates a ZIP file with a custom name."""
        zip_name = "Teste_Eduardo.zip" 
        zip_path = csv_file.parent / zip_name
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_file, arcname=csv_file.name)
        
        logger.info(f"ZIP created: {zip_path}")

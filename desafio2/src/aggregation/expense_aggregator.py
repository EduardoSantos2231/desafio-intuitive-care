import pandas as pd
from pathlib import Path
import logging
import zipfile
from src.utils.formatting import formatar_moeda_br

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
        logger.info("Starting expense aggregation...")

        df = pd.read_csv(
            enriched_file,
            sep=";",
            encoding="utf-8-sig"
        )

        df["ValorDespesas"] = (
            df["ValorDespesas"]
            .str.replace(".", "", regex=False)  
            .str.replace(",", ".", regex=False)  
            .astype(float)
        )

        df["Trimestre"] = pd.to_numeric(df["Trimestre"], errors="coerce")
        df["RegistroCNPJValido"] = df["RegistroCNPJValido"].astype(bool)

        valid_expenses = df["ValorDespesas"].notna()
        logger.info(f"Processing {valid_expenses.sum():,} valid expense records")
        df = df[valid_expenses].copy()

        grouped = df.groupby(["RazaoSocial", "UF", "RegistroCNPJValido"], dropna=False)
        agg_df = grouped.agg({
            "ValorDespesas": ["sum", "mean", "std"],
            "Trimestre": "count"
        }).round(2)

        agg_df.columns = [
            "TotalDespesas",
            "MediaDespesasTrimestral", 
            "DesvioPadraoDespesas",
            "NumeroTrimestres"
        ]
        agg_df = agg_df.reset_index()
        agg_df["DesvioPadraoDespesas"] = agg_df["DesvioPadraoDespesas"].fillna(0.0)

        from src.utils.formatting import formatar_moeda_br
        for col in ["TotalDespesas", "MediaDespesasTrimestral", "DesvioPadraoDespesas"]:
            agg_df[col] = agg_df[col].apply(formatar_moeda_br)

        final_columns = [
            "RazaoSocial", "UF", "RegistroCNPJValido", 
            "TotalDespesas", "MediaDespesasTrimestral", 
            "DesvioPadraoDespesas", "NumeroTrimestres"
        ]
        agg_df = agg_df[final_columns]

        output_file.parent.mkdir(parents=True, exist_ok=True)
        agg_df.to_csv(
            output_file,
            sep=";",  
            index=False,
            encoding="utf-8-sig"
        )

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

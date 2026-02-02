import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class CadopEnricher:
    """
    Enriches the validated expense dataset with CADOP registry data.
    
    Adds columns: RegistroAns, Modalidade, UF
    If CNPJ has no match in CADOP, sets ValorDespesas = NaN.
    """
    
    def enrich_with_cadop(
    self,
    validated_file: Path,
    cadop_file: Path,
    output_file: Path
        ):
        logger.info("Starting CADOP enrichment...")

        # Load validated data
        df_validated = pd.read_csv(
            validated_file,
            sep=",",
            encoding="utf-8-sig",
            dtype=str
        )
        df_validated["ValorDespesas"] = pd.to_numeric(
            df_validated["ValorDespesas"], 
            errors="coerce"
        )

        # Load and prepare CADOP
        df_cadop = self._load_and_prepare_cadop(cadop_file)

        # Perform left join on CNPJ
        df_enriched = df_validated.merge(
            df_cadop[["CNPJ", "UF"]],
            on="CNPJ",
            how="left"
        )

        # Handle non-matches
        no_match_mask = df_enriched["UF"].isna()
        if no_match_mask.any():
            logger.warning(f"CNPJs without CADOP match: {no_match_mask.sum()}")
            df_enriched.loc[no_match_mask, "UF"] = "XX"

        # Keep ESSENTIAL columns for aggregation (including Trimestre and Ano!)
        essential_columns = [
            "RazaoSocial", 
            "UF", 
            "Trimestre",     
            "Ano",            
            "ValorDespesas", 
            "RegistroCNPJValido"
        ]
        df_enriched = df_enriched[essential_columns]

        # Save result
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_enriched.to_csv(
            output_file,
            index=False,
            sep=";",
            encoding="utf-8-sig",
            float_format="%.2f"
        )

        logger.info(f"CADOP enrichment completed. Output saved to: {output_file}")
        return df_enriched

    
    def _load_and_prepare_cadop(self, cadop_file: Path):
        """Loads and prepares CADOP data for enrichment."""
        df = pd.read_csv(
            cadop_file,
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
            on_bad_lines="skip",
            skip_blank_lines=True
        )
        
        # Keep only required columns and clean
        required_cols = ["REGISTRO_OPERADORA", "CNPJ", "Modalidade", "UF"]
        df = df[required_cols].copy()
        
        # Clean CNPJ (remove extra spaces, ensure consistent format)
        df["CNPJ"] = df["CNPJ"].astype(str).str.strip()
        
        logger.info(f"CADOP loaded: {len(df)} records")
        return df

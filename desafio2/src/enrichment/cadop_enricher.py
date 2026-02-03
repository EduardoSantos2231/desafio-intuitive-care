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
    
    def enrich_with_cadop(self, validated_file: Path, cadop_file: Path, output_file: Path):
        logger.info("Starting CADOP enrichment...")

        df_validated = pd.read_csv(
            validated_file,
            sep=";",
            encoding="utf-8-sig"
        )

        df_cadop = self._load_and_prepare_cadop(cadop_file)

        df_enriched = df_validated.merge(
            df_cadop[["CNPJ", "UF"]],
            on="CNPJ",
            how="left"
        )

        no_match_mask = df_enriched["UF"].isna()
        if no_match_mask.any():
            logger.warning(f"CNPJs without CADOP match: {no_match_mask.sum()}")
            df_enriched.loc[no_match_mask, "UF"] = "XX"

        essential_columns = [
            "RazaoSocial", 
            "UF", 
            "Trimestre",     
            "Ano",            
            "ValorDespesas", 
            "RegistroCNPJValido"
        ]
        df_enriched = df_enriched[essential_columns]

        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_enriched.to_csv(
            output_file,
            sep=";",
            index=False,
            encoding="utf-8-sig"
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
        
        required_cols = ["REGISTRO_OPERADORA", "CNPJ", "Modalidade", "UF"]
        df = df[required_cols].copy()
        
        df["CNPJ"] = df["CNPJ"].astype(str).str.strip()
        
        logger.info(f"CADOP loaded: {len(df)} records")
        return df

import logging
from pathlib import Path
from src.transformation.data_validator import DataValidator
from src.enrichment.cadop_enricher import CadopEnricher
from src.aggregation.expense_aggregator import ExpenseAggregator

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main():
    desafio1_output = Path("../desafio1/output/consolidado_despesas.csv")
    desafio1_cadop = Path("../desafio1/raw/Relatorio_cadop.csv")
    
    missing_files = []
    if not desafio1_output.exists():
        missing_files.append("desafio1/output/consolidado_despesas.csv")
    if not desafio1_cadop.exists():
        missing_files.append("desafio1/raw/Relatorio_cadop.csv")
    
    if missing_files:
        logging.error(
            "❌ The necessary files for challenge 1 were not found!\n"
            "Please, make shure to execute Desafio 1 first.\n"
            "Missing files:\n" +
            "\n".join(f"  - {f}" for f in missing_files)
        )
        return

    
    validator = DataValidator()
    validated_file = Path("output/consolidado_validado.csv")
    validator.validate_and_enrich(
        input_file=desafio1_output,
        output_file=validated_file
    )

    enricher = CadopEnricher()
    enriched_file = Path("output/consolidado_enriquecido.csv")
    enricher.enrich_with_cadop(
        validated_file=validated_file,
        cadop_file=desafio1_cadop,
        output_file=enriched_file
    )

    aggregator = ExpenseAggregator()
    aggregator.aggregate_expenses(
        enriched_file=enriched_file,
        output_file=Path("output/despesas_agregadas.csv")
    )

if __name__ == "__main__":
    main()

import pandas as pd
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from src.extractors.downloader import FileDownloader
from src.extractors.zip_extractor import FileExtractor
from src.extractors.crawler import AccountingCrawler, ActiveOperatorsCrawler
from src.processors.factory_processor import ProcessorFactory
from src.transformers.cadop_transformer import run_expense_consolidation_pipeline 

# Configuration
ACCOUNTING_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
CADOP_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"

OUTPUT_FOLDER = Path("rawFiles")
CONSOLIDATED_OUTPUT = Path("output/grupo41_consolidado.csv")

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting ANS data pipeline...")

    urls = collect_all_target_urls()
    if not urls:
        logger.error("No files found in any source. Exiting.")
        return

    download_files(urls)
    extract_all()
    process_and_report()

    logger.info("Pipeline completed successfully.")
    transform_table()
    return None


def transform_table() -> None:
    """Carrega e consolida os dados finais."""
    try:
        df_contabil = pd.read_csv(
            "output/grupo41_consolidado.csv",
            sep=";",
            encoding="utf-8-sig",
            dtype=str
        )
        
        df_cadop = pd.read_csv(
            "rawFiles/Relatorio_cadop.csv",
            sep=";",                    
            encoding="utf-8-sig",          
            dtype=str,
            on_bad_lines='skip',        
            skip_blank_lines=True       
        )
        
        logger.info(f"Dados carregados: {len(df_contabil)} contábil, {len(df_cadop)} CADOP")
        
        
        run_expense_consolidation_pipeline(df_contabil, df_cadop)
        
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {e}")


def collect_all_target_urls() -> list[str]:
    """Orchestrates crawlers to collect URLs from both ANS data sources."""
    accounting_crawler = AccountingCrawler(base_url=ACCOUNTING_URL, max_files=3)
    accounting_urls = accounting_crawler.get_urls()
    logger.info(f"Found {len(accounting_urls)} accounting files.")

    cadop_crawler = ActiveOperatorsCrawler(base_url=CADOP_URL)
    cadop_urls = cadop_crawler.get_urls()
    logger.info(f"Found {len(cadop_urls)} CADOP file(s).")

    return accounting_urls + cadop_urls


def download_files(urls: list[str]) -> None:
    """Downloads all files concurrently using a thread pool."""
    def _download(url: str) -> Path | None:
        return FileDownloader.download(url, OUTPUT_FOLDER)

    logger.info(f"Starting download of {len(urls)} files...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(_download, urls))


def extract_all() -> None:
    """Extracts all ZIP files in the output folder."""
    FileExtractor.process_directory(OUTPUT_FOLDER)


def process_and_report() -> None:
    """Processes all extracted files and generates a consolidated output."""
    if not any(OUTPUT_FOLDER.iterdir()):
        logger.warning("Output folder 'rawFiles' is empty. Nothing to process.")
        return

    CONSOLIDATED_OUTPUT.parent.mkdir(exist_ok=True)
    if CONSOLIDATED_OUTPUT.exists():
        CONSOLIDATED_OUTPUT.unlink()

    processed_count = 0
    for file_path in OUTPUT_FOLDER.iterdir():
        if not file_path.is_file():
            continue

        try:
            processor = ProcessorFactory.create(file_path, CONSOLIDATED_OUTPUT)
            if processor.process(file_path):
                processed_count += 1
                logger.info(f"Processed data from: {file_path.name}")
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {e}")

    logger.info(f"Consolidated output saved to: {CONSOLIDATED_OUTPUT.resolve()}")
    logger.info(f"Total files with relevant data: {processed_count}")


if __name__ == "__main__":
    main()

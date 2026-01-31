import logging
from pathlib import Path

from src.ingestion.crawler import AccountingCrawler, ActiveOperatorsCrawler
from src.ingestion.downloader import FileDownloader
from src.ingestion.zip_extractor import FileExtractor
from src.processing.factory_processor import ProcessorFactory
from src.transformation.pipeline import ExpenseConsolidationPipeline

ACCOUNTING_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
CADOP_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"

RAW_DIR = Path("raw")
OUTPUT_DIR = Path("output")
CONSOLIDATED_ACCOUNTING_FILE = OUTPUT_DIR / "grupo41_consolidado.csv"
FINAL_OUTPUT_FILE = OUTPUT_DIR / "consolidado_despesas.csv"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Orchestrates the full ANS data pipeline using class instances."""
    logger.info("🚀 Starting ANS data pipeline...")

    # Ensure directories exist
    RAW_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

    # === STEP 1: CRAWLING ===
    logger.info("🔍 Discovering source files...")
    accounting_crawler = AccountingCrawler(base_url=ACCOUNTING_URL, max_files=3)
    cadop_crawler = ActiveOperatorsCrawler(base_url=CADOP_URL)
    
    urls = accounting_crawler.get_urls() + cadop_crawler.get_urls()
    logger.info(f"📁 Found {len(urls)} files to download")

    if not urls:
        logger.error("❌ No files found. Exiting.")
        return

    # === STEP 2: DOWNLOAD ===
    logger.info("📥 Downloading files...")
    downloader = FileDownloader()
    for url in urls:
        downloader.download(url, RAW_DIR)
    logger.info("✅ All downloads completed")

    # === STEP 3: EXTRACTION ===
    logger.info("📦 Extracting archives...")
    extractor = FileExtractor()
    extractor.process_directory(RAW_DIR)
    logger.info("✅ All archives extracted")

    # === STEP 4: ACCOUNTING PROCESSING ===
    logger.info("🧹 Processing accounting data...")
    factory = ProcessorFactory()
    factory.process_all_files(
        input_dir=RAW_DIR,
        output_file=CONSOLIDATED_ACCOUNTING_FILE
    )
    logger.info("✅ Accounting data processed")

    # === STEP 5: FINAL CONSOLIDATION ===
    logger.info("📊 Consolidating final dataset...")
    consolidator = ExpenseConsolidationPipeline()
    consolidator.run(
        accounting_file=CONSOLIDATED_ACCOUNTING_FILE,
        cadop_file=RAW_DIR / "Relatorio_cadop.csv",
        output_file=FINAL_OUTPUT_FILE
    )
    logger.info("✅ Pipeline completed successfully!")


if __name__ == "__main__":
    main()

import shutil
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class FileExtractor:
    @staticmethod
    def process_directory(directory: Path) -> None:
        """Extrai todos os zips de uma pasta e os remove."""
        for zip_path in directory.glob("*.zip"):
            try:
                shutil.unpack_archive(zip_path, directory)
                logger.info(f"Extraído: {zip_path.name}")
                zip_path.unlink()
            except Exception as e:
                logger.error(f"Erro ao extrair {zip_path.name}: {e}")

import shutil
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class FileExtractor:
    @staticmethod
    def process_directory(directory: Path) -> None:
        """Extract all files from a given folder"""
        for zip_path in directory.glob("*.zip"):
            try:
                shutil.unpack_archive(zip_path, directory)
                logger.info(f"Extracted: {zip_path.name}")
                zip_path.unlink()
            except Exception as e:
                logger.error(f"Error during extraction: {zip_path.name}: {e}")

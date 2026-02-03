from pathlib import Path
import pandas as pd
import zipfile
import logging

logger = logging.getLogger(__name__)


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
            float_format='%.2f',
            sep=";",
            decimal=","
        )
        logger.info(f"CSV exported: {self.output_path}")

    def compress_to_zip(self) -> None:
        """Compresses the CSV file into a ZIP archive."""
        zip_path = self.output_path.with_suffix('.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(self.output_path, arcname=self.output_path.name)
        logger.info(f"ZIP created: {zip_path}")

from typing import override
from .base_processor import BaseProcessor
from pathlib import Path
import logging
import pandas as pd 

logger = logging.getLogger(__name__)

class CsvProcessor(BaseProcessor):
    def __init__(self, output_file: Path):
        super().__init__(output_file, target_extension=".csv")

    @override
    def process(self, file_path: Path) -> bool:
        if not self._check_extension(file_path):
            return False

        any_saved = False
        try:
            reader = pd.read_csv(file_path, sep=None, engine='python', chunksize=150_000, dtype=str)
            
            for chunk in reader:
                chunk.columns = [c.upper().strip() for c in chunk.columns]
                
                filtered_df = self._extract_target_rows(chunk)

                if not filtered_df.empty:
                    self._save_incremental(filtered_df)
                    any_saved = True
            return any_saved
        except Exception as e:
            logger.error(f"Erro no CSV {file_path.name}: {e}")
            return False

from typing import override
from .base_processor import BaseProcessor
from pathlib import Path
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class TxtProcessor(BaseProcessor):
    """Processador para arquivos TXT da ANS (delimitados por ';')."""

    def __init__(self, output_file: Path) -> None:
        super().__init__(output_file, target_extension=".txt")

    @override
    def process(self, file_path: Path) -> bool:
        """Processa um arquivo TXT em chunks, filtrando e salvando apenas o Grupo 41."""
        if not self._check_extension(file_path):
            return False

        any_saved = False
        try:
            reader = pd.read_csv(
                file_path,
                sep=";",
                encoding="latin1",
                chunksize=150_000,
                dtype=str,
                on_bad_lines="skip",  
                engine="python",     
            )

            for chunk in reader:
                chunk.columns = [col.upper().strip() for col in chunk.columns]

                # Filtra linhas do Grupo 41
                filtered_df = self._extract_target_rows(chunk)

                if not filtered_df.empty:
                    self._save_incremental(filtered_df)
                    any_saved = True

            return any_saved

        except Exception as e:
            logger.error(f"Error during the processing of TXT {file_path.name}: {e}")
            return False

import pandas as pd
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TextIO

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """
    Base class for tabular file processors.
    Provides common infrastructure for:
    - Extension validation,
    - Filtering by accounting account prefix ('41'),
    - Efficient streaming output to CSV.
    """

    def __init__(self, output_file: Path, target_extension: str) -> None:
        self.output_file = output_file
        self.target_extension = target_extension.lower()
        self._TARGET_ACCOUNT_PREFIX = "41"

    @abstractmethod
    def process_with_stream(
        self, 
        file_path: Path, 
        output_stream: TextIO, 
        write_header: bool
    ) -> bool:
        """
        Processa um arquivo e escreve no stream fornecido.
        Deve ser implementado por subclasses.
        """
        raise NotImplementedError

    def _check_extension(self, file_path: Path) -> bool:
        """Check if the file has the expected extension."""
        return file_path.suffix.lower() == self.target_extension

    def _extract_target_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter rows where the column 'CD_CONTA_CONTABIL' starts with '41'.
        Returns a new DataFrame (never modifies the original).
        """
        if "CD_CONTA_CONTABIL" not in df.columns:
            logger.debug("Coluna 'CD_CONTA_CONTABIL' ausente — ignorando.")
            return pd.DataFrame()

        # Trata valores nulos e espaços
        account_col = df["CD_CONTA_CONTABIL"].fillna("").astype(str).str.strip()
        mask = account_col.str.startswith(self._TARGET_ACCOUNT_PREFIX, na=False)
        return df.loc[mask]

    def _save_chunk_to_stream(self, df: pd.DataFrame, output_stream: TextIO, write_header: bool) -> None:
        """Writes a chunk to the provided output stream."""
        df.to_csv(
            output_stream,
            index=False,
            header=write_header,
            sep=";"
        )

import pandas as pd
import logging
from abc import ABC, abstractmethod
from pathlib import Path

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """
    Base class for tabular file processors.   
    Provides common infrastructure for:
    - Extension validation,
    - Filtering by accounting account prefix ('41'),
    - Incremental saving to CSV.
    """

    def __init__(self, output_file: Path, target_extension: str) -> None:
        self.output_file = output_file
        self.target_extension = target_extension.lower()
        self._TARGET_ACCOUNT_PREFIX = "41"  

    @abstractmethod
    def process(self, file_path: Path) -> bool:
        """
        Processes a specific file.
        Should return True if relevant data was saved, False otherwise.
        """
        raise NotImplementedError

    def _check_extension(self, file_path: Path) -> bool:
        """Check if the file has the expected extension."""
        return file_path.suffix.lower() == self.target_extension

    def _extract_target_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter rows where the column 'CD CONTA CONTABIL' starts with '41'.
        Returns a new DataFrame (never modifies the original).      
        """
        if "CD_CONTA_CONTABIL" not in df.columns:
            logger.debug("Coluna 'CD_CONTA_CONTABIL' ausente — ignorando.")
            return pd.DataFrame()

        account_col = df["CD_CONTA_CONTABIL"].astype(str)
        mask = account_col.str.startswith(self._TARGET_ACCOUNT_PREFIX, na=False)

        filtered_df = df.loc[mask]
        return filtered_df

    def _save_incremental(self, df: pd.DataFrame) -> None:
        """
        Saves the DataFrame to the output file in append mode.
        Adds a header only on the first write.       
        """
        write_header = not self.output_file.exists()
        df.to_csv(
            self.output_file,
            mode="a",
            index=False,
            header=write_header,
            sep=";",
            encoding="utf-8-sig",
        )

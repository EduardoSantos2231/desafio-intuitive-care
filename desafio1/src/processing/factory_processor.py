from pathlib import Path
from .base_processor import BaseProcessor
from .csv_processor import CsvProcessor
from .txt_processor import TxtProcessor
import logging

logger = logging.getLogger(__name__)

_PROCESSOR_REGISTRY = {
    ".csv": CsvProcessor,
    ".txt": TxtProcessor,
}

class ProcessorFactory:
    @staticmethod
    def create(file_path: Path, output_file: Path) -> BaseProcessor:
        ext = file_path.suffix.lower()
        processor_class = _PROCESSOR_REGISTRY.get(ext)
        if processor_class is None:
            raise ValueError(f"We need to include a proper processor to: {ext}")
        return processor_class(output_file)

    
    def process_all_files(self, input_dir: Path, output_file: Path) -> None:
        """Process all files in input_dir and consolidate into a single output file."""
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if output_file.exists():
            output_file.unlink()

        with open(output_file, "w", encoding="utf-8-sig") as output_stream:
            header_written = False
            for file_path in input_dir.iterdir():
                if not file_path.is_file() or file_path.suffix.lower() not in _PROCESSOR_REGISTRY:
                    continue

                try:
                    processor = self.create(file_path, output_file)
                    success = processor.process_with_stream(
                        file_path, 
                        output_stream, 
                        not header_written
                    )
                    if success and not header_written:
                        header_written = True
                except Exception as e:
                    logger.error("Something went wrong during the processing") 
                    pass

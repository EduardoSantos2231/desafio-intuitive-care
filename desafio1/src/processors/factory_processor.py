from pathlib import Path
from .base_processor import BaseProcessor
from .csv_processor import CsvProcessor
from .txt_processor import TxtProcessor

_PROCESSOR_REGISTRY = {
    ".csv": CsvProcessor,
    ".txt": TxtProcessor,
    # Adicione mais aqui no futuro: ".xlsx": XlsxProcessor,
}


class ProcessorFactory:
    """
    Fábrica que cria o processador adequado com base na extensão do arquivo.
    """

    @staticmethod
    def create(file_path: Path, output_file: Path) -> BaseProcessor:
        """
        Cria um processador para o arquivo fornecido.
        
        Args:
            file_path: Caminho do arquivo de entrada (ex: dados.txt).
            output_file: Caminho do arquivo CSV de saída consolidado.
            
        Returns:
            Instância de BaseProcessor configurada.
            
        Raises:
            ValueError: Se a extensão do arquivo não for suportada.
        """
        ext = file_path.suffix.lower()
        processor_class = _PROCESSOR_REGISTRY.get(ext)
        
        if processor_class is None:
            supported = ", ".join(_PROCESSOR_REGISTRY.keys())
            raise ValueError(
                f"Extensão '{ext}' não suportada para '{file_path}'. "
                f"Extensões suportadas: {supported}"
            )
        
        return processor_class(output_file=output_file)

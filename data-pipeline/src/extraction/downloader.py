import requests
import logging
from pathlib import Path
from typing import TypeAlias

logger = logging.getLogger(__name__)
Timeout: TypeAlias = float | int | tuple[float, float] | tuple[int, int]

class FileDownloader:
    @staticmethod
    def download(url: str, dest_dir: Path, timeout: Timeout = (5, 60)) -> Path | None:
        dest_dir.mkdir(parents=True, exist_ok=True)
        filename = url.split('/')[-1]
        dest_path = dest_dir / filename

        try:
            with requests.Session() as session:
                with session.get(url, stream=True, timeout=timeout) as r:
                    r.raise_for_status()
                    with open(dest_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)

            logger.info(f"Download concluído: {filename}")
            return dest_path
        except Exception as e:
            logger.error(f"Falha ao baixar {url}: {e}")
            return None


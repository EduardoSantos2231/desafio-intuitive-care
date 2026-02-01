import requests
import logging
from pathlib import Path
from typing import Optional, Tuple
from requests import Session

logger = logging.getLogger(__name__)
Timeout = Tuple[int, int]


class FileDownloader:
    def __init__(
        self,
        timeout: Timeout = (5, 60),
        chunk_size: int = 1024 * 1024,  # 1 MB
    ) -> None:
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.session: Session = requests.Session()

    def download(self, url: str, dest_dir: Path) -> Optional[Path]:
        dest_dir.mkdir(parents=True, exist_ok=True)

        filename = url.split("/")[-1]
        dest_path = dest_dir / filename

        try:
            with self.session.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()

                with open(dest_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            f.write(chunk)

            logger.info(f"Download completed: {filename}")
            return dest_path

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout while downloading {url}: {e}")

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error while downloading {url}: {e}")

        except OSError as e:
            logger.error(f"File system error while saving {url}: {e}")

        except Exception as e:
            logger.exception(f"Unexpected error while downloading {url}: {e}")

        return None

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "FileDownloader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

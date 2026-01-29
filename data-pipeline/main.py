import logging
from pathlib import Path
from src.extraction.downloader import FileDownloader
from src.extraction.zipExtractor import FileExtractor
from src.extraction.crawler import ANSCrawler
from concurrent.futures import ThreadPoolExecutor

OUTPUT_FOLDER = Path("rawFiles")

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def download_one(url: str) -> Path | None:
    return FileDownloader.download(url, OUTPUT_FOLDER)


def main():
    
    BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
    
    crawler = ANSCrawler(base_url=BASE_URL, max_files=3)
    
    urls_to_download: list[str] = crawler.get_latest_zip_urls()

    if not urls_to_download:
        return

    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(download_one, urls_to_download)) 

    FileExtractor.process_directory(OUTPUT_FOLDER)

if __name__ == "__main__":
    main()

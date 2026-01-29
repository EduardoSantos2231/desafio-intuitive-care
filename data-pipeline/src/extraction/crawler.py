import re
import requests
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class ANSCrawler:
    """Responsabilidade: Entender o HTML da ANS e encontrar links de interesse."""
    
    def __init__(self, base_url: str, max_files: int = 3):
        self.base_url: str = base_url.strip().rstrip('/') + '/'
        self.max_files: int = max_files
        self.timeout: int = 3

    def get_latest_zip_urls(self) -> list[str]:
        """Varre o site e retorna a lista de URLs para download."""
        years = self._fetch_available_years()
        if not years:
            return []
            
        return self._collect_zip_links(years)

    def _fetch_available_years(self) -> list[int]:
        try:
            r = requests.get(self.base_url, timeout=self.timeout)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            pattern = re.compile(r'^(\d{4})/$')
            
            years = [int(a['href'][:4]) for a in soup.find_all('a', href=pattern)]
            return sorted(years, reverse=True)
        except Exception as e:
            logger.error(f"Erro ao mapear estrutura de anos: {e}")
            return []

    def _collect_zip_links(self, years: list[int]) -> list[str]:
        collected = []
        for year in years:
            if len(collected) >= self.max_files:
                break
            
            year_url = urljoin(self.base_url, f"{year}/")
            try:
                r = requests.get(year_url, timeout=self.timeout)
                soup = BeautifulSoup(r.text, 'html.parser')
                links = [urljoin(year_url, a['href']) for a in soup.find_all('a', href=True) 
                         if a['href'].lower().endswith('.zip')]
                links.sort(reverse=True)
                
                needed = self.max_files - len(collected)
                collected.extend(links[:needed])
            except Exception:
                continue
        return collected

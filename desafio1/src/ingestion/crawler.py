import re
import logging
import requests
from abc import ABC, abstractmethod
from urllib.parse import urljoin
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class ANSBaseCrawler(ABC):
    """
    Abstract base class for ANS data crawlers.
    Provides shared functionality for HTTP requests and HTML parsing.
    """

    def __init__(self, base_url: str, timeout: int = 10) -> None:
        self.base_url = base_url.strip().rstrip("/") + "/"
        self.timeout = timeout
        self.session = requests.Session()  

    def _get_soup(self, url: str) -> BeautifulSoup | None:
        """Fetches a URL and returns a parsed BeautifulSoup object, or None on failure."""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    @abstractmethod
    def get_urls(self) -> list[str]:
        """Returns a list of target file URLs. Must be implemented by subclasses."""
        raise NotImplementedError


class AccountingCrawler(ANSBaseCrawler):
    """
    Crawler specialized in navigating the accounting statements directory structure,
    which is organized by year (e.g., /2025/).
    """

    def __init__(self, base_url: str, max_files: int = 3) -> None:
        super().__init__(base_url)
        self.max_files = max_files

    def get_urls(self) -> list[str]:
        soup = self._get_soup(self.base_url)
        if not soup:
            return []

        year_pattern = re.compile(r"^(\d{4})/$")
        years = sorted(
            [int(a["href"][:4]) for a in soup.find_all("a", href=year_pattern)],
            reverse=True,
        )
        return self._collect_zip_urls(years)

    def _collect_zip_urls(self, years: list[int]) -> list[str]:
        collected_urls: list[str] = []
        for year in years:
            if len(collected_urls) >= self.max_files:
                break

            year_url = urljoin(self.base_url, f"{year}/")
            soup = self._get_soup(year_url)
            if not soup:
                continue

            zip_urls = [
                urljoin(year_url, a["href"])
                for a in soup.find_all("a", href=True)
                if a["href"].lower().endswith(".zip")
            ]
            zip_urls.sort(reverse=True)  # Prioritize latest quarters (e.g., 4T > 3T)

            needed = self.max_files - len(collected_urls)
            collected_urls.extend(zip_urls[:needed])

        return collected_urls


class ActiveOperatorsCrawler(ANSBaseCrawler):
    """
    Crawler for the flat directory containing active health plan operators (CADOP).
    Finds the main CSV report regardless of minor filename variations.
    """

    def get_urls(self) -> list[str]:
        soup = self._get_soup(self.base_url)
        if not soup:
            return []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if re.search(r"Relatorio_cadop.*\.csv$", href, re.IGNORECASE):
                full_url = urljoin(self.base_url, href)
                logger.info(f"CADOP report found: {full_url}")
                return [full_url]

        logger.warning(f"No CADOP report found at {self.base_url}")
        return []

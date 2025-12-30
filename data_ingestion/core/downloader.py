"""
Robust DownloadManager for CVM monthly files.

Features:
- streaming download with chunked writes and .part temporary file
- automatic monthly -> annual fallback (tries monthly first, then annual on 404)
- exponential backoff for transient errors (doesn't retry 404)
- minimal file-size sanity check
- zip integrity check (zip.testzip)
- atomic rename after successful download
- optional forced re-download and re-validation of existing files
- parallel downloads with ThreadPoolExecutor and a lightweight throttle to avoid CVM rate limits
- safe directory creation and helpful logging

Drop this file into your project and adapt settings/constants imports if needed.
"""
from __future__ import annotations

import logging
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import requests

try:
    # project-specific settings; if not available, sensible defaults will be used
    from config import settings, constants
except Exception:  # pragma: no cover - fallback defaults for standalone use
    class _Defaults:
        RAW_DATA_DIR = Path("./raw")
        RAW_UNZIP_DIR = Path("./raw_unzip")
        DOWNLOAD_TIMEOUT = 30
        DOWNLOAD_RETRIES = 3
        MAX_WORKERS = 4
        MIN_DOWNLOAD_BYTES = 50 * 1024  # 50 KB

    class _Constants:
        BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/2025/"
        HIST_BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/"
        MONTHLY_FILE_PATTERN = "inf_diario_fi_{year_month}.zip"

    settings = _Defaults()
    constants = _Constants()

logger = logging.getLogger(__name__)


@dataclass
class DownloadManager:
    raw_dir: Path = settings.RAW_DATA_DIR
    raw_unzip_dir: Path = settings.RAW_UNZIP_DIR
    timeout: int = settings.DOWNLOAD_TIMEOUT
    retries: int = settings.DOWNLOAD_RETRIES
    max_workers: int = settings.MAX_WORKERS
    min_download_bytes: int = getattr(settings, "MIN_DOWNLOAD_BYTES", 50 * 1024)
    user_agent: str = "CVM-Downloader/1.0 (+https://example.com)"
    throttle_seconds: float = 0.1  # small pause between requests to be polite

    def __post_init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        # Ensure directories exist
        self.raw_dir = Path(self.raw_dir)
        self.raw_unzip_dir = Path(self.raw_unzip_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.raw_unzip_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------- URL / path helpers --------------------------
    def generate_monthly_url(self, year_month: datetime) -> str:
        ym_str = year_month.strftime("%Y%m")
        return f"{constants.BASE_URL}inf_diario_fi_{ym_str}.zip"

    def generate_annual_url(self, year_month: datetime) -> str:
        return f"{constants.HIST_BASE_URL}inf_diario_fi_{year_month.year}.zip"

    def get_local_path(self, year_month: datetime) -> Path:
        ym_str = year_month.strftime("%Y%m")
        filename = getattr(constants, "MONTHLY_FILE_PATTERN", "inf_diario_fi_{year_month}.zip").format(year_month=ym_str)
        return self.raw_dir / filename

    # -------------------------- download primitives -------------------------
    def _stream_download(self, url: str, local_path: Path) -> None:
        """Stream download to a temporary .part file then atomically rename."""
        tmp_path = local_path.with_suffix(local_path.suffix + ".part")

        # Ensure parent exists
        tmp_path.parent.mkdir(parents=True, exist_ok=True)

        with self.session.get(url, stream=True, timeout=self.timeout) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB
                    if chunk:
                        f.write(chunk)
        # atomic rename
        tmp_path.replace(local_path)

    def _validate_file(self, local_path: Path) -> None:
        """Run basic sanity checks: size and zip integrity.

        Raises ValueError if validation fails.
        """
        size = local_path.stat().st_size
        if size < self.min_download_bytes:
            raise ValueError(f"Downloaded file too small: {size} bytes")

        # Validate ZIP
        try:
            with zipfile.ZipFile(local_path, "r") as zf:
                bad = zf.testzip()
                if bad:
                    raise ValueError(f"Corrupted ZIP, first bad file: {bad}")
        except zipfile.BadZipFile:
            raise ValueError("BadZipFile: not a ZIP archive or corrupted")

    def extract_zip(self, zip_path: Path, dest_dir: Optional[Path] = None, delete_zip_after: bool = False) -> bool:
        dest_dir = Path(dest_dir) if dest_dir is not None else self.raw_unzip_dir
        target_dir = dest_dir / zip_path.stem
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(path=target_dir)
            logger.info(f"✓ Extracted {zip_path.name} -> {target_dir}")
            if delete_zip_after:
                try:
                    zip_path.unlink()
                    logger.info(f"✓ Deleted zip file {zip_path.name}")
                except Exception as e:
                    logger.warning(f"Failed to delete zip file {zip_path.name}: {e}")
            return True
        except zipfile.BadZipFile:
            logger.error(f"Bad ZIP file: {zip_path.name}")
            return False
        except Exception as e:
            logger.error(f"Error extracting {zip_path.name}: {e}")
            return False

    # -------------------------- public API --------------------------
    def download_single_month(self, year_month: datetime, force: bool = False) -> Optional[Path]:
        """Download a single month with fallback annual URL on 404.

        Returns Path on success, None on permanent failure (404 or final failure).
        """
        local_path = self.get_local_path(year_month)

        # If file exists and we don't force, validate it and return if valid
        if local_path.exists() and not force:
            try:
                self._validate_file(local_path)
                logger.debug(f"File exists and valid: {local_path.name}")
                return local_path
            except Exception:
                logger.warning(f"Existing file {local_path.name} failed validation; re-downloading")

        # Try monthly first, then annual on 404
        candidates = [self.generate_monthly_url(year_month), self.generate_annual_url(year_month)]

        for url in candidates:
            attempt = 0
            while attempt < self.retries:
                attempt += 1
                try:
                    logger.info(f"Downloading {url} -> {local_path.name} (attempt {attempt}/{self.retries})")
                    self._stream_download(url, local_path)
                    # small throttle
                    time.sleep(self.throttle_seconds)

                    # validate
                    self._validate_file(local_path)
                    logger.info(f"✓ Downloaded & validated {local_path.name}")
                    return local_path

                except requests.exceptions.HTTPError as e:
                    status = getattr(e.response, "status_code", None)
                    if status == 404:
                        logger.warning(f"Not found (404): {url}")
                        # try next candidate (annual) instead of retrying the same URL
                        break
                    logger.warning(f"HTTP error for {url}: {e}; retrying" )
                except (requests.exceptions.RequestException, ValueError) as e:
                    # ValueError raised by validation
                    logger.warning(f"Error downloading {url}: {e}")

                # Exponential backoff for transient errors (don't backoff on 404 since it's handled)
                backoff = 2 ** attempt
                logger.debug(f"Backing off for {backoff}s before retry")
                time.sleep(backoff)

            # end attempts for this candidate
            logger.debug(f"Giving up on {url} after {self.retries} attempts, trying next candidate if any")

        # If we get here, all attempts failed
        logger.error(f"Failed to download file for {year_month.strftime('%Y-%m')}")
        return None

    def download_range(self, start_date: str, end_date: str, max_workers: Optional[int] = None, force: bool = False) -> List[Path]:
        """Download a date range (inclusive) in parallel. Uses generate_month_range from utils if available,
        otherwise a simple internal generator is used.
        """
        max_workers = max_workers or self.max_workers

        # Prefer project helper if available
        try:
            from utils.helpers import generate_month_range as _gen
            months = list(_gen(start_date, end_date))
        except Exception:
            months = list(self._internal_generate_month_range(start_date, end_date))

        logger.info(f"Downloading {len(months)} months with {max_workers} workers")
        downloaded: List[Path] = []

        # Throttle control via simple sleep in worker; you can swap for a more advanced rate limiter
        def worker(month: datetime) -> Optional[Path]:
            return self.download_single_month(month, force=force)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_month = {executor.submit(worker, m): m for m in months}
            for future in as_completed(future_to_month):
                month = future_to_month[future]
                try:
                    res = future.result()
                    if res:
                        downloaded.append(res)
                except Exception as e:
                    logger.error(f"Failed downloading {month.strftime('%Y-%m')}: {e}")

        logger.info(f"Downloaded {len(downloaded)}/{len(months)} files")
        return downloaded

    @staticmethod
    def _internal_generate_month_range(start_date: str, end_date: str) -> Iterable[datetime]:
        """Yield first-of-month datetimes from start_date to end_date inclusive.

        start_date and end_date must be YYYY-MM-DD strings.
        """
        s = datetime.strptime(start_date, "%Y-%m-%d")
        e = datetime.strptime(end_date, "%Y-%m-%d")
        cur = datetime(s.year, s.month, 1)
        while cur <= e:
            yield cur
            # advance month
            y = cur.year + (cur.month // 12)
            m = (cur.month % 12) + 1
            cur = datetime(y, m, 1)


# If run as script for quick manual download
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dm = DownloadManager()
    # example: download last 3 months
    today = datetime.today()
    dm.download_range(f"{today.year}-01-01", f"{today.year}-03-31", max_workers=2)

import zipfile
from pathlib import Path
import logging

logger = logging.getLogger()


def unzipped(zip_path: str | Path, keep_zip: bool = False):
    if isinstance(zip_path, str):
        zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        logger.info(f"Unzipping {zip_path}...")
        zip_ref.extractall(zip_path.parent)
    if not keep_zip:
        logger.info(f"Removing {zip_path}...")
        zip_path.unlink()
    return zip_path.with_suffix('')


def recursive_unzip(top_dir: str | Path, keep_zip: bool = False):
    if isinstance(top_dir, str):
        top_dir = Path(top_dir)
    zips_exist = True
    while zips_exist:
        zips_exist = False
        for zip_path in top_dir.rglob("*.zip"):
            zips_exist = True
            unzipped(zip_path)

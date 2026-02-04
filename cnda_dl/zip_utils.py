import zipfile
from pathlib import Path
import logging

logger = logging.getLogger()


def unzipped(zip_path: str | Path, keep_zip: bool = False, recursive: bool = True):
    zip_members = None
    if isinstance(zip_path, str):
        zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_members = zip_ref.namelist()
        logger.info(f"Unzipping {zip_path}...")
        zip_ref.extractall(zip_path.parent)
    if not keep_zip:
        logger.info(f"Removing {zip_path}...")
        zip_path.unlink()
    if not zip_members:
        logger.warning("The zip file is empty, did not find a top level file/folder")
    top_members = set([Path(zm).parents[-2] for zm in zip_members])
    if recursive:
        for zm in zip_members:
            full_zm = zip_path.parent / zm
            if full_zm.suffix == ".zip":
                unzipped(full_zm)
            elif full_zm.is_dir():
                for sub_zip in full_zm.rglob("*.zip"):
                    unzipped(sub_zip)
    return [zip_path.parent / tm for tm in top_members]

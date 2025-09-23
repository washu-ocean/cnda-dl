'''
Script to download MRI sessions from the CNDA
Authors:
    Joey Scanga (scanga@wustl.edu)
    Ramone Agard (rhagard@wustl.edu)
'''
from glob import glob
from pathlib import Path
import atexit
import re
import argparse
import logging
import os
import shlex
import shutil
import subprocess
import sys
import xml.etree.ElementTree as et
import datetime

import pyxnat as px
import progressbar as pb

from .formatters import ParensOnRightFormatter1
from .zip_utils import recursive_unzip, unzipped

default_log_format = "%(levelname)s:%(funcName)s: %(message)s"
sout_handler = logging.StreamHandler(stream=sys.stdout)
sout_handler.setFormatter(ParensOnRightFormatter1())
logging.basicConfig(level=logging.INFO,
                    handlers=[sout_handler],
                    format=default_log_format)

logger = logging.getLogger()


def handle_dir_creation(dir_path: Path):
    '''
    Creates (or doesn't create) directories specified in the arguments, if any are still needed.

    :param dir_title: how the directory is denoted in prompt messages
    :type dir_title: str
    :param dir_path: string representing path to new directory
    :type dir_path: str
    '''
    prompt_chosen = False
    while not prompt_chosen:
        ans = input(f"input directory does not exist: {dir_path}. \nCreate one? (y/n)\n")
        ans = ans.lower()

        if len(ans) != 1 or ans not in 'yn':
            logger.info("Invalid response")
        elif ans == 'y':
            dir_path.mkdir(parents=True)
            prompt_chosen = True
            logger.info(f"new directory created at {dir_path}")
        elif ans == 'n':
            logger.info("Chose to not create a new directory Aborting")
            sys.exit(0)
        else:
            logger.info("Invalid response")


def download_xml(central: px.Interface,
                 subject_id: str,
                 project_id: str,
                 file_path: Path):

    logger.info("Downloading session xml")
    sub = central.select(f"/projects/{project_id}/subjects/{subject_id}")
    with open(file_path, "w") as f:
        f.write(sub.get().decode())
    return True


def retrieve_experiment(central: px.Interface,
                        session: str,
                        experiment_id: bool = False,
                        project_id: str = None) -> px.jsonutil.JsonTable:

    query_params = {}
    if project_id:
        query_params['project_id'] = project_id
    if experiment_id:
        query_params['experiment_id'] = session
    else:
        query_params['subject_label'] = session

    return central.array.mrsessions(**query_params)


def get_xml_scans(xml_file: Path) -> dict:
    """
    Create a map of downloaded scan IDs to UIDs to later match with the UIDs in the .dat files

    :param xml_file: path to quality XML
    :type xml_file: pathlib.Path
    """
    xml_tree = et.parse(xml_file)
    prefix = "{" + str(xml_tree.getroot()).split("{")[-1].split("}")[0] + "}"
    scan_xml_entries = xml_tree.getroot().find(
        f"./{prefix}experiments/{prefix}experiment/{prefix}scans"
    )
    return scan_xml_entries


def get_scan_types(xml_path):
    # Get unique scan types to include in POST req
    with open(xml_path, "r") as f:
        xml_text = f.read()
    return list(set(
        re.findall(
            r'ID="[\d\w_\-]+"\s+type="([a-zA-Z0-9\-_\.]+)"',
            xml_text
        )
    ))


def get_resources(xml_path):
    # Get "additional resources" that appear on CNDA for the session
    # (usually NORDIC_VOLUMES)
    with open(xml_path, "r") as f:
        xml_text = f.read()
    return list(set(
        re.findall(
            r'resource label="([a-zA-Z0-9\-_]+)"',
            xml_text
        )
    ))


def download_experiment_zip(central: px.Interface,
                            exp: px.jsonutil.JsonTable,
                            dicom_dir: Path,
                            xml_file_path: Path,
                            keep_zip: bool = False):
    '''
    Download scan data as .zip from CNDA.

    :param central: CNDA connection object
    :type central: pyxnat.Interface
    :param exp: object containing experiment information
    :type exp: pyxnat.jsonutil.JsonTable
    :param dicom_dir: Path to session-specific directory where DICOMs should be downloaded
    :type dicom_dir: pathlib.Path
    :param xml_file_path: Path to experiment XML
    :type xml_file_path: pathlib.Path
    :param keep_zip: Will not delete downloaded zip file after unzipping
    :type keep_zip: bool
    '''
    sub_obj = central.select(f"/project/{exp['project']}/subjects/{exp['xnat:mrsessiondata/subject_id']}")
    exp_obj = central.select(f"/project/{exp['project']}/subjects/{exp['xnat:mrsessiondata/subject_id']}/experiments/{exp['ID']}")
    # Step 1: make POST request to prepare .zip download
    res1 = central.post(
        "/xapi/archive/downloadwithsize",
        json={
            "sessions": [f"{exp['project']}:{sub_obj.label()}:{exp_obj.label()}:{exp['ID']}"],
            "projectIds": [exp['project']],
            "scan_formats": ["DICOM"],
            "scan_types": get_scan_types(xml_file_path),
            "resources": get_resources(xml_file_path),
            "options": ["simplified"]
        }
    )
    # Step 2: make GET request with created ID from POST
    cur_bytes, total_bytes = 0, int(res1.json()["size"])

    def _build_progress_bar():
        widgets = [
            pb.DataSize(), '/', pb.DataSize(variable='max_value'),
            pb.Percentage(),
            ' ',
            pb.RotatingMarker(),
            ' ',
            pb.ETA(),
            ' ',
            pb.FileTransferSpeed()
        ]
        return pb.ProgressBar(
            max_value=total_bytes,
            widgets=widgets
        )
    logger.info("Downloading session .zip")
    res2 = central.get(f"/xapi/archive/download/{res1.json()['id']}/zip", timeout=(60, 300))
    res2.raise_for_status()
    with (
        open(zip_path := (dicom_dir / f"{res1.json()['id']}.zip"), "wb") as f,
        _build_progress_bar() as bar
    ):
        logger.info(f"Request headers: {res2.request.headers}")
        logger.info(f"Response headers: {res2.headers}")
        logger.removeHandler(sout_handler)
        for chunk in res2.iter_content(chunk_size=(chunk_size := 1024)):
            if chunk:
                f.write(chunk)
                cur_bytes += chunk_size
                bar.update(cur_bytes)
    logger.addHandler(sout_handler)
    logger.info("Download complete!")
    unzipped_dir = unzipped(zip_path, keep_zip=False)
    recursive_unzip(unzipped_dir, keep_zip=False)  # for NORDIC_VOLUMES already zipped up


def dat_dcm_to_nifti(session: str,
                     dat_directory: Path,
                     xml_file_path: Path,
                     session_dicom_dir: Path,
                     session_nifti_dir: Path,
                     skip_short_runs: bool = False):
    """
    Pair .dcm/.dat files with dcmdat2niix

    :param session: Session identifier
    :type session: str
    :param dat_directory: Directory with .dat files
    :type dat_directory: pathlib.Path
    :param xml_file_path: Path to session XML
    :type xml_file_path: pathlib.Path
    :param session_dicom_dir: Path to directory containing DICOM folders for each series
    :type session_dicom_dir: pathlib.Path
    :param session_nifti_dir: Path to directory containing all .dat files
    :type session_nifti_dir: pathlib.Path
    :param skip_short_runs: Flag which denotes we don't want to run dcmdat2niix on runs stopped short
    :type skip_short_runs: bool
    """
    can_convert = False
    unconverted_series = set()
    error_series = set()
    if shutil.which('dcmdat2niix') is not None:
        can_convert = True
        session_nifti_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Combined .dcm & .dat files (.nii.gz format) will be stored at: {session_nifti_dir}")
    else:
        logger.warning("dcmdat2niix not installed or has not been added to the PATH. Cannot convert data files into NIFTI")

    # find all of the scans that are in the dicom directory for this session
    downloaded_scans = [p.name.split("/")[-1] for p in session_dicom_dir.glob("*")
                        if (p / "DICOM").exists()]
    downloaded_scans.sort()

    xml_scans = get_xml_scans(xml_file=xml_file_path)
    # [:-6] is to ignore the trailing '.0.0.0' at the end of the UID string
    uid_to_id = {s.get("UID")[:-6]:s.get("ID") for s in xml_scans if s.get("ID") in downloaded_scans}

    # collect all of the .dat files and map them to their UIDs
    dat_files = list(dat_directory.rglob("*.dat"))
    uid_to_dats = {uid: [d for d in dat_files if uid in d.name] for uid in uid_to_id.keys()}

    for uid, dats in uid_to_dats.items():
        series_id = uid_to_id[uid]
        series_path = session_dicom_dir / series_id / "DICOM"
        for dat in dats:
            shutil.move(dat.resolve(), series_path.resolve())

        if len(dats) == 0:
            dats = list(series_path.glob("*.dat"))  # see if dats already in series dir

        dcms = list(series_path.glob("*.dcm"))
        logger.info(f"length of dats: {len(dats)}")
        logger.info(f"length of dcms: {len(dcms)}")

        # if we cannot convert to NIFTI then continue
        if not can_convert:
            continue

        # check if there's a mismatch between number of .dcm and .dat files (indicative of run that stopped prematurely)
        if (len(dats) != 0) and (len(dats) != len(dcms)):
            logger.warning(f"WARNING: number of .dat and .dcm files mismatched for series {series_id} with UID {uid}.")
            logger.warning("This mismatch may indicate that one of your runs has ended early")
            if skip_short_runs:
                logger.warning("skipping running dcmdat2niix \n")
                unconverted_series.add(series_id)
                continue
            elif (len(dcms) == len(dats) + 1) and len(dcms) > 1:
                logger.info("Attempting to remove the extra dcm file, and convert the remaing data")
                last_dcm = glob(f"{series_path}/*-{len(dcms)}-*.dcm")
                if len(last_dcm) == 1:
                    logger.info(f"Removing the mismatched dicom: {last_dcm[0]}")
                    os.remove(last_dcm[0])
                else:
                    logger.warning("Could not find the mismatched dicom")

        # run the dcmdat2niix subprocess
        logger.info(f"Running dcmdat2niix on series {series_id}")
        dcmdat2niix_cmd = shlex.split(f"dcmdat2niix -ba y -z o -w 1 -o {session_nifti_dir} {series_path}")
        with subprocess.Popen(dcmdat2niix_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            while p.poll() is None:
                for line in p.stdout:
                    logger.info(line.decode("utf-8", "ignore"))
            if p.poll() == 0:
                logger.info(f"dcmdat2niix complete for series {series_id} \n")
            else:
                logger.error(f"dcmdat2niix ended with a nonzero exit code for series {series_id} \n")
                error_series.add(series_id)

    if len(unconverted_series) > 0:
        logger.warning(f"""
        The following series for session:{session} were
        not converted to NIFTI beause the '--skip_short_runs'
        option was selected
        {unconverted_series}\n""")

    if len(error_series) > 0:
        logger.warning(f"""
        The following series for session:{session} encountered
        an error while being converted to NIFTI. This can be due
        to corrupted dat files (.dat files with zero or very little
        data) or if they are Physiolog acquisitions. Check these
        series for possible causes.
        {error_series}\n""")


def main():
    parser = argparse.ArgumentParser(
        prog="cnda-dl",
        description="A command-line utility for downloading fMRI data from CNDA",
    )
    parser.add_argument('session_list',
                        nargs="+",
                        help="List of either subject labels or experiment ids, separated by spaces.")
    parser.add_argument("-d", "--dicom_dir", type=Path,
                        help="Path to the directory the dicom files should be downloaded to.",
                        required=True)
    parser.add_argument("-x", "--xml_dir", type=Path,
                        help="Path to the directory the session xml file should be downloaded to. If not specified, defaults to path stored in -d.")
    parser.add_argument("-e", "--experiment_id",
                        help="Query by CNDA experiment identifier (default is to query by experiment 'label', which may be ambiguous)",
                        action='store_true')
    parser.add_argument("-p", "--project_id",
                        help="Specify the project ID to narrow down search. Recommended if the session list is not experiment ids.")
    parser.add_argument("--skip_dcmdat2niix",
                        help="If NORDIC_VOLUMES folder is available, don't perform dcmdat2niix pairing step",
                        action='store_true')
    parser.add_argument("--map_dats", type=Path,
                        help="""The path to a directory containting .dat files you wish to pair with DICOM files. Using this argument
                        means that all data is already available locally and the script will only pair Dat files to DICOMs and
                        run dcmdat2niix""")
    parser.add_argument("--log_dir", type=Path,
                        help="Points to a specified directory that will store the log file. Will not make the directory if it doesn't exist.")
    parser.add_argument("--skip_short_runs",
                        action="store_true",
                        help="Flag to indicate that runs stopped short should not be converted to NIFTI")
    parser.add_argument("--dats_only",
                        help="Skip downloading DICOMs, only try to pull .dat files",
                        action='store_true')
    parser.add_argument("--keep_zip",
                        help="Option to keep downloaded .zip file after unzipping",
                        action='store_true')
    parser.add_argument("--xml_only", "-xo",
                        action="store_true",
                        help="Flag to indicate that only the xml file should be downloaded.")
    args = parser.parse_args()

    # validate argument inputs
    if args.log_dir:
        if not args.log_dir.is_dir():
            parser.error(f"'--log_dir' directory does not exist: {args.log_dir}")
    else:
        args.log_dir = Path.home() / ".local" / "share" / "cnda-dl" / "logs"
        args.log_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.log_dir / f"cnda-dl_{datetime.datetime.now().strftime('%m-%d-%y_%I:%M%p')}.log"

    if args.map_dats and not args.map_dats.is_dir():
        parser.error(f"'--map_dats' directory does not exist: {args.map_dats}")

    # set up file logging
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter(default_log_format))
    logger.addHandler(file_handler)
    logger.addHandler(sout_handler)

    logger.info("Starting cnda-dl")
    logger.info(f"Log will be stored at {log_path}")

    # set up data paths
    session_list = args.session_list
    dicom_dir = args.dicom_dir
    if hasattr(args, 'xml_dir') and args.xml_dir is not None:
        xml_path = args.xml_dir
    else:
        xml_path = dicom_dir

    if not dicom_dir.is_dir():
        handle_dir_creation(dicom_dir)
    if not xml_path.is_dir():
        handle_dir_creation(xml_path)

    # set up CNDA connection
    central = None
    if not args.map_dats:
        central = px.Interface(server="https://cnda.wustl.edu/")
        atexit.register(central.disconnect)

    # main loop
    for session in session_list:
        download_success = False
        xml_file_path = xml_path / f"{session}.xml"
        session_dicom_dir = dicom_dir / session
        session_nifti_dir = dicom_dir / f"{session}_nii"
        # if only mapping is needed
        if args.map_dats:
            # map the .dat files to the correct scans and convert the files to NIFTI
            try:
                dat_dcm_to_nifti(session=session,
                                 dat_directory=args.map_dats,
                                 xml_file_path=xml_file_path,
                                 session_dicom_dir=session_dicom_dir,
                                 session_nifti_dir=session_nifti_dir,
                                 skip_short_runs=args.skip_short_runs)
            except Exception:
                logger.exception(f"Error moving the .dat files to the appropriate scan directories and converting to NIFTI for session: {session}")
                download_success = False
            continue

        # download the experiment data
        logger.info(f"Starting download of session {session}")

        # try to retrieve the experiment corresponding to this session
        exp = None
        try:
            exp = retrieve_experiment(central=central,
                                      session=session,
                                      experiment_id=args.experiment_id,
                                      project_id=args.project_id)
            if len(exp) == 0:
                raise RuntimeError("ERROR: CNDA query returned JsonTable object of length 0, meaning there were no results found with the given search parameters.")
            elif len(exp) > 1:
                raise RuntimeError("ERROR: CNDA query returned JsonTable object of length >1, meaning there were multiple results returned with the given search parameters.")

        except Exception:
            logger.exception("Error retrieving the experiment from the given parameters. Double check your inputs or enter more specific parameters.")
            download_success = False
            continue

        download_xml(central=central,
                     subject_id=exp["xnat:mrsessiondata/subject_id"],
                     project_id=exp["project"],
                     file_path=xml_file_path)
        if args.xml_only:
            continue
        if not args.dats_only:
            try:
                download_experiment_zip(central=central,
                                        exp=exp,
                                        dicom_dir=dicom_dir,
                                        xml_file_path=xml_file_path,
                                        keep_zip=args.keep_zip)
            except Exception as e:
                logger.exception(f"Error downloading the experiment data from CNDA for session: {session}")
                logger.exception(f"{e=}")
                download_success = False
                continue

        nordic_dat_dir = session_dicom_dir / "NORDIC_VOLUMES"
        recursive_unzip(nordic_dat_dir)
        if args.skip_dcmdat2niix or not nordic_dat_dir.is_dir():
            continue
        dat_dcm_to_nifti(session=session,
                         dat_directory=nordic_dat_dir,
                         xml_file_path=xml_file_path,
                         session_dicom_dir=session_dicom_dir,
                         session_nifti_dir=session_nifti_dir,
                         skip_short_runs=args.skip_short_runs)
    if download_success:
        logger.info("\nDownloads Complete")


if __name__ == "__main__":
    main()

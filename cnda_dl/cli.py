#!/usr/bin/env python

'''
Script to download MRI sessions from the CNDA
Authors:
    Joey Scanga (scanga@wustl.edu)
    Ramone Agard (rhagard@wustl.edu)
'''

from glob import glob
from matplotlib.ticker import EngFormatter
from pathlib import Path
from pyxnat import Interface
import pyxnat
import argparse
import logging
import os
import progressbar
import shlex
import shutil
import hashlib
import subprocess
import sys
import xml.etree.ElementTree as et
import zipfile
import datetime

default_log_format = "%(levelname)s:%(funcName)s: %(message)s"
sout_handler = logging.StreamHandler(stream=sys.stdout)
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
            logger.info("Invalid response.")
        elif ans == 'y':
            dir_path.mkdir(parents=True)
            prompt_chosen = True
            logger.info(f"new directory created at {dir_path}.")
        elif ans == 'n':
            logger.info("Chose to not create a new directory. Aborting...")
            sys.exit(0)
        else:
            logger.info("Invalid response.")


def download_xml(central: Interface,
                 subject_id: str,
                 project_id: str,
                 file_path: Path):

    logger.info("Downloading session xml...")
    sub = central.select(f"/projects/{project_id}/subjects/{subject_id}")
    with open(file_path, "w") as f:
        f.write(sub.get().decode())
    return True


def retrieve_experiment(central: Interface,
                        session: str,
                        experiment_id: bool = False,
                        project_id: str = None) -> pyxnat.jsonutil.JsonTable:

    query_params = {}
    if project_id:
        query_params['project_id'] = project_id
    if experiment_id:
        query_params['experiment_id'] = session
    else:
        query_params['subject_label'] = session

    return central.array.mrsessions(**query_params)


def get_xml_scans(xml_file: Path,
                  quality_pair: bool = False) -> dict:

    xml_tree = et.parse(xml_file)
    prefix = "{" + str(xml_tree.getroot()).split("{")[-1].split("}")[0] + "}"
    scan_xml_entries = xml_tree.getroot().find(
        f"./{prefix}experiments/{prefix}experiment/{prefix}scans"
    )
    if quality_pair:
        return {s.get("ID"): s.find(f"{prefix}quality").text
                for s in scan_xml_entries}
    return scan_xml_entries


def download_experiment_dicoms(session_experiment: pyxnat.jsonutil.JsonTable,
                               central: Interface,
                               session_dicom_dir: Path,
                               xml_file_path: Path,
                               scan_number_start: str = None,
                               skip_unusable: bool = False):

    project_id = session_experiment["project"]
    exp_id = session_experiment['ID']

    # parse the xml file for the scan quality information
    quality_pairs = get_xml_scans(xml_file=xml_file_path,
                                  quality_pair=True)

    # retrieve the list of scans for this session
    scans = central.select(f"/projects/{project_id}/experiments/{exp_id}/scans/*/").get()
    scans.sort()
    logger.info(f"Found {len(scans)} scans for this session")

    # truncate the scan list if a starting point was given
    if scan_number_start:
        assert scan_number_start in scans, "Specified scan number does not exist for this session/experiment"
        sdex = scans.index(scan_number_start)
        scans = scans[sdex:]
        logger.info(f"Downloading scans for this session starting from series {scan_number_start}")

    # remove the unusable scans from the list if skipping is requested
    if skip_unusable:
        scans = [s for s in scans if quality_pairs[s] != "unusable"]
        logger.info(f"The following scans were marked 'unusable' and will not be downloaded: \n\t {[s for s,q in quality_pairs.items() if q=='unusable']}")

    # Get total number of files
    total_file_count, cur_file_count = 0, 0
    for s in scans:
        files = central.select(f"/projects/{project_id}/experiments/{exp_id}/scans/{s}/resources/files").get("")
        total_file_count += len(files)
    logger.info(f"Total number of files: {total_file_count}")

    # So log message does not interfere with format of the progress bar
    logger.removeHandler(sout_handler)
    downloaded_files = set()
    zero_size_files = set()
    fmt = EngFormatter('B')

    # Download the session files
    with progressbar.ProgressBar(max_value=total_file_count, redirect_stdout=True) as bar:
        for s in scans:
            logger.info(f"  Downloading scan {s}...")
            print(f"Downloading scan {s}...")
            series_path = session_dicom_dir / s / "DICOM"
            series_path.mkdir(parents=True, exist_ok=True)
            files = central.select(f"/projects/{project_id}/experiments/{exp_id}/scans/{s}/resources/files").get("")
            for f in files:
                cur_file_count += 1
                add_file = True
                file_name = series_path / f._uri.split("/")[-1]
                file_size = fmt(int(f.size())) if f.size() else fmt(0)
                file_info = f"File {f.attributes()['Name']}, {file_size} ({cur_file_count} out of {total_file_count})"
                print("\t" + file_info)
                logger.info("\t" + file_info)
                if not f.size():
                    msg = "\t-- File is empty"
                    if file_name in downloaded_files:
                        msg += " -- another copy was already downloaded, skipping download of this file"
                        add_file = False
                    else:
                        zero_size_files.add(file_name)
                    print(msg)
                    logger.info(msg)
                elif file_name in zero_size_files:
                    zero_size_files.remove(file_name)
                if add_file:
                    f.get(file_name)
                    downloaded_files.add(file_name)
                bar.update(cur_file_count)
    logger.addHandler(sout_handler)
    logger.info("Dicom download complete \n")
    if len(zero_size_files) > 0:
        logger.warning(f"The following downloaded files contained no data:\n{[f.label() for f in zero_size_files]} \nCheck these files for unintended missing data!")


def download_nordic_zips(session: str,
                         central: Interface,
                         session_experiment: pyxnat.jsonutil.JsonTable,
                         session_dicom_dir: Path) -> list[Path]:
    dat_dir_list = []
    project_id = session_experiment["project"]
    exp_id = session_experiment['ID']

    def __digests_identical(zip_path: Path,
                            cnda_file: pyxnat.core.resources.File):
        if zip_path.is_file():  # Compare digests of zip on CNDA to see if we need to redownload
            with zip_path.open("rb") as f:
                if hashlib.md5(f.read()).hexdigest() == cnda_file.attributes()['digest']:  # digests match
                    return True
        return False

    # check for zip file from NORDIC sessions
    nordic_volumes = central.select(f"/projects/{project_id}/experiments/{exp_id}/resources/NORDIC_VOLUMES/files").get("")
    logger.info(f"Found {len(nordic_volumes)} 'NORDIC_VOLUMES' for this session")
    for nv in nordic_volumes:
        zip_path = session_dicom_dir / nv._uri.split("/")[-1]
        if not __digests_identical(zip_path, nv):
            logger.info(f"Downloading {zip_path.name}...")
            nv.get(zip_path)
        unzip_path = zip_path.parent / zip_path.stem
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            logger.info(f"Unzipping to {unzip_path}...")
            zip_ref.extractall(unzip_path)
        dat_dir_list.append(unzip_path)

    return dat_dir_list


def dat_dcm_to_nifti(session: str,
                     dat_directory: Path,
                     xml_file_path: Path,
                     session_dicom_dir: Path,
                     nifti_path: Path,
                     skip_short_runs: bool = False):
    # check if the required program is on the current PATH
    can_convert = False
    unconverted_series = set()
    error_series = set()
    if shutil.which('dcmdat2niix') is not None:
        can_convert = True
        nifti_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Combined .dcm & .dat files (.nii.gz format) will be stored at: {nifti_path}")
    else:
        logger.warning("dcmdat2niix not installed or has not been added to the PATH. Cannot convert data files into NIFTI")

    # find all of the scans that are in the dicom directory for this session
    downloaded_scans = [p.name.split("/")[-1] for p in session_dicom_dir.glob("*")
                        if (p / "DICOM").exists()]
    downloaded_scans.sort()

    # create a map of downloaded scan IDs to UIDs to later match with the UIDs in the .dat files
    xml_scans = get_xml_scans(xml_file=xml_file_path)
    # [:-6] is to ignore the trailing '.0.0.0' at the end of the UID string
    uid_to_id = {s.get("UID")[:-6]:s.get("ID") for s in xml_scans if s.get("ID") in downloaded_scans}

    # collect all of the .dat files and map them to their UIDs
    dat_files = list(dat_directory.glob("*.dat"))
    uid_to_dats = {uid: [d for d in dat_files if uid in d.name] for uid in uid_to_id.keys()}

    for uid, dats in uid_to_dats.items():
        series_id = uid_to_id[uid]
        series_path = session_dicom_dir / series_id / "DICOM"
        for dat in dats:
            # if (series_path / dat.name).is_file():
            #     (series_path / dat.name).unlink()
            shutil.move(dat.resolve(), series_path.resolve())

        # Either there are no accompanying dats, or they were already in the series directory
        if len(dats) == 0:
            dats = list(series_path.glob("*.dat"))

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
        logger.info(f"Running dcmdat2niix on series {series_id}...")
        dcmdat2niix_cmd = shlex.split(f"dcmdat2niix -ba n -z o -w 1 -o {nifti_path} {series_path}")
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
        description="download cnda data directly to wallace",
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
                        help="Specify the project ID to narrow down search. Recommended if the session list is not eperiment ids.")
    parser.add_argument("-s", "--scan_number",
                        help="Select the scan number to start the download from (may be used when only ONE session/experiment is specified)")
    parser.add_argument("-n", "--ignore_nordic_volumes",
                        help="Don't download a NORDIC_VOLUMES folder from CNDA if it exists.",
                        action='store_true')
    parser.add_argument("--map_dats", type=Path,
                        help="""The path to a directory containting .dat files you wish to pair with DICOM files. Using this argument
                        means that all data is already available locally and the script will only pair Dat files to DICOMs and
                        run dcmdat2niix""")
    parser.add_argument("--log_dir", type=Path,
                        help="Points to a specified directory that will store the log file. Will not make the directory if it doesn't exist.")
    parser.add_argument("-ssr","--skip_short_runs",
                        action="store_true",
                        help="Flag to indicate that runs stopped short should not be converted to NIFTI")
    parser.add_argument("--skip_unusable",
                        help="Don't download any scans marked as 'unusable' in the XML",
                        action='store_true')
    parser.add_argument("--dats_only",
                        help="Skip downloading DICOMs, only try to pull .dat files",
                        action='store_true')
    args = parser.parse_args()

    # validate argument inputs
    if args.log_dir:
        if not args.log_dir.is_dir():
            parser.error(f"'--log_dir' directory does not exist: {args.log_dir}")
    else:
        args.log_dir = Path.home() / ".local" / "share" / "cnda-dl" / "logs"
        args.log_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.log_dir / f"cnda-dl_{datetime.datetime.now().strftime('%m-%d-%y_%I:%M%p')}.log"

    if args.scan_number and len(args.session_list) > 1:
        parser.error("'--scan_number' can only be specified when there is only one session/experiment to download")

    if args.map_dats and not args.map_dats.is_dir():
        parser.error(f"'--map_dats' directory does not exist: {args.map_dats}")

    # set up file logging
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter(default_log_format))
    logger.addHandler(file_handler)
    logger.addHandler(sout_handler)

    logger.info("Starting cnda-dl...")
    logger.info(f"Log will be stored at {log_path}")

    # set up data paths
    session_list = args.session_list
    dicom_path = args.dicom_dir
    if hasattr(args, 'xml_dir') and args.xml_dir is not None:
        xml_path = args.xml_dir
    else:
        xml_path = dicom_path

    if not dicom_path.is_dir():
        handle_dir_creation(dicom_path)
    if not xml_path.is_dir():
        handle_dir_creation(xml_path)

    # set up CNDA connection
    central = None
    if not args.map_dats:
        central = Interface(server="https://cnda.wustl.edu/")

    # main loop
    for session in session_list:
        xml_file_path = xml_path / f"{session}.xml"
        session_dicom_dir = dicom_path / session

        # if only mapping is needed
        if args.map_dats:
            # map the .dat files to the correct scans and convert the files to NIFTI
            nifti_path = dicom_path / f"{session}_nii"
            try:
                dat_dcm_to_nifti(session=session,
                                 dat_directory=args.map_dats,
                                 xml_file_path=xml_file_path,
                                 session_dicom_dir=session_dicom_dir,
                                 nifti_path=nifti_path,
                                 skip_short_runs=args.skip_short_runs)
            except Exception:
                logger.exception(f"Error moving the .dat files to the appropriate scan directories and converting to NIFTI for session: {session}")
            continue

        # download the experiment data
        logger.info(f"Starting download of session {session}...")

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
            continue

        # download the xml for this session
        download_xml(central=central,
                     subject_id=exp["xnat:mrsessiondata/subject_id"],
                     project_id=exp["project"],
                     file_path=xml_file_path)
        # try to download the files for this experiment
        if not args.dats_only:
            try:
                download_experiment_dicoms(session_experiment=exp,
                                           central=central,
                                           session_dicom_dir=session_dicom_dir,
                                           xml_file_path=xml_file_path,
                                           scan_number_start=args.scan_number,
                                           skip_unusable=args.skip_unusable)
            except Exception:
                logger.exception(f"Error downloading the experiment data from CNDA for session: {session}")
                continue

        # if we are not skipping the NORDIC files
        if not args.ignore_nordic_volumes:
            # try to download NORDIC related files and convert raw data to NIFTI
            try:
                nordic_dat_dirs = download_nordic_zips(session=session,
                                                       central=central,
                                                       session_experiment=exp,
                                                       session_dicom_dir=session_dicom_dir)
                nifti_path = dicom_path / f"{session}_nii"
                for nordic_dat_path in nordic_dat_dirs:
                    dat_dcm_to_nifti(session=session,
                                     dat_directory=nordic_dat_path,
                                     xml_file_path=xml_file_path,
                                     session_dicom_dir=session_dicom_dir,
                                     nifti_path=nifti_path,
                                     skip_short_runs=args.skip_short_runs)
            except Exception:
                logger.exception(f"Error downloading 'NORDIC_VOLUMES' and converting to NIFTI for session: {session}")
                continue

    logger.info("\n...Downloads Complete")


if __name__ == "__main__":
    main()

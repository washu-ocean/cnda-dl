'''
Script to download MRI sessions from the CNDA
Authors:
    Joey Scanga (scanga@wustl.edu)
    Ramone Agard (rhagard@wustl.edu)
'''
from __future__ import annotations
from pathlib import Path
import atexit
import argparse
import logging
import os
import shlex
import shutil
import subprocess
import sys
import datetime
from enum import Enum
import requests

import pyxnat as px
import progressbar as pb

from .formatters import ParensOnRightFormatter1
from .zip_utils import unzipped

default_log_format = "{levelname:^7}|{funcName:^25}| {message}"
sout_handler = logging.StreamHandler(stream=sys.stdout)
sout_handler.setFormatter(logging.Formatter(default_log_format, style="{"))
# --- NEED FIX: prevents exceptions from being displayed ---
# sout_handler.setFormatter(ParensOnRightFormatter1())
logging.basicConfig(level=logging.INFO,
                    handlers=[sout_handler],
                    style="{",
                    format=default_log_format)

logger = logging.getLogger()

VERSION = "1.3.1"

class FileTypes(str, Enum):
    ALL = "all"
    DICOMS = "dicoms"
    DATS = "dats"
    XML = "xml"
    NONE = "none"

    def __str__(self):
        return self.value
        
    def __repr__(self):
        return self.value

    @staticmethod
    def includes(check_list:list[FileTypes], val:FileTypes) -> bool:
        if FileTypes.NONE in check_list:
            return False
        return (FileTypes.ALL in check_list) or (val in check_list)
    

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
                 exp: px.jsonutil.JsonTable,
                 file_path: Path):
    '''
    Download xml metadata for this session from CNDA.
    
    :param central: CNDA connection object
    :type central: pyxnat.Interface
    :param exp: object containing experiment information
    :type exp: pyxnat.jsonutil.JsonTable
    :param file_path: path to the output file for the session xml 
    :type file_path: pathlib.Path
    '''

    logger.info("Downloading session xml")
    sub = central.select.project(exp["project"]).subject(exp["xnat:mrsessiondata/subject_id"])
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


def download_experiment_zip(central: px.Interface,
                            exp: px.jsonutil.JsonTable,
                            dicom_dir: Path,
                            chunk_download: bool = True,
                            get_types: list[FileTypes] = FileTypes.ALL,
                            keep_zip: bool = False):
    '''
    Download scan data as .zip from CNDA.

    :param central: CNDA connection object
    :type central: pyxnat.Interface
    :param exp: object containing experiment information
    :type exp: pyxnat.jsonutil.JsonTable
    :param dicom_dir: Path to session-specific directory where DICOMs should be downloaded
    :type dicom_dir: pathlib.Path
    :param chunk_download: If download will be chunked or all loaded into memory
    :type chunk_download: bool
    :param get_type: An option of 'get_choices' that tells what kind of files should be downloaded
    :type get_type: str
    :param keep_zip: Will not delete downloaded zip file after unzipping
    :type keep_zip: bool
    '''
    sub_obj = central.select.project(exp['project']).subject(exp['xnat:mrsessiondata/subject_id'])
    exp_obj = sub_obj.experiment(exp['ID'])

    # Step 1: make POST json body to prepare .zip download
    post_json = {
        "sessions": [f"{exp['project']}:{sub_obj.label()}:{exp_obj.label()}:{exp['ID']}"],
        "projectIds": [exp['project']],
        "scan_formats": ["DICOM"],
        "options": ["simplified"]
    }
    if FileTypes.includes(get_types, FileTypes.DICOMS):
        post_json["scan_types"] = list({s.attrs.get("type") for s in exp_obj.scans()})
    if FileTypes.includes(get_types, FileTypes.DATS):
        post_json["resources"] = [r.label() for r in exp_obj.resources()]

    zip_path = None

    def _build_progress_bar(max_size):
        widgets = [
            pb.DataSize(), 
            ' of', 
            pb.DataSize(variable='max_value', format='%(scaled)4.1f %(prefix)s%(unit)s'),
            '  ',
            pb.AnimatedMarker(),
            ' ',
            pb.FileTransferSpeed(),
            ' ',
            pb.PercentageLabelBar(left="[", right="]"),
            ' ',
            pb.Timer(),
            ' (',
            pb.SmoothingETA(),
            ')',
        ]
        return pb.ProgressBar(
            max_value=max_size,
            widgets=widgets
        )

    def log_and_cleanup(msg:str, 
                        path:Path, 
                        log_level=logging.ERROR, 
                        exit_code:int=1):
        if sout_handler not in logger.handlers:
            logger.addHandler(sout_handler)
        logger.log(log_level, msg, exc_info=(log_level == logging.ERROR))
        if path is not None:
            path.unlink(missing_ok=True)
        sys.exit(exit_code)

    try:
        # Step 2: send the POST request
        res1 = central.post(
            "/xapi/archive/downloadwithsize",
            json=post_json
        )
        cur_bytes, total_bytes = 0, int(res1.json()["size"])
        logger.info("Downloading session .zip")
        zip_path = (dicom_dir/f"{res1.json()['id']}.zip")
        zip_url = f"/xapi/archive/download/{res1.json()['id']}/zip"
        timeout_params = (60, 300)

        # Step 3: make GET request with created ID from POST
        if chunk_download:
            with central.get(zip_url, timeout=timeout_params, stream=True) as response:
                response.raise_for_status()
                
                with (
                    open(zip_path, "wb") as f,
                    _build_progress_bar(total_bytes*0.7) as pbar
                ):
                    logger.removeHandler(sout_handler)
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            cur_bytes += len(chunk)
                            pbar.update(cur_bytes)
                logger.addHandler(sout_handler)
        else:
            res2 = central.get(zip_url, timeout=timeout_params)
            res2.raise_for_status()
            logger.info(f"Data received successfully ({len(res2.content)} bytes). Writing to disk...")
            with open(zip_path, "wb") as f:
                f.write(res2.content)

    except requests.exceptions.HTTPError as err:
        log_and_cleanup(f"CNDA server returned an HTTP error code during download.", zip_path, exit_code=1)
    except requests.exceptions.ConnectionError as err:
        log_and_cleanup("Failed to connect to the CNDA server. Check your network or server URL.", zip_path, exit_code=1)
    except KeyboardInterrupt:
        log_and_cleanup("[Cancelled] Download interrupted manually by the user (Ctrl+C).", zip_path, log_level=logging.WARN, exit_code=130)
    except Exception as err:
        log_and_cleanup("An unexpected error occurred during download.", zip_path, exit_code=1)

    logger.info("Download complete!")
    top_zip_members = unzipped(zip_path, keep_zip=keep_zip)
    unzipped_dirs = [d for d in top_zip_members if d.is_dir()]
    if len(unzipped_dirs) > 1:
        logger.warning(f"The zip file contained more than one top-level file/folder. Using the first directory member found: {unzipped_dirs[0]}")
    return unzipped_dirs[0]


def dat_dcm_to_nifti(central: px.Interface,
                     exp: px.jsonutil.JsonTable,
                     session_dicom_dir: Path,
                     session_nifti_dir: Path,
                     dat_directory: Path = None,
                     force_nifti: bool = False,
                     skip_short_runs: bool = False):
    """
    Pair .dcm/.dat files with dcmdat2niix

    :param central: CNDA connection object
    :type central: pyxnat.Interface
    :param exp: object containing experiment information
    :type exp: pyxnat.jsonutil.JsonTable
    :param dat_directory: Directory with .dat files
    :type dat_directory: pathlib.Path
    :param session_dicom_dir: Path to directory containing DICOM folders for each series
    :type session_dicom_dir: pathlib.Path
    :param session_nifti_dir: Path to directory containing all .dat files
    :type session_nifti_dir: pathlib.Path
    :param force_nifti: Flag which denotes we want all dicoms converted to nifti if possible
    :type force_nifti: bool
    :param skip_short_runs: Flag which denotes we don't want to run dcmdat2niix on runs stopped short
    :type skip_short_runs: bool
    """
    can_convert = False
    unconverted_series = set()
    error_series = set()
    possible_conversion_programs = ["dcmdat2niix"]
    if force_nifti: possible_conversion_programs.append("dcm2niix")
    conversion_program = None
    for program_name in possible_conversion_programs:
        if shutil.which(program_name) is not None:
            conversion_program = program_name
            can_convert = True
            break

    if not can_convert:
        logger.warning(f"{possible_conversion_programs} not installed or have not been added to the PATH. Cannot convert data files into NIFTI")

    # find all of the scans that are in the dicom directory for this session
    sub_obj = central.select.project(exp['project']).subject(exp['xnat:mrsessiondata/subject_id'])
    scans = list(sub_obj.experiment(exp["ID"]).scans())
    downloaded_scans = {s for s in scans if (session_dicom_dir/str(s.id())/"DICOM").exists()}

    if len(downloaded_scans) > 0:
            session_nifti_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Combined .dcm & .dat files (.nii.gz format) will be stored at: {session_nifti_dir}")

    # collect all of the .dat files and map them to their UIDs
    dat_files = list(dat_directory.rglob("*.dat")) if dat_directory else []

    # [:-6] is to ignore the trailing '.0.0.0' at the end of the UID string
    scan_to_dats = {s: [d for d in dat_files if s.attrs.get("UID")[:-6] in d.name] for s in downloaded_scans}

    for scan, dats in scan_to_dats.items():
        uid = scan.attrs.get("UID")[:-6]
        series_path = session_dicom_dir / scan.id() / "DICOM"
        for dat in dats:
            shutil.move(dat.resolve(), series_path.resolve())

        if len(dats) == 0:
            dats = list(series_path.glob("*.dat"))

        dcms = list(series_path.glob("*.dcm"))
        logger.info(f"length of dats: {len(dats)}")
        logger.info(f"length of dcms: {len(dcms)}")

        if not can_convert:
            continue

        # check if there's a mismatch between number of .dcm and .dat files (indicative of run that stopped prematurely)
        if (len(dats) != 0) and (len(dats) != len(dcms)):
            logger.warning(f"WARNING: number of .dat and .dcm files mismatched for series {scan.id()} with UID {uid}.")
            logger.warning("This mismatch may indicate that one of your runs has ended early")
            if skip_short_runs:
                logger.warning(f"skipping running {conversion_program} \n")
                unconverted_series.add(scan.id())
                continue
            elif (len(dcms) == len(dats) + 1) and len(dcms) > 1:
                logger.info("Attempting to remove the extra dcm file, and convert the remaining data")
                last_dcm = list(series_path.glob(f"*-{len(dcms)}-*.dcm"))
                if len(last_dcm) == 1:
                    logger.info(f"Removing the mismatched dicom: {last_dcm[0]}")
                    os.remove(last_dcm[0])
                else:
                    logger.warning("Could not find the mismatched dicom")

        # run the dcmdat2niix subprocess
        logger.info(f"Running {conversion_program} on series {scan.id()}")
        dcmdat2niix_cmd = shlex.split(f"{conversion_program} -ba y -z o -w 1 -o {session_nifti_dir} {series_path}")
        with subprocess.Popen(dcmdat2niix_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            while p.poll() is None:
                for line in p.stdout:
                    logger.info(line.decode("utf-8", "ignore"))
            if p.poll() == 0:
                logger.info(f"{conversion_program} complete for series {scan.id()} \n")
            else:
                logger.error(f"{conversion_program} ended with a nonzero exit code for series {scan.id()} \n")
                error_series.add(scan.id())

    if len(unconverted_series) > 0:
        logger.warning(f"""
        The following series for session:{sub_obj.label()} were
        not converted to NIFTI beause the '--skip_short_runs'
        option was selected
        {sorted(unconverted_series)}\n""")

    if len(error_series) > 0:
        logger.warning(f"""
        The following series for session:{sub_obj.label()} encountered
        an error while being converted to NIFTI. This can be due
        to corrupted dat files (.dat files with zero or very little
        data) or if they are Physiolog acquisitions. Check these
        series for possible causes.
        {sorted(error_series)}\n""")


description = """
A command-line utility for downloading fMRI data from CNDA.

=============================================================
                    IMPORTANT UPDATE!
=============================================================
With the update to CNDA2, an alias token and secret are now 
required to use applications such as this one. To learn how to
generate an alias/secret pair for your CNDA account, please visit: 

https://cnda-help.wustl.edu/CNDA_User_Guide_and_Tutorials/Access/Generate_and_Use_an_XNAT_Alias_Token.html. 

Once those values are generated, feel free to store them as 
enviroment variables under the names, 'CNDA_ALIAS' and 'CNDA_SECRET' 
and this program will read them in automatically. You will still
be able to enter them manually if desired.
"""

def main():
    parser = argparse.ArgumentParser(
        prog="cnda-dl",
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {VERSION}")
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
    parser.add_argument("--nifti",
                        help="Run dcmdat2niix or dcm2niix even if dat files are not present",
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
    parser.add_argument("--get_files", choices=FileTypes, type=FileTypes, default=[FileTypes.ALL], nargs="+",
                        help="Specify which files to downloads. Can be a list or single value. (default is 'all')",)
    parser.add_argument("--keep_zip",
                        help="Option to keep downloaded .zip file after unzipping",
                        action='store_true')
    parser.add_argument("--memory_download", "-md",
                        action="store_true",
                        help="If the zip file should be downloaded all at once into memory instead of downloaded in chunks")
    args = parser.parse_args()

    # validate argument inputs
    if args.log_dir:
        if not args.log_dir.is_dir():
            parser.error(f"'--log_dir' directory does not exist: {args.log_dir}")
    else:
        args.log_dir = Path.home() / ".local" / "share" / "cnda-dl" / "logs"
        args.log_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.log_dir / f"cnda-dl_{datetime.datetime.now().strftime('%m-%d-%y_%I:%M%p')}.log"

    if args.map_dats:
        args.get_files.append(FileTypes.NONE)
        if not args.map_dats.is_dir():
            parser.error(f"'--map_dats' directory does not exist: {args.map_dats}")

    # set up file logging
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter(default_log_format, style="{"))
    logger.addHandler(file_handler)
    logger.addHandler(sout_handler)

    logger.info("Starting cnda-dl")
    logger.info(f"Log will be stored at {log_path}")

    # set up data paths
    session_list = args.session_list
    dicom_dir = Path(args.dicom_dir)
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
    if FileTypes.NONE not in args.get_files:
        alias = os.environ.get("CNDA_ALIAS", None)
        secret = os.environ.get("CNDA_SECRET", None)
        if (alias is not None) and (secret is not None):
            central = px.Interface(
                server="https://cnda.wustl.edu/",
                user=alias,
                password=secret
            )
        else:
            central = px.Interface(server="https://cnda.wustl.edu/")
        atexit.register(lambda : (logging.info("disconnecting from CNDA"), central.disconnect()))

    # main loop
    for session in session_list:
        download_success = True
        xml_file_path = xml_path / f"{session}.xml"
        session_dicom_dir = dicom_dir / session
        session_nifti_dir = dicom_dir / f"{session}_nii"

        if FileTypes.NONE not in args.get_files:
            # download the experiment data
            logger.info(f"Starting download of session {session}")

            # try to retrieve the experiment corresponding to this session
            exp = None
            try:
                exp = retrieve_experiment(
                    central=central,
                    session=session,
                    experiment_id=args.experiment_id,
                    project_id=args.project_id
                )
                if len(exp) == 0:
                    raise RuntimeError("ERROR: CNDA query returned JsonTable object of length 0, meaning there were no results found with the given search parameters.")
                elif len(exp) > 1:
                    raise RuntimeError("ERROR: CNDA query returned JsonTable object of length >1, meaning there were multiple results returned with the given search parameters.")

            except Exception:
                logger.exception("Error retrieving the experiment from the given parameters. Double check your inputs or enter more specific parameters.")
                download_success = False
                continue

            # update the directory names with session label
            session_name = central.select.project(exp['project']).subject(exp['xnat:mrsessiondata/subject_id']).label()
            xml_file_path = xml_path / f"{session_name}.xml"
            session_dicom_dir = dicom_dir / session_name
            session_nifti_dir = dicom_dir / f"{session_name}_nii"
            
            if (not xml_file_path.exists()) and (not FileTypes.includes(args.get_files, FileTypes.XML)):
                args.get_files.append(FileTypes.XML)

            # If the XML file is requested
            if FileTypes.includes(args.get_files, FileTypes.XML):
                download_xml(
                    central=central,
                    exp=exp,
                    file_path=xml_file_path
                )

            # If dicoms or dats are requested
            if any([FileTypes.includes(args.get_files, ft) for ft in [FileTypes.DICOMS, FileTypes.DATS]]):
                try:
                    unzip_session_dicom_dir = download_experiment_zip(
                        central=central,
                        exp=exp,
                        dicom_dir=dicom_dir,
                        chunk_download=(not args.memory_download),
                        get_types=args.get_files,
                        keep_zip=args.keep_zip
                    )
                    if unzip_session_dicom_dir.name != session_dicom_dir.name:
                        os.rename(unzip_session_dicom_dir.resolve(), session_dicom_dir.resolve())
                except FileExistsError:
                    logger.warning(f"could not rename {unzip_session_dicom_dir} to {session_dicom_dir} because the directory already exists")
                    session_dicom_dir = Path(unzip_session_dicom_dir)
                except Exception as e:
                    logger.exception(f"Error downloading the experiment data from CNDA for session: {session}")
                    logger.exception(f"{e=}")
                    download_success = False
                    continue

        # If dicoms are present
        if session_dicom_dir.is_dir():
            nordic_dat_dir = args.map_dats if args.map_dats else session_dicom_dir / "NORDIC_VOLUMES"
            if not args.map_dats:
                if args.skip_dcmdat2niix or (not nordic_dat_dir.is_dir() and not args.nifti):
                    continue
            # map the .dat files to the correct scans and convert the files to NIFTI
            try:
                dat_dcm_to_nifti(
                    central=central,
                    exp=exp,
                    dat_directory=nordic_dat_dir,
                    session_dicom_dir=session_dicom_dir,
                    session_nifti_dir=session_nifti_dir,
                    force_nifti=args.nifti,
                    skip_short_runs=args.skip_short_runs
                )
            except Exception:
                logger.exception(f"Error moving the .dat files to the appropriate scan directories and converting to NIFTI for session: {session}")
                download_success = False
            
        if download_success:
            logger.info(f"\n\tDownloads Complete for {session}\n")


if __name__ == "__main__":
    main()
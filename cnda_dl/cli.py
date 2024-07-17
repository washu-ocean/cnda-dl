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
import argparse
import logging
import os
import progressbar
import re
import shlex
import shutil
import subprocess
import sys
import xml.etree.ElementTree as et
import zipfile
from textwrap import dedent

# Function to format number of bytes per file into MB, GB, etc.
fmt = EngFormatter('B')

log_path = f"{Path.home().as_posix()}/cnda-dl.log"
logging.basicConfig(level=logging.INFO,
                    handlers=[
                        logging.FileHandler(log_path)
                    ])
sout_handler = logging.StreamHandler(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.addHandler(sout_handler)
logger.info("Starting cnda-dl...")
logger.info(f"Log will be stored at {log_path}")

def handle_dir_creation(dir_title, path_str):
    '''
    Creates (or doesn't create) directories specified in the arguments, if any are still needed.

    :param dir_title: how the directory is denoted in prompt messages
    :type dir_title: str
    :param path_str: string representing path to new directory
    :type path_str: str
    '''
    prompt, prompt_chosen = input(f"{dir_title} directory does not exist: {path_str}. Create one? (y/n)\n"), False
    while not prompt_chosen:
        if len(prompt) < 1 or prompt[0].lower() not in 'yn':
            logger.info("Invalid response.")
            prompt = input(f"{dir_title} directory does not exist: {path_str}. Create one? (y/n)\n")
        elif prompt[0].lower() == 'y':
            new_path_str = Path(path_str)
            new_path_str.mkdir(parents=True)
            prompt_chosen = True
            logger.info(f"{dir_title} directory created at {new_path_str.as_posix()}.")
            del new_path_str
        elif prompt[0].lower() == 'n':
            logger.info(f"Chose to not create new {dir_title} directory. Aborting...")
            sys.exit(0)
        else:
            logger.info("Invalid response.")
            prompt = input(f"{dir_title} directory does not exist: {path_str}. Create one? (y/n)\n")

def main():
    parser = argparse.ArgumentParser(
        prog = "cnda-dl",
        description= "download cnda data directly to wallace",
    )
    parser.add_argument('session_list',
                        nargs= "+",
                        help="List of either subject labels or experiment ids, separated by spaces.")
    parser.add_argument("-d", "--dicom_dir",
                        help="Path to the directory the dicom files should be downloaded to.",
                        required=True)
    parser.add_argument("-x", "--xml_dir",
                        help="Path to the directory the session xml file should be downloaded to. If not specified, defaults to path stored in -d.")
    parser.add_argument("-e", "--experiment_id",
                        help="Query by CNDA experiment identifier (default is to query by experiment 'label', which may be ambiguous)",
                        action='store_true')
    parser.add_argument("-p", "--project_id",
                        help="Specify the project ID to narrow down search. Required if -e is not specified.")
    parser.add_argument("-s", "--scan_number",
                        help="Select the scan number to start the download from (may be used when only ONE session/experiment is specified)")
    parser.add_argument("-n", "--ignore_nordic_volumes",
                        help="Don't download a NORDIC_VOLUMES folder from CNDA if it exists.",
                        action='store_true')
    parser.add_argument("--skip_unusable",
                        help="Don't download any scans marked as 'unusable' in the XML",
                        action='store_true')
    args = parser.parse_args()


    if not args.experiment_id and not hasattr(args, 'project_id'):
        raise RuntimeError("ERROR: Must specify --project_id (or -p) if querying using subject labels instead of experiment ids.")

    session_list = args.session_list
    dicom_path = Path(args.dicom_dir).as_posix()
    if hasattr(args, 'xml_dir') and args.xml_dir != None:
        xml_path = Path(args.xml_dir).as_posix()
    else:
        xml_path = dicom_path
    scan_num = None

    if hasattr(args, "scan_number"):
        assert len(session_list) == 1, "ERROR: Scan number is specified but there is more than one session/experiment to download"
        scan_num = args.scan_number

    if not os.path.isdir(dicom_path):
        handle_dir_creation("DICOM", dicom_path)
    if not os.path.isdir(xml_path):
        handle_dir_creation("XML", xml_path)
    
    try:
        central = Interface(server=os.environ['CNDADL_XNAT_URL'])
    except KeyError:
        logger.critical(dedent("""
        CNDADL_XNAT_URL environment variable not found. Make sure to add the following line to your ~/.bashrc file:
        
        export CNDADL_XNAT_URL="<url-to-xnat-database>

        Exiting...
        """))
        sys.exit(1)

    for session in session_list:
        logger.info(f"Starting download of session {session}...")
        query_params = {}
        if hasattr(args, 'project_id'):
            query_params['project_id'] = args.project_id
        if args.experiment_id:
            query_params['experiment_id'] = session
        else:
            query_params['subject_label'] = session

        exp = central.array.mrsessions(**query_params)
        
        if len(exp) == 0:
            raise RuntimeError("ERROR: query returned JsonTable object of length 0, meaning there were no results found with the given search parameters.")

        if len(exp) > 1:
            raise RuntimeError("ERROR: query returned JsonTable object of length >1, meaning there were multiple results returned with the given search parameters.")

        project_id = exp["project"]
        subject_id = exp["xnat:mrsessiondata/subject_id"]

        logger.info("Downloading session xml...")
        sub = central.select(f"/projects/{project_id}/subjects/{subject_id}")
        with open(xml_path+f"/{session}.xml", "w") as f:
            f.write(sub.get().decode())

        xml_path = xml_path + f"/{session}.xml"
        tree = et.parse(xml_path)
        prefix = "{" + str(tree.getroot()).split("{")[-1].split("}")[0] + "}"
        scan_xml_entries = tree.getroot().find(
            f"./{prefix}experiments/{prefix}experiment/{prefix}scans"
        )
        quality_pairs = {s.get("ID") : s.find(f"{prefix}quality").text
                         for s in scan_xml_entries}

        scans = central.select(f"/projects/{project_id}/experiments/{exp['ID']}/scans/*/").get()

        if args.skip_unusable:
            i = 0
            while i < len(scans):
                if quality_pairs[scans[i]] == "unusable":
                    logger.info(f"Not downloading scan {scans[i]} (marked unusable)")
                    del scans[i]
                else:
                    i += 1

        if scan_num:
            assert scan_num in scans, "Specified scan number does not exist for this session/experiment"
            sdex = scans.index(scan_num)
            scans = scans[sdex:]

            
        # Get total number of files
        total_file_count, cur_file_count = 0, 0
        for s in scans:
            files = central.select(f"/projects/{project_id}/experiments/{exp['ID']}/scans/{s}/resources/files").get("")
            total_file_count += len(files)
        
        logger.info(f"Total number of files: {total_file_count}")
        with progressbar.ProgressBar(max_value=total_file_count, redirect_stdout=True) as bar:
            for s in scans:
                print(f"Downloading scan {s}...")
                series_path = dicom_path+f"/{session}/{s}/DICOM"
                os.makedirs(series_path, exist_ok=True)
                files = central.select(f"/projects/{project_id}/experiments/{exp['ID']}/scans/{s}/resources/files").get("")
                for f in files:
                    print(f"\tFile {f.attributes()['Name']}, {fmt(int(f.size()))} ({cur_file_count+1} out of {total_file_count})")
                    f.get(series_path + "/" +f._uri.split("/")[-1])
                    cur_file_count += 1
                    bar.update(cur_file_count)

        if not args.ignore_nordic_volumes:
            # Check for NORDIC files in this session
            nv = central.select(f"/projects/{project_id}/experiments/{exp['ID']}/resources/NORDIC_VOLUMES/files").get("")
            if len(nv) == 0:
                logger.warning(f"No NORDIC_VOLUMES folder found for this session.")
                continue

            # Check if NORDIC dat files can be converted using dcmdat2niix
            can_convert = True
            nii_path = f"{dicom_path}/{session}_nii"
            if shutil.which('dcmdat2niix') != None:
                os.mkdir(nii_path)
                logger.info(f"Combined .dcm & .dat files (.nii.gz format) will be stored at: {nii_path}")
            else:
                logger.info("dcmdat2niix not installed or has not been added to the PATH. Cannot convert NORDIC files into NIFTI")
                can_convert = False
            unconverted_series = set()    
            
            # Create dict mapping series number to timestamp in the name of the .dat file
            uid_to_id = {s.get("UID")[:-6] : s.get("ID") for s in scan_xml_entries} # [:-6] is to ignore the trailing '.0.0.0' at the end of the UID string
            for f in nv:
                zip_path = f"{dicom_path}/{session}/" + f._uri.split("/")[-1]
                logger.info(f"Downloading {zip_path.split('/')[-1]}...")
                f.get(zip_path)
                unzip_path = zip_path[:-4]
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    logger.info(f"Unzipping to {unzip_path}...")
                    zip_ref.extractall(unzip_path)
                dat_files = glob(unzip_path + "/*.dat")
                timestamp_to_dats = {t: [d for d in dat_files if t in d] for t in uid_to_id.keys()}
                for timestamp, dats in timestamp_to_dats.items():
                    series_id = uid_to_id[timestamp]
                    series_path = f"{dicom_path}/{session}/{series_id}/DICOM"
                    for dat in dats:
                        shutil.move(dat, series_path)

                    # Check if there's a mismatch between number of .dcm and .dat files (indicative of run that stopped prematurely)
                    dcms = glob(series_path + "/*.dcm")
                    if (len(dats) != 0) and (len(dats) != len(dcms)):
                        logger.warning(f"WARNING: number of .dat and .dcm files mismatched for series {series_id} with timestamp {timestamp}.")
                        logger.warning(f"\t.# of dats: {len(dats)}")
                        logger.warning(f"\t.# of dcms: {len(dcms)}")
                        logger.warning(f"This mismatch may indicate that one of your runs has ended early; skipping running dcmdat2niix")
                        unconverted_series.add(series_id)
                        continue

                    # Convert DICOM and DAT files to NIFTI
                    if can_convert:
                        logger.info(f"Running dcmdat2niix on series {series_id}...")
                        dcmdat2niix_cmd = shlex.split(f"dcmdat2niix -ba n -z o -w 1 -o {nii_path} {series_path}")
                        with subprocess.Popen(dcmdat2niix_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
                            while p.poll() == None:
                                text = p.stdout.read1().decode("utf-8", "ignore")
                                print(text, end="", flush=True)
                            if p.poll() != 0:
                                logger.error(f"dcmdat2niix ended with a nonzero exit code for series {series_id}")
                        print()
                        
                if len(unconverted_series) > 0:
                    logger.warning(f"""
                    The following series for session:{session}, experiment-ID:{exp['ID']} were
                    not converted to NIFTI due to inconsistent number of DICOM and Dat files:
                    {unconverted_series}
                                    """)
            
              

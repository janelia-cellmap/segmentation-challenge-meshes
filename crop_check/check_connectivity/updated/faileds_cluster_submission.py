# %%
import pandas as pd
import os
import shutil

date = "20241211"
df = pd.read_csv(f"failed_meshification_{date}.csv")
directories = [
    f"/groups/cellmap/cellmap/ackermand/new_meshes/scripts/single_resolution/crop_check_updated_{date}/{p}"
    for p in df["failed_paths"].to_list()
]
print(directories)
# %%
import subprocess
import time


def get_bjobs_matching(status, pattern=None):
    try:
        # Run bjobs with the -r option to get only running jobs
        result = subprocess.run(
            ["bjobs", "-w", status], capture_output=True, text=True, check=True
        )
        # Optionally filter lines that match a given pattern
        if pattern:
            jobs = [line for line in result.stdout.splitlines() if pattern in line]
        else:
            jobs = result.stdout.splitlines()
        return jobs
    except subprocess.CalledProcessError as e:
        print("Error running bjobs:", e)
        return []


# Example usage
while len(directories) >= 0:
    time.sleep(0.25)
    running_meshify_jobs = len(get_bjobs_matching("-r", "meshify"))
    running_igneous_daskified_jobs = len(get_bjobs_matching("-r", "igneous-daskified"))
    pending_meshify_jobs = len(get_bjobs_matching("-p", "meshify"))
    pending_igneous_daskified_jobs = len(get_bjobs_matching("-p", "igneous-daskified"))
    num_allowed_difference = 20
    if (
        running_meshify_jobs - running_igneous_daskified_jobs <= num_allowed_difference
        and (
            pending_meshify_jobs <= num_allowed_difference
            and pending_igneous_daskified_jobs <= num_allowed_difference
        )
    ):
        directory = directories.pop()
        print(f"Submitting jobs for {directory}, {len(directories)} directories left")
        # Change to the directory
        base_directory, organelle = directory.rsplit("/", maxsplit=1)
        os.chdir(base_directory)
        shutil.copyfile(
            f"/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/default_meshes/larger_mem_dask-config.yaml",
            f"{directory}/dask-config.yaml",
        )
        # Define your bsub command (replace with your actual command)
        command = (
            f"bsub -n 32 -P cellmap -o /dev/null -e /dev/null meshify -n 12 {organelle}"
        )
        # print(os.getcwd(), command)
        # Run the command
        process = subprocess.run(command, shell=True, check=True)

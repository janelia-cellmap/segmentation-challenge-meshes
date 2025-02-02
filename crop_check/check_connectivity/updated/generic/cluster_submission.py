# %%
# paths = glob.glob(
#     "/groups/cellmap/cellmap/ackermand/new_meshes/scripts/single_resolution/crop_check_updated_20241211/jrc_fly-vnc-1/**",
#     recursive=True,
# )
import glob
import os
import click


def has_subdirectories(directory):
    return any(
        os.path.isdir(os.path.join(directory, entry)) for entry in os.listdir(directory)
    )


@click.command()
@click.option(
    "--path", "-p", type=str, help="Mesh path to search for submittable directories"
)
def main(path):
    paths = glob.glob(
        f"{path}/**/*",
        recursive=True,
    )
    directories = [
        p
        for p in paths
        if (os.path.isdir(p) and "." not in p and not has_subdirectories(p))
    ]
    print(directories)
    # for dataset in *; do cd $dataset; for crop in *; do cd $crop; for organelle in *; do bsub -n 4 -P cellmap -o /dev/null -e /dev/null meshify -n 4 $organelle; done; cd ..; done; sleep 90s; cd ..; done;

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
        running_igneous_daskified_jobs = len(
            get_bjobs_matching("-r", "igneous-daskified")
        )
        pending_meshify_jobs = len(get_bjobs_matching("-p", "meshify"))
        pending_igneous_daskified_jobs = len(
            get_bjobs_matching("-p", "igneous-daskified")
        )
        num_allowed_difference = 20
        if (
            running_meshify_jobs - running_igneous_daskified_jobs
            <= num_allowed_difference
            and (
                pending_meshify_jobs <= num_allowed_difference
                and pending_igneous_daskified_jobs <= num_allowed_difference
            )
        ):
            directory = directories.pop()
            print(
                f"Submitting jobs for {directory}, {len(directories)} directories left"
            )
            # Change to the directory
            base_directory, organelle = directory.rsplit("/", maxsplit=1)
            os.chdir(base_directory)
            # Define your bsub command (replace with your actual command)
            command = f"bsub -n 4 -P cellmap -o /dev/null -e /dev/null meshify -n 12 {organelle}"
            # print(os.getcwd(), command)
            # Run the command
            process = subprocess.run(command, shell=True, check=True)


if __name__ == "__main__":
    main()

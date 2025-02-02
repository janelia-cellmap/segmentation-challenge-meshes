import os
import yaml
import click
import pandas as pd
import neuroglancer
import os
from funlib.persistence import open_ds
import json
import numpy as np

# %%
# get date as string
from datetime import datetime

date = datetime.now().strftime("%Y%m%d")

instance_classes = [
    "nuc",
    "vim",
    "ves",
    "endo",
    "lyso",
    "ld",
    "perox",
    "mito",
    "np",
    "mt",
    "cell",
]


def is_dir_empty_except_hidden(path):
    return all(f.startswith(".") for f in os.listdir(path))


@click.group()
def cli():
    """A CLI with multiple commands."""
    pass


@click.command()
@click.option("--crop_path", "-c", type=str, prompt="Path to crops")
@click.option("--input_csv", "-i", type=str, prompt="Path to csv")
def generate_mesh_config(crop_path, input_csv):
    df = pd.read_csv(input_csv)
    collections = df.collection.unique()
    # delete rows where column collection equals jrc_mus-pancreas-1
    df = df[df.collection != "jrc_mus-pancreas-1"]

    # assert len(df["collection"].unique()) == 22

    for current_collection in collections:
        collection_df = df[df["collection"] == current_collection]
        zarr_path = f"{crop_path}/{current_collection}/groundtruth.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"
        for _, row in collection_df.iterrows():

            crop = f"crop{row.crop_number}"
            if crop in ["crop253"]:
                # Vimentin and mt crops we will ignore
                continue

            for dataset_name in os.listdir(f"{zarr_path}/{crop}"):
                if not os.path.isdir(f"{zarr_path}/{crop}/{dataset_name}"):
                    continue

                if is_dir_empty_except_hidden(f"{zarr_path}/{crop}/{dataset_name}/s0"):
                    # or (
                    #     (dataset_name not in instance_classes) and dataset_name != "all"
                    # ):
                    continue

                crop_dir = f"/groups/cellmap/cellmap/ackermand/new_meshes/scripts/single_resolution/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}"
                os.makedirs(name=crop_dir, exist_ok=True)
                os.system(
                    f"cp ../../../default_meshes/dask-config.yaml {crop_dir}/dask-config.yaml"
                )

                run_config_yaml = {
                    # "input_path": f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr/{crop}/{dataset_name}/s0",
                    "input_path": f"{zarr_path}/{crop}/{dataset_name}/s0",
                    "output_directory": f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}",
                    "read_write_block_shape_pixels": [1250, 1250, 1250],
                    "downsample_factor": 0,
                    "target_reduction": 0.5,
                    "n_smoothing_iter": 0,
                    "check_mesh_validity": False,
                    "remove_smallest_components": False,
                    "do_analysis": False,
                    "do_legacy_neuroglancer": True,
                }

                with open(f"{crop_dir}/run-config.yaml", "w") as f:
                    yaml.dump(run_config_yaml, f)


# %%
# ## Write out neuroglancer jsons


@click.command()
@click.option("--crop_path", "-c", type=str, prompt="Path to crops")
@click.option("--mesh_path", "-p", type=str, prompt="Path to meshes")
@click.option("--input_csv", "-i", type=str, prompt="Path to csv")
def get_neuroglancer_links(crop_path, mesh_path, input_csv):
    def is_dir_empty_except_hidden(path):
        return all(f.startswith(".") for f in os.listdir(path))

    # neuroglancer.set_server_bind_address("0.0.0.0")

    df_contrasts = pd.read_csv("SegmentationChallengeContrast.csv")
    df = pd.read_csv(input_csv)
    df = df[df.collection != "jrc_mus-pancreas-1"]
    df.sort_values(by=["collection", "crop_number"], inplace=True)
    collections = df.collection.unique()

    # assert len(collections) == 22
    all_info = []
    failed_meshification = []
    only_do_alls = False
    for current_collection in collections:
        collection_contrast = df_contrasts.loc[
            df_contrasts["collection"] == current_collection
        ]
        contrast_min = int(collection_contrast["min"].iloc[0])
        contrast_max = int(collection_contrast["max"].iloc[0])

        output_dir = f"/nrs/cellmap/ackermand/viewer_states/crop_check_updated_{date}/janelia-cosem-datasets/{current_collection}"

        os.makedirs(output_dir, exist_ok=True)

        collection_df = df[df["collection"] == current_collection]

        zarr_path = f"{crop_path}/{current_collection}/groundtruth.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"

        viewer = neuroglancer.Viewer()
        with viewer.txn() as s:
            s.layers["raw"] = neuroglancer.ImageLayer(
                source=f"zarr://s3://janelia-cosem-datasets/{current_collection}/{current_collection}.zarr/recon-1/em/fibsem-uint8",
                shaderControls={"normalized": {"range": [contrast_min, contrast_max]}},
            )
        have_set_position = False
        for _, row in collection_df.iterrows():
            if not only_do_alls:
                have_set_position = False
            crop = f"crop{row.crop_number}"
            if crop in ["crop253"]:
                # Vimentin and mt crops we will ignore
                continue

            full_crop_path = f"{zarr_path}/{crop}"
            if only_do_alls:
                dataset_names = ["all"]
                raw_ds = open_ds(
                    f"/nrs/cellmap/data/{current_collection}/{current_collection}.zarr",
                    "recon-1/em/fibsem-uint8/s0",
                )
                raw_roi_shape = raw_ds.roi.shape
            else:
                viewer = neuroglancer.Viewer()
                with viewer.txn() as s:
                    s.layers["raw"] = neuroglancer.ImageLayer(
                        source=f"zarr://s3://janelia-cosem-datasets/{current_collection}/{current_collection}.zarr/recon-1/em/fibsem-uint8",
                        shaderControls={
                            "normalized": {"range": [contrast_min, contrast_max]}
                        },
                    )
                dataset_names = os.listdir(full_crop_path)

            for dataset_name in dataset_names:
                if not os.path.isdir(f"{full_crop_path}/{dataset_name}"):
                    continue

                if is_dir_empty_except_hidden(f"{full_crop_path}/{dataset_name}/s0"):
                    continue

                solid_chunk = False
                try:
                    if "info" not in os.listdir(
                        f"{mesh_path}/{current_collection}/{crop}/{dataset_name}/meshes"
                    ):
                        ds = open_ds(zarr_path, f"{crop}/{dataset_name}/s0")
                        data = ds.to_ndarray()
                        uniques = np.unique(data)
                        if len(uniques) == 1:
                            # then is solid chunk
                            solid_chunk = True
                            print(f"{crop}/{dataset_name} solid chunk")
                        else:
                            raise Exception(
                                "Failed to complete meshify process: either no objects or object(s) too big"
                            )

                    segments = [
                        int(mesh_id.split(":0")[0])
                        for mesh_id in os.listdir(
                            f"{mesh_path}/{current_collection}/{crop}/{dataset_name}/meshes"
                        )
                        if ":0" in mesh_id
                    ]
                    # sort segments by integer value
                    segments = sorted(segments, key=lambda x: int(x))
                except Exception as e:
                    print(e, f"{current_collection}/{crop}/{dataset_name}")
                    failed_meshification.append(
                        f"{current_collection}/{crop}/{dataset_name}"
                    )
                    continue
                image_source = f'zarr://https://cellmap-vm1.int.janelia.org/{full_crop_path.replace("/nrs/cellmap/","/nrs/")}/{dataset_name}'
                # image_source = f"zarr://s3://janelia-cosem-datasets/{current_collection}/{current_collection}.zarr/recon-1/labels/groundtruth/{crop}/{dataset_name}"
                if solid_chunk:
                    s.layers[f"{crop}_{dataset_name}"] = neuroglancer.SegmentationLayer(
                        source=[image_source]
                    )
                else:
                    mesh_source = f"precomputed://https://cellmap-vm1.int.janelia.org/nrs/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}/meshes"
                    # mesh_source = f"precomputed://s3://janelia-cosem-datasets/{current_collection}/neuroglancer/mesh/groundtruth/{crop}/{dataset_name}"
                    s.layers[f"{crop}_{dataset_name}"] = neuroglancer.SegmentationLayer(
                        source=[image_source, mesh_source], segments=segments
                    )

                if dataset_name != "all":
                    s.layers[f"{crop}_{dataset_name}"].visible = False

                if not have_set_position:
                    ds = open_ds(
                        zarr_path,
                        f"{crop}/{dataset_name}/s0",
                    )
                    position = (ds.roi.begin + ds.roi.end) / 2
                    s.position = position[::-1]

                    # empirically good
                    s.crossSectionScale = np.max(ds.roi.shape) / (4 * 100)
                    if only_do_alls:
                        max_xy = np.max(raw_roi_shape[1:])
                        s.projectionScale = 2.5 * max_xy
                    else:
                        max_xy = np.max(ds.roi.shape[1:])
                        s.projectionScale = 2 * max_xy

                    s.dimensions = neuroglancer.CoordinateSpace(
                        names=["x", "y", "z"],
                        units=["nm", "nm", "nm"],
                        scales=[1, 1, 1],
                        coordinate_arrays=[
                            None,
                            None,
                            None,
                        ],
                    )
                    s.showSlices = False
                    have_set_position = True

            if not only_do_alls:
                viewer_state = s.to_json()
                output_file = (
                    f"{output_dir}/neuroglancer-{current_collection}-{crop}.json"
                )
                with open(f"{output_file}", "w") as json_file:
                    json.dump(viewer_state, json_file, indent=4)
                output_file = output_file.replace("/nrs/cellmap/", "nrs/")
                neuroglancer_url = f"https://neuroglancer-demo.appspot.com#!https://cellmap-vm1.int.janelia.org/{output_file}"
                all_info.append((crop, current_collection, neuroglancer_url))

        if only_do_alls:
            # Assuming viewer.state.to_json() returns a dictionary
            viewer_state = s.to_json()
            first_seg_layer = True
            for layer in viewer_state["layers"]:
                if layer["type"] == "segmentation":
                    if not first_seg_layer and "segments" in layer:
                        # then has a mesh layer, but we want meshes off
                        layer["segments"] = [f"!{seg}" for seg in layer["segments"]]
                    first_seg_layer = False

        # Write the JSON data to a file
        if only_do_alls:
            output_file = f"{output_dir}/neuroglancer-{current_collection}.json"
            with open(f"{output_file}", "w") as json_file:
                json.dump(viewer_state, json_file, indent=4)

            output_file = output_file.replace("/nrs/cellmap/", "nrs/")
            neuroglancer_url = f"https://neuroglancer-demo.appspot.com#!https://cellmap-vm1.int.janelia.org/{output_file}"
            all_info.append((current_collection, neuroglancer_url))

    if only_do_alls:
        output_csv_name = f"SegmentationChallengeWithNeuroglancerURLs_{date}_all"
        df = pd.DataFrame(all_info, columns=["collection", "url"])
    else:
        output_csv_name = f"SegmentationChallengeWithNeuroglancerURLs_{date}"
        df = pd.DataFrame(all_info, columns=["crop_number", "collection", "url"])

        df.to_csv(f"{output_csv_name}.csv", index=False)
        # df["url"] = df["url"].replace(
        #     f"https://cellmap-vm1.int.janelia.org/nrs/ackermand/viewer_states/crop_check_updated_{date}/",
        #     "s3://",
        #     regex=True,
        # )

        # ## until i update my paths to match s3
        # df["url"] = df["url"].replace(
        #     "neuroglancer-j",
        #     "neuroglancer/viewer_states/neuroglancer-j",
        #     regex=True,
        # )
        # ##
        # #
        # df.to_csv(f"{output_csv_name}_s3.csv", index=False)

        # if len(failed_meshification) > 0:
        failed_df = pd.DataFrame(failed_meshification, columns=["failed_paths"])
        failed_df.to_csv(f"failed_meshification_{date}.csv", index=False)
        print(failed_df)


cli.add_command(generate_mesh_config)
cli.add_command(get_neuroglancer_links)

if __name__ == "__main__":
    cli()

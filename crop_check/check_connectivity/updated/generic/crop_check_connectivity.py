from image_data_interface import ImageDataInterface
import pandas as pd
import os
import numpy as np
import logging
import skimage
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import socket
import os
import neuroglancer
import glob
import json
import os
from funlib.persistence import open_ds
import click
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


def get_neuroglancer_url(row, crop_path, mesh_path):
    collection = row["collection"]
    raw_zarr_path = f"/nrs/cellmap/data/{collection}/{collection}.zarr"
    output_dir = f"/nrs/cellmap/ackermand/crop_check_updated/viewer_states/{collection}"
    os.makedirs(output_dir, exist_ok=True)

    viewer = neuroglancer.Viewer()
    have_set_position = False
    with viewer.txn() as s:
        s.layers["raw"] = neuroglancer.ImageLayer(
            source=f"zarr://https://cellmap-vm1.int.janelia.org/nrs/data/{collection}/{collection}.zarr/recon-1/em/fibsem-uint8",
        )
        crop = f"crop{row.crop_number}"
        # crop_path = f"/nrs/cellmap/zubovy/crop_splits/combined/{collection}/groundtruth.zarr/{crop}"
        full_crop_path = f"{crop_path}/{collection}/groundtruth.zarr/{crop}"

        for organelle_path in glob.glob(f"{full_crop_path}/*"):
            organelle_name = os.path.basename(organelle_path)
            if is_dir_empty_except_hidden(f"{organelle_path}/s0"):
                continue

            if not (organelle_name in instance_classes or organelle_name == "all"):
                continue

            try:
                segments = [
                    int(mesh_id.split(":0")[0])
                    for mesh_id in os.listdir(
                        f"{mesh_path}/{collection}/{crop}/{organelle_name}/meshes"
                    )
                    if ":0" in mesh_id
                ]
            except:
                print(f"Error in {collection}/{crop}/{organelle_name}")
                continue
            
            image_source = f'zarr://https://cellmap-vm1.int.janelia.org/{full_crop_path.replace("/nrs/cellmap/","/nrs/")}/{organelle_name}'
            mesh_path_for_neuroglancer = mesh_path.replace("/nrs/cellmap/", "/nrs/")
            mesh_source = f"precomputed://https://cellmap-vm1.int.janelia.org/{mesh_path_for_neuroglancer}/{collection}/{crop}/{organelle_name}/meshes"
            s.layers[f"{crop}_{organelle_name}"] = neuroglancer.SegmentationLayer(
                source=[image_source, mesh_source], segments=segments
            )

            if organelle_name != "all":
                s.layers[f"{crop}_{organelle_name}"].visible = False

            if not have_set_position:
                ds = open_ds(
                    f"{crop_path}/{collection}/groundtruth.zarr",
                    f"{crop}/{organelle_name}/s0",
                )
                position = (ds.roi.begin + ds.roi.end) / 2
                s.position = position[::-1]
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

                have_set_position = True

    # Assuming viewer.state.to_json() returns a dictionary
    viewer_state = viewer.state.to_json()
    # Write the JSON data to a file
    with open(f"{output_dir}/{crop}.json", "w") as json_file:
        json.dump(viewer_state, json_file, indent=4)
    neuroglancer_url = f"https://neuroglancer-demo.appspot.com#!https://cellmap-vm1.int.janelia.org/nrs/ackermand/crop_check_updated/viewer_states/{collection}/{crop}.json"
    return neuroglancer_url


def process_rows(rows, crop_path, connectivity):
    logging.getLogger().setLevel(logging.WARNING)
    # create empty dataframe that matches the input
    output = []
    try:
        for _, row in rows.iterrows():
            collection = row["collection"]
            crop_number = row["crop_number"]
            crop = f"crop{crop_number}"
            organelle_name = row["organelle_name"]
            url = row["url"]
            if organelle_name not in instance_classes:
                continue

            try:
                idi = ImageDataInterface(
                    f"{crop_path}/{collection}/groundtruth.zarr/crop{crop_number}/{organelle_name}/s0"
                )
                data = idi.to_ndarray_ts()
                del idi
            except:
                raise Exception(
                    f"{crop_path}/{collection}/groundtruth.zarr/crop{crop_number}/{organelle_name}/s0"
                )
            reconnected = skimage.measure.label(data > 0, connectivity=connectivity)
            ids, counts = np.unique(reconnected[reconnected > 0], return_counts=True)
            ids = ids[counts <= 5]
            if len(ids) > 100:
                small_ids = "more than 100"
            else:
                small_ids = []
                for id in ids:
                    original_id = np.unique(data[reconnected == id])[0]
                    small_ids.append(original_id)
            del reconnected, data
            if len(small_ids) > 0:
                output.append(
                    [
                        collection,
                        crop,
                        organelle_name,
                        url,
                        small_ids,
                    ]
                )
        return pd.DataFrame(output, columns=rows.columns)
    except Exception as e:
        raise Exception(f"Error in {collection} {crop} {organelle_name}: {e}")


# get local ip
@click.command()
@click.option("--crop_path", "-c", type=str, prompt="Path to crops")
@click.option("--mesh_path", "-p", type=str, prompt="Path to meshes")
@click.option("--input_csv", "-i", type=str, prompt="Path to csv")
@click.option("--output_dir", "-o", type=str, prompt="Path to output directory")
def main(crop_path, mesh_path, input_csv, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    original_df = pd.read_csv(input_csv)
    new_df_data = []
    for _, row in original_df.iterrows():
        url = get_neuroglancer_url(row, crop_path, mesh_path)
        crop_number = row["crop_number"]
        collection = row["collection"]
        # crop_path = f"/nrs/cellmap/zubovy/crop_splits/combined/{collection}/groundtruth.zarr/crop{crop_number}"
        full_crop_path = f"{crop_path}/{collection}/groundtruth.zarr/crop{crop_number}"
        for organelle_path in glob.glob(f"{full_crop_path}/*"):
            organelle_name = os.path.basename(organelle_path)
            if organelle_name in instance_classes:
                new_df_data.append([collection, crop_number, organelle_name, url, []])

    new_df = pd.DataFrame(
        new_df_data,
        columns=[
            "collection",
            "crop_number",
            "organelle_name",
            "url",
            "small_object_ids",
        ],
    )
    for connectivity in [1, 2, 3]:
        # new_df.to_csv("testing.csv")
        cluster = LocalCluster(
            n_workers=int(os.cpu_count() / 2), threads_per_worker=1, memory_limit="64GB"
        )
        with Client(cluster) as client:
            ddf = dd.from_pandas(
                new_df,
                npartitions=min(100, len(new_df)),
            )
            print(
                f'Check {client.cluster.dashboard_link.replace("127.0.0.1",socket.gethostname())}'
            )
            result_ddf = ddf.map_partitions(
                process_rows, crop_path=crop_path, connectivity=connectivity, meta=ddf
            )
            output_df = result_ddf.compute()

            output_df.to_csv(
                f"{output_dir}/isolated_pixels_connectivity_{connectivity}_{date}.csv",
                index=False,
            )


if __name__ == "__main__":
    main()

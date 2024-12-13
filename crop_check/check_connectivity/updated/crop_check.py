# %%

import pandas as pd
import os
import yaml
import shutil

date = "20241211"
# %%
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


df = pd.read_csv(
    "./Segmentation Challenge _ List of Collections and Crops  - Sheet1.csv"
)
collections = df.collection.unique()
# delete rows where column collection equals jrc_mus-pancreas-1
df = df[df.collection != "jrc_mus-pancreas-1"]

assert len(df["collection"].unique()) == 22

for current_collection in collections:
    collection_df = df[df["collection"] == current_collection]
    zarr_path = f"/nrs/cellmap/zubovy/crop_splits/new/combined/{current_collection}/groundtruth.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"
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
            os.system(f"cp ../../default_meshes/dask-config.yaml {crop_dir}")

            run_config_yaml = {
                # "input_path": f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr/{crop}/{dataset_name}/s0",
                "input_path": f"{zarr_path}/{crop}/{dataset_name}/s0",
                "output_directory": f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}",
                "read_write_block_shape_pixels": [256, 256, 256],
                "downsample_factor": 0,
                "target_reduction": 0.5,
                "n_smoothing_iter": 0,
                "remove_smallest_components": False,
                "do_analysis": False,
                "do_legacy_neuroglancer": True,
            }

            with open(f"{crop_dir}/run-config.yaml", "w") as f:
                yaml.dump(run_config_yaml, f)

# %%
# ## Write out neuroglancer jsons


import pandas as pd
import neuroglancer
import os
from funlib.persistence import open_ds
import json
import numpy as np


def is_dir_empty_except_hidden(path):
    return all(f.startswith(".") for f in os.listdir(path))


neuroglancer.set_server_bind_address("0.0.0.0")

df_contrasts = pd.read_csv("SegmentationChallengeContrast.csv")
df = pd.read_csv(
    "/groups/scicompsoft/home/ackermand/Programming/segmentation-challenge-meshes/crop_check/check_connectivity/updated/Segmentation Challenge _ List of Collections and Crops  - Sheet1.csv"
)
df = df[df.collection != "jrc_mus-pancreas-1"]
df.sort_values(by=["collection", "crop_number"], inplace=True)
collections = df.collection.unique()

assert len(collections) == 22
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

    zarr_path = f"/nrs/cellmap/zubovy/crop_splits/new/combined//{current_collection}/groundtruth.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"

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

        crop_path = f"{zarr_path}/{crop}"
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
            dataset_names = os.listdir(crop_path)

        for dataset_name in dataset_names:
            if not os.path.isdir(f"{crop_path}/{dataset_name}"):
                continue

            if is_dir_empty_except_hidden(f"{crop_path}/{dataset_name}/s0"):
                continue

            solid_chunk = False
            try:
                print(
                    f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}/meshes"
                )
                if "info" not in os.listdir(
                    f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}/meshes"
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
                        f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}/meshes"
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
                pass
            # image_source = f'zarr://https://cellmap-vm1.int.janelia.org/{crop_path.replace("/nrs/cellmap/","/nrs/")}/{dataset_name}'
            image_source = f"zarr://s3://janelia-cosem-datasets/{current_collection}/{current_collection}.zarr/recon-1/labels/groundtruth/{crop}/{dataset_name}"
            if solid_chunk:
                s.layers[f"{crop}_{dataset_name}"] = neuroglancer.SegmentationLayer(
                    source=[image_source]
                )
            else:
                # mesh_source = f"precomputed://https://cellmap-vm1.int.janelia.org/nrs/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}/meshes"
                mesh_source = f"precomputed://s3://janelia-cosem-datasets/{current_collection}/neuroglancer/mesh/groundtruth/{crop}/{dataset_name}"
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
            output_file = f"{output_dir}/neuroglancer-{current_collection}-{crop}.json"
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

import pandas as pd

if only_do_alls:
    output_csv_name = f"SegmentationChallengeWithNeuroglancerURLs_{date}_all"
    df = pd.DataFrame(all_info, columns=["collection", "url"])
else:
    output_csv_name = f"SegmentationChallengeWithNeuroglancerURLs_{date}"
    df = pd.DataFrame(all_info, columns=["crop_number", "collection", "url"])

df.to_csv(f"{output_csv_name}.csv", index=False)
df["url"] = df["url"].replace(
    f"https://cellmap-vm1.int.janelia.org/nrs/ackermand/viewer_states/crop_check_updated_{date}/",
    "s3://",
    regex=True,
)

## until i update my paths to match s3
df["url"] = df["url"].replace(
    "neuroglancer-j",
    "neuroglancer/viewer_states/neuroglancer-j",
    regex=True,
)
##
#
df.to_csv(f"{output_csv_name}_s3.csv", index=False)

# if len(failed_meshification) > 0:
failed_df = pd.DataFrame(failed_meshification, columns=["failed_paths"])
failed_df.to_csv(f"failed_meshification_{date}.csv", index=False)
print(failed_df)
# %%
f"https://cellmap-vm1.int.janelia.org/nrs/ackermand/viewer_states/crop_check_updated_{date}/"

# %%
raw_ds = open_ds(
    f"/nrs/cellmap/data/{current_collection}/{current_collection}.zarr",
    "recon-1/em/fibsem-uint8/s0",
)
raw_roi_shape = raw_ds.roi.shape
raw_roi_shape
# %% check overlap rois:
import pandas as pd
import neuroglancer
import os
from funlib.persistence import open_ds
import json
import numpy as np


def is_dir_empty_except_hidden(path):
    return all(f.startswith(".") for f in os.listdir(path))


neuroglancer.set_server_bind_address("0.0.0.0")


df = pd.read_csv(
    "/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/check_connectivity/updated/Segmentation Challenge _ List of Collections and Crops  - Sheet1.csv"
)
df = df[df.collection != "jrc_mus-pancreas-1"]
df.sort_values(by=["collection", "crop_number"], inplace=True)
collections = df.collection.unique()

assert len(collections) == 22
for current_collection in collections:
    collection_df = df[df["collection"] == current_collection]

    zarr_path = f"/nrs/cellmap/zubovy/crop_splits/new/combined//{current_collection}/groundtruth.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"

    raw_ds = open_ds(
        f"/nrs/cellmap/data/{current_collection}/{current_collection}.zarr",
        "recon-1/em/fibsem-uint8/s0",
    )
    raw_roi = raw_ds.roi

    for _, row in collection_df.iterrows():
        crop = f"crop{row.crop_number}"
        if crop in ["crop253"]:
            # Vimentin and mt crops we will ignore
            continue

        crop_path = f"{zarr_path}/{crop}"
        dataset_names = ["all"]
        for dataset_name in dataset_names:
            if not os.path.isdir(f"{crop_path}/{dataset_name}"):
                continue

            if is_dir_empty_except_hidden(f"{crop_path}/{dataset_name}/s0"):
                continue

            # if "info" not in os.listdir(
            #     f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}/meshes"
            # ):
            ds = open_ds(zarr_path, f"{crop}/{dataset_name}/s0")
            if ds.roi != ds.roi.intersect(raw_roi):
                print(
                    f"Roi does not match {current_collection} {dataset_name} {crop}, {ds.roi, raw_roi}"
                )


# import json

# # Assuming viewer.state.to_json() returns a dictionary
# viewer_state = viewer.state.to_json()

# # Write the JSON data to a file
# with open("/nrs/cellmap/ackermand/test/viewer_state.json", "w") as json_file:
#     json.dump(viewer_state, json_file, indent=4)


# # ## Check for empty pixels

# # In[8]:


# import pandas as pd
# import os
# import numpy as np
# from funlib.persistence import open_ds
# from image_data_interface import ImageDataInterface
# import logging
# from tqdm import tqdm

# logging.getLogger().setLevel(logging.WARNING)


# def is_dir_empty_except_hidden(path):
#     return all(f.startswith(".") for f in os.listdir(path))


# crops_with_uncertainty = [163, 157, 319, 320, 115, 191, 196, 214, 224]
# df = pd.read_csv(
#     "/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/Segmentation Challenge _ List of Collections and Crops  - Sheet1.csv"
# )
# collections = df.collection.unique()
# for current_collection in collections:
#     collection_df = df[df["collection"] == current_collection]

#     zarr_path = f"/nrs/cellmap/data/{current_collection}/{current_collection}.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"
#     for _, row in collection_df.iterrows():
#         if row.crop_number not in crops_with_uncertainty:
#             crop = f"crop{row.crop_number}"
#             crop_path = f"{zarr_path}/recon-1/labels/groundtruth/{crop}"
#             for dataset_name in os.listdir(crop_path):
#                 if not os.path.isdir(f"{crop_path}/{dataset_name}"):
#                     continue

#                 if is_dir_empty_except_hidden(f"{crop_path}/{dataset_name}/s0"):
#                     continue

#                 if dataset_name == "all":
#                     data = ImageDataInterface(
#                         f"{zarr_path}/recon-1/labels/groundtruth/{crop}/all/s0"
#                     ).to_ndarray_ds()
#                     # find if data array has any zeros
#                     unique_ids, unique_counts = np.unique(data, return_counts=True)
#                     if unique_ids[0] == 0:
#                         print(
#                             f"{current_collection}/{crop}/all has {len(unique_ids)} unique ids and {unique_counts[0]} empty voxels"
#                         )


# # ## Check for small isolated pixels

# # In[22]:


# from image_data_interface import ImageDataInterface
# import pandas as pd
# import os
# import numpy as np
# import logging

# logging.getLogger().setLevel(logging.WARNING)


# def is_dir_empty_except_hidden(path):
#     return all(f.startswith(".") for f in os.listdir(path))


# df = pd.read_csv(
#     "/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/Segmentation Challenge _ List of Collections and Crops  - Sheet1.csv"
# )
# collections = df.collection.unique()
# for current_collection in collections:
#     collection_df = df[df["collection"] == current_collection]

#     zarr_path = f"/nrs/cellmap/data/{current_collection}/{current_collection}.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"
#     for _, row in collection_df.iterrows():
#         crop = f"crop{row.crop_number}"
#         crop_path = f"{zarr_path}/recon-1/labels/groundtruth/{crop}"
#         for dataset_name in os.listdir(crop_path):
#             if not os.path.isdir(f"{crop_path}/{dataset_name}"):
#                 continue

#             if is_dir_empty_except_hidden(f"{crop_path}/{dataset_name}/s0"):
#                 continue

#             if dataset_name != "all":
#                 data = ImageDataInterface(
#                     f"{zarr_path}/recon-1/labels/groundtruth/{crop}/{dataset_name}/s0"
#                 ).to_ndarray_ts()
#                 ids, counts = np.unique(data[data > 0], return_counts=True)
#                 if np.min(counts) <= 5:
#                     print(
#                         f"{current_collection}/{crop}/{dataset_name} has small objects: {ids[counts<=5]}"
#                     )


# # ### Write out with links

# # In[59]:


# import pandas as pd

# df = pd.read_csv(
#     "/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/SegmentationChallengeWithNeuroglancerURLs.csv"
# )

# # copy the above to isolated_pixels.txt
# with open("isolated_pixels.txt", "r") as file:
#     file_contents = file.read()

# output = []
# # split at newlines
# file_contents = file_contents.replace("\n", "")
# file_contents = file_contents.split("]")[:-1]
# for line in file_contents:
#     line = line.replace(" has small objects: ", "")
#     path_info, small_object_ids = line.split("[")
#     if "..." in small_object_ids:
#         # then there are lots of small objects:
#         small_object_ids = "lots (thousands?)"
#     else:
#         small_object_ids = [int(num) for num in small_object_ids.split()]
#     dataset, crop, dataset_name = path_info.split("/")
#     url = df.loc[
#         (df["collection"] == dataset) & (df["crop_number"] == crop), "url"
#     ].iloc[0]
#     # read in csv

#     output.append([dataset, crop, dataset_name, url, small_object_ids])

# # write out csv
# df = pd.DataFrame(
#     output,
#     columns=["collection", "crop_number", "dataset_name", "url", "small_object_ids"],
# )
# df.to_csv("isolated_pixels.csv", index=False)


# # ### Recheck with connectivity 2

# # In[1]:


# from image_data_interface import ImageDataInterface
# import pandas as pd
# import os
# import numpy as np
# import logging
# import skimage
# import dask.dataframe as dd
# from dask.distributed import Client, LocalCluster

# cluster = LocalCluster(n_workers=8, threads_per_worker=1)


# def process_rows(rows):
#     logging.getLogger().setLevel(logging.WARNING)
#     # create empty dataframe that matches the input
#     output = []
#     for _, row in rows.iterrows():
#         collection = row["collection"]
#         crop = row["crop_number"]
#         dataset_name = row["dataset_name"]
#         url = row["url"]
#         try:
#             data = ImageDataInterface(
#                 f"/nrs/cellmap/data/{collection}/{collection}.zarr/recon-1/labels/groundtruth/{crop}/{dataset_name}/s0"
#             ).to_ndarray_ts()
#         except:
#             raise Exception(
#                 f"Nothing found at path /nrs/cellmap/data/{collection}/{collection}.zarr/recon-1/labels/groundtruth/{crop}/{dataset_name}/s0"
#             )
#         reconnected = skimage.measure.label(data > 0, connectivity=2)
#         ids, counts = np.unique(reconnected[reconnected > 0], return_counts=True)
#         ids = ids[counts <= 5]

#         small_ids = []
#         for id in ids:
#             original_id = np.unique(data[reconnected == id])[0]
#             small_ids.append(original_id)

#         if len(small_ids) > 0:
#             output.append(
#                 [
#                     collection,
#                     crop,
#                     dataset_name,
#                     url,
#                     small_ids,
#                 ]
#             )
#     return pd.DataFrame(output, columns=rows.columns)


# output = []
# isolated_pixels_connectivity_1_df = pd.read_csv(
#     "/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/isolated_pixels_connectivity_1.csv"
# )

# # get local ip

# with Client(cluster) as client:

#     ddf = dd.from_pandas(isolated_pixels_connectivity_1_df, npartitions=100)
#     print(f'Check {client.cluster.dashboard_link.replace("127.0.0.1","ackermand-ws2")}')
#     result_ddf = ddf.map_partitions(process_rows, meta=ddf)
#     output = result_ddf.compute()
# # for _, row in tqdm(
# #     isolated_pixels_connectivity_1_df.iterrows(),
# #     total=len(isolated_pixels_connectivity_1_df),
# # ):
# #     collection = row["collection"]
# #     crop = row["crop_number"]
# #     dataset_name = row["dataset_name"]
# #     url = row["url"]
# #     data = ImageDataInterface(
# #         f"/nrs/cellmap/data/{collection}/{collection}.zarr/recon-1/labels/groundtruth/{crop}/{dataset_name}/s0"
# #     ).to_ndarray_ts()
# #     reconnected = skimage.measure.label(data > 0, connectivity=2)
# #     ids, counts = np.unique(reconnected[reconnected > 0], return_counts=True)
# #     ids = ids[counts <= 5]

# #     small_ids = []
# #     for id in ids:
# #         original_id = np.unique(data[reconnected == id])[0]
# #         small_ids.append(original_id)
# #     if len(small_ids) > 0:
# #         output.append(
# #             [
# #                 collection,
# #                 crop,
# #                 dataset_name,
# #                 url,
# #                 small_ids,
# #             ]
# #         )


# # In[10]:


# output


# # In[8]:


# process_rows(isolated_pixels_connectivity_1_df)


# # In[1]:


# import socket

# # Get the local hostname
# hostname = socket.gethostname()


# # In[2]:


# hostname


# # In[ ]:
# %%
import numpy as np

np.max(ds.roi.shape) / (4 * 1000)
# %%
import zarr
import s3fs

# Set up S3 filesystem
fs = s3fs.S3FileSystem(anon=True)  # anon=True for public buckets

# Path to your Zarr dataset
s3_path = f"s3://janelia-cosem-datasets/jrc_macrophage-2/jrc_macrophage-2.zarr"

# Open the dataset
zarr_store = zarr.storage.FSStore(s3_path)

import zarr

ds = zarr.open(f"s3://janelia-cosem-datasets/jrc_macrophage-2/jrc_macrophage-2.zarr")
# %%
ds["reccon-1/em/fibsem-uint8"]
# %%
zarr_store["recon-1/em/fibsem-uint8"]

# %%

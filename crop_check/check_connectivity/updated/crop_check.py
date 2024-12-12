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
# # for dataset in *; do cd $dataset; for crop in *; do cd $crop; for organelle in *; do bsub -n 4 -P cellmap -o /dev/null -e /dev/null meshify -n 4 $organelle; done; cd ..; done; sleep 90s; cd ..; done;
# # %%

# import pandas as pd
# import os
# import yaml
# import shutil

# organelles_that_failed = [
#     "jrc_cos7-1a/crop243/all",
#     "jrc_cos7-1a/crop243/ecs",
#     "jrc_cos7-1a/crop292/cell",
#     "jrc_fly-mb-1a/crop123/cell",
#     "jrc_fly-mb-1a/crop134/cell",
#     "jrc_hela-2/crop54/all",
#     "jrc_hela-2/crop54/ecs",
#     "jrc_hela-2/crop55/all",
#     "jrc_hela-2/crop55/ecs",
#     "jrc_hela-2/crop56/all",
#     "jrc_hela-2/crop56/ecs",
#     "jrc_hela-2/crop57/all",
#     "jrc_hela-2/crop57/ecs",
#     "jrc_hela-2/crop58/all",
#     "jrc_hela-2/crop58/ecs",
#     "jrc_hela-2/crop59/all",
#     "jrc_hela-2/crop59/ecs",
#     "jrc_hela-2/crop94/all",
#     "jrc_hela-2/crop94/nuc",
#     "jrc_hela-2/crop95/all",
#     "jrc_hela-2/crop95/nuc",
#     "jrc_hela-2/crop96/all",
#     "jrc_hela-2/crop96/nuc",
#     "jrc_hela-2/crop113/all",
#     "jrc_hela-2/crop113/cyto",
#     "jrc_hela-2/crop113/er_mem",
#     "jrc_hela-2/crop113/er_mem_all",
#     "jrc_hela-2/crop155/all",
#     "jrc_hela-2/crop155/chrom",
#     "jrc_hela-2/crop155/er_mem_all",
#     "jrc_hela-2/crop155/hchrom",
#     "jrc_hela-2/crop155/nucpl",
#     "jrc_hela-3/crop60/all",
#     "jrc_hela-3/crop60/ecs",
#     "jrc_hela-3/crop61/all",
#     "jrc_hela-3/crop61/ecs",
#     "jrc_hela-3/crop62/all",
#     "jrc_hela-3/crop62/ecs",
#     "jrc_hela-3/crop63/all",
#     "jrc_hela-3/crop63/ecs",
#     "jrc_hela-3/crop64/all",
#     "jrc_hela-3/crop64/ecs",
#     "jrc_hela-3/crop65/all",
#     "jrc_hela-3/crop65/ecs",
#     "jrc_hela-3/crop85/all",
#     "jrc_hela-3/crop85/nuc",
#     "jrc_hela-3/crop86/all",
#     "jrc_hela-3/crop86/nuc",
#     "jrc_hela-3/crop87/all",
#     "jrc_hela-3/crop87/nuc",
#     "jrc_hela-3/crop111/all",
#     "jrc_hela-3/crop111/cyto",
#     "jrc_hela-3/crop111/er_mem",
#     "jrc_hela-3/crop111/er_mem_all",
#     "jrc_jurkat-1/crop66/all",
#     "jrc_jurkat-1/crop66/ecs",
#     "jrc_jurkat-1/crop67/all",
#     "jrc_jurkat-1/crop67/ecs",
#     "jrc_jurkat-1/crop68/all",
#     "jrc_jurkat-1/crop68/ecs",
#     "jrc_jurkat-1/crop69/all",
#     "jrc_jurkat-1/crop69/ecs",
#     "jrc_jurkat-1/crop70/all",
#     "jrc_jurkat-1/crop70/ecs",
#     "jrc_jurkat-1/crop71/all",
#     "jrc_jurkat-1/crop71/ecs",
#     "jrc_jurkat-1/crop91/all",
#     "jrc_jurkat-1/crop91/nuc",
#     "jrc_jurkat-1/crop92/all",
#     "jrc_jurkat-1/crop92/nuc",
#     "jrc_jurkat-1/crop93/all",
#     "jrc_jurkat-1/crop93/nuc",
#     "jrc_macrophage-2/crop72/all",
#     "jrc_macrophage-2/crop72/ecs",
#     "jrc_macrophage-2/crop73/all",
#     "jrc_macrophage-2/crop73/ecs",
#     "jrc_macrophage-2/crop74/all",
#     "jrc_macrophage-2/crop74/ecs",
#     "jrc_macrophage-2/crop75/all",
#     "jrc_macrophage-2/crop75/ecs",
#     "jrc_macrophage-2/crop76/all",
#     "jrc_macrophage-2/crop76/ecs",
#     "jrc_macrophage-2/crop77/all",
#     "jrc_macrophage-2/crop77/ecs",
#     "jrc_macrophage-2/crop88/all",
#     "jrc_macrophage-2/crop88/nuc",
#     "jrc_macrophage-2/crop89/all",
#     "jrc_macrophage-2/crop89/nuc",
#     "jrc_macrophage-2/crop90/all",
#     "jrc_macrophage-2/crop90/nuc",
#     "jrc_macrophage-2/crop110/all",
#     "jrc_macrophage-2/crop110/cyto",
#     "jrc_macrophage-2/crop110/er_mem",
#     "jrc_macrophage-2/crop110/er_mem_all",
#     "jrc_mus-kidney/crop156/er_mem",
#     "jrc_mus-kidney/crop158/cell",
#     "jrc_mus-kidney/crop159/cell",
#     "jrc_mus-liver/crop132/cell",
#     "jrc_mus-liver/crop135/cell",
#     "jrc_mus-liver/crop136/cell",
#     "jrc_mus-liver/crop137/cell",
#     "jrc_mus-liver/crop138/cell",
#     "jrc_mus-liver/crop142/cell",
#     "jrc_mus-liver/crop143/cell",
#     "jrc_mus-liver/crop145/all",
#     "jrc_mus-liver/crop145/cyto",
#     "jrc_mus-liver/crop145/er_mem",
#     "jrc_mus-liver/crop145/er_mem_all",
#     "jrc_mus-liver/crop150/cell",
#     "jrc_mus-liver/crop151/cell",
#     "jrc_mus-liver-zon-1/crop266/cell",
#     "jrc_mus-liver-zon-1/crop268/cell",
#     "jrc_mus-liver-zon-1/crop269/cell",
#     "jrc_mus-liver-zon-1/crop270/cell",
#     "jrc_mus-liver-zon-1/crop272/cell",
#     "jrc_mus-liver-zon-1/crop273/cell",
#     "jrc_mus-liver-zon-1/crop274/cell",
#     "jrc_mus-liver-zon-1/crop275/cell",
#     "jrc_mus-liver-zon-1/crop276/cell",
#     "jrc_mus-liver-zon-1/crop280/all",
#     "jrc_mus-liver-zon-1/crop280/cell",
#     "jrc_mus-liver-zon-1/crop280/er_mem",
#     "jrc_mus-liver-zon-1/crop280/er_mem_all",
#     "jrc_mus-liver-zon-1/crop313/cell",
#     "jrc_mus-liver-zon-1/crop322/all",
#     "jrc_mus-liver-zon-1/crop322/cell",
#     "jrc_mus-liver-zon-1/crop322/er_mem",
#     "jrc_mus-liver-zon-1/crop322/er_mem_all",
#     "jrc_mus-liver-zon-1/crop323/cell",
#     "jrc_mus-liver-zon-1/crop325/cell",
#     "jrc_mus-liver-zon-1/crop325/mito_mem",
#     "jrc_mus-liver-zon-1/crop326/cell",
#     "jrc_mus-liver-zon-1/crop377/cell",
#     "jrc_mus-liver-zon-2/crop340/cell",
#     "jrc_mus-liver-zon-2/crop368/cell",
#     "jrc_mus-liver-zon-2/crop369/cell",
#     "jrc_mus-liver-zon-2/crop370/cell",
#     "jrc_mus-liver-zon-2/crop376/cell",
#     "jrc_mus-nacc-1/crop115/all",
#     "jrc_mus-nacc-1/crop115/cyto",
#     "jrc_sum159-1/crop25/all",
#     "jrc_sum159-1/crop25/ecs",
#     "jrc_sum159-1/crop26/all",
#     "jrc_sum159-1/crop26/ecs",
#     "jrc_sum159-1/crop81/all",
#     "jrc_sum159-1/crop81/ecs",
#     "jrc_sum159-1/crop82/all",
#     "jrc_sum159-1/crop82/ecs",
#     "jrc_sum159-1/crop83/all",
#     "jrc_sum159-1/crop83/ecs",
#     "jrc_sum159-1/crop84/all",
#     "jrc_sum159-1/crop84/ecs",
#     "jrc_sum159-1/crop97/all",
#     "jrc_sum159-1/crop97/nuc",
#     "jrc_sum159-1/crop98/all",
#     "jrc_sum159-1/crop98/nuc",
#     "jrc_sum159-1/crop99/all",
#     "jrc_sum159-1/crop99/nuc",
#     "jrc_ut21-1413-003/crop192/all",
#     "jrc_ut21-1413-003/crop192/pm",
#     "jrc_ut21-1413-003/crop198/cell",
#     "jrc_ut21-1413-003/crop199/cell",
# ]
# for organelle_that_failed in organelles_that_failed:
#     print(organelle_that_failed)
#     current_collection, crop, dataset_name = organelle_that_failed.split("/")
#     zarr_path = f"/nrs/cellmap/zubovy/crop_splits/combined/{current_collection}/groundtruth.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"
#     crop_dir = f"/groups/cellmap/cellmap/ackermand/new_meshes/scripts/single_resolution/crop_check_updated_20241019/{current_collection}/{crop}/{dataset_name}"
#     os.makedirs(name=crop_dir, exist_ok=True)
#     os.system(f"cp ../../default_meshes/dask-config.yaml {crop_dir}")
#     if os.path.exists(
#         f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_20241019/{current_collection}/{crop}/{dataset_name}"
#     ):
#         shutil.rmtree(
#             f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_20241019/{current_collection}/{crop}/{dataset_name}"
#         )
#     run_config_yaml = {
#         # "input_path": f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr/{crop}/{dataset_name}/s0",
#         "input_path": f"{zarr_path}/{crop}/{dataset_name}/s0",
#         "output_directory": f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_20241019/{current_collection}/{crop}/{dataset_name}",
#         "read_write_roi": ["0:512", "0:512", "0:512"],
#         "downsample_factor": 0,
#         "target_reduction": 0.5,
#         "n_smoothing_iter": 0,
#         "remove_smallest_components": False,
#         "do_analysis": False,
#         "do_legacy_neuroglancer": True,
#     }

#     with open(f"{crop_dir}/run-config.yaml", "w") as f:
#         yaml.dump(run_config_yaml, f)

# # %%
# # print out command:
# for organelle_that_failed in organelles_that_failed:
#     current_collection, crop, dataset_name = organelle_that_failed.split("/")
#     print(
#         f"cd /groups/cellmap/cellmap/ackermand/new_meshes/scripts/single_resolution/crop_check_updated_20241019/{current_collection}/{crop}; bsub -n 12 -P cellmap -o /dev/null -e /dev/null meshify -n 12 {dataset_name}; "
#     )

# import pandas as pd
# import neuroglancer
# from funlib.persistence import open_ds
# import os


# def add_local_volumes(collection, crop, state):
#     zarr_path = f"/nrs/cellmap/bennettd/data/crop_tests/{collection}.zarr"
#     print(f"{zarr_path}/{crop}")
#     for dataset_name in os.listdir(f"{zarr_path}/{crop}"):
#         if not os.path.isdir(f"{zarr_path}/{crop}/{dataset_name}"):
#             continue
#         ds = open_ds(zarr_path, crop + f"/{dataset_name}/s0")
#         data = ds.to_ndarray()
#         local_volume = neuroglancer.LocalVolume(
#             data=data,
#             dimensions=neuroglancer.CoordinateSpace(
#                 names=["z", "y", "x"],
#                 units=["nm", "nm", "nm"],
#                 scales=ds.voxel_size,
#                 coordinate_arrays=[
#                     None,
#                     None,
#                     None,
#                 ],
#             ),
#             voxel_offset=ds.roi.begin,
#         )

#         state.layers.append(name=f"{crop}_{dataset_name}", layer=local_volume)


# df = pd.read_csv(
#     "/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/Segmentation Challenge _ List of Collections and Crops  - Sheet1.csv"
# )

# collections = df.collection.unique()
# neuroglancer.set_server_bind_address("0.0.0.0")
# viewer = neuroglancer.Viewer()

# current_collection = collections[0]
# with viewer.txn() as s:
#     s.layers["raw"] = neuroglancer.ImageLayer(
#         source="precomputed://gs://neuroglancer-public-data/flyem_fib-25/image",
#     )
#     for _, row in df.iterrows():
#         if row.collection == current_collection:
#             crop = f"crop{row.crop_number}"
#             add_local_volumes(row.collection, crop, s)
# print(viewer)

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


df = pd.read_csv(
    "/groups/scicompsoft/home/ackermand/Programming/segmentation_challenge_meshes/crop_check/check_connectivity/updated/Segmentation Challenge _ List of Collections and Crops  - Sheet1.csv"
)
df = df[df.collection != "jrc_mus-pancreas-1"]
df.sort_values(by=["collection", "crop_number"], inplace=True)
collections = df.collection.unique()

assert len(collections) == 22
all_info = []
failed_meshification = []
only_do_alls = True
for current_collection in collections:
    if only_do_alls:
        output_dir = (
            f"/nrs/cellmap/ackermand/crop_check_updated_{date}/viewer_states_all/"
        )
    else:
        output_dir = f"/nrs/cellmap/ackermand/crop_check_updated_{date}/viewer_states/{current_collection}"
    os.makedirs(output_dir, exist_ok=True)

    collection_df = df[df["collection"] == current_collection]

    zarr_path = f"/nrs/cellmap/zubovy/crop_splits/new/combined//{current_collection}/groundtruth.zarr"  # f"/nrs/cellmap/bennettd/data/crop_tests/{current_collection}.zarr"

    viewer = neuroglancer.Viewer()
    with viewer.txn() as s:
        s.layers["raw"] = neuroglancer.ImageLayer(
            source=f"zarr://s3://janelia-cosem-datasets/{current_collection}/{current_collection}.zarr/recon-1/em/fibsem-uint8",
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
            except Exception as e:
                print(e, f"{current_collection}/{crop}/{dataset_name}")
                failed_meshification.append(
                    f"{current_collection}/{crop}/{dataset_name}"
                )
                pass
            image_source = f'zarr://https://cellmap-vm1.int.janelia.org/{crop_path.replace("/nrs/cellmap/","/nrs/")}/{dataset_name}'
            if solid_chunk:
                s.layers[f"{crop}_{dataset_name}"] = neuroglancer.SegmentationLayer(
                    source=[image_source]
                )
            else:
                mesh_source = f"precomputed://https://cellmap-vm1.int.janelia.org/nrs/ackermand/new_meshes/meshes/crop_check_updated_{date}/{current_collection}/{crop}/{dataset_name}/meshes"
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

    # Assuming viewer.state.to_json() returns a dictionary
    viewer_state = s.to_json()

    if only_do_alls:
        first_seg_layer = True
        for layer in viewer_state["layers"]:
            if layer["type"] == "segmentation":
                if not first_seg_layer and "segments" in layer:
                    # then has a mesh layer, but we want meshes off
                    layer["segments"] = [f"!{seg}" for seg in layer["segments"]]
                first_seg_layer = False

    # Write the JSON data to a file
    with open(f"{output_dir}/{current_collection}.json", "w") as json_file:
        json.dump(viewer_state, json_file, indent=4)
    if only_do_alls:
        neuroglancer_url = f"https://neuroglancer-demo.appspot.com#!https://cellmap-vm1.int.janelia.org/nrs/ackermand/crop_check_updated_{date}/viewer_states_all/{current_collection}.json"
        all_info.append((current_collection, neuroglancer_url))
    else:
        neuroglancer_url = f"https://neuroglancer-demo.appspot.com#!https://cellmap-vm1.int.janelia.org/nrs/ackermand/crop_check_updated_{date}/viewer_states/{current_collection}/{crop}.json"
        all_info.append((crop, current_collection, neuroglancer_url))

import pandas as pd

if only_do_alls:
    df = pd.DataFrame(all_info, columns=["collection", "url"])
    df.to_csv(f"SegmentationChallengeWithNeuroglancerURLs_all_{date}.csv", index=False)
else:
    df = pd.DataFrame(all_info, columns=["crop_number", "collection", "url"])
    df.to_csv(f"SegmentationChallengeWithNeuroglancerURLs_{date}.csv", index=False)


# if len(failed_meshification) > 0:
failed_df = pd.DataFrame(failed_meshification, columns=["failed_paths"])
failed_df.to_csv(f"failed_meshification_{date}.csv", index=False)
print(failed_df)
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

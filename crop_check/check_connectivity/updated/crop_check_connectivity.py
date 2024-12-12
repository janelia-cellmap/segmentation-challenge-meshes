#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import os
import yaml


def is_dir_empty_except_hidden(path):
    return all(f.startswith(".") for f in os.listdir(path))


collections = ["jrc_mus-nacc-1", "jrc_zf-cardiac-1", "jrc_zf-cardiac-1"]
crops = [
    "crop115",
    "crop378",
    "crop380",
]  # [crop115_c1.zarr  crop115_c2.zarr  crop115_c3.zarr  crop378_c1.zarr  crop378_c2.zarr  crop380_c1.zarr  crop380_c2.zarr", "crop380_c3.zarr""]
for collection, current_crop in zip(collections, crops):
    for connectivity in [1, 2, 3]:
        crop = f"{current_crop}_c{connectivity}"
        zarr_path = f"/nrs/cellmap/bennettd/tmp/{crop}.zarr"
        if os.path.isdir(zarr_path):
            for dataset_name in os.listdir(f"{zarr_path}"):
                if not os.path.isdir(f"{zarr_path}/{dataset_name}"):
                    continue

                if is_dir_empty_except_hidden(f"{zarr_path}/{dataset_name}/s0"):
                    continue

                crop_dir = f"/groups/cellmap/cellmap/ackermand/new_meshes/scripts/single_resolution/crop_check_connectivity/{collection}/{crop}/{dataset_name}"
                os.makedirs(name=crop_dir, exist_ok=True)
                os.system(f"cp ./default_meshes/dask-config.yaml {crop_dir}")

                run_config_yaml = {
                    "input_path": f"{zarr_path}/{dataset_name}/s0",
                    "output_directory": f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_connectivity/{collection}/{crop}/{dataset_name}",
                    "read_write_roi": ["0:256", "0:256", "0:256"],
                    "downsample_factor": 0,
                    "target_reduction": 0.0,
                    "n_smoothing_iter": 0,
                    "remove_smallest_components": False,
                    "do_analysis": False,
                    "do_legacy_neuroglancer": True,
                }

                with open(f"{crop_dir}/run-config.yaml", "w") as f:
                    yaml.dump(run_config_yaml, f)


# In[22]:


import pandas as pd
import neuroglancer
import os
from funlib.persistence import open_ds
import json


def is_dir_empty_except_hidden(path):
    return all(f.startswith(".") for f in os.listdir(path))


neuroglancer.set_server_bind_address("0.0.0.0")


all_info = []
collections = ["jrc_mus-nacc-1", "jrc_zf-cardiac-1", "jrc_zf-cardiac-1"]
crops = [
    "crop115",
    "crop378",
    "crop380",
]  # [crop115_c1.zarr  crop115_c2.zarr  crop115_c3.zarr  crop378_c1.zarr  crop378_c2.zarr  crop380_c1.zarr  crop380_c2.zarr", "crop380_c3.zarr""]
for collection, current_crop in zip(collections, crops):
    for connectivity in [1, 2, 3]:
        crop = f"{current_crop}_c{connectivity}"
        output_dir = (
            f"/nrs/cellmap/ackermand/crop_check_connectivity/viewer_states/{collection}"
        )
        os.makedirs(output_dir, exist_ok=True)
        zarr_path = f"/nrs/cellmap/bennettd/tmp/{crop}.zarr"
        viewer = neuroglancer.Viewer()
        have_set_position = False
        with viewer.txn() as s:
            s.layers["raw"] = neuroglancer.ImageLayer(
                source=f"zarr://https://cellmap-vm1.int.janelia.org/nrs/data/{collection}/{collection}.zarr/recon-1/em/fibsem-uint8",
            )
            if os.path.isdir(zarr_path):
                for dataset_name in os.listdir(f"{zarr_path}"):
                    if not os.path.isdir(f"{zarr_path}/{dataset_name}"):
                        continue

                    if is_dir_empty_except_hidden(f"{zarr_path}/{dataset_name}/s0"):
                        continue

                    segments = [
                        int(mesh_id.split(":0")[0])
                        for mesh_id in os.listdir(
                            f"/nrs/cellmap/ackermand/new_meshes/meshes/crop_check_connectivity/{collection}/{crop}/{dataset_name}/meshes"
                        )
                        if ":0" in mesh_id
                    ]
                    image_source = f"zarr://https://cellmap-vm1.int.janelia.org/nrs/bennettd/tmp/{crop}.zarr/{dataset_name}"
                    mesh_source = f"precomputed://https://cellmap-vm1.int.janelia.org/nrs/ackermand/new_meshes/meshes/crop_check_connectivity/{collection}/{crop}/{dataset_name}/meshes"
                    s.layers[f"{crop}_{dataset_name}"] = neuroglancer.SegmentationLayer(
                        source=[image_source, mesh_source], segments=segments
                    )

                    if dataset_name != "all":
                        s.layers[f"{crop}_{dataset_name}"].visible = False

                    if not have_set_position:
                        ds = open_ds(
                            f"/nrs//cellmap/bennettd/tmp/{crop}.zarr",
                            f"{dataset_name}/s0",
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
        neuroglancer_url = f"https://neuroglancer-demo.appspot.com#!https://cellmap-vm1.int.janelia.org/nrs/ackermand/crop_check_connectivity/viewer_states/{collection}/{crop}.json"
        all_info.append((crop, collection, neuroglancer_url))

df = pd.DataFrame(all_info, columns=["crop_number", "collection", "url"])

df.to_csv("SegmentationChallengeCheckConnectivityWithNeuroglancerURLs.csv", index=False)


# In[ ]:





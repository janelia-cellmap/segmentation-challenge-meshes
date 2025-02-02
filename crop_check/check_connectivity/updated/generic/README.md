1. Create a csv file formatted like this:
    ```
    crop_number,collection
    986,jrc_ctl-id8-1
    ```
2. cd to `/groups/scicompsoft/home/ackermand/Programming/segmentation-challenge-meshes/crop_check/check_connectivity/updated/generic` and run `python crop_check.py generate-mesh-config -c /nrs/cellmap/zubovy/crop_splits_2025/combined/ -i test_crops_20250128.csv`
where -c is the path to the crop location and the csv is the aforementioned csv. This will create the mesh configs
3. Login to submit node and change to the same directory. run `python cluster_submission.py -p /groups/cellmap/cellmap/ackermand/new_meshes/scripts/single_resolution/crop_check_updated_20250128/`. This will run the meshifcation but will ensure that not too many are run at once to prevent it from hanging by having all possible cores being used and drivers waiting for workers.
4. Run `python crop_check.py get-neuroglancer-links -c /nrs/cellmap/zubovy/crop_splits_2025/combined/ -i test_crops_20250131.csv -p /nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_20250131` to get neuroglancer links containing everything. Will also spit out if anything failed.
5. Run `python crop_check_connectivity.py -c /nrs/cellmap/zubovy/crop_splits_2025/combined/ -i test_crops_20250131.csv -p /nrs/cellmap/ackermand/new_meshes/meshes/crop_check_updated_20250131 -o ./` to check connectivities and make sure that there aren't small isolated chunks
#!/usr/bin/env bash

# Script to inspect the HDF5 file structure

echo "Inspecting HDF5 file structure..."
docker exec -it lakesoul-ann-spark python3 - << 'EOF'
import h5py

# Open the HDF5 file
file_path = "/data/embeddings/fashion-mnist-784-euclidean.hdf5"
h5f = h5py.File(file_path, 'r')

# Print the root level keys
print("\nRoot level keys in the HDF5 file:")
print("=================================")
for key in h5f.keys():
    print(f"- {key}")
    
    # Check if it's a group or dataset
    item = h5f[key]
    if isinstance(item, h5py.Group):
        print(f"  (Group with keys: {list(item.keys())})")
    elif isinstance(item, h5py.Dataset):
        print(f"  (Dataset with shape: {item.shape}, dtype: {item.dtype})")
        
        # If the dataset is small enough, print a few sample values
        if len(item.shape) > 0 and item.shape[0] > 0:
            sample_size = min(3, item.shape[0])
            print(f"  Sample data (first {sample_size} items):")
            print(f"  {item[:sample_size]}")

# Check specific structure for ANN benchmark datasets
print("\nDetail of specific datasets:")
print("==========================")

# Common dataset names in ANN benchmark files
possible_datasets = ['train', 'test', 'distances', 'neighbors', 'labels']

for dataset in possible_datasets:
    if dataset in h5f:
        ds = h5f[dataset]
        print(f"Dataset '{dataset}':")
        print(f"  - Shape: {ds.shape}")
        print(f"  - Type: {ds.dtype}")
        if len(ds.shape) > 0 and ds.shape[0] > 0:
            sample_size = min(2, ds.shape[0])
            if len(ds.shape) == 1 or ds.shape[1] < 10:
                print(f"  - First {sample_size} entries: {ds[:sample_size]}")
            else:
                print(f"  - First entry (truncated): {ds[0][:5]}...")

print("\nDone inspecting HDF5 file.")
h5f.close()
EOF

echo "Inspection complete." 
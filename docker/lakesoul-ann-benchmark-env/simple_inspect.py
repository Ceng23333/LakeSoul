import h5py

# Open the HDF5 file
file_path = "data/embeddings/fashion-mnist-784-euclidean.hdf5"
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

print("\nDone inspecting HDF5 file.")
h5f.close() 
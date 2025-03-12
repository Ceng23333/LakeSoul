#!/usr/bin/env bash

set -e

LAKESOUL_VERSION=2.6.2-SNAPSHOT
SPARK_LAKESOUL_JAR=lakesoul-spark-3.3-${LAKESOUL_VERSION}.jar
SPARK_LAKESOUL_TEST_JAR=lakesoul-spark-3.3-${LAKESOUL_VERSION}-tests.jar

download_hdf5_file() {
    local FILE_NAME=$1
    local EXPECTED_MD5=$2
    local DOWNLOAD_URL=$3

    if [[ -f ${FILE_NAME} ]]; then
        local FILE_MD5
        echo "compare md5 of ${FILE_NAME} with ${EXPECTED_MD5}"
        FILE_MD5=$(md5sum ${FILE_NAME} | awk '{print $1}')
        if [[ "${FILE_MD5}" != "${EXPECTED_MD5}" ]]; then
            echo "md5 not match, expected: ${EXPECTED_MD5}, actual: ${FILE_MD5}"
            rm "${FILE_NAME}"
            wget "${DOWNLOAD_URL}/${FILE_NAME}"
        fi
    else
        echo "download ${FILE_NAME} "
        wget "${DOWNLOAD_URL}/${FILE_NAME}"
    fi
}

mkdir -p data/embeddings

download_hdf5_file "fashion-mnist-784-euclidean.hdf5" "23249362dbd58a321c05b1a85275adba" "http://ann-benchmarks.com"
cp fashion-mnist-784-euclidean.hdf5 data/embeddings/

# First check if Python 3.8 is available
if ! command -v python3.8 &> /dev/null; then
    echo "Error: Python 3.8 is required to prepare dependencies for the container"
    echo "Please install Python 3.8 and try again"
    exit 1
fi

echo "Preparing Python 3.8 environment for dependencies..."
# Create a Python 3.8 virtual environment
if [ ! -d ".venv38" ]; then
    python3.8 -m venv .venv38
    source .venv38/bin/activate
    pip install --upgrade pip
    # Install necessary Python packages (excluding pyspark which is already in the container)
    pip install numpy pandas h5py scipy scikit-learn
    echo "Python 3.8 environment created and packages installed"
else
    source .venv38/bin/activate
    # Make sure all packages are up to date (excluding pyspark)
    pip install --upgrade numpy pandas h5py scipy scikit-learn
    echo "Using existing Python 3.8 environment"
fi

# Create a directory with Python 3.8 site-packages to copy to the container
SITE_PACKAGES_DIR=$(python -c "import site; print(site.getsitepackages()[0])")
echo "Using site-packages from: $SITE_PACKAGES_DIR"

# Prepare a directory with Python packages to mount
mkdir -p .python38_packages
cp -r $SITE_PACKAGES_DIR/* .python38_packages/

# Deactivate the Python 3.8 environment
deactivate

mkdir -p packages/jars


echo "Copying ${SPARK_LAKESOUL_JAR} and ${SPARK_LAKESOUL_TEST_JAR} to packages/jars/"
cp ../../lakesoul-spark/target/${SPARK_LAKESOUL_JAR} packages/jars/
cp ../../lakesoul-spark/target/${SPARK_LAKESOUL_TEST_JAR} packages/jars/
ls -l packages/jars/

# Make the run scripts executable
chmod +x run_ann_load.sh
chmod +x run_ann_query.sh

echo "Start docker-compose..."
# Start docker-compose with environment file
docker compose -f docker-compose.yml --env-file docker-compose.env up -d

# Copy Python 3.8 packages to the container
echo "Copying Python packages to the container..."
docker exec lakesoul-ann-spark mkdir -p /opt/python_packages
docker cp .python38_packages/. lakesoul-ann-spark:/opt/python_packages/

# Add Python packages to PYTHONPATH
docker exec lakesoul-ann-spark bash -c "echo 'export PYTHONPATH=/opt/python_packages:\$PYTHONPATH' >> /opt/bitnami/spark/conf/spark-env.sh"

echo "Waiting for MinIO to be ready..."
sleep 10  # Give MinIO some startup time

# Configure MinIO with proper access control
echo "Setting up MinIO buckets and permissions..."
docker exec lakesoul-ann-minio mc alias set local http://localhost:9000 admin password

# Check if bucket exists before creating it
if ! docker exec lakesoul-ann-minio mc ls local | grep -q lakesoul-test-bucket; then
  docker exec lakesoul-ann-minio mc mb local/lakesoul-test-bucket
  echo "Created new bucket: lakesoul-test-bucket"
else
  echo "Bucket lakesoul-test-bucket already exists, skipping creation"
fi

# Set write policy for the bucket (readwrite access)
docker exec lakesoul-ann-minio mc anonymous set download local/lakesoul-test-bucket
docker exec lakesoul-ann-minio mc anonymous set upload local/lakesoul-test-bucket

# Create the lakesoul-test directory inside the bucket to ensure it exists
docker exec lakesoul-ann-minio touch /tmp/empty.txt && docker cp /tmp/empty.txt lakesoul-ann-minio:/tmp/empty.txt && docker exec lakesoul-ann-minio mc cp /tmp/empty.txt local/lakesoul-test-bucket/lakesoul-test/ && rm /tmp/empty.txt

echo "MinIO configuration completed!"


# bash prepare_ann_table.sh

echo "============================================================================="
echo "Success to launch LSH ANN benchmark environment!"
echo "You can:"
echo "    'bash start_spark_sql.sh' to login into spark"
echo "    'bash run_ann_load.sh' to load data and build the ANN index"
echo "    'bash run_ann_query.sh' to run ANN queries after loading data"
echo "============================================================================="


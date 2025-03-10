#!/usr/bin/env bash

# Script to clean PostgreSQL and MinIO storage for LakeSoul ANN benchmark environment

set -e  # Exit immediately if a command exits with a non-zero status

# Define colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# MinIO configuration
MINIO_CONTAINER="lakesoul-ann-minio"
MINIO_BUCKET="lakesoul-test-bucket"
MINIO_PREFIX="lakesoul-test"  # The prefix/folder where LakeSoul data is stored
MINIO_ALIAS="local"
MINIO_ADMIN="admin"
MINIO_PASSWORD="password"

# PostgreSQL configuration
POSTGRES_CONTAINER="lakesoul-ann-pg"
POSTGRES_USER="lakesoul_test"
POSTGRES_PASSWORD="lakesoul_test"
POSTGRES_DB="lakesoul_test"

# SQL scripts
SQL_DIR="./sql"
CLEANUP_SQL="meta_cleanup.sql"
INIT_SQL="meta_init.sql"

# Print section header
print_header() {
    echo -e "\n${YELLOW}==== $1 ====${NC}"
}

# Print success message
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Print error message
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if SQL files exist
check_sql_files() {
    print_header "Checking SQL scripts"
    
    if [ ! -f "$SQL_DIR/$CLEANUP_SQL" ]; then
        print_error "Cleanup SQL file not found: $SQL_DIR/$CLEANUP_SQL"
        return 1
    else
        print_success "Cleanup SQL file found."
    fi
    
    if [ ! -f "$SQL_DIR/$INIT_SQL" ]; then
        print_error "Init SQL file not found: $SQL_DIR/$INIT_SQL"
        return 1
    else
        print_success "Init SQL file found."
    fi
    
    return 0
}

# Check if containers are running
check_containers() {
    print_header "Checking containers"
    
    local minio_running=$(docker ps --format '{{.Names}}' | grep -w $MINIO_CONTAINER || echo "")
    local postgres_running=$(docker ps --format '{{.Names}}' | grep -w $POSTGRES_CONTAINER || echo "")
    
    if [ -z "$minio_running" ]; then
        print_error "MinIO container ($MINIO_CONTAINER) is not running."
        return 1
    else
        print_success "MinIO container is running."
    fi
    
    if [ -z "$postgres_running" ]; then
        print_error "PostgreSQL container ($POSTGRES_CONTAINER) is not running."
        return 1
    else
        print_success "PostgreSQL container is running."
    fi
    
    return 0
}

# Clean MinIO storage
clean_minio() {
    print_header "Cleaning MinIO storage"
    
    echo "Setting up MinIO client alias..."
    docker exec $MINIO_CONTAINER mc alias set $MINIO_ALIAS http://localhost:9000 $MINIO_ADMIN $MINIO_PASSWORD > /dev/null
    
    # Check if bucket exists
    local bucket_exists=$(docker exec $MINIO_CONTAINER mc ls $MINIO_ALIAS | grep -w $MINIO_BUCKET || echo "")
    
    if [ -z "$bucket_exists" ]; then
        print_error "Bucket $MINIO_BUCKET doesn't exist. Nothing to clean."
    else
        echo "Removing all objects from $MINIO_BUCKET/$MINIO_PREFIX..."
        docker exec $MINIO_CONTAINER mc rm -r --force "$MINIO_ALIAS/$MINIO_BUCKET/$MINIO_PREFIX" > /dev/null
        
        echo "Recreating $MINIO_PREFIX directory..."
        docker exec $MINIO_CONTAINER mc mb -p "$MINIO_ALIAS/$MINIO_BUCKET/$MINIO_PREFIX" > /dev/null
        
        print_success "MinIO storage cleaned successfully."
    fi
}

# Clean and initialize PostgreSQL database
clean_postgres() {
    print_header "Cleaning and initializing PostgreSQL database"
    
    echo "Connecting to PostgreSQL database..."
    
    # Copy SQL files to the container
    echo "Copying SQL scripts to PostgreSQL container..."
    docker cp "$SQL_DIR/$CLEANUP_SQL" "$POSTGRES_CONTAINER:/tmp/$CLEANUP_SQL"
    docker cp "$SQL_DIR/$INIT_SQL" "$POSTGRES_CONTAINER:/tmp/$INIT_SQL"
    
    # Execute cleanup SQL script
    echo "Executing cleanup SQL script..."
    docker exec $POSTGRES_CONTAINER psql -U $POSTGRES_USER -d $POSTGRES_DB -f "/tmp/$CLEANUP_SQL"
    
    # Execute initialization SQL script
    echo "Executing initialization SQL script..."
    docker exec $POSTGRES_CONTAINER psql -U $POSTGRES_USER -d $POSTGRES_DB -f "/tmp/$INIT_SQL"
    
    print_success "PostgreSQL database cleaned and initialized successfully."
}

# Main execution
main() {
    echo "Starting environment cleanup and initialization for LakeSoul ANN benchmark..."
    
    if check_sql_files && check_containers; then
        clean_minio
        clean_postgres
        
        print_header "Cleanup Summary"
        echo "MinIO storage: $MINIO_BUCKET/$MINIO_PREFIX has been cleaned."
        echo "PostgreSQL database: $POSTGRES_DB has been cleaned and initialized using SQL scripts."
        print_success "Environment cleanup and initialization completed successfully!"
        echo "You can now run a fresh ANN benchmark."
    else
        print_error "Environment cleanup failed."
        echo "Please check the error messages above and try again."
        exit 1
    fi
}

# Run the script
main 
#!/bin/bash
# Script to manually build CodeQL database for artdaq
# This script can be run inside a container or development environment

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
CODEQL_DB_NAME="${CODEQL_DB_NAME:-artdaq-codeql-db}"
CODEQL_DB_PATH="${CODEQL_DB_PATH:-./codeql-database}"
BUILD_DIR="${BUILD_DIR:-./build}"
SOURCE_DIR="${SOURCE_DIR:-$(pwd)}"
CODEQL_CONFIG="${CODEQL_CONFIG:-${SOURCE_DIR}/.github/codeql/codeql-config.yml}"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if CodeQL is installed
check_codeql() {
    if ! command -v codeql &> /dev/null; then
        print_error "CodeQL CLI is not installed or not in PATH"
        print_info "Please install CodeQL CLI from: https://github.com/github/codeql-cli-binaries"
        print_info "Or run: gh extension install github/gh-codeql"
        exit 1
    fi
    
    CODEQL_VERSION=$(codeql version --format=text 2>/dev/null | head -n 1 || echo "unknown")
    print_info "Found CodeQL: ${CODEQL_VERSION}"
}

# Function to clean up previous database
cleanup_database() {
    if [ -d "${CODEQL_DB_PATH}" ]; then
        print_warn "Removing existing CodeQL database at ${CODEQL_DB_PATH}"
        rm -rf "${CODEQL_DB_PATH}"
    fi
}

# Function to setup build environment
setup_build_env() {
    print_info "Setting up build environment..."
    
    # Check if we're in a UPS environment
    if [ -n "${CETPKG_SOURCE}" ]; then
        print_info "Detected UPS/cetmodules environment"
        print_info "CETPKG_QUAL: ${CETPKG_QUAL}"
        print_info "CETPKG_TYPE: ${CETPKG_TYPE}"
    else
        print_warn "UPS environment variables not detected"
        print_info "If building with cetmodules, please source setup_for_development first"
    fi
}

# Function to create build directory
setup_build_dir() {
    if [ ! -d "${BUILD_DIR}" ]; then
        print_info "Creating build directory: ${BUILD_DIR}"
        mkdir -p "${BUILD_DIR}"
    fi
}

# Function to build CodeQL database
build_codeql_database() {
    print_info "Building CodeQL database..."
    print_info "Database name: ${CODEQL_DB_NAME}"
    print_info "Database path: ${CODEQL_DB_PATH}"
    print_info "Source directory: ${SOURCE_DIR}"
    
    # Prepare CodeQL command with optional config
    local CONFIG_ARG=""
    if [ -f "${CODEQL_CONFIG}" ]; then
        print_info "Using CodeQL config: ${CODEQL_CONFIG}"
        CONFIG_ARG="--codescanning-config=${CODEQL_CONFIG}"
    fi
    
    # Build the database
    # For CMake projects, CodeQL needs to trace the build
    cd "${BUILD_DIR}"
    
    codeql database create \
        "${CODEQL_DB_PATH}" \
        --language=cpp \
        --source-root="${SOURCE_DIR}" \
        ${CONFIG_ARG} \
        --command="cmake --build . --config Release" \
        --overwrite \
        || {
            print_error "Failed to create CodeQL database"
            print_info "Trying alternative build command..."
            
            # Alternative: trace the entire CMake configure and build
            cd "${SOURCE_DIR}"
            codeql database create \
                "${CODEQL_DB_PATH}" \
                --language=cpp \
                --source-root="${SOURCE_DIR}" \
                ${CONFIG_ARG} \
                --command="bash -c 'cd ${BUILD_DIR} && cmake ${SOURCE_DIR} -DCMAKE_BUILD_TYPE=Release && make -j\$(nproc)'" \
                --overwrite
        }
    
    print_info "CodeQL database created successfully at ${CODEQL_DB_PATH}"
}

# Function to analyze the database (optional)
analyze_database() {
    if [ "$1" = "--analyze" ]; then
        print_info "Running CodeQL analysis..."
        
        local RESULTS_DIR="${CODEQL_DB_PATH}-results"
        mkdir -p "${RESULTS_DIR}"
        
        codeql database analyze \
            "${CODEQL_DB_PATH}" \
            --format=sarif-latest \
            --output="${RESULTS_DIR}/results.sarif" \
            cpp-security-and-quality
        
        print_info "Analysis results saved to ${RESULTS_DIR}/results.sarif"
    fi
}

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Build a CodeQL database for artdaq project.

OPTIONS:
    -h, --help              Show this help message
    -n, --name NAME         Set database name (default: ${CODEQL_DB_NAME})
    -o, --output PATH       Set database output path (default: ${CODEQL_DB_PATH})
    -b, --build-dir PATH    Set build directory (default: ${BUILD_DIR})
    -s, --source-dir PATH   Set source directory (default: ${SOURCE_DIR})
    -c, --config PATH       Set CodeQL config file (default: ${CODEQL_CONFIG})
    --analyze               Run analysis after building database
    --clean                 Clean existing database before building

ENVIRONMENT VARIABLES:
    CODEQL_DB_NAME          Database name
    CODEQL_DB_PATH          Database output path
    BUILD_DIR               Build directory
    SOURCE_DIR              Source directory
    CODEQL_CONFIG           CodeQL configuration file path

EXAMPLES:
    # Basic usage (assumes CodeQL is installed)
    $0

    # Build with custom output path
    $0 --output /tmp/my-codeql-db

    # Build and analyze
    $0 --analyze

    # Clean and rebuild
    $0 --clean

    # In a container with UPS environment
    source /path/to/setup_for_development
    $0 --build-dir ./build --analyze

EOF
}

# Main script
main() {
    local DO_ANALYZE=false
    local DO_CLEAN=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -n|--name)
                CODEQL_DB_NAME="$2"
                shift 2
                ;;
            -o|--output)
                CODEQL_DB_PATH="$2"
                shift 2
                ;;
            -b|--build-dir)
                BUILD_DIR="$2"
                shift 2
                ;;
            -s|--source-dir)
                SOURCE_DIR="$2"
                shift 2
                ;;
            -c|--config)
                CODEQL_CONFIG="$2"
                shift 2
                ;;
            --analyze)
                DO_ANALYZE=true
                shift
                ;;
            --clean)
                DO_CLEAN=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    print_info "=== artdaq CodeQL Database Builder ==="
    
    # Check prerequisites
    check_codeql
    
    # Clean if requested
    if [ "$DO_CLEAN" = true ]; then
        cleanup_database
    fi
    
    # Setup environment
    setup_build_env
    setup_build_dir
    
    # Build CodeQL database
    build_codeql_database
    
    # Optionally analyze
    if [ "$DO_ANALYZE" = true ]; then
        analyze_database --analyze
    fi
    
    print_info "=== Complete ==="
    print_info "CodeQL database ready at: ${CODEQL_DB_PATH}"
    print_info ""
    print_info "Next steps:"
    print_info "  1. Analyze: codeql database analyze ${CODEQL_DB_PATH} --format=sarif-latest --output=results.sarif"
    print_info "  2. Upload to GitHub: codeql github upload-results --sarif=results.sarif"
    print_info "  3. Or use GitHub Code Scanning UI for analysis"
}

# Run main function
main "$@"

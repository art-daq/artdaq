# CodeQL Database Building for artdaq

This directory contains configuration and documentation for building CodeQL databases for security analysis of the artdaq project.

## Overview

CodeQL is GitHub's semantic code analysis engine that helps find security vulnerabilities and coding errors. This setup enables manual building of CodeQL databases within container environments or local development setups.

## Prerequisites

### Installing CodeQL CLI

You have several options to install the CodeQL CLI:

1. **Via GitHub CLI** (recommended):
   ```bash
   gh extension install github/gh-codeql
   ```

2. **Direct Download**:
   - Download from: https://github.com/github/codeql-cli-binaries/releases
   - Extract and add to PATH:
     ```bash
     export PATH="/path/to/codeql:$PATH"
     ```

3. **In a Container**:
   ```bash
   # Add to your Dockerfile or run in container
   curl -L https://github.com/github/codeql-cli-binaries/releases/latest/download/codeql-linux64.zip -o codeql.zip
   unzip codeql.zip -d /opt
   export PATH="/opt/codeql:$PATH"
   ```

### Build Environment

artdaq uses the UPS (Unified Package System) and cetmodules for building. Before running CodeQL analysis, ensure your build environment is set up:

```bash
# Source the UPS setup script
source /path/to/artdaq/ups/setup_for_development

# Or if in a container with pre-configured environment
# The environment variables should already be set
```

## Quick Start

### Basic Usage

```bash
# Navigate to artdaq source directory
cd /path/to/artdaq

# Run the CodeQL database builder
./tools/build-codeql-database.sh
```

This will:
1. Create a build directory if needed
2. Configure and build the project while tracing with CodeQL
3. Generate a CodeQL database at `./codeql-database`

### Build and Analyze

To build the database and immediately run security analysis:

```bash
./tools/build-codeql-database.sh --analyze
```

### Custom Output Location

```bash
./tools/build-codeql-database.sh --output /tmp/my-codeql-db
```

### In a Container

If you're running inside an artdaq development container:

```bash
# Ensure UPS environment is loaded (usually done in container entrypoint)
source /path/to/setup_for_development

# Build CodeQL database
./tools/build-codeql-database.sh --build-dir /build --analyze
```

## Script Options

The `build-codeql-database.sh` script supports the following options:

```
Usage: build-codeql-database.sh [OPTIONS]

OPTIONS:
    -h, --help              Show help message
    -n, --name NAME         Set database name (default: artdaq-codeql-db)
    -o, --output PATH       Set database output path (default: ./codeql-database)
    -b, --build-dir PATH    Set build directory (default: ./build)
    -s, --source-dir PATH   Set source directory (default: current directory)
    -c, --config PATH       Set CodeQL config file path
    --analyze               Run analysis after building database
    --clean                 Clean existing database before building
```

## Configuration

The CodeQL configuration is stored in `codeql-config.yml` in this directory. It defines:

- **Paths to analyze**: Main source directories (artdaq, tools, test)
- **Paths to ignore**: Documentation and prototype code
- **Query suites**: Uses `security-and-quality` queries for C++

You can customize this configuration to:
- Add or remove directories from analysis
- Change query suites (e.g., `security-only`, `security-extended`)
- Add custom queries

## Manual CodeQL Commands

If you prefer to run CodeQL commands manually:

```bash
# Create database
codeql database create codeql-db \
    --language=cpp \
    --source-root=. \
    --codescanning-config=.github/codeql/codeql-config.yml \
    --command="cmake --build ./build"

# Analyze database
codeql database analyze codeql-db \
    --format=sarif-latest \
    --output=results.sarif \
    cpp-security-and-quality

# View results
cat results.sarif
```

## Integration with GitHub

### Upload Results to GitHub

After building and analyzing the database, you can upload results to GitHub Code Scanning:

```bash
# Using CodeQL CLI
codeql github upload-results \
    --repository=art-daq/artdaq \
    --ref=refs/heads/your-branch \
    --commit=$(git rev-parse HEAD) \
    --sarif=results.sarif

# Or using GitHub CLI
gh api /repos/art-daq/artdaq/code-scanning/sarifs \
    -F sarif=@results.sarif \
    -F ref=refs/heads/your-branch \
    -F commit_sha=$(git rev-parse HEAD)
```

### Automated Workflow

To automate CodeQL analysis, you can add a GitHub Actions workflow. Example:

```yaml
name: "CodeQL"
on:
  push:
    branches: [develop, stable]
  pull_request:
    branches: [develop]
  schedule:
    - cron: '0 0 * * 0'  # Weekly

jobs:
  analyze:
    name: Analyze
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
    
    - name: Initialize CodeQL
      uses: github/codeql-action/init@v3
      with:
        languages: cpp
        config-file: .github/codeql/codeql-config.yml
    
    - name: Setup UPS environment
      run: |
        # Setup your build environment
        source setup_for_development
    
    - name: Build
      run: |
        mkdir build && cd build
        cmake .. -DCMAKE_BUILD_TYPE=Release
        make -j${NPROC:-4}
    
    - name: Perform CodeQL Analysis
      uses: github/codeql-action/analyze@v3
```

## Troubleshooting

### CodeQL Not Found

If you get "CodeQL not found" error:
1. Verify installation: `which codeql`
2. Check PATH: `echo $PATH`
3. Reinstall using one of the methods above

### Build Fails During Database Creation

If the build fails during CodeQL tracing:
1. Verify your build works without CodeQL: `cd build && cmake .. && make`
2. Check that all dependencies are available
3. Ensure UPS environment is properly configured
4. Try with verbose output: Add `--verbose` to the build command in the script

### Database Already Exists

Use the `--clean` flag to remove existing database:
```bash
./tools/build-codeql-database.sh --clean
```

### Missing Dependencies

artdaq requires several dependencies (see CMakeLists.txt):
- TRACE (3.17.04+)
- artdaq-core (3.08.04+)
- artdaq-utilities (1.07.02+)
- art, canvas, messagefacility, fhiclcpp, cetlib
- Boost, XMLRPC

Ensure all are available in your environment before building.

## Resources

- [CodeQL Documentation](https://codeql.github.com/docs/)
- [CodeQL CLI Reference](https://codeql.github.com/docs/codeql-cli/)
- [C++ CodeQL Queries](https://github.com/github/codeql/tree/main/cpp)
- [GitHub Code Scanning](https://docs.github.com/en/code-security/code-scanning)

## Support

For issues or questions:
- Check the [artdaq documentation](https://art-daq.github.io/artdaq_doxygen/artdaq)
- Open an issue in the artdaq repository
- Contact the artdaq development team

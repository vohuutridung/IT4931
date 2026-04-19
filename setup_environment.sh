#!/bin/bash

# Setup script for IT4931 Conda Environment
# Creates and configures a Conda environment with all dependencies

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                IT4931 CONDA ENVIRONMENT SETUP                               ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Function to print colored messages
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check if conda is installed
log_info "Checking if Conda is installed..."
if ! command -v conda &> /dev/null; then
    log_error "Conda not found. Please install Miniconda or Anaconda."
    echo ""
    echo "Install from: https://docs.conda.io/projects/miniconda/en/latest/"
    exit 1
fi

log_success "Conda found: $(conda --version)"
echo ""

# Get environment name
ENV_NAME="${1:-it4931-streaming}"
log_info "Environment name: ${ENV_NAME}"

# Check if environment already exists
if conda env list | grep -q "^${ENV_NAME}"; then
    log_warn "Environment '${ENV_NAME}' already exists"
    echo ""
    read -p "Do you want to recreate it? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Removing existing environment..."
        conda env remove -n ${ENV_NAME} -y
        log_success "Environment removed"
    else
        log_info "Using existing environment"
        exit 0
    fi
fi

echo ""
log_info "Creating Conda environment from environment.yml..."
log_warn "This may take 5-10 minutes (downloading dependencies)..."
echo ""

# Create environment from yml file
if [ -f "environment.yml" ]; then
    # Use --environment-spec flag for newer conda versions
    conda env create -n ${ENV_NAME} -f environment.yml --environment-spec environment.yml -y || \
    conda env create -n ${ENV_NAME} -f environment.yml -y
else
    log_error "environment.yml not found in current directory"
    exit 1
fi

log_success "Conda environment created"
echo ""

# Get conda prefix
CONDA_PREFIX=$(conda run -n ${ENV_NAME} python -c "import sys; print(sys.prefix)")

echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                        SETUP COMPLETE                                        ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

echo -e "${GREEN}Environment Details:${NC}"
echo "  Name:      ${ENV_NAME}"
echo "  Location:  ${CONDA_PREFIX}"
echo ""

echo -e "${GREEN}Next Steps:${NC}"
echo ""
echo "1. Activate the environment:"
echo "   ${BLUE}conda activate ${ENV_NAME}${NC}"
echo ""

echo "2. Verify installation:"
echo "   ${BLUE}python test_integration.py${NC}"
echo ""

echo "3. Start the pipeline:"
echo "   Terminal 1 - Start Kafka:"
echo "     ${BLUE}docker-compose up -d${NC}"
echo ""
echo "   Terminal 2 - Run Ingestion:"
echo "     ${BLUE}python -m ingestion.main${NC}"
echo ""
echo "   Terminal 3 - Run Spark Streaming:"
echo "     ${BLUE}python -m streaming.main${NC}"
echo ""

echo -e "${GREEN}Useful Commands:${NC}"
echo "  List packages:        ${BLUE}conda list -n ${ENV_NAME}${NC}"
echo "  Remove environment:   ${BLUE}conda env remove -n ${ENV_NAME}${NC}"
echo "  Deactivate:          ${BLUE}conda deactivate${NC}"
echo "  Update packages:     ${BLUE}conda env update -n ${ENV_NAME} -f environment.yml${NC}"
echo ""

log_success "Setup complete!"

#!/bin/bash
#
# opiRunner v1.3.0 - Setup Script
# Automatic installation and configuration
#

set -e  # Exit on error

echo "=========================================="
echo "opiRunner v1.3.0 - Setup"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Python version
echo "Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ]); then
    echo -e "${RED}ERROR: Python 3.8+ required. Found: $PYTHON_VERSION${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Python $PYTHON_VERSION found${NC}"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
if [ -d "venv" ]; then
    echo -e "${YELLOW}⚠ venv directory already exists. Skipping creation.${NC}"
else
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
fi
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"
echo ""

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip --quiet
echo -e "${GREEN}✓ pip upgraded${NC}"
echo ""

# Install requirements
echo "Installing core dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt --quiet
    echo -e "${GREEN}✓ Core dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠ requirements.txt not found. Installing minimal dependencies...${NC}"
    pip install pymongo --quiet
    echo -e "${GREEN}✓ Minimal dependencies installed${NC}"
fi
echo ""

# Install development dependencies (optional)
read -p "Install development dependencies (pytest, pylint, etc.)? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Installing development dependencies..."
    if [ -f "requirements-dev.txt" ]; then
        pip install -r requirements-dev.txt --quiet
        echo -e "${GREEN}✓ Development dependencies installed${NC}"
    else
        echo -e "${YELLOW}⚠ requirements-dev.txt not found${NC}"
    fi
    echo ""
fi

# Create directory structure
echo "Creating directory structure..."
mkdir -p configs/production
mkdir -p configs/staging
mkdir -p configs/templates
mkdir -p scripts
mkdir -p runlogs
mkdir -p data/input
mkdir -p data/output
echo -e "${GREEN}✓ Directory structure created${NC}"
echo ""

# Test installation
echo "Testing installation..."
python3 opirunner.py --version
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Installation test passed${NC}"
else
    echo -e "${RED}✗ Installation test failed${NC}"
    exit 1
fi
echo ""

# Print summary
echo "=========================================="
echo -e "${GREEN}Setup completed successfully!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Activate virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Place your pipeline configuration in:"
echo "   configs/production/your_pipeline.json"
echo ""
echo "3. Run opiRunner:"
echo "   python opirunner.py -c configs/production/your_pipeline.json"
echo ""
echo "4. For parallel execution:"
echo "   python opirunner.py -c config.json --parallel --max-workers 6"
echo ""
echo "5. For help:"
echo "   python opirunner.py --help"
echo ""
echo "Documentation: README.md"
echo "Changelog: CHANGELOG.md"
echo ""

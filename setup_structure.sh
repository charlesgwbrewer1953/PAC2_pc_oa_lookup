#!/bin/bash
# Setup modular structure for Shiny app refactoring
# Run this from your project root directory

echo "=========================================="
echo "Creating Modular Shiny App Structure"
echo "=========================================="

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "ERROR: Not in a git repository. Please run 'git init' first or navigate to your project folder."
    exit 1
fi

# Check current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"

# Commit any uncommitted changes on current branch
if [ -n "$(git status --porcelain)" ]; then
    echo ""
    echo "You have uncommitted changes. Committing them now..."
    git add .
    git commit -m "Working version before refactoring - $(date +%Y%m%d)"
    echo "Changes committed."
else
    echo "No uncommitted changes."
fi

# Push current branch
echo ""
echo "Pushing current branch to remote..."
git push origin $CURRENT_BRANCH

# Create and checkout new branch
echo ""
echo "Creating new branch 'refactor-modular'..."
git checkout -b refactor-modular 2>/dev/null || git checkout refactor-modular

# Create directory structure
echo ""
echo "Creating directories..."
mkdir -p R
mkdir -p modules
mkdir -p www

# Create empty files
echo "Creating R/ files..."
touch R/utils.R
touch R/data_loaders.R
touch R/bigquery.R
touch R/geometry_helpers.R
touch R/map_export.R

echo "Creating modules/ files..."
touch modules/ui_sidebar.R
touch modules/server_map.R

echo "Creating root files..."
touch global.R

# Create .rshinyignore with content
echo "Creating .rshinyignore..."
cat > .rshinyignore << 'EOF'
^data/.*\.parquet$
^\.RData$
^\.Rhistory$
^\.git$
EOF

# Create README.md if it doesn't exist
if [ ! -f "README.md" ]; then
    echo "Creating README.md..."
    cat > README.md << 'EOF'
# OA → PCON Lookup and Map Tool

Shiny application for looking up Output Areas (OAs) and their associated Parliamentary Constituencies (PCONs).

## Structure

- `app.R` - Main application entry point
- `global.R` - Shared constants, libraries, authentication
- `R/` - Helper functions and utilities
  - `utils.R` - General utility functions
  - `data_loaders.R` - GCS and parquet data loading
  - `bigquery.R` - BigQuery query functions
  - `geometry_helpers.R` - Spatial geometry functions
  - `map_export.R` - Map export/print functionality
- `modules/` - Shiny modules
  - `ui_sidebar.R` - Sidebar UI module
  - `server_map.R` - Map server logic
- `data/` - Authentication and cached data files
- `www/` - Static web assets (CSS, JS)

## Branches

- `main` - Production version
- `refactor-modular` - Modular refactored version

EOF
fi

# Show structure
echo ""
echo "=========================================="
echo "Directory structure created successfully!"
echo "=========================================="
echo ""
tree -L 2 2>/dev/null || find . -maxdepth 2 -not -path '*/\.*' -print | sed 's|^\./||' | sort

# Commit the structure
echo ""
echo "Committing new structure..."
git add .
git commit -m "Created modular file structure - empty files"

# Push new branch
echo ""
echo "Pushing 'refactor-modular' branch to remote..."
git push -u origin refactor-modular

echo ""
echo "=========================================="
echo "✓ Setup Complete!"
echo "=========================================="
echo ""
echo "You are now on branch: refactor-modular"
echo ""
echo "Next steps:"
echo "  1. Start populating the empty files with code"
echo "  2. To switch back to original: git checkout $CURRENT_BRANCH"
echo "  3. To return to refactoring: git checkout refactor-modular"
echo ""
echo "Files created:"
echo "  - R/utils.R"
echo "  - R/data_loaders.R"
echo "  - R/bigquery.R"
echo "  - R/geometry_helpers.R"
echo "  - R/map_export.R"
echo "  - modules/ui_sidebar.R"
echo "  - modules/server_map.R"
echo "  - global.R"
echo "  - .rshinyignore"
echo "  - README.md"
echo ""
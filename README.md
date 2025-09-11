# PAC2_pc_oa_lookup
# OA Postcode Map

**Version:** 0.9.18 (Development)

A Shiny application that maps UK Output Areas (OAs) to their Parliamentary Constituencies (PCONs) and associated postcodes using Google Cloud Storage data and BigQuery lookups.

## Overview

This tool allows users to:
- Enter a single Output Area code (OA21CD)
- View the parliamentary constituency containing that OA
- See all postcodes within the OA
- Visualize the constituency boundaries and highlight the specific OA on an interactive map

## Features

- **Interactive mapping**: Leaflet-based visualization with constituency boundaries and OA highlighting
- **Postcode lookup**: BigQuery integration for comprehensive postcode data
- **Automated zoom**: Intelligently focuses on the selected OA with appropriate buffer
- **Layer control**: Toggle between constituency boundaries, all OAs, and selected OA
- **Real-time diagnostics**: Optional logging for debugging and monitoring

## Data Sources

### Google Cloud Storage
- **Geometry data**: `pac10_geojson_parquet/ED_geoJSON_pcon.parquet`
- **Electoral data**: `demographikon_shared/ED_complete_20250720.parquet`

### BigQuery
- **Project**: `politicalmaps`
- **Dataset**: `PAC_reference_data`  
- **Table**: `PAC_PC_Xref_5_2024`
- **Schema**: `oa21` (Output Area), `pcd2` (Postcode)

## Prerequisites

### R Dependencies
```r
# Core packages
library(shiny)
library(googleCloudStorageR)
library(arrow)
library(dplyr)
library(sf)
library(leaflet)
library(jsonlite)
library(bigrquery)
```

### Authentication
- Google Cloud Service Account JSON key file
- Default location: `./data/astral-name-419808-ab8473ded5ad.json`
- Requires permissions for:
  - Google Cloud Storage (read access to specified buckets)
  - BigQuery (query access to specified dataset)

## Installation & Setup

1. **Clone/download** the application files
2. **Install R dependencies**:
   ```r
   install.packages(c("shiny", "googleCloudStorageR", "arrow", 
                      "dplyr", "sf", "leaflet", "jsonlite", "bigrquery"))
   ```
3. **Set up authentication**:
   - Place your Google Cloud service account JSON in `./data/`
   - Update `AUTH_JSON` path if using different location
4. **Run the application**:
   ```r
   shiny::runApp()
   ```

## Usage

1. **Enter OA Code**: Input a valid UK Output Area code (e.g., `E00092757`)
2. **Click "Run"**: The application will:
   - Fetch associated postcodes from BigQuery
   - Identify the parliamentary constituency
   - Download and cache required geometry data
   - Display results on the interactive map
3. **Explore**: Use map controls to zoom, pan, and toggle layers

### Example OA Codes
- `E00092757` (England)
- `W00000001` (Wales)
- `S00090001` (Scotland)

## Configuration

### Key Constants
```r
# Version and status
version_no <- "0.9.18"
op_status  <- "DEV"

# Google Cloud Storage buckets
GEO_BUCKET <- "pac10_geojson_parquet"
ED_BUCKET  <- "demographikon_shared"

# BigQuery configuration
BQ_PROJECT <- "politicalmaps"
BQ_DATASET <- "PAC_reference_data"
BQ_TABLE   <- "PAC_PC_Xref_5_2024"
```

## Data Processing

### Caching Strategy
- Geometry and electoral data are cached locally in `./data/`
- Files are only downloaded if missing locally
- Reduces load times and API calls

### Geometry Handling
- Automatic CRS setting to WGS84 (EPSG:4326)
- Geometry validation and repair for robust mapping
- Intelligent zoom buffering around OA centroids

### Error Handling
- Graceful degradation for missing data
- User notifications for invalid inputs
- Comprehensive logging for debugging

## Map Features

### Layers
- **PCON**: Parliamentary constituency boundaries (blue)
- **OA Boundaries**: All Output Area boundaries within constituency (grey)
- **OA Selected**: Highlighted selected Output Area (red/orange)

### Controls
- **Zoom**: Automatic intelligent zoom to selected OA
- **Layer Toggle**: Show/hide different boundary layers
- **Popup Info**: Click polygons for OA/PCON details

## Troubleshooting

### Common Issues

**"OA not found"**
- Verify OA code format and validity
- Check that electoral data contains the specified OA

**"No geometry for PCON"**
- Constituency may not be included in geometry dataset
- Check data completeness and OA-PCON mapping

**Authentication errors**
- Verify service account JSON path and permissions
- Ensure BigQuery and Cloud Storage APIs are enabled

### Diagnostics
Uncomment diagnostic sections in the UI/server for detailed logging:
```r
# Uncomment these lines for debugging
# h4("Diagnostics"),
# verbatimTextOutput("diag"),
```

## Development Notes

- Built with Shiny reactive framework
- Modular design for easy extension
- Comprehensive error handling and validation
- Performance optimized with local caching

## Limitations

- Single OA lookup only (no batch processing)
- Requires active internet connection for data fetching
- UK-specific data and projections
- Development version - not production ready

## License & Support

Development version - use at your own risk. For production use, implement additional error handling, authentication security, and performance optimizations.

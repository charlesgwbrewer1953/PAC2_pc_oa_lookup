###############################################################################
# Demographikon OA → PCON Lookup and Map Tool
# ---------------------------------------------------------------------------
# Purpose:
#   This Shiny application allows users to look up Output Areas (OAs) and
#   their associated Parliamentary Constituencies (PCONs) using either an OA
#   code or a postcode.  The app displays the relevant PCON boundary and the
#   selected OA on an interactive leaflet map.
#
# Data sources:
#   • Local parquet files (downloaded from Google Cloud Storage) containing
#     OA → PCON mappings and constituency geometries.
#   • BigQuery table (PAC_PC_Xref_5_2024) providing OA ↔ Postcode references.
#
# Core workflow:
#   1. User enters either a postcode or an OA code.
#   2. The app retrieves matching records from BigQuery (if postcode given)
#      and looks up the OA’s PCON using the ED parquet.
#   3. The corresponding PCON boundary and OA polygon are drawn on a leaflet
#      map, with optional postcode list and zoom-to-fit logic.
#
# Key interactive features:
#   • Adjustable OA fill opacity (slider control).
#   • Drawing tools (leaflet.extras) for user annotations on the map.
#   • “Print / Save Map” option exporting the current map view to a PNG
#     image using htmlwidgets + webshot2.
#
# Notes:
#   – OA fill opacity can be changed in real time.
#   – Drawn lines/polygons remain on the map and are included in exports.
#   – Exported PNGs omit OA fill (zero opacity) and include a label box
#     showing OA and PCON identifiers.
#
# Version : 0.9.21 (DEV)
# Author  : [Your Name or Team]
# Date    : [Current Date]
###############################################################################



# OA_Postcode_Map — Single OA → PCON map + BQ Postcodes
# Version: 0.9.1
# Status : DEV

# =================== Libraries ===================
library(shiny)
library(shinyjs)
library(googleCloudStorageR)
library(arrow)
library(dplyr)
library(stringr)
library(sf)
library(leaflet)
library(jsonlite)
library(bigrquery)
library(leaflet.extras)
library(mapview)
library(webshot)
library(webshot2)


# =================== Constants ===================
if (file.exists(".rshinyignore")) {
  writeLines(c("^data/.*\\.parquet$", "^\\.RData$"), ".rshinyignore")
}

version_no <- "0.10.0"
op_status  <- "DEV"

AUTH_JSON <- "./data/astral-name-419808-ab8473ded5ad.json"

# GCS objects (cached locally)
GEO_BUCKET <- "pac10_geojson_parquet"
GEO_OBJECT <- "ED_geoJSON_pcon.parquet"
ED_BUCKET  <- "demographikon_shared"
ED_OBJECT  <- "ED_complete_20250720.parquet"

dir.create("./data", showWarnings = FALSE, recursive = TRUE)
GEO_LOCAL <- file.path("data", basename(GEO_OBJECT))
ED_LOCAL  <- file.path("data", basename(ED_OBJECT))

# BigQuery config (inline SQL — original working logic)
BQ_PROJECT <- "politicalmaps"
BQ_DATASET <- "PAC_reference_data"
BQ_TABLE   <- "PAC_PC_Xref_5_2024"   # columns: oa21, pcd2

# =================== Utilities ===================

download_if_missing <- function(bucket, object, dest) {
  if (!file.exists(dest)) {
    message(sprintf("Downloading %s/%s -> %s", bucket, object, dest))
    googleCloudStorageR::gcs_get_object(
      object_name = object, bucket = bucket,
      saveToDisk = dest, overwrite = TRUE
    )
  } else {
    message(sprintf("Using cached file: %s", dest))
  }
}

must_have_col <- function(df, colname) {
  if (!(colname %in% names(df))) stop(sprintf("Required column '%s' not found.", colname))
  colname
}

# Normalise a postcode: uppercase and remove spaces
normalize_postcode <- function(x) {
  x <- trimws(x)
  x <- toupper(x)
  gsub("\\s+", "", x)
}

# Inline-BQ: OA -> Postcodes
fetch_postcodes_bq <- function(oa_code) {
  query <- paste0(
    "SELECT pcd2 FROM `", BQ_PROJECT, ".", BQ_DATASET, ".", BQ_TABLE, "` ",
    "WHERE oa21 = '", oa_code, "'"
  )
  tryCatch({
    job <- bigrquery::bq_project_query(BQ_PROJECT, query)
    df  <- bigrquery::bq_table_download(job)
    if (nrow(df) > 0) sort(unique(df$pcd2)) else character(0)
  }, error = function(e) {
    warning(paste("Error executing BigQuery for", oa_code, ":", e$message))
    character(0)
  })
}

# Inline-BQ: Postcode -> OA (match on pcd2 ignoring spaces / case)
fetch_oa_by_postcode <- function(postcode_raw) {
  pcd <- normalize_postcode(postcode_raw)
  if (!nzchar(pcd)) return(character(0))
  query <- paste0(
    "SELECT DISTINCT oa21 FROM `", BQ_PROJECT, ".", BQ_DATASET, ".", BQ_TABLE, "` ",
    "WHERE REPLACE(UPPER(pcd2), ' ', '') = '", pcd, "'"
  )
  tryCatch({
    job <- bigrquery::bq_project_query(BQ_PROJECT, query)
    df  <- bigrquery::bq_table_download(job)
    if (nrow(df) > 0) unique(df$oa21) else character(0)
  }, error = function(e) {
    warning(paste("Error executing BigQuery for postcode", postcode_raw, ":", e$message))
    character(0)
  })
}

# =================== Auth & Data Load ===================
message("Checking auth file exists: ", file.exists(AUTH_JSON))
googleCloudStorageR::gcs_auth(AUTH_JSON)
bigrquery::bq_auth(path = AUTH_JSON)

download_if_missing(GEO_BUCKET, GEO_OBJECT, GEO_LOCAL)
download_if_missing(ED_BUCKET,  ED_OBJECT,  ED_LOCAL)

# ED table (OA -> PCON, optional OA centroid columns)
ed_raw <- as.data.frame(arrow::read_parquet(ED_LOCAL))
must_have_col(ed_raw, "oa21cd")
must_have_col(ed_raw, "pcon25cd")

lat_col <- if ("lat" %in% names(ed_raw)) "lat" else if ("latitude" %in% names(ed_raw)) "latitude" else NULL
lon_col <- if ("lon" %in% names(ed_raw)) "lon" else if ("longitude" %in% names(ed_raw)) "longitude" else NULL

ed_slim <- ed_raw %>% select(oa21cd, pcon25cd, all_of(c(lat_col, lon_col)))

# Constituency parquet -> sf (exactly like your working app)
# ------------------ Lazy geometry loader ------------------
# Only read the specific PCON geometry when needed
geo_dataset <- arrow::open_dataset(GEO_LOCAL)

fetch_pcon_geo <- function(pcon_code) {
  geo_subset <- geo_dataset %>%
    dplyr::filter(pcon25cd == !!pcon_code) %>%
    dplyr::select(pcon25cd, pcon25nm, oa21cd, geometry) %>%
    dplyr::collect() %>%
    sf::st_as_sf(crs = 4326)
  return(geo_subset)
}
# Prefer human-readable name if present (e.g., pcon25nm or any *nm)
geo_name_col <- if ("pcon25nm" %in% names(geo_sf)) "pcon25nm" else {
  nm_alt <- grep("nm$", names(geo_sf), value = TRUE)
  if (length(nm_alt)) nm_alt[1] else NULL
}

# =================== UI ===================
# =================== UI ===================
ui <- fluidPage(
  # ---------- Head: scripts and custom JS ----------
  tags$head(
    # html2canvas library for browser-side PNG capture
    tags$script(src = "https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"),
    
    # Custom handlers: fix map resize + export map PNG
    tags$script(HTML("
      // Fix leaflet sizing after re-render
      Shiny.addCustomMessageHandler('leafletInvalidate', function(_) {
        var map = $('#map').data('leaflet-map');
        if (map) { map.invalidateSize(true); }
      });

      // Handler: export OA map as PNG with correct bounding box and label
      Shiny.addCustomMessageHandler('exportLeafletMap', function(data) {
        // Create off-screen container for clean rendering
        const tempDiv = document.createElement('div');
        tempDiv.id = 'exportMap';
        tempDiv.style.width = '1200px';
        tempDiv.style.height = '900px';
        tempDiv.style.position = 'absolute';
        tempDiv.style.left = '-9999px';
        document.body.appendChild(tempDiv);

        // Create a new Leaflet map
        const map = L.map('exportMap', {zoomControl:false, attributionControl:false});
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
          maxZoom: 19,
          minZoom: 1
        }).addTo(map);

        // Copy all visible geometries from main map, but make OA fill transparent
        const mainMap = $('#map').data('leaflet-map');
        if (mainMap) {
          mainMap.eachLayer(function(layer){
            if (layer.toGeoJSON) {
              const geo = layer.toGeoJSON();
              const opts = layer.options || {};
              const isOA = (layer.options.group === 'oa_selected');
              L.geoJSON(geo, {
                color: opts.color || '#555',
                weight: opts.weight || 1,
                opacity: opts.opacity || 1,
                fillColor: opts.fillColor || '#ccc',
                fillOpacity: isOA ? 0.0 : (opts.fillOpacity || 0)
              }).addTo(map);
            }
          });
        }

        // Fit the map to the provided OA bounding box
        if (data.bbox && data.bbox.length === 4) {
          map.fitBounds([[data.bbox[1], data.bbox[0]], [data.bbox[3], data.bbox[2]]]);
        }

        // Add title box (PCON + OA/postcode)
        const label = L.control({position:'topleft'});
        label.onAdd = function(){
          const div = L.DomUtil.create('div');
          div.innerHTML = `<div style='background:white;padding:8px 12px;font-size:14px;font-weight:bold;
                            border-radius:6px;box-shadow:1px 1px 3px rgba(0,0,0,0.3);line-height:1.3;'>
                            ${data.title}</div>`;
          return div;
        };
        label.addTo(map);

        // Wait for tiles to load completely, then render to PNG
        setTimeout(function(){
          html2canvas(tempDiv, {useCORS:true, scale:2}).then(function(canvas){
            canvas.toBlob(function(blob){
              const link = document.createElement('a');
              link.download = data.filename || 'map.png';
              link.href = URL.createObjectURL(blob);
              link.click();

              // Optional cloud upload stub
              if (data.cloudUpload) {
                console.log('Cloud upload placeholder triggered.');
                // e.g. fetch('/upload', {method:'POST', body:blob});
              }

              // Clean up
              document.body.removeChild(tempDiv);
            });
          });
        }, 3000); // allow 3 seconds for tile load
      });
    "))
  ),
  
  # ---------- Title ----------
  titlePanel("OA locator → PCON map + Postcodes"),
  
  # ---------- Layout ----------
  sidebarLayout(
    sidebarPanel(
      useShinyjs(),
      helpText("Identify Output Areas (OAs) and their corresponding Parliamentary Constituencies (PCONs)."),
      
      textInput("postcode", "Enter Postcode (no spaces)", placeholder = "e.g. SW1A1AA"),
      helpText("If a postcode is entered, the app finds the OA for that postcode."),
      textInput("oa_value", "or enter OA code directly:", value = "", placeholder = "e.g. E00092757"),
      
      # Opacity slider
      sliderInput("oa_opacity", "OA fill opacity", min = 0, max = 1, value = 0.45, step = 0.05),
      
      # Action buttons
      actionButton("run_lookup", "Run Lookup", icon = icon("play")),
      actionButton("save_png", "Download OA PNG", icon = icon("download")),
      
      tags$hr(),
      helpText("* OAs vary in size — adjust map zoom with +/– as needed.")
    ),
    
    mainPanel(
      leafletOutput("map", height = 680),
      tags$hr(),
      h4("Result"),
      tableOutput("results")
    )
  )
)


# =================== Server ===================
server <- function(input, output, session) {
  ## ---------------- diagnostics ----------------
  rv <- reactiveValues(log = character(0))
  logit <- function(...) {
    msg <- paste0(format(Sys.time(), "%H:%M:%S"), " | ", paste0(..., collapse = ""))
    rv$log <- c(rv$log, msg)
    message(msg)
  }
  output$diag <- renderText(paste(rv$log, collapse = "\n"))
  
  ## Disable OA field whenever postcode has content; re-enable when cleared
  observe({
    pc_now <- normalize_postcode(input$postcode)
    if (nzchar(pc_now)) {
      shinyjs::disable("oa_value")
    } else {
      shinyjs::enable("oa_value")
    }
  })
  
  ##------------- Reactive variables -------------
  # Debounced OA opacity (updates ~0.5s after user stops sliding)
  oa_opacity_reactive <- reactive({
    input$oa_opacity
  }) %>% debounce(500)
  
  # store selected geometries for later export or updates
  selected_data <- reactiveValues(sel_oa = NULL, pcon_sf = NULL)
  
  ## ---------------- helpers ----------------
  pad_bbox <- function(bb, frac = 0.10) {
    bb <- as.numeric(bb); names(bb) <- c("xmin","ymin","xmax","ymax")
    dx <- (bb["xmax"] - bb["xmin"]) * frac
    dy <- (bb["ymax"] - bb["ymin"]) * frac
    c(xmin = bb["xmin"] - dx,
      ymin = bb["ymin"] - dy,
      xmax = bb["xmax"] + dx,
      ymax = bb["ymax"] + dy)
  }
  
  # Build a zoom box around the OA by buffering its interior point (meters)
  oa_zoom_box <- function(sfx, meters = 1000) {
    if (is.null(sfx) || nrow(sfx) == 0) return(NULL)
    g <- sf::st_geometry(sfx)
    if (any(sf::st_is_empty(g))) return(NULL)
    
    # Clean up geometry 
    g <- tryCatch(sf::st_make_valid(g), error = function(e) g)
    g <- tryCatch(sf::st_collection_extract(g, "POLYGON"), error = function(e) g)
    g <- tryCatch(sf::st_zm(g, drop = TRUE, what = "ZM"), error = function(e) g)
    if (any(sf::st_is_empty(g))) return(NULL)
    
    # Use interior point of the union, buffer in meters in 3857, then back to 4326
    old <- sf::sf_use_s2(); sf::sf_use_s2(FALSE); on.exit(sf::sf_use_s2(old), add = TRUE)
    ctr  <- tryCatch(sf::st_point_on_surface(sf::st_union(g)), error = function(e) NULL)
    if (is.null(ctr) || any(sf::st_is_empty(ctr))) return(NULL)
    
    ctrm <- tryCatch(sf::st_transform(ctr, 3857), error = function(e) NULL)
    if (is.null(ctrm)) return(NULL)
    
    bufm <- tryCatch(sf::st_buffer(ctrm, meters), error = function(e) NULL)
    if (is.null(bufm)) return(NULL)
    
    bb4326 <- tryCatch(sf::st_bbox(sf::st_transform(bufm, 4326)), error = function(e) NULL)
    if (is.null(bb4326)) return(NULL)
    
    as.numeric(bb4326[c("xmin", "ymin", "xmax", "ymax")])
  }
  
  oa_centroid <- function(sfx) {
    if (!nrow(sfx)) return(NULL)
    g <- sf::st_geometry(sfx)
    g <- tryCatch(sf::st_make_valid(g), error = function(e) g)
    g <- tryCatch(sf::st_zm(g, drop = TRUE, what = "ZM"), error = function(e) g)
    if (all(sf::st_is_empty(g))) return(NULL)
    old <- sf::sf_use_s2(); on.exit(sf::sf_use_s2(old), add = TRUE); sf::sf_use_s2(FALSE)
    p <- tryCatch(sf::st_point_on_surface(sf::st_union(g)), error = function(e) NULL)
    if (is.null(p)) return(NULL)
    p <- tryCatch(sf::st_transform(p, 4326), error = function(e) p)
    xy <- suppressWarnings(tryCatch(sf::st_coordinates(p)[1,], error = function(e) c(NA_real_, NA_real_)))
    if (any(!is.finite(xy))) return(NULL)
    list(lng = xy[1], lat = xy[2])
  }
  
  ## ---------------- outputs ----------------
  results_rv <- reactiveVal(
    data.frame(OA = character(), PCON = character(), Postcodes = character(), stringsAsFactors = FALSE)
  )
  output$results <- renderTable(results_rv(), rownames = FALSE)
  output$postcodes_ui <- renderUI(NULL)
  
  # Base map: build once
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(zoomControl = TRUE)) %>%
      addTiles() %>%
      setView(lng = -1.8, lat = 52.8, zoom = 6) %>%  # UK-ish start
      addDrawToolbar(
        targetGroup = "draw",
        editOptions = editToolbarOptions(selectedPathOptions = selectedPathOptions()),
        polylineOptions  = drawPolylineOptions(shapeOptions = drawShapeOptions(weight = 2)),
        polygonOptions   = drawPolygonOptions(showArea = TRUE, shapeOptions = drawShapeOptions(fillOpacity = 0.2)),
        rectangleOptions = drawRectangleOptions(shapeOptions = drawShapeOptions(fillOpacity = 0.2)),
        circleOptions = FALSE,
        markerOptions = FALSE
      ) %>%
      addLayersControl(
        overlayGroups = c("pcon", "oa_boundaries", "oa_selected"),
        options = layersControlOptions(collapsed = FALSE)
      )
  })
  
  # Optional client diagnostics
  observeEvent(input$map_bounds, ignoreInit = TRUE, {
    b <- input$map_bounds
    logit(sprintf("Leaflet moved: [%.6f, %.6f, %.6f, %.6f]", b$west, b$south, b$east, b$north))
  })
  observeEvent(input$map_zoom, ignoreInit = TRUE, {
    logit(sprintf("Leaflet zoom: %s", as.character(input$map_zoom)))
  })
  
  ## ---------------- main action ----------------
  observeEvent(input$run_lookup, {
    # Decide input source
    oa_in <- NA_character_
    pc_raw <- input$postcode
    pc_norm <- normalize_postcode(pc_raw)
    
    if (nzchar(pc_norm)) {
      # Postcode path: find OA, then proceed
      oas <- fetch_oa_by_postcode(pc_norm)
      logit("Postcode input: ", pc_norm, " | OA candidates: ", length(oas))
      if (length(oas) == 0) {
        showNotification("No OA found for that postcode.", type = "error")
        return()
      }
      if (length(oas) > 1) {
        showNotification(sprintf("Multiple OAs matched postcode; using the first (%s).", oas[1]), type = "message")
      }
      oa_in <- oas[1]
      updateTextInput(session, "oa_value", value = oa_in)
    } else {
      oa_in <- trimws(input$oa_value)
    }
    
    # Clear previous layers
    proxy <- leafletProxy("map") %>%
      clearGroup("pcon") %>%
      clearGroup("oa_boundaries") %>%
      clearGroup("oa_selected") %>%
      clearGroup("oa_center")
    
    results_rv(data.frame(OA = character(), PCON = character(), Postcodes = character()))
    output$postcodes_ui <- renderUI(NULL)
    
    if (oa_in == "" || is.na(oa_in)) {
      showNotification("Enter an OA code or a postcode.", type = "warning")
      return()
    }
    logit("OA input (effective): ", oa_in)
    
    # (1) Postcodes
    pcs_vec <- fetch_postcodes_bq(oa_in)
    pcs_str <- if (length(pcs_vec)) paste(pcs_vec, collapse = ", ") else NA_character_
    logit("Postcodes returned (n): ", length(pcs_vec))
    
    # (2) OA -> PCON lookup
    ed_row <- dplyr::filter(ed_slim, oa21cd == oa_in)
    logit("Rows in ED for OA: ", nrow(ed_row))
    if (nrow(ed_row) == 0) { showNotification("OA not found in ED parquet.", type = "error"); return() }
    pcon <- ed_row$pcon25cd[1]
    if (is.na(pcon)) { showNotification("PCON missing for this OA.", type = "error"); return() }
    logit("Derived PCON25CD: ", pcon)
    
    results_rv(data.frame(OA = oa_in, PCON = pcon, Postcodes = pcs_str, stringsAsFactors = FALSE))
    
    # (3) Geo filter and draw
    pcon_oas <- fetch_pcon_geo(pcon)
    logit("Geo rows matched in constituency parquet: ", nrow(pcon_oas))
    if (nrow(pcon_oas) == 0) { showNotification(paste("No geometry for PCON:", pcon), type = "error"); return() }
    pcon_oas$geometry <- sf::st_make_valid(pcon_oas$geometry)
    
    sel_oa <- pcon_oas %>%
      dplyr::filter(oa21cd == oa_in) %>%
      dplyr::slice_head(n = 1)
    logit("Selected OA rows in geo_sf: ", nrow(sel_oa))
    if (nrow(sel_oa) == 1) sel_oa$geometry <- sf::st_make_valid(sel_oa$geometry)
    
    # union PCON
    pcon_union <- suppressWarnings(sf::st_union(pcon_oas))
    if (inherits(pcon_union, "sfc_GEOMETRYCOLLECTION"))
      pcon_union <- sf::st_collection_extract(pcon_union, "POLYGON")
    pcon_sf <- sf::st_as_sf(data.frame(pcon25cd = pcon), geometry = pcon_union)
    
    leafletProxy("map") %>%
      addPolygons(
        data = pcon_sf, group = "pcon",
        color = "#2b8cbe", weight = 1, opacity = 0.9,
        fillColor = "#a6cee3", fillOpacity = 0.3
      ) %>%
      addPolylines(
        data = pcon_oas, group = "oa_boundaries",
        color = "#666666", weight = 1, opacity = 0.8
      )
    
    if (nrow(sel_oa) == 1) {
      leafletProxy("map") %>%
        addPolygons(
          data = sel_oa, group = "oa_selected",
          color = "#d7301f", weight = 3, opacity = 1.0,
          fillColor = "#fdae61", fillOpacity = oa_opacity_reactive()
        )
    }
    
    ## Save selected geometries for later
    selected_data$sel_oa  <- sel_oa
    selected_data$pcon_sf <- pcon_sf
    
    ## (4) Zoom logic
    zb <- oa_zoom_box(sel_oa, meters = 2000)
    if (!is.null(zb) && all(is.finite(zb))) {
      logit(sprintf("Zoom (OA centroid-buffer): [%.6f, %.6f, %.6f, %.6f]", zb[1], zb[2], zb[3], zb[4]))
      session$sendCustomMessage("leafletInvalidate", list())
      proxy %>% fitBounds(zb[1], zb[2], zb[3], zb[4])
    } else {
      bbp <- sf::st_bbox(pcon_sf)
      logit(sprintf("Zoom fallback (PCON bbox): [%.6f, %.6f, %.6f, %.6f]",
                    bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
      session$sendCustomMessage("leafletInvalidate", list())
      proxy %>% fitBounds(bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"])
    }
  })
  
  ## ---------------- opacity updater ----------------
  observe({
    op <- oa_opacity_reactive()
    proxy <- leafletProxy("map")
    isolate({
      sel_oa <- try(selected_data$sel_oa, silent = TRUE)
      if (!inherits(sel_oa, "try-error") && !is.null(sel_oa) && nrow(sel_oa) == 1) {
        proxy %>%
          clearGroup("oa_selected") %>%
          addPolygons(
            data = sel_oa,
            group = "oa_selected",
            color = "#d7301f", weight = 3, opacity = 1.0,
            fillColor = "#fdae61", fillOpacity = op,
            popup = ~paste0("<b>OA:</b> ", htmltools::htmlEscape(oa21cd))
          )
      }
    })
  })
  
  ## ---------------- trigger png save ----------------  
  observeEvent(input$save_png, {
    req(selected_data$sel_oa, selected_data$pcon_sf)
    sel <- selected_data$sel_oa
    pcon <- selected_data$pcon_sf
    
    # Ensure CRS
    if (is.na(sf::st_crs(sel)))  sf::st_crs(sel)  <- 4326
    if (is.na(sf::st_crs(pcon))) sf::st_crs(pcon) <- 4326
    
    # Defensive: validate geometry
    sel <- tryCatch(sf::st_make_valid(sel), error = function(e) sel)
    if (any(sf::st_is_empty(sel))) {
      showNotification("Selected OA geometry is empty or invalid.", type = "error")
      return()
    }
    
    # Compute bbox + buffer safely
    bb <- sf::st_bbox(sel)
    if (any(!is.finite(bb))) {
      showNotification("Bounding box not finite.", type = "error")
      return()
    }
    
    # Choose buffer in meters
    buf_m <- if (identical(selected_data$source, "postcode")) 100 else 300
    
    # Transform safely to metric CRS (3857)
    bb_sfc <- tryCatch(sf::st_as_sfc(bb, crs = 4326), error = function(e) NULL)
    if (is.null(bb_sfc)) {
      showNotification("Failed to build bounding box geometry.", type = "error")
      return()
    }
    
    bbm <- tryCatch(sf::st_transform(bb_sfc, 3857), error = function(e) NULL)
    if (is.null(bbm)) {
      showNotification("Failed to transform bbox to metric CRS.", type = "error")
      return()
    }
    
    buf <- tryCatch(sf::st_buffer(bbm, dist = buf_m), error = function(e) NULL)
    if (is.null(buf)) {
      showNotification("Failed to buffer bbox geometry.", type = "error")
      return()
    }
    
    bb4326 <- sf::st_bbox(sf::st_transform(buf, 4326))
    bbox_vals <- as.numeric(bb4326[c("xmin","ymin","xmax","ymax")])
    
    # --- Build title text (unchanged) ---
    df <- isolate(results_rv())
    pcon_code <- df$PCON[1]
    pcon_name <- tryCatch({
      nm <- geo_sf$pcon25nm[match(pcon_code, geo_sf$pcon25cd)]
      if (length(nm) > 0 && !is.na(nm)) nm[1] else ""
    }, error = function(e) "")
    title_html <- sprintf(
      "PCON: %s  |  %s<br>%s: %s",
      htmltools::htmlEscape(pcon_code),
      htmltools::htmlEscape(pcon_name),
      if (identical(selected_data$source, "postcode")) "Postcode" else "OA",
      if (identical(selected_data$source, "postcode"))
        htmltools::htmlEscape(normalize_postcode(input$postcode))
      else htmltools::htmlEscape(df$OA[1])
    )
    
    # --- Send message to browser for PNG creation ---
    session$sendCustomMessage("exportLeafletMap", list(
      bbox = bbox_vals,
      title = title_html,
      filename = sprintf("OA_%s_%s.png", df$OA[1], format(Sys.Date(), "%Y%m%d")),
      cloudUpload = FALSE
    ))
  })
  

  # ---------------- Print selected OA map (using mapview for better tile handling) ----------------
  output$print_map <- downloadHandler(
    filename = function() {
      df <- try(results_rv(), silent = TRUE)
      oa <- if (!inherits(df, "try-error") && nrow(df) > 0 && nzchar(df$OA[1])) df$OA[1] else "OA"
      paste0("OA_", oa, "_print_", format(Sys.Date(), "%Y%m%d"), ".png")
    },
    content = function(file) {
      req(selected_data$sel_oa, selected_data$pcon_sf)
      
      sel  <- selected_data$sel_oa
      pcon <- selected_data$pcon_sf
      
      # Ensure WGS84
      if (is.na(sf::st_crs(sel)))  sf::st_crs(sel)  <- 4326
      if (is.na(sf::st_crs(pcon))) sf::st_crs(pcon) <- 4326
      
      # === Exact OA bounding box (no buffer) ===
      bb <- sf::st_bbox(sel)
      zb <- as.numeric(bb[c("xmin","ymin","xmax","ymax")])
      
      # Title text
      # title (now includes PCON name + conditional OA/Postcode line)
      df <- try(results_rv(), silent = TRUE)
      if (!inherits(df, "try-error") && nrow(df) > 0) {
        pcon_code <- df$PCON[1]
        # look up pcon25nm if present in geo_sf
        pcon_name <- tryCatch({
          nm <- geo_sf$pcon25nm[match(pcon_code, geo_sf$pcon25cd)]
          if (length(nm) > 0 && !is.na(nm)) nm[1] else NA_character_
        }, error = function(e) NA_character_)
        
        if (identical(selected_data$source, "postcode")) {
          postcode <- normalize_postcode(input$postcode)
          title_text <- sprintf(
            "PCON: %s  |  %s<br>Postcode: %s",
            htmltools::htmlEscape(pcon_code),
            htmltools::htmlEscape(pcon_name),
            htmltools::htmlEscape(postcode)
          )
        } else {
          oa_code <- df$OA[1]
          title_text <- sprintf(
            "PCON: %s  |  %s<br>OA: %s",
            htmltools::htmlEscape(pcon_code),
            htmltools::htmlEscape(pcon_name),
            htmltools::htmlEscape(oa_code)
          )
        }
      } else {
        title_text <- "OA / PCON map"
      }
      
      # Build export map using leaflet directly (mapview-compatible style)
      # PCON outline only, OA outline only (no fills)
      export_map <- leaflet(options = leafletOptions(zoomControl = FALSE, attributionControl = FALSE)) %>%
        addProviderTiles(
          leaflet::providers$OpenStreetMap.Mapnik,
          options = providerTileOptions(
            maxZoom = 19,
            minZoom = 1
          )
        ) %>%
        addPolygons(
          data = pcon,
          group = "pcon",
          color = "#2b8cbe", 
          weight = 2, 
          opacity = 1.0,
          fillColor = "#ffffff", 
          fillOpacity = 0.0     # no fill for PCON
        ) %>%
        addPolygons(
          data = sel,
          group = "oa_selected",
          color = "#d7301f", 
          weight = 3, 
          opacity = 1.0,
          fillColor = "#fdae61", 
          fillOpacity = 0.0     # no fill for OA
        ) %>%
        addControl(
          html = sprintf(
            "<div style='background:white;padding:8px 12px;font-size:14px;font-weight:bold;
                       border-radius:6px;box-shadow:1px 1px 3px rgba(0,0,0,0.3);'>
             %s
     </div>", title_text),
          position = "topleft"
        ) %>%
        fitBounds(zb[1], zb[2], zb[3], zb[4])
      
      # Use mapshot for better tile handling (requires mapview package)
      if (requireNamespace("mapview", quietly = TRUE)) {
        tryCatch({
          mapview::mapshot(
            export_map,
            file = file,
            remove_controls = c("zoomControl", "layersControl", "homeButton", "scaleBar"),
            vwidth = 1300,
            vheight = 950,
            delay = 3  # mapshot handles tiles more reliably
          )
        }, error = function(e) {
          # Fallback to webshot2 if mapshot fails
          message("mapshot failed, falling back to webshot2: ", e$message)
          tmp_html <- tempfile(fileext = ".html")
          htmlwidgets::saveWidget(export_map, tmp_html, selfcontained = TRUE)
          webshot2::webshot(
            tmp_html,
            file = file,
            vwidth = 1300,
            vheight = 950,
            delay = 15,
            zoom = 2.2,
            cliprect = "viewport"
          )
        })
      } else {
        # Fallback if mapview not installed - use improved webshot2
        message("mapview package not available, using webshot2")
        tmp_html <- tempfile(fileext = ".html")
        htmlwidgets::saveWidget(export_map, tmp_html, selfcontained = TRUE)
        
        capture_map <- function(output_file, delay_time = 15) {
          webshot2::webshot(
            tmp_html,
            file = output_file,
            vwidth = 1300,
            vheight = 950,
            delay = delay_time,
            zoom = 2.2,
            cliprect = "viewport"
          )
        }
        
        # First attempt with longer delay
        capture_map(file, delay_time = 15)
        
        # Check image size; if too small, retry with even longer delay
        info <- tryCatch(file.info(file)$size, error = function(e) 0)
        if (is.na(info) || info < 50000) {
          message("First capture too small — retrying with 20s delay...")
          capture_map(file, delay_time = 20)
        }
      }
    }
  )
}

# =================== Run ===================
shinyApp(ui, server)
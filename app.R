# OA_Postcode_Map — Single OA → PCON map + BQ Postcodes
# Version: 0.9.1
# Status : DEV

# =================== Libraries ===================
library(shiny)
library(googleCloudStorageR)
library(arrow)
library(dplyr)
library(sf)
library(leaflet)
library(jsonlite)
library(bigrquery)

# =================== Constants ===================
version_no <- "0.9.2"
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

# Inline-BQ postcode lookup
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
geo_data <- arrow::read_parquet(GEO_LOCAL)
geo_sf   <- sf::st_as_sf(geo_data)
sf::st_crs(geo_sf) <- 4326
must_have_col(geo_sf, "pcon25cd")
# Prefer human-readable name if present (e.g., pcon25nm or any *nm)
geo_name_col <- if ("pcon25nm" %in% names(geo_sf)) "pcon25nm" else {
  nm_alt <- grep("nm$", names(geo_sf), value = TRUE)
  if (length(nm_alt)) nm_alt[1] else NULL
}

# =================== UI ===================
ui <- fluidPage(
  # Force Leaflet to recalc size before zooms
  tags$head(tags$script(HTML("
    Shiny.addCustomMessageHandler('leafletInvalidate', function(_) {
      var map = $('#map').data('leaflet-map');
      if (map) { map.invalidateSize(true); }
    });
  "))),
  
  titlePanel("OA locator → PCON map + Postcodes"),
  sidebarLayout(
    sidebarPanel(
      helpText("Enter a single OA code (exact). The app derives PCON, draws the constituency, and highlights the OA."),
      textInput("oa_value", "OA code:", value = "", placeholder = "e.g. E00092757"),
      actionButton("submit", "Run"),
      tags$hr(),
      uiOutput("postcodes_ui"),
      tags$hr(),
      h4("Diagnostics"),
      verbatimTextOutput("diag")
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
  # ---------------------- diagnostics ----------------------
  rv <- reactiveValues(log = character(0))
  logit <- function(...) {
    msg <- paste0(format(Sys.time(), "%H:%M:%S"), " | ", paste0(..., collapse = ""))
    rv$log <- c(rv$log, msg)
    message(msg)
  }
  output$diag <- renderText(paste(rv$log, collapse = "\n"))
  
  # ---------------------- helpers ----------------------
  # Compute a robust interior point for an OA polygon and return c(lon, lat)
  compute_oa_center <- function(sf_row) {
    if (is.null(sf_row) || nrow(sf_row) == 0) return(c(NA_real_, NA_real_))
    g <- sf::st_geometry(sf_row)
    g <- tryCatch(sf::st_make_valid(g), error = function(e) g)
    if (inherits(g, "sfc_GEOMETRYCOLLECTION")) {
      g <- tryCatch(sf::st_collection_extract(g, "POLYGON"), error = function(e) g)
    }
    g <- tryCatch(sf::st_zm(g, drop = TRUE, what = "ZM"), error = function(e) g)
    if (all(sf::st_is_empty(g))) return(c(NA_real_, NA_real_))
    
    # Disable s2 for stability on some platforms
    old_s2 <- sf::sf_use_s2()
    sf::sf_use_s2(FALSE)
    on.exit(sf::sf_use_s2(old_s2), add = TRUE)
    
    pos <- tryCatch(sf::st_point_on_surface(sf::st_union(g)), error = function(e) NULL)
    if (is.null(pos)) return(c(NA_real_, NA_real_))
    pos <- tryCatch(sf::st_transform(pos, 4326), error = function(e) pos)
    xy  <- suppressWarnings(tryCatch(sf::st_coordinates(pos)[1, ], error = function(e) c(NA_real_, NA_real_)))
    if (length(xy) < 2) return(c(NA_real_, NA_real_))
    c(xy[1], xy[2])  # lon, lat
  }
  
  # Invalidate map, then setView; uses message() only (safe anywhere)
  zoom_to_lonlat <- function(session, mapId, lon, lat, zoom = 13, label = "zoom_to_lonlat") {
    if (!is.finite(lon) || !is.finite(lat)) { message(label, ": non-finite center"); return(FALSE) }
    session$sendCustomMessage("leafletInvalidate", list())
    message(sprintf("%s: setView [%.6f, %.6f], z=%s", label, lon, lat, as.character(zoom)))
    leafletProxy(mapId) %>% setView(lng = lon, lat = lat, zoom = zoom)
    TRUE
  }
  
  # Fit to bbox; used for PCON fallback
  safe_fit_bounds <- function(lng1, lat1, lng2, lat2, label = "fitBounds") {
    vals <- c(lng1, lat1, lng2, lat2)
    if (any(!is.finite(vals))) { logit(label, " skipped: non-finite"); return(FALSE) }
    if (abs(lng2 - lng1) < 1e-10 || abs(lat2 - lat1) < 1e-10) {
      logit(label, " skipped: degenerate box"); return(FALSE)
    }
    session$sendCustomMessage("leafletInvalidate", list())
    logit(sprintf("%s: [%.6f, %.6f, %.6f, %.6f]", label, lng1, lat1, lng2, lat2))
    leafletProxy("map") %>% fitBounds(lng1, lat1, lng2, lat2)
    TRUE
  }
  
  # Draw but don't crash if the geometry is unhappy
  safe_add_polygons <- function(proxy, ...) {
    tryCatch({
      proxy %>% addPolygons(...)
    }, error = function(e) {
      message("addPolygons error: ", e$message)
      proxy
    })
  }
  safe_add_polylines <- function(proxy, ...) {
    tryCatch({
      proxy %>% addPolylines(...)
    }, error = function(e) {
      message("addPolylines error: ", e$message)
      proxy
    })
  }
  safe_add_markers <- function(proxy, ...) {
    tryCatch({
      proxy %>% addCircleMarkers(...)
    }, error = function(e) {
      message("addCircleMarkers error: ", e$message)
      proxy
    })
  }
  
  # Pad a bbox by a fraction (expects names xmin,ymin,xmax,ymax)
  pad_bbox <- function(bb, frac = 0.10) {
    dx <- (bb["xmax"] - bb["xmin"]) * frac
    dy <- (bb["ymax"] - bb["ymin"]) * frac
    c(
      xmin = bb["xmin"] - dx,
      ymin = bb["ymin"] - dy,
      xmax = bb["xmax"] + dx,
      ymax = bb["ymax"] + dy
    )
  }
  
  # Robust interior point for a polygon sf row; returns c(lon,lat) or c(NA,NA)
  poly_point_lonlat <- function(sfx) {
    if (nrow(sfx) == 0) return(c(NA_real_, NA_real_))
    g <- sf::st_geometry(sfx)
    g <- tryCatch(sf::st_make_valid(g), error = function(e) g)
    g <- tryCatch(sf::st_zm(g, drop = TRUE, what = "ZM"), error = function(e) g)
    if (all(sf::st_is_empty(g))) return(c(NA_real_, NA_real_))
    old <- sf::sf_use_s2(); sf::sf_use_s2(FALSE); on.exit(sf::sf_use_s2(old), add = TRUE)
    pos <- tryCatch(sf::st_point_on_surface(sf::st_union(g)), error = function(e) NULL)
    if (is.null(pos)) return(c(NA_real_, NA_real_))
    pos <- tryCatch(sf::st_transform(pos, 4326), error = function(e) pos)
    xy  <- suppressWarnings(tryCatch(sf::st_coordinates(pos)[1, ], error = function(e) c(NA_real_, NA_real_)))
    if (length(xy) < 2) c(NA_real_, NA_real_) else c(xy[1], xy[2])
  }
  
  
  # ---------------------- outputs ----------------------
  results_rv <- reactiveVal(
    data.frame(OA = character(), PCON = character(), Postcodes = character(), stringsAsFactors = FALSE)
  )
  output$results <- renderTable(results_rv(), rownames = FALSE)
  output$postcodes_ui <- renderUI(NULL)
  
output$map <- renderLeaflet({
  # Minimal, guaranteed base map
  message("Rendering base Leaflet map...")
  leaflet(options = leafletOptions(zoomControl = TRUE)) %>%
    addTiles() %>%
    setView(lng = -1.8, lat = 52.8, zoom = 6) %>%  # UK-ish start
    addLayersControl(
      overlayGroups = c("pcon", "oa_boundaries", "oa_selected", "oa_point"),
      options = layersControlOptions(collapsed = FALSE)
    )
})

# After the widget is mounted in the browser, make sure size is valid
session$onFlushed(function() {
  message("onFlushed: invalidating Leaflet size and re-centering base view")
  session$sendCustomMessage("leafletInvalidate", list())
  leafletProxy("map") %>% setView(lng = -1.8, lat = 52.8, zoom = 6)
}, once = TRUE)
  
  # Log actual client-side movements
  observeEvent(input$map_bounds, ignoreInit = TRUE, {
    b <- input$map_bounds
    logit(sprintf("Leaflet moved: bounds now [%.6f, %.6f, %.6f, %.6f]",
                  b$west, b$south, b$east, b$north))
  })
  observeEvent(input$map_zoom, ignoreInit = TRUE, {
    logit(sprintf("Leaflet zoom now: %s", as.character(input$map_zoom)))
  })
  
  # ---------------------- main action ----------------------
  observeEvent(input$submit, {
    # --- read and reset ---
    oa_in <- trimws(input$oa_value)
    
    # clear just the relevant groups so a new OA redraws cleanly
    proxy <- leafletProxy("map") %>%
      clearGroup("pcon") %>%
      clearGroup("oa_boundaries") %>%
      clearGroup("oa_selected") %>%
      clearGroup("oa_point")
    
    if (oa_in == "" || is.na(oa_in)) {
      showNotification("Enter a single OA code (oa21cd).", type = "warning")
      return()
    }
    
    # --- (1) Postcodes: exactly as before ---
    pcs_vec <- fetch_postcodes_bq(oa_in)
    pcs_str <- if (length(pcs_vec)) paste(pcs_vec, collapse = ", ") else NA_character_
    
    # get PCON for table from ED parquet
    ed_row <- dplyr::filter(ed_slim, oa21cd == oa_in)
    pcon   <- if (nrow(ed_row)) ed_row$pcon25cd[1] else NA_character_
    
    results_rv(data.frame(OA = oa_in, PCON = pcon, Postcodes = pcs_str, stringsAsFactors = FALSE))
    output$postcodes_ui <- renderUI({
      if (!is.na(pcs_str)) tagList(tags$strong("Postcodes: "), tags$span(pcs_str))
      else tags$em("No postcodes returned.")
    })
    
    # --- guard ---
    if (is.na(pcon)) {
      showNotification("PCON not found for this OA.", type = "error")
      return()
    }
    
    # --- (2) OA geometries in this PCON + selected OA ---
    pcon_oas <- dplyr::filter(geo_sf, pcon25cd == pcon)
    if (nrow(pcon_oas) == 0) {
      showNotification(paste("No geometry for PCON:", pcon), type = "error")
      return()
    }
    pcon_oas$geometry <- sf::st_make_valid(pcon_oas$geometry)
    
    sel_oa <- dplyr::filter(pcon_oas, oa21cd == oa_in) |> dplyr::slice_head(n = 1)
    if (nrow(sel_oa) == 1) sel_oa$geometry <- sf::st_make_valid(sel_oa$geometry)
    
    # --- (3) PCON union polygon ---
    pcon_union <- suppressWarnings(sf::st_union(pcon_oas))
    if (inherits(pcon_union, "sfc_GEOMETRYCOLLECTION"))
      pcon_union <- sf::st_collection_extract(pcon_union, "POLYGON")
    pcon_sf <- sf::st_as_sf(data.frame(pcon25cd = pcon), geometry = pcon_union)
    
    # Draw PCON (light fill)
    proxy %>% addPolygons(
      data = pcon_sf,
      group = "pcon",
      color = "#2b8cbe", weight = 1, opacity = 0.9,
      fillColor = "#a6cee3", fillOpacity = 0.3,
      popup = ~paste0("<b>OA:</b> ", htmltools::htmlEscape(oa_in),
                      "<br><b>PCON:</b> ", htmltools::htmlEscape(pcon))
    )
    
    # OA boundaries (thin grey lines)
    proxy %>% addPolylines(
      data = pcon_oas,
      group = "oa_boundaries",
      color = "#666666", weight = 1, opacity = 0.8
    )
    
    # Highlight selected OA (thick outline + different fill), if present
    if (nrow(sel_oa) == 1) {
      proxy %>% addPolygons(
        data = sel_oa,
        group = "oa_selected",
        color = "#d7301f", weight = 3, opacity = 1.0,
        fillColor = "#fdae61", fillOpacity = 0.45,
        popup = ~paste0("<b>OA:</b> ", htmltools::htmlEscape(oa_in),
                        "<br><b>PCON:</b> ", htmltools::htmlEscape(pcon))
      )
      
      # Pre-compute targets now so they’re captured in the onFlushed closure
      bb  <- suppressWarnings(tryCatch(sf::st_bbox(sel_oa), error = function(e) c(xmin=NA,ymin=NA,xmax=NA,ymax=NA)))
      ctr <- poly_point_lonlat(sel_oa)
      
      
      if (all(is.finite(bb))) {
        bbp <- pad_bbox(bb, 0.10)
        logit(sprintf("Leaflet zoom strategy: OA bounding box - bounds [%.6f, %.6f, %.6f, %.6f], zoom: fitBounds", 
                      bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
        session$sendCustomMessage("leafletInvalidate", list())
        proxy %>% fitBounds(bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"])
      } else {
        ctr <- poly_point_lonlat(sel_oa)
        # Defer the zoom until after Shiny flushes the draw commands
        session$onFlushed(function() {
          if (all(is.finite(bb))) {
            bbp <- pad_bbox(bb, 0.10)
            logit(sprintf("Leaflet zoom (onFlushed): OA bbox [%.6f, %.6f, %.6f, %.6f] → fitBounds",
                          bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
            session$sendCustomMessage("leafletInvalidate", list())
            leafletProxy("map") %>% fitBounds(bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"])
          } else if (is.finite(ctr[1]) && is.finite(ctr[2])) {
            logit(sprintf("Leaflet zoom (onFlushed): OA center lng=%.6f lat=%.6f z=13 → setView",
                          ctr[1], ctr[2]))
            session$sendCustomMessage("leafletInvalidate", list())
            leafletProxy("map") %>% setView(lng = ctr[1], lat = ctr[2], zoom = 13)
          } else {
            bbp <- sf::st_bbox(pcon_sf)
            logit(sprintf("Leaflet zoom (onFlushed): PCON fallback %s bbox [%.6f, %.6f, %.6f, %.6f] → fitBounds",
                          as.character(pcon), bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
            session$sendCustomMessage("leafletInvalidate", list())
            leafletProxy("map") %>% fitBounds(bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"])
          }
        }, once = TRUE)}
    } else {
      # No OA polygon (unexpected) → PCON bbox
      bbp <- sf::st_bbox(pcon_sf)
      session$onFlushed(function() {
        logit(sprintf("Leaflet zoom (onFlushed): No OA polygon → PCON %s bbox [%.6f, %.6f, %.6f, %.6f] → fitBounds",
                      as.character(pcon), bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
        session$sendCustomMessage("leafletInvalidate", list())
        leafletProxy("map") %>% fitBounds(bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"])
      }, once = TRUE)
    }
  })
}

# =================== Run ===================
shinyApp(ui, server)
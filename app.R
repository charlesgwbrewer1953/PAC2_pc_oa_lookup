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
version_no <- "0.9.17"
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
      helpText("Enter a single OA code (exact). The app derives PCON, draws the constituency, and highlights the OA*."),
      textInput("oa_value", "OA code:", value = "", placeholder = "e.g. E00092757"),
      actionButton("submit", "Run"),
      tags$hr(),
    #  uiOutput("postcodes_ui"),
      tags$hr(),
     
    # h4("Diagnostics"),
    #  verbatimTextOutput("diag"),
      helpText("* Due to the highly variable size of OAs, it may be necessary to resize manually. Use the + / - box to do this.")
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
# =================== Server ===================
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
    
    # Clean up geometry a bit (doesn't need to be perfect)
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
  
  # Base map: build once, no onFlushed, no forced invalidation
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(zoomControl = TRUE)) %>%
      addTiles() %>%
      setView(lng = -1.8, lat = 52.8, zoom = 6) %>%  # UK-ish start
      addLayersControl(
        overlayGroups = c("pcon", "oa_boundaries", "oa_selected"),
        options = layersControlOptions(collapsed = FALSE)
      )
  })
  
  # Optional client diagnostics; safe
  observeEvent(input$map_bounds, ignoreInit = TRUE, {
    b <- input$map_bounds
    logit(sprintf("Leaflet moved: [%.6f, %.6f, %.6f, %.6f]", b$west, b$south, b$east, b$north))
  })
  observeEvent(input$map_zoom, ignoreInit = TRUE, {
    logit(sprintf("Leaflet zoom: %s", as.character(input$map_zoom)))
  })
  
  ## ---------------- main action ----------------
  observeEvent(input$submit, {
    oa_in <- trimws(input$oa_value)
    
    # Clear previous layers (not the base map)
    proxy <- leafletProxy("map") %>%
      clearGroup("pcon") %>%
      clearGroup("oa_boundaries") %>%
      clearGroup("oa_selected") %>%
      clearGroup("oa_center")
    
    results_rv(data.frame(OA = character(), PCON = character(), Postcodes = character()))
    output$postcodes_ui <- renderUI(NULL)
    
    if (oa_in == "" || is.na(oa_in)) {
      showNotification("Enter a single OA code (oa21cd).", type = "warning")
      return()
    }
    logit("OA input: ", oa_in)
    
    # (1) Postcodes — your working BQ logic
    pcs_vec <- fetch_postcodes_bq(oa_in)
    pcs_str <- if (length(pcs_vec)) paste(pcs_vec, collapse = ", ") else NA_character_
    logit("Postcodes returned (n): ", length(pcs_vec))
    
    # OA -> PCON from ED parquet
    ed_row <- dplyr::filter(ed_slim, oa21cd == oa_in)
    logit("Rows in ED for OA: ", nrow(ed_row))
    if (nrow(ed_row) == 0) { showNotification("OA not found in ED parquet.", type = "error"); return() }
    pcon <- ed_row$pcon25cd[1]
    if (is.na(pcon)) { showNotification("PCON (pcon25cd) missing for this OA.", type = "error"); return() }
    logit("Derived PCON25CD: ", pcon)
    
    results_rv(data.frame(OA = oa_in, PCON = pcon, Postcodes = pcs_str, stringsAsFactors = FALSE))
    output$postcodes_ui <- renderUI({
      if (!is.na(pcs_str)) tagList(tags$strong("Postcodes: "), tags$span(pcs_str))
      else tags$em("No postcodes returned.")
    })
    
    # (2) OA geometries within this PCON + selected OA
    pcon_oas <- dplyr::filter(geo_sf, pcon25cd == pcon)
    logit("Geo rows matched in constituency parquet: ", nrow(pcon_oas))
    if (nrow(pcon_oas) == 0) { showNotification(paste("No geometry for PCON:", pcon), type = "error"); return() }
    pcon_oas$geometry <- sf::st_make_valid(pcon_oas$geometry)
    
    sel_oa <- pcon_oas %>%
      dplyr::filter(oa21cd == oa_in) %>%
      dplyr::slice_head(n = 1)
    logit("Selected OA rows in geo_sf: ", nrow(sel_oa))
    if (nrow(sel_oa) == 1) sel_oa$geometry <- sf::st_make_valid(sel_oa$geometry)
    
    # (3) PCON union + draw
    pcon_union <- suppressWarnings(sf::st_union(pcon_oas))
    if (inherits(pcon_union, "sfc_GEOMETRYCOLLECTION"))
      pcon_union <- sf::st_collection_extract(pcon_union, "POLYGON")
    pcon_sf <- sf::st_as_sf(data.frame(pcon25cd = pcon), geometry = pcon_union)
    
    leafletProxy("map") %>%
      addPolygons(
        data = pcon_sf,
        group = "pcon",
        color = "#2b8cbe", weight = 1, opacity = 0.9,
        fillColor = "#a6cee3", fillOpacity = 0.3,
        popup = ~paste0("<b>PCON:</b> ", htmltools::htmlEscape(pcon),
                        "<br><b>OA:</b> ", htmltools::htmlEscape(oa_in))
      ) %>%
      addPolylines(
        data = pcon_oas,
        group = "oa_boundaries",
        color = "#666666", weight = 1, opacity = 0.8
      )
    
    if (nrow(sel_oa) == 1) {
      leafletProxy("map") %>%
        addPolygons(
          data = sel_oa,
          group = "oa_selected",
          color = "#d7301f", weight = 3, opacity = 1.0,
          fillColor = "#fdae61", fillOpacity = 0.45,
          popup = ~paste0("<b>OA:</b> ", htmltools::htmlEscape(oa_in),
                          "<br><b>PCON:</b> ", htmltools::htmlEscape(pcon))
        )
    }
    
    ## (4) compute zoom target(s) now (plain R values), then apply immediately
    # Prefer OA bbox; fallback to centroid; finally PCON bbox
    zoom_done <- FALSE
    # --- (4) Zoom SEPARATELY using robust centroid-buffer box ---
    zb <- oa_zoom_box(sel_oa, meters = 2000)   # ~1 km box; adjust if you want tighter/looser
    if (!is.null(zb) && all(is.finite(zb))) {
      logit(sprintf("Zoom (OA centroid-buffer): [%.6f, %.6f, %.6f, %.6f]", zb[1], zb[2], zb[3], zb[4]))
      session$sendCustomMessage("leafletInvalidate", list())
      proxy %>% fitBounds(zb[1], zb[2], zb[3], zb[4])
    } else {
      # Fall back to PCON bbox if OA buffer fails for any reason
      bbp <- sf::st_bbox(pcon_sf)
      logit(sprintf("Zoom fallback (PCON bbox): [%.6f, %.6f, %.6f, %.6f]",
                    bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
      session$sendCustomMessage("leafletInvalidate", list())
      proxy %>% fitBounds(bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"])
    }
  })
}

# =================== Run ===================
shinyApp(ui, server)
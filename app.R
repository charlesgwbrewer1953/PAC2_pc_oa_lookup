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
# =================== Server ===================
# =================== Server ===================
server <- function(input, output, session) {
  # ---------------------- diagnostics ----------------------
  rv <- reactiveValues(log = character(0))
  ilog <- function(...) {
    msg <- paste0(format(Sys.time(), "%H:%M:%S"), " | ", paste0(..., collapse = ""))
    isolate({
      rv$log <- c(rv$log, msg)   # safe both inside/outside reactive contexts
    })
    message(msg)
  }
  output$diag <- renderText(paste(rv$log, collapse = "\n"))
  
  # Plan for zoom (computed during draw; executed later)
  zoom_target <- reactiveVal(NULL)
  
  # ---------------------- helpers ----------------------
  pad_bbox <- function(bb, frac = 0.10) {
    # bb can be a bbox object; coerce to numeric first
    bb_num <- as.numeric(bb)
    names(bb_num) <- c("xmin","ymin","xmax","ymax")
    dx <- (bb_num["xmax"] - bb_num["xmin"]) * frac
    dy <- (bb_num["ymax"] - bb_num["ymin"]) * frac
    c(xmin = bb_num["xmin"] - dx,
      ymin = bb_num["ymin"] - dy,
      xmax = bb_num["xmax"] + dx,
      ymax = bb_num["ymax"] + dy)
  }
  
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
    message("Rendering base Leaflet map...")
    leaflet(options = leafletOptions(zoomControl = TRUE)) %>%
      addTiles() %>%
      setView(lng = -1.8, lat = 52.8, zoom = 6) %>%
      addLayersControl(
        overlayGroups = c("pcon", "oa_boundaries", "oa_selected", "oa_point"),
        options = layersControlOptions(collapsed = FALSE)
      )
  })
  
  session$onFlushed(function() {
    message("onFlushed(init): invalidating Leaflet size and re-centering base view")
    session$sendCustomMessage("leafletInvalidate", list())
    leafletProxy("map") %>% setView(lng = -1.8, lat = 52.8, zoom = 6)
  }, once = TRUE)
  
  observeEvent(input$map_bounds, ignoreInit = TRUE, {
    b <- input$map_bounds
    ilog(sprintf("Leaflet moved: bounds now [%.6f, %.6f, %.6f, %.6f]", b$west, b$south, b$east, b$north))
  })
  observeEvent(input$map_zoom, ignoreInit = TRUE, {
    ilog(sprintf("Leaflet zoom now: %s", as.character(input$map_zoom)))
  })
  
  # ---------------------- DRAW + PLAN ZOOM ----------------------
  observeEvent(input$submit, {
    oa_in <- trimws(input$oa_value)
    
    # Clear visible layers for a clean redraw
    proxy <- leafletProxy("map") %>%
      clearGroup("pcon") %>%
      clearGroup("oa_boundaries") %>%
      clearGroup("oa_selected") %>%
      clearGroup("oa_point")
    
    # Clear any old plan
    zoom_target(NULL)
    
    if (oa_in == "" || is.na(oa_in)) {
      showNotification("Enter a single OA code (oa21cd).", type = "warning")
      return()
    }
    
    # (1) Postcodes
    pcs_vec <- fetch_postcodes_bq(oa_in)
    pcs_str <- if (length(pcs_vec)) paste(pcs_vec, collapse = ", ") else NA_character_
    
    # OA -> PCON from ED parquet
    ed_row <- dplyr::filter(ed_slim, oa21cd == oa_in)
    pcon   <- if (nrow(ed_row)) ed_row$pcon25cd[1] else NA_character_
    
    results_rv(data.frame(OA = oa_in, PCON = pcon, Postcodes = pcs_str, stringsAsFactors = FALSE))
    output$postcodes_ui <- renderUI({
      if (!is.na(pcs_str)) tagList(tags$strong("Postcodes: "), tags$span(pcs_str))
      else tags$em("No postcodes returned.")
    })
    
    if (is.na(pcon)) {
      showNotification("PCON not found for this OA.", type = "error"); return()
    }
    
    # (2) OA geometries in PCON, selected OA
    pcon_oas <- dplyr::filter(geo_sf, pcon25cd == pcon)
    if (nrow(pcon_oas) == 0) {
      showNotification(paste("No geometry for PCON:", pcon), type = "error"); return()
    }
    pcon_oas$geometry <- sf::st_make_valid(pcon_oas$geometry)
    
    sel_oa <- dplyr::filter(pcon_oas, oa21cd == oa_in) |> dplyr::slice_head(n = 1)
    if (nrow(sel_oa) == 1) sel_oa$geometry <- sf::st_make_valid(sel_oa$geometry)
    
    # (3) PCON union + draw layers
    pcon_union <- suppressWarnings(sf::st_union(pcon_oas))
    if (inherits(pcon_union, "sfc_GEOMETRYCOLLECTION"))
      pcon_union <- sf::st_collection_extract(pcon_union, "POLYGON")
    pcon_sf <- sf::st_as_sf(data.frame(pcon25cd = pcon), geometry = pcon_union)
    
    proxy %>% addPolygons(
      data = pcon_sf,
      group = "pcon",
      color = "#2b8cbe", weight = 1, opacity = 0.9,
      fillColor = "#a6cee3", fillOpacity = 0.30,
      popup = ~paste0("<b>OA:</b> ", htmltools::htmlEscape(oa_in),
                      "<br><b>PCON:</b> ", htmltools::htmlEscape(pcon))
    )
    
    proxy %>% addPolylines(
      data = pcon_oas,
      group = "oa_boundaries",
      color = "#666666", weight = 1, opacity = 0.8
    )
    
    if (nrow(sel_oa) == 1) {
      proxy %>% addPolygons(
        data = sel_oa,
        group = "oa_selected",
        color = "#d7301f", weight = 3, opacity = 1.0,
        fillColor = "#fdae61", fillOpacity = 0.45,
        popup = ~paste0("<b>OA:</b> ", htmltools::htmlEscape(oa_in),
                        "<br><b>PCON:</b> ", htmltools::htmlEscape(pcon))
      )
      
      # ---- Prepare ZOOM PLAN (no zoom yet) ----
      bb <- suppressWarnings(tryCatch(sf::st_bbox(sel_oa), error = function(e) NULL))
      if (!is.null(bb)) {
        bb_num <- as.numeric(bb)
        if (all(is.finite(bb_num))) {
          names(bb_num) <- c("xmin","ymin","xmax","ymax")
          bbp <- pad_bbox(bb_num, 0.10)
          ilog(sprintf("Zoom plan (OA bbox): [%.6f, %.6f, %.6f, %.6f]",
                       bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
          zoom_target(list(type = "bounds",
                           xmin = bbp["xmin"], ymin = bbp["ymin"],
                           xmax = bbp["xmax"], ymax = bbp["ymax"]))
        } else {
          ctr <- poly_point_lonlat(sel_oa)
          if (is.finite(ctr[1]) && is.finite(ctr[2])) {
            ilog(sprintf("Zoom plan (OA center): lng=%.6f lat=%.6f z=13", ctr[1], ctr[2]))
            zoom_target(list(type = "center", lng = ctr[1], lat = ctr[2], zoom = 13))
          } else {
            bbp <- as.numeric(sf::st_bbox(pcon_sf)); names(bbp) <- c("xmin","ymin","xmax","ymax")
            ilog(sprintf("Zoom plan (PCON bbox fallback): [%.6f, %.6f, %.6f, %.6f]",
                         bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
            zoom_target(list(type = "bounds",
                             xmin = bbp["xmin"], ymin = bbp["ymin"],
                             xmax = bbp["xmax"], ymax = bbp["ymax"]))
          }
        }
      } else {
        # no bbox at all -> center or pcon bbox
        ctr <- poly_point_lonlat(sel_oa)
        if (is.finite(ctr[1]) && is.finite(ctr[2])) {
          ilog(sprintf("Zoom plan (OA center no bbox): lng=%.6f lat=%.6f z=13", ctr[1], ctr[2]))
          zoom_target(list(type = "center", lng = ctr[1], lat = ctr[2], zoom = 13))
        } else {
          bbp <- as.numeric(sf::st_bbox(pcon_sf)); names(bbp) <- c("xmin","ymin","xmax","ymax")
          ilog(sprintf("Zoom plan (PCON bbox fallback no bbox): [%.6f, %.6f, %.6f, %.6f]",
                       bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
          zoom_target(list(type = "bounds",
                           xmin = bbp["xmin"], ymin = bbp["ymin"],
                           xmax = bbp["xmax"], ymax = bbp["ymax"]))
        }
      }
    } else {
      # No OA polygon (unexpected) → PCON bbox plan
      bbp <- as.numeric(sf::st_bbox(pcon_sf)); names(bbp) <- c("xmin","ymin","xmax","ymax")
      ilog(sprintf("Zoom plan (no OA polygon → PCON bbox): [%.6f, %.6f, %.6f, %.6f]",
                   bbp["xmin"], bbp["ymin"], bbp["xmax"], bbp["ymax"]))
      zoom_target(list(type = "bounds",
                       xmin = bbp["xmin"], ymin = bbp["ymin"],
                       xmax = bbp["xmax"], ymax = bbp["ymax"]))
    }
  })
  
  # ---------------------- separate zoom executor ----------------------
  observeEvent(zoom_target(), ignoreInit = TRUE, {
    plan <- zoom_target(); req(plan)
    
    session$onFlushed(function() {
      # IMPORTANT: don't touch rv$log here; use message() to avoid reactive write
      session$sendCustomMessage("leafletInvalidate", list())
      proxy <- leafletProxy("map")
      
      if (identical(plan$type, "bounds")) {
        message(sprintf("Executing zoom: fitBounds [%.6f, %.6f, %.6f, %.6f]",
                        plan$xmin, plan$ymin, plan$xmax, plan$ymax))
        proxy %>% fitBounds(plan$xmin, plan$ymin, plan$xmax, plan$ymax)
      } else if (identical(plan$type, "center")) {
        message(sprintf("Executing zoom: setView lng=%.6f lat=%.6f z=%s",
                        plan$lng, plan$lat, as.character(plan$zoom)))
        proxy %>% setView(lng = plan$lng, lat = plan$lat, zoom = plan$zoom)
      }
      
      # clear the plan after applying
      zoom_target(NULL)
    }, once = TRUE)
  })
}

# =================== Run ===================
shinyApp(ui, server)
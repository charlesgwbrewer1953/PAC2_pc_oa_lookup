# OA_Postcode_Map — Single OA → PCON map + BQ Postcodes
# Uses st_as_sf(geo_data) + st_crs(4326) exactly like your working app
# Version: 0.6.2
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
version_no <- "0.7.0"
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

# BigQuery config (inline SQL — your original working logic)
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

# Inline-BQ postcode lookup (exactly as your snippet does it)
fetch_postcodes_bq <- function(oa_code) {
  query <- paste0(
    "SELECT pcd2 FROM `", BQ_PROJECT, ".", BQ_DATASET, ".", BQ_TABLE, "` ",
    "WHERE oa21 = '", oa_code, "'"
  )
  result <- tryCatch({
    job <- bigrquery::bq_project_query(BQ_PROJECT, query)
    df  <- bigrquery::bq_table_download(job)
    if (nrow(df) > 0) sort(unique(df$pcd2)) else character(0)
  }, error = function(e) {
    warning(paste("Error executing BigQuery for", oa_code, ":", e$message))
    character(0)
  })
  result
}

# OA bbox padding helper
pad_bbox <- function(bb, frac = 0.15) {
  dx <- (bb["xmax"] - bb["xmin"]) * frac
  dy <- (bb["ymax"] - bb["ymin"]) * frac
  c(lng1 = bb["xmin"] - dx, lat1 = bb["ymin"] - dy, lng2 = bb["xmax"] + dx, lat2 = bb["ymax"] + dy)
}

# =================== Auth & Data Load ===================
message("Checking auth file exists: ", file.exists(AUTH_JSON))
googleCloudStorageR::gcs_auth(AUTH_JSON)
bigrquery::bq_auth(path = AUTH_JSON)

download_if_missing(GEO_BUCKET, GEO_OBJECT, GEO_LOCAL)
download_if_missing(ED_BUCKET,  ED_OBJECT,  ED_LOCAL)

# ED table (mapping OA -> PCON, and possibly OA geometry/centroid)
ed_raw <- as.data.frame(arrow::read_parquet(ED_LOCAL))
must_have_col(ed_raw, "oa21cd")
must_have_col(ed_raw, "pcon25cd")

# optional OA centroid columns if present
lat_col <- if ("lat" %in% names(ed_raw)) "lat" else if ("latitude" %in% names(ed_raw)) "latitude" else NULL
lon_col <- if ("lon" %in% names(ed_raw)) "lon" else if ("longitude" %in% names(ed_raw)) "longitude" else NULL

ed_slim <- ed_raw %>% select(oa21cd, pcon25cd, all_of(c(lat_col, lon_col)))

# ======= >>> THIS is the important replacement using your working logic <<< =======
# Constituency parquet -> sf using st_as_sf(geo_data) then set WGS84
geo_data <- arrow::read_parquet(GEO_LOCAL)
geo_sf   <- sf::st_as_sf(geo_data)      # <-- your app's approach
sf::st_crs(geo_sf) <- 4326              # <-- your app: force WGS84 for Leaflet
must_have_col(geo_sf, "pcon25cd")
# Prefer human-readable name if present (e.g., pcon25nm or any *nm)
geo_name_col <- if ("pcon25nm" %in% names(geo_sf)) "pcon25nm" else {
  nm_alt <- grep("nm$", names(geo_sf), value = TRUE)
  if (length(nm_alt)) nm_alt[1] else NULL
}
# ==================================================================================

# =================== UI ===================
ui <- fluidPage(
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
  rv <- reactiveValues(log = character(0))
  logit <- function(...) { msg <- paste0(format(Sys.time(), "%H:%M:%S"), " | ", paste0(..., collapse = "")); rv$log <- c(rv$log, msg); message(msg) }
  output$diag <- renderText(paste(rv$log, collapse = "\n"))
  
  results_rv <- reactiveVal(data.frame(OA = character(), PCON = character(), Postcodes = character(), stringsAsFactors = FALSE))
  output$results <- renderTable(results_rv(), rownames = FALSE)
  output$postcodes_ui <- renderUI(NULL)
  
  output$map <- renderLeaflet({
    leaflet() |>
      addTiles() |>
      addLayersControl(
        overlayGroups = c("pcon", "oa_boundaries", "oa_selected", "oa_point"),
        options = layersControlOptions(collapsed = FALSE)
      )
  })
  
  observeEvent(input$submit, {
    # -----------------------------------------------------------
    # Helpers (local to this observer)
    # -----------------------------------------------------------
    zoom_after_flush <- function(lng1, lat1, lng2, lat2) {
      # sanity checks
      vals <- c(lng1, lat1, lng2, lat2)
      if (any(!is.finite(vals))) return(invisible(FALSE))
      if (abs(lng2 - lng1) < 1e-8 || abs(lat2 - lat1) < 1e-8) return(invisible(FALSE))
      # defer until leaflet has finished adding layers
      session$onFlushed(function() {
        leafletProxy("map") %>% fitBounds(lng1, lat1, lng2, lat2)
        leafletProxy("map") %>% flyToBounds(lng1, lat1, lng2, lat2)
      }, once = TRUE)
      TRUE
    }
    
    viewport_from_geom <- function(sf_geom, pad_frac = 0.15) {
      # If polygon has area, use padded bbox; else 1.2km buffer around centroid
      a <- suppressWarnings(as.numeric(sf::st_area(sf::st_union(sf_geom))))
      if (is.finite(a) && a > 0) {
        bb <- sf::st_bbox(sf_geom)
        dx <- (bb["xmax"] - bb["xmin"]) * pad_frac
        dy <- (bb["ymax"] - bb["ymin"]) * pad_frac
        return(c(bb["xmin"] - dx, bb["ymin"] - dy, bb["xmax"] + dx, bb["ymax"] + dy))
      }
      ctr <- sf::st_centroid(sf::st_union(sf_geom))
      ctrm <- sf::st_transform(ctr, 3857)
      buf  <- sf::st_buffer(ctrm, dist = 1200)
      bb   <- sf::st_bbox(sf::st_transform(buf, 4326))
      c(bb["xmin"], bb["ymin"], bb["xmax"], bb["ymax"])
    }
    
    # -----------------------------------------------------------
    # Start fresh
    # -----------------------------------------------------------
    oa_in <- trimws(input$oa_value)
    rv$log <- character(0)
    
    leafletProxy("map") |> clearShapes() |> clearMarkers()
    output$postcodes_ui <- renderUI(NULL)
    results_rv(data.frame(OA = character(), PCON = character(), Postcodes = character()))
    
    if (identical(oa_in, "") || is.na(oa_in)) {
      showNotification("Please enter a single OA code.", type = "warning")
      logit("No OA provided.")
      return()
    }
    logit("OA input: ", oa_in)
    
    # -----------------------------------------------------------
    # OA -> PCON (from ED parquet)
    # -----------------------------------------------------------
    ed_oa <- ed_slim %>% dplyr::filter(oa21cd == oa_in)
    logit("Rows in ED for OA: ", nrow(ed_oa))
    if (nrow(ed_oa) == 0) {
      showNotification("OA not found in ED parquet.", type = "error", duration = 8)
      logit("FAIL: OA not found.")
      return()
    }
    
    sel_pcon <- ed_oa$pcon25cd[which(!is.na(ed_oa$pcon25cd))][1]
    logit("Derived PCON25CD: ", sel_pcon)
    if (is.na(sel_pcon)) {
      showNotification("PCON (pcon25cd) missing for this OA.", type = "error", duration = 8)
      logit("FAIL: pcon25cd is NA.")
      return()
    }
    
    # -----------------------------------------------------------
    # Postcodes (BigQuery) — your exact inline logic via fetch_postcodes_bq()
    # -----------------------------------------------------------
    pcs_vec <- fetch_postcodes_bq(oa_in)
    pcs_str <- if (length(pcs_vec)) paste(pcs_vec, collapse = ", ") else NA_character_
    results_rv(data.frame(OA = oa_in, PCON = sel_pcon, Postcodes = pcs_str, stringsAsFactors = FALSE))
    output$postcodes_ui <- renderUI({
      if (!is.na(pcs_str)) tagList(tags$strong("Postcodes: "), tags$span(pcs_str)) else tags$em("No postcodes returned.")
    })
    logit("Postcodes returned (n): ", length(pcs_vec))
    
    # -----------------------------------------------------------
    # PCON polygon (union of all rows in geo_sf for this code)
    # -----------------------------------------------------------
    # Diagnostics summary
    logit("geo_sf: class = ", paste(class(geo_sf), collapse = "/"))
    logit("geo_sf: nrow = ", nrow(geo_sf), ", names = ", paste(utils::head(names(geo_sf), 8), collapse = ", "))
    logit("geo_sf: has pcon25cd? ", "pcon25cd" %in% names(geo_sf), ", has oa21cd? ", "oa21cd" %in% names(geo_sf))
    
    geo_rows <- geo_sf %>% dplyr::filter(pcon25cd == sel_pcon)
    logit("Geo rows matched in constituency parquet: ", nrow(geo_rows))
    if (nrow(geo_rows) == 0) {
      showNotification(sprintf("No constituency geometry found for pcon25cd=%s", sel_pcon),
                       type = "error", duration = 10)
      logit("FAIL: No PCON geometry.")
      return()
    }
    
    # Union & draw PCON
    bb_before <- sf::st_bbox(geo_rows)
    logit(sprintf("PCON bbox BEFORE union: [%.6f, %.6f, %.6f, %.6f]",
                  bb_before["xmin"], bb_before["ymin"], bb_before["xmax"], bb_before["ymax"]))
    
    geo_rows$geometry <- sf::st_make_valid(geo_rows$geometry)
    pcon_union <- sf::st_union(geo_rows)
    if (inherits(pcon_union, "sfc_GEOMETRYCOLLECTION")) {
      pcon_union <- sf::st_collection_extract(pcon_union, "POLYGON")
    }
    pcon_sf <- sf::st_as_sf(data.frame(pcon25cd = sel_pcon, stringsAsFactors = FALSE), geometry = pcon_union)
    
    bb_after <- sf::st_bbox(pcon_sf)
    logit(sprintf("PCON bbox AFTER union:  [%.6f, %.6f, %.6f, %.6f]",
                  bb_after["xmin"], bb_after["ymin"], bb_after["xmax"], bb_after["ymax"]))
    
    pcon_label <- if ("pcon25nm" %in% names(geo_rows)) {
      geo_rows$pcon25nm[ which.max(sf::st_area(geo_rows)) ]
    } else sel_pcon
    logit("PCON label: ", ifelse(is.na(pcon_label), "<NA>", as.character(pcon_label)))
    
    leafletProxy("map") |>
      clearShapes() |>
      clearMarkers() |>
      addPolygons(
        data = pcon_sf,
        weight = 1, opacity = 0.9, fillOpacity = 0.20,
        color = "#555555", fillColor = "#99c2ff",
        popup = paste0(
          "<strong>Constituency:</strong> ", htmltools::htmlEscape(pcon_label),
          "<br><strong>PCON25CD:</strong> ", htmltools::htmlEscape(sel_pcon),
          "<br><strong>OA:</strong> ", htmltools::htmlEscape(oa_in)
        ),
        group = "pcon"
      )
    logit("PCON polygon drawn (union).")
    
    # -----------------------------------------------------------
    # OA boundaries from geo_sf (all OAs in this PCON)
    # -----------------------------------------------------------
    oa_in_pcon <- geo_sf %>% dplyr::filter(pcon25cd == sel_pcon)
    logit("OA boundary candidates (geo_sf) for PCON: ", nrow(oa_in_pcon))
    
    if (nrow(oa_in_pcon) > 0) {
      # draw as thin outlines
      leafletProxy("map") |>
        addPolylines(
          data = oa_in_pcon,
          color = "#666666", weight = 1, opacity = 0.8,
          group = "oa_boundaries"
        )
      logit("OA boundaries drawn from geo_sf.")
    } else {
      logit("No OA rows for this PCON in geo_sf (unexpected).")
    }
    
    # -----------------------------------------------------------
    # Selected OA polygon (from geo_sf) + Deferrred Zoom
    # -----------------------------------------------------------
    zoom_done <- FALSE
    sel_oa_poly <- geo_sf %>%
      dplyr::filter(oa21cd == oa_in) %>%
      dplyr::slice_head(n = 1)
    logit("Selected OA rows in geo_sf: ", nrow(sel_oa_poly))
    
    if (nrow(sel_oa_poly) == 1) {
      sel_oa_poly$geometry <- sf::st_make_valid(sel_oa_poly$geometry)
      
      leafletProxy("map") |>
        addPolygons(
          data = sel_oa_poly,
          weight = 3, opacity = 1.0, color = "#d7301f",
          fillOpacity = 0.35, fillColor = "#fc8d59",
          popup = paste0(
            "<strong>OA:</strong> ", htmltools::htmlEscape(oa_in),
            "<br><strong>PCON25CD:</strong> ", htmltools::htmlEscape(sel_pcon),
            if (!is.na(pcon_label)) paste0("<br><strong>Constituency:</strong> ", htmltools::htmlEscape(pcon_label)) else ""
          ),
          group = "oa_selected"
        )
      
      vp <- viewport_from_geom(sel_oa_poly)
      ok <- zoom_after_flush(vp[1], vp[2], vp[3], vp[4])
      if (isTRUE(ok)) {
        zoom_done <- TRUE
        logit("Selected OA highlighted & zoomed (deferred).")
      } else {
        logit("Viewport invalid after OA polygon; will try centroid/PCON.")
      }
    } else {
      logit("Selected OA not found as polygon in geo_sf; will try centroid/PCON.")
    }
    
    # -----------------------------------------------------------
    # Fallback: OA centroid (~1.2 km) → PCON bbox
    # -----------------------------------------------------------
    if (!zoom_done && !is.null(lat_col) && !is.null(lon_col) &&
        !is.na(ed_oa[[lat_col]][1]) && !is.na(ed_oa[[lon_col]][1])) {
      
      sel_lat <- suppressWarnings(as.numeric(ed_oa[[lat_col]][1]))
      sel_lon <- suppressWarnings(as.numeric(ed_oa[[lon_col]][1]))
      
      leafletProxy("map") |>
        addCircleMarkers(
          lng = sel_lon, lat = sel_lat, radius = 7,
          color = "#d7301f", fillColor = "#fc8d59", fillOpacity = 0.95, weight = 2,
          popup = paste0(
            "<strong>OA:</strong> ", htmltools::htmlEscape(oa_in),
            "<br><strong>PCON25CD:</strong> ", htmltools::htmlEscape(sel_pcon),
            if (!is.na(pcon_label)) paste0("<br><strong>Constituency:</strong> ", htmltools::htmlEscape(pcon_label)) else ""
          ),
          group = "oa_point"
        )
      
      # 1.2 km viewport around centroid
      pt  <- sf::st_sfc(sf::st_point(c(sel_lon, sel_lat)), crs = 4326)
      ptm <- sf::st_transform(pt, 3857); buf <- sf::st_buffer(ptm, dist = 1200)
      bb  <- sf::st_bbox(sf::st_transform(buf, 4326))
      ok  <- zoom_after_flush(bb["xmin"], bb["ymin"], bb["xmax"], bb["ymax"])
      if (isTRUE(ok)) {
        zoom_done <- TRUE
        logit("OA centroid drawn & zoomed (deferred).")
      }
    }
    
    if (!zoom_done) {
      bb <- sf::st_bbox(pcon_sf)
      ok <- zoom_after_flush(bb["xmin"], bb["ymin"], bb["xmax"], bb["ymax"])
      if (isTRUE(ok)) {
        logit("Zoomed to PCON bbox (deferred).")
      } else {
        logit("Fallback zoom failed sanity checks; leaving current view.")
      }
    }
  })
  
}

# =================== Run ===================
shinyApp(ui, server)
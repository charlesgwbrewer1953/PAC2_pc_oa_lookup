# OA_Postcode_Map — single OA → PCON map & BigQuery postcode lookup
# Version: 0.5.0
# Status : DEV

# =============== Libraries ===============
library(shiny)
library(googleCloudStorageR)
library(arrow)
library(dplyr)
library(sf)
library(leaflet)
library(geojsonsf)
library(jsonlite)
library(bigrquery)

# =============== Constants ===============
version_no <- "0.5.0"
op_status  <- "DEV"

# Auth file (used for both GCS and BigQuery)
AUTH_JSON <- "./data/astral-name-419808-ab8473ded5ad.json"

# GCS objects (cached locally)
GEO_BUCKET <- "pac10_geojson_parquet"
GEO_OBJECT <- "ED_geoJSON_pcon.parquet"
ED_BUCKET  <- "demographikon_shared"
ED_OBJECT  <- "ED_complete_20250720.parquet"

dir.create("./data", showWarnings = FALSE, recursive = TRUE)
GEO_LOCAL <- file.path("data", basename(GEO_OBJECT))
ED_LOCAL  <- file.path("data", basename(ED_OBJECT))

# BigQuery config for postcodes
BQ_PROJECT   <- "politicalmaps"
BQ_DATASET   <- "PAC_reference_data"
BQ_TABLE     <- "PAC_PC_Xref_5_2024"   # expects columns: oa21, pcd2

# =============== Utilities ===============
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

first_match_col <- function(nms, candidates) {
  nms_lo <- tolower(nms)
  for (cand in tolower(candidates)) {
    hit <- which(nms_lo == cand)
    if (length(hit)) return(nms[hit[1]])
  }
  return(NA_character_)
}

is_list_of_raw <- function(x) {
  is.list(x) && length(x) > 0 && all(vapply(x, is.raw, logical(1)))
}

to_char_vector <- function(x) {
  if (is.character(x)) return(x)
  if (is.list(x)) {
    out <- vapply(x, function(el) {
      if (is.null(el)) return(NA_character_)
      if (is.character(el) && length(el) == 1) return(el)
      if (is.raw(el)) return(rawToChar(el))  # only if text
      tryCatch(jsonlite::toJSON(el, auto_unbox = TRUE, null = "null"),
               error = function(...) NA_character_)
    }, character(1))
    return(out)
  }
  if (is.raw(x)) return(rawToChar(x))
  as.character(x)
}

coerce_to_sf <- function(df) {
  nms <- names(df)
  id_col  <- first_match_col(nms, c("pcon25cd","pcon","pcon_name","constituency","const_name","name"))
  if (is.na(id_col)) stop("Geo parquet: missing PCON id column (pcon25cd/pcon/pcon_name/...).")
  geom_col <- first_match_col(nms, c("wkb","geom","geometry","geojson","wkt"))
  if (is.na(geom_col)) stop("Geo parquet: missing geometry column (wkb/geom/geometry/geojson/wkt).")
  
  x <- df[[geom_col]]
  if (is_list_of_raw(x)) {
    sfc <- sf::st_as_sfc(structure(x, class = "WKB"), crs = 4326)
    sfd <- sf::st_as_sf(data.frame(df[[id_col]], stringsAsFactors = FALSE), geometry = sfc)
    names(sfd)[1] <- id_col
  } else if (is.character(x) || (is.list(x) && all(vapply(x, function(el) is.character(el) && length(el) == 1, logical(1))))) {
    gtxt <- to_char_vector(x)
    looks_json <- !is.na(gtxt) & grepl("^\\s*[\\{\\[]", gtxt)
    if (any(looks_json, na.rm = TRUE)) {
      sfd <- geojsonsf::geojson_sf(gtxt)
      if (!(id_col %in% names(sfd))) sfd[[id_col]] <- df[[id_col]]
    } else {
      tmp <- data.frame(df[[id_col]], wkt = gtxt, stringsAsFactors = FALSE)
      names(tmp)[1] <- id_col
      sfd <- sf::st_as_sf(tmp, wkt = "wkt", crs = 4326)
    }
  } else {
    stop(sprintf("Geometry column '%s' has unsupported class: %s",
                 geom_col, paste(class(x), collapse = "/")))
  }
  
  if (is.na(sf::st_crs(sfd))) sf::st_crs(sfd) <- 4326 else sfd <- sf::st_transform(sfd, 4326)
  attr(sfd, "pcon_id_col") <- id_col
  sfd
}

locate_lat_lon_cols <- function(df) {
  nms <- tolower(names(df))
  lat_candidates <- c("lat","latitude","oa_lat","oa_latitude","centroid_lat","lat_cen","oa_lat_centroid")
  lon_candidates <- c("lon","lng","long","longitude","oa_lon","oa_longitude","centroid_lon","centroid_lng","long_cen","oa_lon_centroid")
  lat_col <- names(df)[match(lat_candidates, nms, nomatch = 0)]
  lon_col <- names(df)[match(lon_candidates, nms, nomatch = 0)]
  list(lat = if (length(lat_col)) lat_col[1] else NULL,
       lon = if (length(lon_col)) lon_col[1] else NULL)
}

find_geo_rows_by_pcon <- function(geo_sf, val) {
  cand_cols <- intersect(names(geo_sf), c("pcon25cd","pcon","pcon_name","constituency","const_name","name"))
  for (cc in cand_cols) {
    hit <- which(!is.na(geo_sf[[cc]]) & geo_sf[[cc]] == val)
    if (length(hit)) return(geo_sf[hit, , drop = FALSE])
  }
  v_low <- tolower(as.character(val))
  for (cc in cand_cols) {
    col_low <- tolower(as.character(geo_sf[[cc]]))
    hit <- which(!is.na(col_low) & col_low == v_low)
    if (length(hit)) return(geo_sf[hit, , drop = FALSE])
  }
  geo_sf[0, , drop = FALSE]
}

buffer_bbox_from_point <- function(lon, lat, meters = 1200) {
  pt <- sf::st_sfc(sf::st_point(c(lon, lat)), crs = 4326)
  ptm <- sf::st_transform(pt, 3857)               # Web Mercator (meters)
  buf <- sf::st_buffer(ptm, dist = meters)
  bb  <- sf::st_bbox(sf::st_transform(buf, 4326)) # back to WGS84
  c(lng1 = bb["xmin"], lat1 = bb["ymin"], lng2 = bb["xmax"], lat2 = bb["ymax"])
}

# BigQuery postcode fetch for a single OA (returns character vector of pcd2)
# BigQuery postcode fetch for a single OA (returns character vector of pcd2)
fetch_postcodes_bq <- function(oa_code) {
  query <- paste0(
    "SELECT pcd2 FROM `", BQ_PROJECT, ".", BQ_DATASET, ".", BQ_TABLE, "` ",
    "WHERE oa21 = '", oa_code, "'"
  )
  
  result <- tryCatch({
    job <- bigrquery::bq_project_query(BQ_PROJECT, query)
    df  <- bigrquery::bq_table_download(job)
    if (nrow(df) > 0) unique(df$pcd2) else character(0)
  }, error = function(e) {
    warning(paste("Error executing BigQuery for", oa_code, ":", e$message))
    character(0)
  })
  
  result
}

# =============== Auth & Data Load (once) ===============
message("Checking auth file exists: ", file.exists(AUTH_JSON))
googleCloudStorageR::gcs_auth(AUTH_JSON)
bigrquery::bq_auth(path = AUTH_JSON)

download_if_missing(GEO_BUCKET, GEO_OBJECT, GEO_LOCAL)
download_if_missing(ED_BUCKET,  ED_OBJECT,  ED_LOCAL)

geo_raw <- as.data.frame(arrow::read_parquet(GEO_LOCAL))
ed_raw  <- as.data.frame(arrow::read_parquet(ED_LOCAL))

geo_sf  <- coerce_to_sf(geo_raw)
geo_id  <- attr(geo_sf, "pcon_id_col")

# ED columns (for OA → PCON and OA lat/lon centering)
oa_col       <- first_match_col(names(ed_raw), c("oa21cd","oa","oa_code","oaid","oa_2011","oa_2021"))
pcon_col     <- first_match_col(names(ed_raw), c("pcon25cd","pcon","constituency","pcon_name","const_name"))
if (is.na(oa_col) || is.na(pcon_col)) {
  stop("ED parquet: need OA (~oa21cd/oa_code/...) and PCON (~pcon25cd/pcon/constituency/...).")
}
latlon_cols <- locate_lat_lon_cols(ed_raw)

keep_cols <- unique(na.omit(c(oa_col, pcon_col, latlon_cols$lat, latlon_cols$lon)))
ed_slim <- ed_raw %>% dplyr::select(dplyr::all_of(keep_cols))

# =============== UI ===============
ui <- fluidPage(
  titlePanel("OA → PCON map + Postcodes (BigQuery)"),
  sidebarLayout(
    sidebarPanel(
      helpText("Enter a single OA code. The map centers tightly on that OA and shows its constituency."),
      textInput("oa_value", "OA code:", value = "", placeholder = "e.g. E00000001"),
      actionButton("submit", "Show Map & Postcodes"),
      tags$hr(),
      uiOutput("postcodes_ui")
    ),
    mainPanel(
      leafletOutput("map", height = 650),
      tags$hr(),
      h4("Result"),
      tableOutput("results")
    )
  )
)

# =============== Server ===============
server <- function(input, output, session) {
  results_rv <- reactiveVal(
    data.frame(OA = character(), PCON = character(), Postcodes = character(), stringsAsFactors = FALSE)
  )
  
  output$results <- renderTable(results_rv(), rownames = FALSE)
  output$postcodes_ui <- renderUI(NULL)
  
  output$map <- renderLeaflet({
    leaflet() |> addTiles()
  })
  
  observeEvent(input$submit, {
    oa_in <- trimws(input$oa_value %||% "")
    
    # Enforce single OA: if multiple tokens, take the first and warn
    if (grepl(",", oa_in, fixed = TRUE) || grepl("\\s+", oa_in) && length(strsplit(oa_in, "\\s+")[[1]]) > 1) {
      tokens <- strsplit(oa_in, "[,\\s]+")[[1]]
      tokens <- tokens[tokens != ""]
      if (!length(tokens)) { showNotification("Please enter a single OA code.", type = "warning"); return() }
      if (length(tokens) > 1) showNotification(sprintf("Multiple values detected. Using first: %s", tokens[1]),
                                               type = "message", duration = 6)
      oa_in <- tokens[1]
    }
    if (oa_in == "") { showNotification("Please enter a single OA code.", type = "warning"); return() }
    
    # Subset ED rows for this OA (to derive PCON and OA lat/lon for centering)
    ed_oa <- ed_slim %>% dplyr::filter(.data[[oa_col]] == oa_in)
    
    if (nrow(ed_oa) == 0) {
      results_rv(data.frame(OA = oa_in, PCON = NA_character_, Postcodes = NA_character_))
      output$postcodes_ui <- renderUI(tags$em("No rows found for that OA in ED parquet."))
      leafletProxy("map") |> clearShapes() |> clearMarkers()
      return()
    }
    
    # PCON value (first non-NA)
    sel_pcon <- ed_oa[[pcon_col]][which(!is.na(ed_oa[[pcon_col]]))][1]
    if (is.na(sel_pcon)) {
      results_rv(data.frame(OA = oa_in, PCON = NA_character_, Postcodes = NA_character_))
      output$postcodes_ui <- renderUI(tags$em("No PCON found for that OA."))
      leafletProxy("map") |> clearShapes() |> clearMarkers()
      return()
    }
    
    # --- BigQuery postcode lookup for this OA ---
    pcs_vec <- character(0)
    err_msg <- NULL
    tryCatch({
      pcs_vec <- fetch_postcodes_bq(oa_in)
    }, error = function(e) {
      err_msg <<- paste("BigQuery postcode lookup failed:", conditionMessage(e))
    })
    
    pcs_str <- if (length(pcs_vec)) paste(pcs_vec, collapse = ", ") else NA_character_
    
    # Update table & UI
    results_rv(data.frame(OA = oa_in, PCON = sel_pcon, Postcodes = pcs_str, stringsAsFactors = FALSE))
    output$postcodes_ui <- renderUI({
      if (!is.null(err_msg)) {
        tagList(tags$strong("Postcodes: "), tags$em(err_msg))
      } else if (!is.na(pcs_str)) {
        tagList(tags$strong("Postcodes: "), tags$span(pcs_str))
      } else {
        tags$em("No postcodes returned for this OA.")
      }
    })
    
    # Geometry of PCON
    geo_row <- find_geo_rows_by_pcon(geo_sf, sel_pcon)
    leafletProxy("map") |> clearShapes() |> clearMarkers()
    
    if (nrow(geo_row) == 0) {
      showNotification(paste("No geometry found for PCON:", sel_pcon), type = "error"); return()
    }
    
    # Draw PCON polygon (underlay)
    human_col <- first_match_col(names(geo_row), c("pcon_name","constituency","const_name","name", geo_id))
    popup_vals <- paste0("<strong>", human_col, ":</strong> ", geo_row[[human_col]])
    leafletProxy("map") |>
      addPolygons(
        data = geo_row,
        weight = 1, opacity = 0.9, fillOpacity = 0.20,
        color = "#555555", fillColor = "#99c2ff",
        popup = popup_vals,
        group = "pcon"
      )
    
    # Zoom & OA markers:
    lat_col <- locate_lat_lon_cols(ed_slim)$lat
    lon_col <- locate_lat_lon_cols(ed_slim)$lon
    
    if (!is.null(lat_col) && !is.null(lon_col) && all(c(lat_col, lon_col) %in% names(ed_slim))) {
      # All OA centroids in this PCON
      ed_pcon <- ed_slim %>% dplyr::filter(.data[[pcon_col]] == sel_pcon) %>%
        dplyr::filter(!is.na(.data[[lat_col]]), !is.na(.data[[lon_col]]))
      
      # Selected OA point (highlight)
      sel_row <- ed_pcon %>% dplyr::filter(.data[[oa_col]] == oa_in) %>% dplyr::slice_head(n = 1)
      if (nrow(sel_row) > 0) {
        sel_lat <- suppressWarnings(as.numeric(sel_row[[lat_col]][1]))
        sel_lon <- suppressWarnings(as.numeric(sel_row[[lon_col]][1]))
        # Selected OA in warm tone
        leafletProxy("map") |>
          addCircleMarkers(
            lng = sel_lon, lat = sel_lat, radius = 7,
            color = "#d7301f", fillColor = "#fc8d59", fillOpacity = 0.95, weight = 2,
            popup = paste("OA:", oa_in), group = "selected_oa"
          )
        # Other OAs in cool tone
        others <- ed_pcon %>% dplyr::filter(.data[[oa_col]] != oa_in)
        if (nrow(others) > 0) {
          leafletProxy("map") |>
            addCircleMarkers(
              lng = as.numeric(others[[lon_col]]),
              lat = as.numeric(others[[lat_col]]),
              radius = 4, color = "#2b8cbe", fillColor = "#a6bddb", fillOpacity = 0.7,
              popup = paste("OA:", others[[oa_col]]),
              group = "other_oas"
            )
        }
        # Tight zoom (~1.2 km box) around selected OA
        bb <- buffer_bbox_from_point(sel_lon, sel_lat, meters = 1200)
        leafletProxy("map") |> fitBounds(bb["lng1"], bb["lat1"], bb["lng2"], bb["lat2"])
      } else {
        # Fallback to PCON bounds
        bb <- sf::st_bbox(geo_row)
        leafletProxy("map") |> fitBounds(bb["xmin"], bb["ymin"], bb["xmax"], bb["ymax"])
      }
    } else {
      # No lat/lon available — center on PCON bounds
      bb <- sf::st_bbox(geo_row)
      leafletProxy("map") |> fitBounds(bb["xmin"], bb["ymin"], bb["xmax"], bb["ymax"])
      showNotification("No OA lat/lon columns found; centered on constituency.", type = "message", duration = 6)
    }
  })
}

# =============== Run App ===============
shinyApp(ui, server)
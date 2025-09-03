# OA_Postcode_Map — OA -> PCON lookup and map (with postcodes)
# Version: 0.4.3
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
library(stringr)

# =============== Constants ===============
version_no <- "0.4.3"
op_status  <- "DEV"

AUTH_JSON <- "./data/astral-name-419808-ab8473ded5ad.json"

GEO_BUCKET <- "pac10_geojson_parquet"
GEO_OBJECT <- "ED_geoJSON_pcon.parquet"

ED_BUCKET  <- "demographikon_shared"
ED_OBJECT  <- "ED_complete_20250720.parquet"

dir.create("./data", showWarnings = FALSE, recursive = TRUE)
GEO_LOCAL <- file.path("data", basename(GEO_OBJECT))
ED_LOCAL  <- file.path("data", basename(ED_OBJECT))

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
      if (is.raw(el)) return(rawToChar(el))  # only if actually text
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
  if (is.na(id_col)) stop("No PCON id column in geo parquet (expected one of: pcon25cd, pcon, pcon_name, constituency, const_name, name).")
  geom_col <- first_match_col(nms, c("wkb","geom","geometry","geojson","wkt"))
  if (is.na(geom_col)) stop("No geometry column (expected one of: wkb, geom, geometry, geojson, wkt).")
  
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
    stop(sprintf("Don't know how to convert geometry column '%s' (class: %s). If it's WKB, it should be list<raw>.",
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
  list(
    lat = if (length(lat_col)) lat_col[1] else NULL,
    lon = if (length(lon_col)) lon_col[1] else NULL
  )
}

# Try multiple geo ID columns when matching a PCON value (code or name)
find_geo_rows_by_pcon <- function(geo_sf, val) {
  cand_cols <- intersect(names(geo_sf), c("pcon25cd","pcon","pcon_name","constituency","const_name","name"))
  # exact match over all candidates
  for (cc in cand_cols) {
    hit <- which(!is.na(geo_sf[[cc]]) & geo_sf[[cc]] == val)
    if (length(hit)) return(geo_sf[hit, , drop = FALSE])
  }
  # case-insensitive fallback
  v_low <- tolower(as.character(val))
  for (cc in cand_cols) {
    col_low <- tolower(as.character(geo_sf[[cc]]))
    hit <- which(!is.na(col_low) & col_low == v_low)
    if (length(hit)) return(geo_sf[hit, , drop = FALSE])
  }
  geo_sf[0, , drop = FALSE]
}

# =============== Auth & Data Load (once) ===============
message("Checking auth file exists: ", file.exists(AUTH_JSON))
gcs_auth(AUTH_JSON)

download_if_missing(GEO_BUCKET, GEO_OBJECT, GEO_LOCAL)
download_if_missing(ED_BUCKET,  ED_OBJECT,  ED_LOCAL)

geo_raw <- as.data.frame(arrow::read_parquet(GEO_LOCAL))
ed_raw  <- as.data.frame(arrow::read_parquet(ED_LOCAL))

geo_sf  <- coerce_to_sf(geo_raw)
geo_id  <- attr(geo_sf, "pcon_id_col")

# ED column detection
oa_col     <- first_match_col(names(ed_raw), c("oa21cd","oa","oa_code","oaid","oa_2011","oa_2021"))
pcon_col   <- first_match_col(names(ed_raw), c("pcon25cd","pcon","constituency","pcon_name","const_name"))
postcode_col <- first_match_col(names(ed_raw), c("pcd2","pcds","postcode","pc","post_code"))

if (is.na(oa_col) || is.na(pcon_col)) {
  stop("ED parquet: missing OA and/or PCON columns (need OA ~ oa21cd/oa..., PCON ~ pcon25cd/pcon/constituency/...).")
}

latlon_cols <- locate_lat_lon_cols(ed_raw)

# Slim lookup: keep OA, PCON, optional lat/lon, and optional postcode
keep_cols <- unique(na.omit(c(oa_col, pcon_col, latlon_cols$lat, latlon_cols$lon, postcode_col)))
ed_slim <- ed_raw %>% dplyr::select(dplyr::all_of(keep_cols))

# =============== UI ===============
ui <- fluidPage(
  titlePanel("OA → PCON map (with postcodes)"),
  sidebarLayout(
    sidebarPanel(
      helpText("Enter OA codes (comma-separated). The first valid OA will be used to center the map."),
      textAreaInput("fieldValue", "OA codes:", value = "", rows = 4, placeholder = "e.g. E00000001, E00000002"),
      actionButton("submit", "Submit"),
      tags$hr(),
      downloadButton("downloadData", "Download OA → (PCON, Postcodes)")
    ),
    mainPanel(
      leafletOutput("map", height = 600),
      tags$hr(),
      h4("Results"),
      tableOutput("results")
    )
  )
)

# =============== Server ===============
server <- function(input, output, session) {
  results_rv <- reactiveVal(
    data.frame(OA = character(), PCON = character(),
               Postcodes = character(), stringsAsFactors = FALSE)
  )
  
  output$results <- renderTable(results_rv(), rownames = FALSE)
  
  output$downloadData <- downloadHandler(
    filename = function() paste0("OA_to_PCON_Postcodes_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv"),
    content  = function(file) write.csv(results_rv(), file, row.names = FALSE)
  )
  
  output$map <- renderLeaflet({
    leaflet() |> addTiles()
  })
  
  observeEvent(input$submit, {
    req(input$fieldValue)
    
    # Parse OA list
    oas <- unlist(strsplit(input$fieldValue, ",", fixed = TRUE), use.names = FALSE)
    oas <- trimws(oas)
    oas <- oas[oas != ""]
    
    if (!length(oas)) {
      results_rv(data.frame(OA = character(), PCON = character(), Postcodes = character()))
      leafletProxy("map") |> clearShapes() |> clearMarkers()
      return()
    }
    
    # Bring in all rows for the submitted OAs (needed to aggregate postcodes)
    subset_ed <- ed_slim %>% dplyr::filter(.data[[oa_col]] %in% oas)
    
    # Build OA -> PCON + aggregated postcodes
    if (!is.na(postcode_col)) {
      agg <- subset_ed %>%
        dplyr::group_by(.data[[oa_col]], .data[[pcon_col]]) %>%
        dplyr::summarise(Postcodes = paste(sort(unique(na.omit(.data[[postcode_col]]))), collapse = ", "),
                         .groups = "drop")
    } else {
      agg <- subset_ed %>%
        dplyr::distinct(.data[[oa_col]], .data[[pcon_col]]) %>%
        dplyr::mutate(Postcodes = NA_character_)
    }
    
    # Preserve input order and include any OA not found
    input_df <- setNames(data.frame(OA = oas, stringsAsFactors = FALSE), "OA")
    names(input_df)[1] <- oa_col
    joined <- input_df %>%
      dplyr::left_join(agg, by = setNames(oa_col, oa_col))
    
    # Display table as OA, PCON, Postcodes
    out_tbl <- joined %>%
      dplyr::transmute(
        OA        = .data[[oa_col]],
        PCON      = .data[[pcon_col]],
        Postcodes = .data[["Postcodes"]]
      )
    results_rv(out_tbl)
    
    leafletProxy("map") |> clearShapes() |> clearMarkers()
    
    # Find first OA with a PCON to drive the map
    first_row <- joined %>%
      dplyr::filter(!is.na(.data[[pcon_col]])) %>%
      dplyr::slice_head(n = 1)
    
    if (nrow(first_row) == 0) {
      showNotification("No valid PCON found for submitted OA(s).", type = "warning")
      return()
    }
    
    sel_pcon <- first_row[[pcon_col]][1]
    
    # Try to match the constituency geometry using multiple possible id columns
    geo_row <- find_geo_rows_by_pcon(geo_sf, sel_pcon)
    if (nrow(geo_row) == 0) {
      showNotification(paste("No geometry found for PCON:", sel_pcon,
                             "Tried columns:", paste(intersect(names(geo_sf),
                                                               c('pcon25cd','pcon','pcon_name','constituency','const_name','name')),
                                                     collapse=", ")),
                       type = "error", duration = 8)
      return()
    }
    
    # Popup values
    popup_vals <- {
      # Prefer showing any human-readable name if available
      human_col <- first_match_col(names(geo_row), c("pcon_name","constituency","const_name","name", geo_id))
      paste0("<strong>", human_col, ":</strong> ", geo_row[[human_col]])
    }
    
    # Draw polygon
    leafletProxy("map") |>
      addPolygons(
        data = geo_row,
        weight = 1, opacity = 0.9, fillOpacity = 0.25,
        popup = popup_vals
      )
    
    # Zoom preference: OA lat/lon if available, else PCON bounds/centroid
    lat_col <- locate_lat_lon_cols(ed_slim)$lat
    lon_col <- locate_lat_lon_cols(ed_slim)$lon
    has_latlon <- !is.null(lat_col) && !is.null(lon_col) &&
      !is.na(first_row[[lat_col]]) && !is.na(first_row[[lon_col]])
    
    if (has_latlon) {
      lat <- suppressWarnings(as.numeric(first_row[[lat_col]][1]))
      lon <- suppressWarnings(as.numeric(first_row[[lon_col]][1]))
      if (is.finite(lat) && is.finite(lon)) {
        leafletProxy("map") |>
          addCircleMarkers(lng = lon, lat = lat, radius = 5,
                           popup = paste("OA:", first_row[[oa_col]][1])) |>
          setView(lng = lon, lat = lat, zoom = 13)
        return()
      }
    }
    
    # If OA lat/lon unavailable, try PCON centroid for a clean center
    bb <- sf::st_bbox(geo_row)
    ctr <- sf::st_coordinates(sf::st_centroid(sf::st_as_sfc(bb)))
    if (is.finite(ctr[2]) && is.finite(ctr[1])) {
      leafletProxy("map") |>
        setView(lng = ctr[1], lat = ctr[2], zoom = 10)
    } else {
      leafletProxy("map") |>
        fitBounds(lng1 = bb["xmin"], lat1 = bb["ymin"], lng2 = bb["xmax"], lat2 = bb["ymax"])
    }
  })
}

# =============== Run App ===============
shinyApp(ui, server)
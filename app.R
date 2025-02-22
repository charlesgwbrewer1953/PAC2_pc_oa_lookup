# Constants
version_no <- "0.3.0"
op_status <- "DEPLOYED"

# Libraries
library(shiny)
library(bigrquery)
library(googleCloudStorageR)

# Check if the JSON file path is correct
print("Checking file path...")
print(file.exists("./data/astral-name-419808-ab8473ded5ad.json"))

# Authenticate with Google Cloud
print("Authenticating with Google Cloud Storage...")
gcs_auth("./data/astral-name-419808-ab8473ded5ad.json")
bq_auth(path = "./data/astral-name-419808-ab8473ded5ad.json")

# Define BigQuery project, dataset, and table
project_id <- "politicalmaps"
dataset_id <- "PAC_reference_data"  # Replace with your actual dataset ID
table_id <- "PAC_PC_Xref_5_2024"  # Replace with your actual table ID

# Define UI
ui <- fluidPage(
    titlePanel("Retrieve Postcode for OA"),
    sidebarLayout(
        sidebarPanel(
            textAreaInput("fieldValue", "Enter OA values (separated by commas):", value = ""),
            actionButton("submit", "Submit"),
            downloadButton("downloadData", "Download Results")
        ),
        mainPanel(
            tableOutput("results")
        )
    )
)

# Define Server Logic
server <- function(input, output, session) {
    # Reactive expression to query BigQuery
    query_result <- reactiveVal()

    observeEvent(input$submit, {
        field_values <- unlist(strsplit(input$fieldValue, ","))

        # Clean up whitespace and filter out empty values
        field_values <- trimws(field_values)
        field_values <- field_values[field_values != ""]

        if (length(field_values) == 0) {
            # If no valid inputs, return empty table
            query_result(data.frame(OA = character(), postCodes = character()))
            return()
        }

        # Prepare to store results
        results <- data.frame(OA = character(length(field_values)), postCodes = character(length(field_values)))

        for (i in seq_along(field_values)) {
            field_value <- field_values[i]

            # SQL query to select rows based on input field value
            query <- paste0(
                "SELECT pcd2 FROM `", project_id, ".", dataset_id, ".", table_id, "` ",
                "WHERE oa21 = '", field_value, "'"
            )

            # Execute the query
            result <- tryCatch({
                bq_project_query(project_id, query)
            }, error = function(e) {
                print(paste("Error executing query for", field_value, ":", e$message))
                return(NULL)
            })

            if (!is.null(result)) {
                data <- bq_table_download(result)
                postCodes <- paste(data$pcd2, collapse = ",")
                results[i, ] <- list(field_value, postCodes)
            } else {
                results[i, ] <- list(field_value, "Query failed or no data")
            }
        }

        # Update the reactive value with the query results
        query_result(results)
    })

    # Render the query results in a table
    output$results <- renderTable({
        query_result()
    }, rownames = FALSE)

    # Prepare data for download
    output$downloadData <- downloadHandler(
        filename = function() {
            paste("OA_PC_", gsub(",", "_", input$fieldValue), ".csv", sep = "")
        },
        content = function(file) {
            write.csv(query_result(), file, row.names = FALSE)
        }
    )
}

# Run the application
shinyApp(ui = ui, server = server)

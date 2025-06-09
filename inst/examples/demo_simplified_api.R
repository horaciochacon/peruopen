# Demo of the Simplified Peru Open Data API
# This shows how the new API simplifies data discovery and access

library(peruopen)

# ==============================================
# OLD WAY (complex, multiple functions)
# ==============================================

# Old: Need to know about limits, pagination, caching
datasets <- po_search_datasets(limit = 50, use_cache = TRUE)
more_data <- po_search_datasets_paginated(max_results = 500, page_size = 100)
metadata <- po_get_structured_metadata(limit = 100)

# Old: Complex workflow to get data
dataset_info <- po_get_dataset_metadata("dataset-id", use_cache = TRUE)
resources <- po_list_resources("dataset-id", format = "CSV")
if (nrow(resources) > 0) {
  data <- po_download_resource(resources$resource_id[1])
}

# ==============================================
# NEW WAY (simple, intuitive)
# ==============================================

# 1. SEARCH - One function for everything
# ---------------------------------------

# Search all data (no limits to worry about!)
all_data <- po_search()

# Search with a query
covid_data <- po_search("covid")

# Find specific formats
csv_files <- po_search(formats = "CSV")

# Combine searches
minsa_csvs <- po_search(organizations = "MINSA", formats = "CSV")

# Access results easily
covid_data$datasets     # All matching datasets
covid_data$resources    # All files from those datasets
covid_data$summary      # Quick statistics


# 2. GET DATA - Smart and simple
# -------------------------------

# Just give it a name, it figures out the rest
data <- po_get("malaria-2024")

# It automatically:
# - Finds the dataset
# - Picks the best format (CSV > Excel > JSON)
# - Downloads and loads the data
# - Cleans column names for R

# Want a specific format?
excel_data <- po_get("dataset-name", format = "XLSX")

# Just want info?
info <- po_get("dataset-name", what = "info")

# Want everything?
complete <- po_get("dataset-name", what = "all")
complete$data          # The actual data
complete$dataset_info  # Metadata about dataset
complete$resource_info # Info about the file


# 3. EXPLORE - Discover what's available
# ---------------------------------------

# General exploration
explore <- po_explore()

# See what organizations publish most data
explore$by_organization

# Find largest datasets
explore$by_size$largest

# Recent updates
explore$recent

# Get recommendations
explore$recommendations

# Focused exploration
health_explore <- po_explore("salud")
health_explore$recommendations$csv_available  # Health datasets with CSV


# 4. CATALOG - Work with everything locally
# ------------------------------------------

# Get EVERYTHING once (cached for 6 hours)
catalog <- po_catalog()

# Now use dplyr to filter locally (super fast!)
library(dplyr)

# Find all dengue CSV files under 50MB
dengue_csvs <- catalog$resources %>%
  filter(grepl("dengue", dataset_title, ignore.case = TRUE),
         format == "CSV",
         size_mb < 50)

# Find datasets updated in 2024
recent_2024 <- catalog$datasets %>%
  filter(substr(last_updated, 1, 4) == "2024")

# Which organization has the most data?
catalog$datasets %>%
  group_by(organization) %>%
  summarise(
    n_datasets = n(),
    total_gb = sum(total_size_mb) / 1024
  ) %>%
  arrange(desc(n_datasets))


# ==============================================
# REAL WORKFLOW EXAMPLES
# ==============================================

# Example 1: COVID Vaccination Analysis
# -------------------------------------

# 1. Explore what's available
vac_explore <- po_explore("vacuna")

# 2. Search for specific data
vac_search <- po_search("vacunacion covid", formats = "CSV")

# 3. Get the data
vac_data <- po_get(vac_search$datasets$dataset_name[1])

# 4. Analyze
library(ggplot2)
vac_data %>%
  group_by(fecha) %>%
  summarise(total = sum(dosis_aplicadas)) %>%
  ggplot(aes(fecha, total)) +
  geom_line()


# Example 2: Download all MINSA CSV files
# ----------------------------------------

# 1. Get catalog
catalog <- po_catalog()

# 2. Filter to MINSA CSVs
minsa_csvs <- catalog$resources %>%
  filter(grepl("MINSA", dataset_title),
         format == "CSV",
         size_mb < 100)  # Only files under 100MB

# 3. Download them all
minsa_data <- lapply(minsa_csvs$resource_id, function(id) {
  tryCatch(
    po_get(id),
    error = function(e) NULL
  )
})


# Example 3: Find and compare similar datasets
# ---------------------------------------------

# Search for all malaria data
malaria_all <- po_search("malaria")

# See what years are available
malaria_all$datasets %>%
  select(dataset_name, title, last_updated)

# Get multiple years for comparison
malaria_2023 <- po_get("malaria-2023")
malaria_2024 <- po_get("malaria-2024")

# Combine for analysis
library(data.table)
malaria_combined <- rbindlist(list(
  "2023" = malaria_2023,
  "2024" = malaria_2024
), idcol = "year")


# ==============================================
# KEY BENEFITS OF NEW API
# ==============================================

# 1. NO PAGINATION WORRIES
#    - po_search() gets everything
#    - po_catalog() has all 3,900+ datasets

# 2. SMART DEFAULTS
#    - po_get() picks best format automatically
#    - Clean text by default
#    - Sensible caching

# 3. UNIFIED INTERFACE
#    - Search datasets AND resources together
#    - Clear hierarchy (datasets contain resources)
#    - Consistent return structures

# 4. ANALYTICAL WORKFLOW READY
#    - Returns tibbles ready for dplyr
#    - Clean column names
#    - Proper date formatting

# 5. DISCOVERABLE
#    - po_explore() helps find relevant data
#    - Recommendations based on query
#    - Organized views by multiple dimensions

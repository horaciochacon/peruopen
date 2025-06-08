# Demo: Enhanced po_get functionality
# This script demonstrates all the new features of po_get

library(peruopen)
library(dplyr)

# Feature 1: Download multiple resources by ID
# --------------------------------------------
cat("\n=== Feature 1: Multiple resource downloads ===\n")

# Get catalog first
catalog <- po_catalog(target_size = 100, verbose = FALSE)

# Get 3 CSV resources
csv_resources <- catalog$resources %>%
  filter(format == "CSV") %>%
  slice_head(n = 3)

# Download multiple resources at once
multi_data <- po_get(csv_resources$resource_id, verbose = TRUE)

# Result is a named list
cat("\nDownloaded", length(multi_data), "resources\n")
cat("Names:", names(multi_data), "\n")

# Feature 2: Direct tibble input from catalog/search
# -------------------------------------------------
cat("\n=== Feature 2: Tibble input from catalog ===\n")

# Search for specific resources
health_search <- po_search("salud", formats = "CSV", verbose = FALSE)

# Pass tibble directly to po_get
if (nrow(health_search$resources) > 0) {
  health_resources <- health_search$resources %>% slice_head(n = 2)
  health_data <- po_get(health_resources, verbose = TRUE)
  
  cat("\nDownloaded from search results:", length(health_data), "files\n")
}

# Feature 3: Encoding support for Spanish characters
# -------------------------------------------------
cat("\n=== Feature 3: Encoding support ===\n")

# Find a resource likely to have Spanish characters
spanish_resource <- catalog$resources %>%
  filter(grepl("Perú|año|información", resource_name, ignore.case = TRUE)) %>%
  slice_head(n = 1)

if (nrow(spanish_resource) > 0) {
  # Try different encodings
  data_utf8 <- po_get(spanish_resource$resource_id, encoding = "UTF-8", verbose = FALSE)
  data_latin1 <- po_get(spanish_resource$resource_id, encoding = "Latin1", verbose = FALSE)
  data_auto <- po_get(spanish_resource$resource_id, encoding = "auto", verbose = FALSE)
  
  cat("\nTested encodings for:", spanish_resource$resource_name, "\n")
  cat("- UTF-8: success\n")
  cat("- Latin1: success\n")
  cat("- Auto: success\n")
}

# Feature 4: Save files locally with original names
# ------------------------------------------------
cat("\n=== Feature 4: Local file saving ===\n")

# Create temporary directory for demo
temp_dir <- file.path(tempdir(), "peru_data_demo")
dir.create(temp_dir, showWarnings = FALSE)

# Download and save files
excel_resources <- catalog$resources %>%
  filter(format %in% c("XLSX", "XLS")) %>%
  slice_head(n = 2)

if (nrow(excel_resources) > 0) {
  saved_data <- po_get(excel_resources$resource_id, 
                      save_to = temp_dir, 
                      verbose = TRUE)
  
  # List saved files
  saved_files <- list.files(temp_dir, full.names = TRUE)
  cat("\nSaved files:\n")
  for (file in saved_files) {
    cat("- ", basename(file), " (", file.size(file), " bytes)\n", sep = "")
  }
}

# Feature 5: Mixed usage - dataset with save option
# ------------------------------------------------
cat("\n=== Feature 5: Dataset download with save ===\n")

# Get a dataset (will pick best resource) and save it
dataset_name <- catalog$datasets$dataset_name[1]
dataset_data <- po_get(dataset_name, 
                      save_to = temp_dir,
                      verbose = TRUE)

cat("\nDataset downloaded and saved successfully\n")

# Clean up
unlink(temp_dir, recursive = TRUE)

cat("\n=== Demo completed! ===\n")
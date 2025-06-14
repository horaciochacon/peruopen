# Demo: Enhanced encoding support for Spanish characters
# This script demonstrates how to handle encoding issues with Spanish text

library(peruopen)
library(dplyr)

# Get a sample dataset that likely contains Spanish characters
cat("=== Testing Enhanced Encoding Support ===\n")

# Get catalog and find resources likely to have Spanish text
catalog <- po_catalog(target_size = 100, verbose = FALSE)

# Look for resources with Spanish-related names
spanish_resources <- catalog$resources %>%
  filter(grepl("Peru|año|información|salud|educación|niño|población",
               resource_name, ignore.case = TRUE)) %>%
  slice_head(n = 3)

if (nrow(spanish_resources) == 0) {
  # Fallback to any CSV resources
  spanish_resources <- catalog$resources %>%
    filter(format == "CSV") %>%
    slice_head(n = 3)
}

cat("\nFound", nrow(spanish_resources), "resources to test encoding\n")

# Test different encoding approaches
for (i in seq_len(min(2, nrow(spanish_resources)))) {
  resource <- spanish_resources[i, ]
  cat("\n--- Testing Resource:", resource$resource_name, "---\n")

  # Test 1: Auto-detection (should work best now)
  cat("1. Auto-detection encoding:\n")
  tryCatch({
    data_auto <- po_get(resource$resource_id, encoding = "auto", verbose = FALSE)

    # Check for Spanish characters in the data
    text_content <- paste(unlist(data_auto), collapse = " ")
    spanish_chars <- sum(grepl("[ñáéíóúÑÁÉÍÓÚ]", text_content))
    corrupted_chars <- sum(grepl("[ï¿½�]", text_content))

    cat("   Spanish chars found:", spanish_chars, "\n")
    cat("   Corrupted chars found:", corrupted_chars, "\n")
    cat("   Success:", spanish_chars > 0 && corrupted_chars == 0, "\n")
  }, error = function(e) {
    cat("   Error:", e$message, "\n")
  })

  # Test 2: Windows-1252 (often best for Spanish text)
  cat("2. Windows-1252 encoding:\n")
  tryCatch({
    data_win <- po_get(resource$resource_id, encoding = "Windows-1252", verbose = FALSE)

    text_content <- paste(unlist(data_win), collapse = " ")
    spanish_chars <- sum(grepl("[ñáéíóúÑÁÉÍÓÚ]", text_content))
    corrupted_chars <- sum(grepl("[ï¿½�]", text_content))

    cat("   Spanish chars found:", spanish_chars, "\n")
    cat("   Corrupted chars found:", corrupted_chars, "\n")
    cat("   Success:", spanish_chars > 0 && corrupted_chars == 0, "\n")
  }, error = function(e) {
    cat("   Error:", e$message, "\n")
  })

  # Test 3: ISO-8859-1
  cat("3. ISO-8859-1 encoding:\n")
  tryCatch({
    data_iso <- po_get(resource$resource_id, encoding = "ISO-8859-1", verbose = FALSE)

    text_content <- paste(unlist(data_iso), collapse = " ")
    spanish_chars <- sum(grepl("[ñáéíóúÑÁÉÍÓÚ]", text_content))
    corrupted_chars <- sum(grepl("[ï¿½�]", text_content))

    cat("   Spanish chars found:", spanish_chars, "\n")
    cat("   Corrupted chars found:", corrupted_chars, "\n")
    cat("   Success:", spanish_chars > 0 && corrupted_chars == 0, "\n")
  }, error = function(e) {
    cat("   Error:", e$message, "\n")
  })
}

cat("\n=== Encoding Test Summary ===\n")
cat("The enhanced po_get() function now:\n")
cat("- Supports Windows-1252 and ISO-8859-1 encodings\n")
cat("- Has improved auto-detection for Spanish characters\n")
cat("- Uses base R read.csv() as fallback for better encoding support\n")
cat("- Scores encodings based on Spanish character detection\n")

cat("\nFor your specific case, try:\n")
cat("po_get(cdc$resources[1,], encoding = 'Windows-1252')\n")
cat("po_get(cdc$resources[1,], encoding = 'ISO-8859-1')\n")
cat("po_get(cdc$resources[1,], encoding = 'auto')\n")

cat("\nEncoding test completed!\n")

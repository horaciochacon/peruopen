#' Download a resource file
#'
#' @param resource_id Character. The resource ID
#' @param path Character. Local file path (optional, uses temp directory if NULL)
#' @param overwrite Logical. Whether to overwrite existing files (default FALSE)
#' @param use_cache Logical. Use cached files if available (default TRUE)
#' @return Character path to downloaded file
# Internal function - use po_get() instead
#' @importFrom httr2 request req_user_agent req_timeout req_perform resp_body_raw
#' @importFrom fs path path_temp
#' @importFrom readr read_csv
#' @importFrom readxl read_excel
#' @importFrom jsonlite fromJSON
#' @importFrom rlang abort warn

po_download_resource <- function(resource_id, path = NULL, overwrite = FALSE, use_cache = TRUE) {
  if (missing(resource_id) || is.null(resource_id) || resource_id == "") {
    rlang::abort("Resource ID is required", class = "po_input_error")
  }
  
  resource_meta <- po_get_resource_metadata(resource_id)
  
  if (is.na(resource_meta$url) || resource_meta$url == "") {
    rlang::abort("Resource has no download URL", class = "po_download_error")
  }
  
  # Check cache first if enabled and no custom path specified
  if (use_cache && is.null(path)) {
    if (is_resource_cache_valid(resource_id, resource_meta)) {
      cached_file <- get_cached_resource_file(resource_id, resource_meta$format)
      if (!is.null(cached_file)) {
        return(cached_file)
      }
    }
  }
  
  # Determine target path
  if (is.null(path)) {
    file_ext <- if (!is.na(resource_meta$format)) {
      paste0(".", tolower(resource_meta$format))
    } else {
      ""
    }
    # Use temp path for download, will cache afterwards
    path <- fs::path(fs::path_temp(), paste0("resource_", resource_id, file_ext))
  }
  
  if (file.exists(path) && !overwrite) {
    rlang::warn(paste("File already exists:", path, "Use overwrite = TRUE to replace"))
    return(path)
  }
  
  # Download the file
  tryCatch({
    req <- httr2::request(resource_meta$url) |>
      httr2::req_user_agent("peruopen R package") |>
      httr2::req_timeout(120)
    
    resp <- httr2::req_perform(req)
    
    writeBin(httr2::resp_body_raw(resp), path)
    
    if (!file.exists(path)) {
      rlang::abort("Failed to write downloaded file", class = "po_download_error")
    }
    
    # Cache the file if using default temp path and caching is enabled
    if (use_cache && fs::path_dir(path) == fs::path_temp()) {
      cached_path <- cache_resource_file(resource_id, path, resource_meta)
      return(cached_path)
    }
    
    return(path)
    
  }, error = function(e) {
    rlang::abort(paste("Failed to download resource:", e$message), 
                class = "po_download_error")
  })
}

#' Load a resource directly into R
#'
#' @param resource_id Character. The resource ID
#' @param format Character. Resource format (auto-detected if NULL)
#' @param clean_names Logical. Clean column names for R compatibility (default TRUE)
#' @param encoding Character. Encoding to use: "UTF-8", "Latin1", "Windows-1252", "ISO-8859-1", or "auto" (default "UTF-8")
#' @param path Character. Custom save path for the downloaded file (optional)
#' @param use_cache Logical. Use cached files if available (default TRUE)
#' @param ... Additional arguments passed to reading functions
#' @return Data frame or file path for unsupported formats
# Internal function - use po_get() instead
po_load_resource <- function(resource_id, format = NULL, clean_names = TRUE, 
                           encoding = "UTF-8", path = NULL, use_cache = TRUE, ...) {
  if (missing(resource_id) || is.null(resource_id) || resource_id == "") {
    rlang::abort("Resource ID is required", class = "po_input_error")
  }
  
  resource_meta <- po_get_resource_metadata(resource_id)
  
  if (is.null(format)) {
    format <- resource_meta$format
  }
  
  if (is.na(format) || format == "") {
    rlang::abort("Cannot determine resource format for loading", class = "po_load_error")
  }
  
  file_path <- po_download_resource(resource_id, path = path, use_cache = use_cache)
  
  format_lower <- tolower(format)
  
  tryCatch({
    # Determine encoding
    if (encoding == "auto") {
      encoding_to_use <- detect_best_encoding(file_path, format_lower)
    } else {
      encoding_to_use <- encoding
    }
    
    result <- if (format_lower %in% c("csv", "text/csv")) {
      # For better encoding support, prefer base R for non-UTF-8 encodings
      if (encoding_to_use %in% c("Windows-1252", "ISO-8859-1", "Latin1")) {
        # Use base R for better encoding handling
        result <- read.csv(file_path, fileEncoding = encoding_to_use, stringsAsFactors = FALSE, ...)
        # Convert to tibble for consistency
        if (!inherits(result, "tbl_df")) {
          result <- tibble::as_tibble(result)
        }
        result
      } else {
        # Use readr for UTF-8
        readr::read_csv(file_path, locale = readr::locale(encoding = encoding_to_use), show_col_types = FALSE, ...)
      }
    } else if (format_lower %in% c("xlsx", "xls", "excel")) {
      readxl::read_excel(file_path, ...)
    } else if (format_lower %in% c("json", "application/json")) {
      jsonlite::fromJSON(file_path, ...)
    } else {
      rlang::warn(paste("Unsupported format:", format, "- returning file path"))
      return(file_path)
    }
    
    # Try to fix encoding issues in character columns
    if (is.data.frame(result)) {
      result <- fix_encoding_issues(result)
    }
    
    # Clean column names if requested and result is a data frame
    if (clean_names && is.data.frame(result)) {
      names(result) <- make.names(names(result), unique = TRUE)
    }
    
    result
  }, error = function(e) {
    rlang::abort(paste("Failed to load resource data:", e$message), 
                class = "po_load_error")
  })
}

# Helper function to detect best encoding for Spanish text
detect_best_encoding <- function(file_path, format_lower) {
  if (!format_lower %in% c("csv", "text/csv")) {
    return("UTF-8")  # Only try encoding detection for CSV files
  }
  
  # Extended list of encodings to try, including more Latin American variants
  encodings_to_try <- c("UTF-8", "Latin1", "ISO-8859-1", "Windows-1252", "CP1252")
  
  # Read first few lines to test
  best_encoding <- "UTF-8"
  best_score <- -999
  encoding_scores <- list()
  
  for (enc in encodings_to_try) {
    score <- tryCatch({
      # Try to read a sample of the file
      lines <- readLines(file_path, n = 100, encoding = enc, warn = FALSE)
      
      # Check if reading was successful (no empty result)
      if (length(lines) == 0) {
        return(-500)
      }
      
      text <- paste(lines, collapse = " ")
      
      # Score based on presence of correctly decoded Spanish characters
      spanish_chars <- c("ñ", "Ñ", "á", "é", "í", "ó", "ú", "Á", "É", "Í", "Ó", "Ú")
      corruption_indicators <- c("ï¿½", "�", "Ã±", "Ã¡", "Ã©", "Ã­", "Ã³", "Ãº", "Ã\"", "Ã", "â")
      
      # Count Spanish characters (positive score)
      spanish_count <- sum(sapply(spanish_chars, function(char) {
        length(gregexpr(char, text, fixed = TRUE)[[1]]) - 
        ifelse(gregexpr(char, text, fixed = TRUE)[[1]][1] == -1, 1, 0)
      }))
      
      # Count corruption indicators (heavy negative score)
      corruption_count <- sum(sapply(corruption_indicators, function(char) {
        length(gregexpr(char, text, fixed = TRUE)[[1]]) - 
        ifelse(gregexpr(char, text, fixed = TRUE)[[1]][1] == -1, 1, 0)
      }))
      
      # Look for common Spanish place names that should be correctly encoded
      place_names <- c("FERREÑAFE", "CAÑARIS", "PIÑON", "NIÑO", "ESPAÑOL", "AÑOS")
      place_count <- sum(sapply(place_names, function(place) {
        length(gregexpr(place, text, fixed = TRUE)[[1]]) - 
        ifelse(gregexpr(place, text, fixed = TRUE)[[1]][1] == -1, 1, 0)
      }))
      
      # Penalize if we see obvious encoding errors for these place names
      bad_place_patterns <- c("FERRE.*AFE", "CA.*ARIS", "PI.*ON", "NI.*O", "ESPA.*OL", "A.*OS")
      bad_place_count <- sum(sapply(bad_place_patterns, function(pattern) {
        length(gregexpr(pattern, text, perl = TRUE)[[1]]) - 
        ifelse(gregexpr(pattern, text, perl = TRUE)[[1]][1] == -1, 1, 0)
      }))
      
      # Final score calculation
      score <- (spanish_count * 10) + (place_count * 50) - (corruption_count * 20) - (bad_place_count * 30)
      
      # Bonus for UTF-8 if no corruption detected
      if (enc == "UTF-8" && corruption_count == 0) {
        score <- score + 5
      }
      
      score
    }, error = function(e) {
      -1000  # Severe penalty if encoding completely fails
    })
    
    encoding_scores[[enc]] <- score
    
    if (score > best_score) {
      best_score <- score
      best_encoding <- enc
    }
  }
  
  # Debug output (only when explicitly requested)
  if (getOption("peruopen.encoding.debug", FALSE)) {
    message("Encoding detection results:")
    for (enc in names(encoding_scores)) {
      message("  ", enc, ": ", encoding_scores[[enc]])
    }
    message("  Best encoding: ", best_encoding, " (score: ", best_score, ")")
  }
  
  return(best_encoding)
}

# Helper function to fix common encoding issues in data frames
fix_encoding_issues <- function(df) {
  if (!is.data.frame(df)) return(df)
  
  # Common encoding fixes for Spanish text
  encoding_fixes <- list(
    # UTF-8 mojibake patterns -> correct Spanish
    "FERRE�AFE" = "FERREÑAFE",
    "CA�ARIS" = "CAÑARIS", 
    "PI�ON" = "PIÑÓN",
    "NI�O" = "NIÑO",
    "A�O" = "AÑO",
    "A�OS" = "AÑOS",
    "ESPA�OL" = "ESPAÑOL",
    "EDUCACI�N" = "EDUCACIÓN",
    "INFORMACI�N" = "INFORMACIÓN",
    "POBLACI�N" = "POBLACIÓN",
    "COMUNICACI�N" = "COMUNICACIÓN",
    
    # Windows-1252 mojibake patterns -> correct Spanish  
    "FERREï¿½AFE" = "FERREÑAFE",
    "CAï¿½ARIS" = "CAÑARIS",
    "PIï¿½ON" = "PIÑÓN", 
    "NIï¿½O" = "NIÑO",
    "Aï¿½O" = "AÑO",
    "Aï¿½OS" = "AÑOS",
    "ESPAï¿½OL" = "ESPAÑOL"
  )
  
  # Apply fixes to character columns
  for (col in names(df)) {
    if (is.character(df[[col]])) {
      for (pattern in names(encoding_fixes)) {
        df[[col]] <- gsub(pattern, encoding_fixes[[pattern]], df[[col]], fixed = TRUE)
      }
    }
  }
  
  return(df)
}

#' Get the download URL for a resource
#'
#' @param resource_id Character. The resource ID
#' @return Character URL for the resource
# Internal function - use po_get() instead
po_get_resource_url <- function(resource_id) {
  if (missing(resource_id) || is.null(resource_id) || resource_id == "") {
    rlang::abort("Resource ID is required", class = "po_input_error")
  }
  
  resource_meta <- po_get_resource_metadata(resource_id)
  resource_meta$url
}

# Resource file caching functions
get_resource_cache_dir <- function() {
  cache_dir <- rappdirs::user_cache_dir("peruopen", "R")
  resource_cache_dir <- file.path(cache_dir, "resources")
  if (!dir.exists(resource_cache_dir)) {
    dir.create(resource_cache_dir, recursive = TRUE)
  }
  return(resource_cache_dir)
}

get_cached_resource_path <- function(resource_id, format = NULL) {
  cache_dir <- get_resource_cache_dir()
  
  if (!is.null(format) && format != "" && !is.na(format)) {
    file_ext <- paste0(".", tolower(format))
  } else {
    file_ext <- ""
  }
  
  cache_file <- file.path(cache_dir, paste0("resource_", resource_id, file_ext))
  meta_file <- file.path(cache_dir, paste0("resource_", resource_id, ".meta"))
  
  list(
    file = cache_file,
    meta = meta_file
  )
}

is_resource_cache_valid <- function(resource_id, resource_meta = NULL) {
  cache_paths <- get_cached_resource_path(resource_id, resource_meta$format)
  
  if (!file.exists(cache_paths$file)) {
    return(FALSE)
  }
  
  if (file.exists(cache_paths$meta)) {
    cached_meta <- readRDS(cache_paths$meta)
    
    if (!is.null(resource_meta) && !is.null(resource_meta$last_modified) && 
        !is.na(resource_meta$last_modified) && resource_meta$last_modified != "") {
      
      cached_last_modified <- cached_meta$last_modified %||% ""
      
      if (cached_last_modified != resource_meta$last_modified) {
        return(FALSE)
      }
    }
    
    if (!is.null(cached_meta$cached_at)) {
      cache_age <- difftime(Sys.time(), cached_meta$cached_at, units = "hours")
      if (cache_age > 24) {
        return(FALSE)
      }
    }
  } else {
    cache_age <- difftime(Sys.time(), file.info(cache_paths$file)$mtime, units = "hours")
    if (cache_age > 24) {
      return(FALSE)
    }
  }
  
  return(TRUE)
}

cache_resource_file <- function(resource_id, source_path, resource_meta) {
  cache_paths <- get_cached_resource_path(resource_id, resource_meta$format)
  
  if (file.copy(source_path, cache_paths$file, overwrite = TRUE)) {
    meta_data <- list(
      resource_id = resource_id,
      format = resource_meta$format,
      last_modified = resource_meta$last_modified,
      url = resource_meta$url,
      cached_at = Sys.time()
    )
    saveRDS(meta_data, cache_paths$meta)
    return(cache_paths$file)
  } else {
    warning("Failed to cache resource file")
    return(source_path)
  }
}

get_cached_resource_file <- function(resource_id, format = NULL) {
  cache_paths <- get_cached_resource_path(resource_id, format)
  if (file.exists(cache_paths$file)) {
    return(cache_paths$file)
  }
  return(NULL)
}
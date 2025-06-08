#' @importFrom dplyr filter mutate
#' @importFrom rlang abort

filter_resources_by_format <- function(resources_df, formats) {
  if (missing(formats) || is.null(formats) || length(formats) == 0) {
    return(resources_df)
  }
  
  if (nrow(resources_df) == 0) {
    return(resources_df)
  }
  
  format_pattern <- paste(formats, collapse = "|")
  format_pattern <- paste0("(?i)(", format_pattern, ")")
  
  resources_df |>
    dplyr::filter(grepl(format_pattern, format, perl = TRUE))
}

extract_tags_from_metadata <- function(metadata_list) {
  if (length(metadata_list) == 0) {
    return(character(0))
  }
  
  all_tags <- unique(unlist(lapply(metadata_list, function(x) {
    if (is.list(x$tags) && length(x$tags) > 0) {
      x$tags[[1]]
    } else {
      character(0)
    }
  })))
  
  all_tags[!is.na(all_tags)]
}

extract_organizations_from_metadata <- function(metadata_list) {
  if (length(metadata_list) == 0) {
    return(character(0))
  }
  
  orgs <- sapply(metadata_list, function(x) x$organization %||% NA_character_)
  unique(orgs[!is.na(orgs)])
}

validate_dataset_id <- function(dataset_id) {
  if (missing(dataset_id) || is.null(dataset_id) || length(dataset_id) != 1) {
    rlang::abort("Dataset ID must be a single non-null value", class = "po_validation_error")
  }
  
  if (is.na(dataset_id) || dataset_id == "" || !is.character(dataset_id)) {
    rlang::abort("Dataset ID must be a non-empty character string", class = "po_validation_error")
  }
  
  invisible(TRUE)
}

validate_resource_id <- function(resource_id) {
  if (missing(resource_id) || is.null(resource_id) || length(resource_id) != 1) {
    rlang::abort("Resource ID must be a single non-null value", class = "po_validation_error")
  }
  
  if (is.na(resource_id) || resource_id == "" || !is.character(resource_id)) {
    rlang::abort("Resource ID must be a non-empty character string", class = "po_validation_error")
  }
  
  invisible(TRUE)
}

format_file_size <- function(size_bytes) {
  if (is.na(size_bytes) || !is.numeric(size_bytes)) {
    return(NA_character_)
  }
  
  if (size_bytes < 1024) {
    paste(size_bytes, "B")
  } else if (size_bytes < 1024^2) {
    paste(round(size_bytes / 1024, 1), "KB")
  } else if (size_bytes < 1024^3) {
    paste(round(size_bytes / 1024^2, 1), "MB")
  } else {
    paste(round(size_bytes / 1024^3, 1), "GB")
  }
}

format_peru_date <- function(date_string) {
  if (is.na(date_string) || date_string == "") {
    return(NA_character_)
  }
  
  # Peru CKAN returns dates in various formats
  # Try different parsing approaches
  tryCatch({
    # First try: standard ISO format
    if (grepl("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", date_string)) {
      parsed_date <- as.POSIXct(date_string, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")
      return(format(parsed_date, "%Y-%m-%d %H:%M:%S"))
    }
    
    # Second try: Spanish format (from web interface)
    if (grepl("\\w+, \\d{2}/\\d{2}/\\d{4}", date_string)) {
      # Extract date part after comma and space
      date_part <- sub(".*?, ", "", date_string)
      # Remove extra time info if present
      date_part <- sub(" - .*", "", date_part)
      parsed_date <- as.Date(date_part, format = "%d/%m/%Y")
      return(format(parsed_date, "%Y-%m-%d"))
    }
    
    # Third try: date only format
    if (grepl("\\d{4}-\\d{2}-\\d{2}$", date_string)) {
      parsed_date <- as.Date(date_string, format = "%Y-%m-%d")
      return(format(parsed_date, "%Y-%m-%d"))
    }
    
    # If no format matches, return original
    return(date_string)
    
  }, error = function(e) {
    # If parsing fails, return original
    return(date_string)
  })
}

clean_string <- function(text, clean_text = TRUE) {
  if (!clean_text || is.na(text) || text == "") return(text)
  
  # Very conservative cleaning - only fix spacing issues
  text <- trimws(text)  # Remove leading/trailing whitespace
  text <- gsub("\\s+", " ", text)  # Fix multiple spaces to single space
  text <- gsub("[\r\n\t]", " ", text)  # Replace actual line breaks with spaces (FIXED!)
  
  return(text)
}

clean_html_text <- function(html_text, clean_text = TRUE) {
  if (!clean_text || is.na(html_text) || html_text == "") return(html_text)
  
  # Remove HTML tags carefully
  clean_text <- gsub("<[^>]*>", " ", html_text)
  # Remove common HTML entities
  clean_text <- gsub("&nbsp;", " ", clean_text)
  clean_text <- gsub("&amp;", "&", clean_text)
  clean_text <- gsub("&lt;", "<", clean_text)
  clean_text <- gsub("&gt;", ">", clean_text)
  clean_text <- gsub("&quot;", "\"", clean_text)
  # Remove other HTML entities
  clean_text <- gsub("&[a-zA-Z0-9#]+;", "", clean_text)
  # Clean up spacing
  clean_text <- gsub("\\s+", " ", clean_text)
  clean_text <- trimws(clean_text)
  
  # If result is too short and original was long, return original
  if (nchar(clean_text) < 20 && nchar(html_text) > 100) {
    return(html_text)
  }
  
  return(clean_text)
}

clean_email <- function(email) {
  if (is.na(email) || email == "") return(NA_character_)
  
  # Clean whitespace first
  email <- trimws(email)
  
  # Basic email validation - more permissive
  if (grepl("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", email)) {
    return(tolower(email))
  }
  
  return(NA_character_)
}

parse_formatted_size <- function(size_string) {
  if (is.na(size_string) || size_string == "" || !is.character(size_string)) {
    return(0)
  }
  
  # Clean the string and extract number and unit
  size_string <- trimws(toupper(size_string))
  
  # Extract numeric part and unit using regex
  match <- regexpr("([0-9]+\\.?[0-9]*)\\s*([KMGT]?B)?", size_string)
  if (match == -1) {
    return(0)
  }
  
  # Extract the matched parts
  matched_text <- substr(size_string, match, match + attr(match, "match.length") - 1)
  
  # Split into number and unit
  parts <- strsplit(matched_text, "\\s+")[[1]]
  if (length(parts) == 1) {
    # Try to separate number from unit
    num_match <- regexpr("^[0-9]+\\.?[0-9]*", parts[1])
    if (num_match > 0) {
      number <- as.numeric(substr(parts[1], 1, attr(num_match, "match.length")))
      unit <- substr(parts[1], attr(num_match, "match.length") + 1, nchar(parts[1]))
    } else {
      return(0)
    }
  } else if (length(parts) >= 2) {
    number <- as.numeric(parts[1])
    unit <- parts[2]
  } else {
    return(0)
  }
  
  if (is.na(number)) {
    return(0)
  }
  
  # Convert to bytes based on unit
  unit <- gsub("^([KMGT]?)B?$", "\\1", unit)  # Remove B suffix, keep prefix
  
  multiplier <- switch(unit,
    "K" = 1024,       # KB
    "M" = 1024^2,     # MB  
    "G" = 1024^3,     # GB
    "T" = 1024^4,     # TB
    1                 # default to bytes if unknown (including empty string)
  )
  
  return(round(number * multiplier))
}
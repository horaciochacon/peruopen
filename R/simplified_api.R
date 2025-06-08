#' Get searchable catalog of Peru open data with smart loading
#'
#' @description
#' Downloads and returns a catalog of datasets and resources from Peru's open 
#' data portal. Uses intelligent chunked loading to handle API limitations.
#' Can get partial or complete catalogs based on target size.
#'
#' @param refresh Logical. Force refresh of cached catalog (default FALSE)
#' @param verbose Logical. Show progress messages (default TRUE)
#' @param target_size Integer. Number of datasets to fetch (default NULL = try all 3954)
#' @param extend_existing Logical. Add more data to existing catalog (default FALSE)
#' 
#' @return A list containing:
#' \describe{
#'   \item{datasets}{Tibble with all datasets and summary information}
#'   \item{resources}{Tibble with all resources linked to parent datasets}
#'   \item{summary}{List with catalog statistics and metadata}
#' }
#'
#' @export
#' @examples
#' \dontrun{
#' # Get a working subset (1000-1500 datasets)
#' catalog <- po_catalog(target_size = 1500)
#' 
#' # Get more coverage progressively
#' catalog <- po_catalog(target_size = 2500)
#' 
#' # Try to get everything (may timeout)
#' catalog <- po_catalog()
#' 
#' # Extend existing catalog with more data
#' more_catalog <- po_catalog(target_size = 3000, extend_existing = TRUE)
#' 
#' # Find all CSV files under 50MB
#' csv_files <- catalog$resources %>%
#'   filter(format == "CSV", size_mb < 50)
#' 
#' # Find datasets by organization
#' minsa_data <- catalog$datasets %>%
#'   filter(grepl("MINSA", organization))
#' }
po_catalog <- function(refresh = FALSE, verbose = TRUE, target_size = NULL, extend_existing = FALSE) {
  
  # Check cache first
  if (!refresh && !extend_existing) {
    cached <- get_cached_catalog()
    if (!is.null(cached)) {
      if (verbose) message("Using cached catalog (", format(attr(cached, "cached_at"), "%Y-%m-%d %H:%M"), ")")
      
      # If user wants more data than cached, continue loading
      if (!is.null(target_size) && cached$summary$n_datasets < target_size) {
        if (verbose) message("Cached catalog has ", cached$summary$n_datasets, " datasets, extending to ", target_size)
        extend_existing <- TRUE
      } else {
        return(cached)
      }
    }
  }
  
  # Determine starting point and target
  start_offset <- 0
  existing_data <- list()
  
  if (extend_existing) {
    cached <- get_cached_catalog()
    if (!is.null(cached)) {
      # Convert cached data back to raw format for processing
      if (verbose) message("Extending existing catalog from ", cached$summary$n_datasets, " datasets")
      start_offset <- cached$summary$n_datasets
      # We'll merge at the end
    }
  }
  
  # Set intelligent target size based on API testing
  if (is.null(target_size)) {
    target_size <- 3954  # Try full catalog first
  }
  
  if (verbose) {
    if (start_offset > 0) {
      message("Extending catalog from ", start_offset, " to ", target_size, " datasets...")
    } else {
      message("Building catalog with target: ", target_size, " datasets...")
    }
  }
  
  # Strategy 1: Try single large request (original approach)
  if (start_offset == 0 && target_size <= 1000) {
    if (verbose) message("Attempting single request for ", target_size, " datasets...")
    
    single_result <- tryCatch({
      data <- ckan_get_current_packages_with_resources(limit = target_size, offset = 0)
      list(success = TRUE, data = data)
    }, error = function(e) {
      if (verbose) message("Single request failed: ", e$message)
      list(success = FALSE)
    })
    
    if (single_result$success) {
      all_datasets <- single_result$data
      if (verbose) message("✓ Single request succeeded: ", length(all_datasets), " datasets")
    }
  }
  
  # Strategy 2: Chunked loading (if single request failed or for large targets)
  if (!exists("all_datasets") || (exists("all_datasets") && length(all_datasets) == 0)) {
    if (verbose) message("Using chunked loading approach...")
    
    all_datasets <- list()
    current_offset <- start_offset
    
    # Smart batch sizing based on API testing
    if (current_offset < 500) {
      batch_size <- 250  # Safe size for early offsets
    } else if (current_offset < 1000) {
      batch_size <- 200  # Smaller for middle range
    } else {
      batch_size <- 150  # Very conservative for high offsets
    }
    
    max_batches <- ceiling((target_size - start_offset) / batch_size)
    successful_batches <- 0
    failed_attempts <- 0
    
    for (batch_num in 1:max_batches) {
      if (length(all_datasets) + start_offset >= target_size) break
      
      remaining <- target_size - (length(all_datasets) + start_offset)
      current_batch_size <- min(batch_size, remaining)
      
      if (verbose) {
        cat("Batch", batch_num, ": fetching", current_batch_size, "datasets (offset:", current_offset, ")\n")
      }
      
      # Progressive delay for API stability
      if (current_offset > 0 && batch_num > 1) {
        delay <- min(2 + (current_offset / 500), 10)
        if (verbose) cat("  Waiting", round(delay, 1), "seconds for API stability...\n")
        Sys.sleep(delay)
      }
      
      start_time <- Sys.time()
      
      batch_result <- tryCatch({
        batch_data <- ckan_get_current_packages_with_resources(
          limit = current_batch_size,
          offset = current_offset
        )
        
        elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
        
        if (verbose) {
          cat("  ✓ SUCCESS: Got", length(batch_data), "datasets in", round(elapsed, 1), "seconds\n")
        }
        
        list(success = TRUE, data = batch_data)
        
      }, error = function(e) {
        elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
        
        if (verbose) {
          cat("  ✗ FAILED after", round(elapsed, 1), "seconds\n")
        }
        
        list(success = FALSE, error = e$message)
      })
      
      if (batch_result$success) {
        all_datasets <- append(all_datasets, batch_result$data)
        current_offset <- current_offset + length(batch_result$data)
        successful_batches <- successful_batches + 1
        failed_attempts <- 0
        
        # Stop if we got fewer items than requested (end of data)
        if (length(batch_result$data) < current_batch_size) {
          if (verbose) cat("  → Reached end of available data\n")
          break
        }
        
      } else {
        failed_attempts <- failed_attempts + 1
        
        if (failed_attempts >= 2) {
          if (verbose) cat("  → Too many failures, stopping with", length(all_datasets), "new datasets\n")
          break
        }
        
        # Try with smaller batch
        if (verbose) cat("  → Retrying with smaller batch...\n")
        Sys.sleep(5)
        retry_result <- tryCatch({
          smaller_data <- ckan_get_current_packages_with_resources(
            limit = max(50, current_batch_size / 2),
            offset = current_offset
          )
          list(success = TRUE, data = smaller_data)
        }, error = function(e) {
          list(success = FALSE)
        })
        
        if (retry_result$success) {
          all_datasets <- append(all_datasets, retry_result$data)
          current_offset <- current_offset + length(retry_result$data)
          if (verbose) cat("  ✓ Retry succeeded with", length(retry_result$data), "datasets\n")
        }
      }
    }
    
    total_new <- length(all_datasets)
    total_with_existing <- total_new + start_offset
    
    if (verbose) {
      message("\nChunked loading complete:")
      message("  New datasets fetched: ", total_new)
      message("  Total datasets: ", total_with_existing)
      message("  Coverage: ~", round(100 * total_with_existing / 3954), "% of Peru open data")
      message("  Successful batches: ", successful_batches, "/", max_batches)
    }
  }
  
  # If extending existing catalog, merge with cached data
  if (extend_existing && start_offset > 0) {
    cached <- get_cached_catalog()
    if (!is.null(cached) && length(all_datasets) > 0) {
      if (verbose) message("Merging new data with existing catalog...")
      
      # Convert cached datasets back to raw format for processing
      cached_raw <- list()
      for (i in 1:nrow(cached$datasets)) {
        dataset_row <- cached$datasets[i, ]
        
        # Get resources for this dataset
        dataset_resources <- cached$resources[cached$resources$dataset_id == dataset_row$dataset_id, ]
        
        # Reconstruct raw format
        resources_list <- list()
        if (nrow(dataset_resources) > 0) {
          for (j in 1:nrow(dataset_resources)) {
            res_row <- dataset_resources[j, ]
            resources_list[[j]] <- list(
              id = res_row$resource_id,
              name = res_row$resource_name,
              format = res_row$format,
              size = res_row$size_mb * 1024 * 1024,  # Convert back to bytes
              url = res_row$url,
              description = res_row$description,
              created = res_row$created,
              last_modified = res_row$last_modified
            )
          }
        }
        
        # Reconstruct tags
        tags_list <- list()
        if (!is.na(dataset_row$tags) && dataset_row$tags != "") {
          tag_names <- strsplit(dataset_row$tags, ", ")[[1]]
          tags_list <- lapply(tag_names, function(tag) list(name = tag))
        }
        
        # Reconstruct groups (organization)
        groups_list <- list()
        if (!is.na(dataset_row$organization)) {
          groups_list <- list(list(title = dataset_row$organization))
        }
        
        cached_raw[[i]] <- list(
          id = dataset_row$dataset_id,
          name = dataset_row$dataset_name,
          title = dataset_row$title,
          notes = dataset_row$notes,
          metadata_created = dataset_row$created,
          metadata_modified = dataset_row$last_updated,
          resources = resources_list,
          tags = tags_list,
          groups = groups_list
        )
      }
      
      # Combine cached data with new data
      all_datasets <- c(cached_raw, all_datasets)
      if (verbose) message("Combined ", length(cached_raw), " cached + ", length(all_datasets) - length(cached_raw), " new = ", length(all_datasets), " total datasets")
    }
  }
  
  total_fetched <- length(all_datasets)
  
  if (total_fetched == 0) {
    stop("Could not fetch any datasets - API may be temporarily unavailable")
  }
  
  # Process into clean tibbles
  if (verbose) message("Processing catalog structure...")
  
  datasets_list <- vector("list", length(all_datasets))
  resources_list <- vector("list", length(all_datasets))
  
  for (i in seq_along(all_datasets)) {
    pkg <- all_datasets[[i]]
    
    # Extract organization
    org_name <- if (!is.null(pkg$groups) && length(pkg$groups) > 0) {
      clean_string(pkg$groups[[1]]$title %||% NA_character_, TRUE)
    } else {
      NA_character_
    }
    
    # Extract tags
    tags <- if (!is.null(pkg$tags) && length(pkg$tags) > 0) {
      paste(sapply(pkg$tags, function(x) x$name %||% ""), collapse = ", ")
    } else {
      ""
    }
    
    # Calculate resource summary
    resources <- pkg$resources %||% list()
    n_resources <- length(resources)
    
    if (n_resources > 0) {
      formats <- unique(sapply(resources, function(r) r$format %||% "Unknown"))
      formats_str <- paste(sort(formats), collapse = ", ")
      
      # Calculate total size
      sizes <- sapply(resources, function(r) {
        size <- r$size %||% 0
        if (is.character(size)) size <- as.numeric(size)
        if (is.na(size)) size <- 0
        size
      })
      total_size_mb <- round(sum(sizes) / 1024 / 1024, 2)
    } else {
      formats_str <- ""
      total_size_mb <- 0
    }
    
    # Clean dates
    last_updated <- format_peru_date(pkg$metadata_modified %||% NA_character_)
    created <- format_peru_date(pkg$metadata_created %||% NA_character_)
    
    # Dataset record
    datasets_list[[i]] <- list(
      dataset_id = pkg$id %||% NA_character_,
      dataset_name = pkg$name %||% NA_character_,
      title = clean_string(pkg$title %||% NA_character_, TRUE),
      organization = org_name,
      tags = tags,
      n_resources = n_resources,
      formats_available = formats_str,
      total_size_mb = total_size_mb,
      created = created,
      last_updated = last_updated,
      notes = clean_html_text(pkg$notes %||% NA_character_, TRUE)
    )
    
    # Resource records
    if (n_resources > 0) {
      res_list <- vector("list", n_resources)
      for (j in seq_along(resources)) {
        res <- resources[[j]]
        
        # Calculate size in MB
        size_bytes <- res$size %||% 0
        if (is.character(size_bytes)) size_bytes <- as.numeric(size_bytes)
        if (is.na(size_bytes)) size_bytes <- 0
        size_mb <- round(size_bytes / 1024 / 1024, 2)
        
        res_list[[j]] <- list(
          resource_id = res$id %||% NA_character_,
          dataset_id = pkg$id %||% NA_character_,
          dataset_name = pkg$name %||% NA_character_,
          dataset_title = clean_string(pkg$title %||% NA_character_, TRUE),
          resource_name = clean_string(res$name %||% NA_character_, TRUE),
          format = res$format %||% NA_character_,
          size_mb = size_mb,
          url = res$url %||% NA_character_,
          description = clean_html_text(res$description %||% NA_character_, TRUE),
          created = format_peru_date(res$created %||% NA_character_),
          last_modified = format_peru_date(res$last_modified %||% NA_character_)
        )
      }
      resources_list[[i]] <- res_list
    }
  }
  
  # Convert to tibbles
  datasets <- dplyr::bind_rows(datasets_list)
  resources <- dplyr::bind_rows(unlist(resources_list, recursive = FALSE))
  
  # Calculate summary statistics
  summary <- list(
    n_datasets = nrow(datasets),
    n_resources = nrow(resources),
    total_size_gb = round(sum(datasets$total_size_mb, na.rm = TRUE) / 1024, 2),
    organizations = sort(unique(datasets$organization[!is.na(datasets$organization)])),
    n_organizations = length(unique(datasets$organization[!is.na(datasets$organization)])),
    formats = sort(table(resources$format), decreasing = TRUE),
    last_updated = Sys.time(),
    catalog_date = max(datasets$last_updated, na.rm = TRUE)
  )
  
  # Create result
  result <- list(
    datasets = datasets,
    resources = resources,
    summary = summary
  )
  
  # Cache the result
  cache_catalog(result)
  
  if (verbose) {
    message("\nCatalog summary:")
    message("  - Datasets: ", format(summary$n_datasets, big.mark = ","))
    message("  - Resources: ", format(summary$n_resources, big.mark = ","))
    message("  - Total size: ", summary$total_size_gb, " GB")
    message("  - Organizations: ", summary$n_organizations)
    message("  - Formats: ", paste(names(head(summary$formats, 5)), collapse = ", "))
  }
  
  return(result)
}

# Helper functions for catalog caching
get_cache_dir <- function() {
  cache_dir <- rappdirs::user_cache_dir("peruopen", "R")
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE)
  }
  return(cache_dir)
}

get_cached_catalog <- function() {
  cache_dir <- get_cache_dir()
  cache_file <- file.path(cache_dir, "complete_catalog.rds")
  
  if (file.exists(cache_file)) {
    # Check if cache is still fresh (6 hours)
    cache_age <- difftime(Sys.time(), file.info(cache_file)$mtime, units = "hours")
    if (cache_age < 6) {
      catalog <- readRDS(cache_file)
      attr(catalog, "cached_at") <- file.info(cache_file)$mtime
      return(catalog)
    }
  }
  
  return(NULL)
}

cache_catalog <- function(catalog) {
  cache_dir <- get_cache_dir()
  cache_file <- file.path(cache_dir, "complete_catalog.rds")
  saveRDS(catalog, cache_file)
  invisible(TRUE)
}

#' Unified search across Peru's open data catalog
#'
#' @description
#' Search across all datasets and resources in Peru's open data portal.
#' Returns results organized by datasets and their associated resources.
#'
#' @param query Character string to search for (searches titles, descriptions, organizations, tags)
#' @param tags Character vector of tags to search for in tag fields only
#' @param type What to search: "all" (default), "datasets", or "resources"
#' @param formats Character vector of resource formats to filter (e.g., c("CSV", "XLSX"))
#' @param organizations Character vector of organizations to filter
#' @param search_tags_only Logical. If TRUE and query provided, search only in tag fields (default FALSE)
#' @param use_cache Logical. Use cached catalog (default TRUE)
#' @param verbose Logical. Show search progress (default FALSE)
#'
#' @return A list containing:
#' \describe{
#'   \item{datasets}{Tibble of matching datasets}
#'   \item{resources}{Tibble of resources from matching datasets}
#'   \item{summary}{Search result statistics}
#' }
#'
#' @export
#' @examples
#' \dontrun{
#' # Search everything related to COVID
#' covid_data <- po_search("covid")
#' 
#' # Find all CSV files about dengue
#' dengue_csv <- po_search("dengue", formats = "CSV")
#' 
#' # Get all resources from MINSA
#' minsa <- po_search(organizations = "MINSA", type = "resources")
#' 
#' # Search by specific tags
#' health_data <- po_search(tags = c("salud", "medicina"))
#' 
#' # Search only in tag fields (not titles/descriptions)
#' tag_only <- po_search("covid", search_tags_only = TRUE)
#' 
#' # Just search for resources directly
#' csv_files <- po_search(type = "resources", formats = "CSV")
#' }
po_search <- function(query = NULL, 
                     tags = NULL,
                     type = c("all", "datasets", "resources"),
                     formats = NULL,
                     organizations = NULL,
                     search_tags_only = FALSE,
                     use_cache = TRUE,
                     verbose = FALSE) {
  
  type <- match.arg(type)
  
  # For performance, use existing efficient search for simple queries
  # Only build full catalog for complex multi-filter searches
  is_simple_query <- is.null(formats) && is.null(organizations) && is.null(tags) && !search_tags_only && type == "all"
  
  # Skip the "efficient search" path for now - it doesn't search all fields
  # Always use the full catalog search to ensure complete results
  
  # For complex queries or when simple search fails, use full catalog
  if (verbose) message("Building complete catalog for complex search...")
  catalog <- tryCatch({
    po_catalog(refresh = !use_cache, verbose = verbose)
  }, error = function(e) {
    # If catalog building fails, fall back to simple search
    if (verbose) message("Catalog building failed, using simple search")
    return(NULL)
  })
  
  if (is.null(catalog)) {
    # Fallback to simple search
    if (!is.null(query)) {
      simple_result <- po_search_datasets(query = query, limit = 100, clean_text = TRUE)
      
      # Convert to new format
      summary <- list(
        query = query,
        n_datasets = nrow(simple_result),
        n_resources = sum(simple_result$num_resources %||% 0),
        total_size_gb = 0,
        formats_found = table(character(0)),
        organizations_found = unique(simple_result$organization[!is.na(simple_result$organization)])
      )
      
      # Create minimal resources tibble
      resources <- tibble::tibble(
        resource_id = character(0),
        dataset_id = character(0),
        dataset_name = character(0),
        resource_name = character(0),
        format = character(0),
        size_mb = numeric(0)
      )
      
      result <- list(
        datasets = simple_result,
        resources = resources,
        summary = summary
      )
      
      class(result) <- c("po_search_result", "list")
      return(result)
    } else {
      stop("Unable to search: API connection failed and no query provided")
    }
  }
  
  # Start with full catalog
  datasets <- catalog$datasets
  resources <- catalog$resources
  
  # Apply query filter if provided
  if (!is.null(query) && query != "") {
    pattern <- paste0("(?i)", gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", query))
    
    if (search_tags_only) {
      # Search only in tag fields
      dataset_matches <- datasets$dataset_id[
        grepl(pattern, datasets$tags, perl = TRUE)
      ]
      # Resources don't have separate tag fields, so only search datasets
      resource_dataset_matches <- character(0)
    } else {
      # Search in all fields (default behavior)
      dataset_matches <- datasets$dataset_id[
        grepl(pattern, datasets$title, perl = TRUE) |
        grepl(pattern, datasets$notes, perl = TRUE) |
        grepl(pattern, datasets$organization, perl = TRUE) |
        grepl(pattern, datasets$tags, perl = TRUE) |
        grepl(pattern, datasets$dataset_name, perl = TRUE)
      ]
      
      # Search in resources
      resource_dataset_matches <- unique(resources$dataset_id[
        grepl(pattern, resources$resource_name, perl = TRUE) |
        grepl(pattern, resources$description, perl = TRUE) |
        grepl(pattern, resources$dataset_title, perl = TRUE)
      ])
    }
    
    # Combine matches
    all_matching_ids <- unique(c(dataset_matches, resource_dataset_matches))
    
    # Filter based on search type
    if (type == "datasets") {
      datasets <- datasets[datasets$dataset_id %in% dataset_matches, ]
      resources <- resources[resources$dataset_id %in% dataset_matches, ]
    } else if (type == "resources") {
      # For resource search, include datasets that have matching resources
      datasets <- datasets[datasets$dataset_id %in% all_matching_ids, ]
      resources <- resources[resources$dataset_id %in% all_matching_ids, ]
    } else {
      # type == "all"
      datasets <- datasets[datasets$dataset_id %in% all_matching_ids, ]
      resources <- resources[resources$dataset_id %in% all_matching_ids, ]
    }
  }
  
  # Apply explicit tags filter if provided
  if (!is.null(tags) && length(tags) > 0) {
    # Create pattern for any of the specified tags
    tags_pattern <- paste0("(?i)(", paste(sapply(tags, function(x) {
      gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", x)
    }), collapse = "|"), ")")
    
    # Filter datasets that have any of the specified tags
    tag_matching_ids <- datasets$dataset_id[
      grepl(tags_pattern, datasets$tags, perl = TRUE)
    ]
    
    datasets <- datasets[datasets$dataset_id %in% tag_matching_ids, ]
    resources <- resources[resources$dataset_id %in% tag_matching_ids, ]
  }
  
  # Apply format filter
  if (!is.null(formats)) {
    format_pattern <- paste0("(?i)(", paste(formats, collapse = "|"), ")")
    matching_resources <- resources[grepl(format_pattern, resources$format, perl = TRUE), ]
    matching_dataset_ids <- unique(matching_resources$dataset_id)
    
    datasets <- datasets[datasets$dataset_id %in% matching_dataset_ids, ]
    resources <- matching_resources
  }
  
  # Apply organization filter
  if (!is.null(organizations)) {
    org_pattern <- paste0("(?i)(", paste(organizations, collapse = "|"), ")")
    datasets <- datasets[grepl(org_pattern, datasets$organization, perl = TRUE), ]
    resources <- resources[resources$dataset_id %in% datasets$dataset_id, ]
  }
  
  # Calculate summary
  search_description <- if (!is.null(query) && !is.null(tags)) {
    paste0("'", query, "' + tags: ", paste(tags, collapse = ", "))
  } else if (!is.null(query)) {
    if (search_tags_only) {
      paste0("'", query, "' (tags only)")
    } else {
      query
    }
  } else if (!is.null(tags)) {
    paste0("tags: ", paste(tags, collapse = ", "))
  } else {
    "(all data)"
  }
  
  summary <- list(
    query = search_description,
    search_tags_only = search_tags_only,
    tags_searched = tags,
    n_datasets = nrow(datasets),
    n_resources = nrow(resources),
    total_size_gb = round(sum(resources$size_mb, na.rm = TRUE) / 1024, 2),
    formats_found = sort(table(resources$format), decreasing = TRUE),
    organizations_found = sort(unique(datasets$organization[!is.na(datasets$organization)]))
  )
  
  result <- list(
    datasets = datasets,
    resources = resources,
    summary = summary
  )
  
  class(result) <- c("po_search_result", "list")
  
  return(result)
}

#' Print method for po_search results
#' @export
print.po_search_result <- function(x, ...) {
  cat("Peru Open Data Search Results\n")
  cat("Query:", x$summary$query, "\n")
  cat("Found:", x$summary$n_datasets, "datasets with", x$summary$n_resources, "resources\n")
  cat("Total size:", x$summary$total_size_gb, "GB\n")
  
  if (length(x$summary$formats_found) > 0) {
    cat("\nTop formats:\n")
    top_formats <- head(x$summary$formats_found, 5)
    for (i in seq_along(top_formats)) {
      cat(sprintf("  %s: %d files\n", names(top_formats)[i], top_formats[i]))
    }
  }
  
  if (x$summary$n_datasets > 0) {
    cat("\nSample datasets:\n")
    sample_data <- head(x$datasets[, c("title", "organization", "n_resources")], 5)
    for (i in 1:nrow(sample_data)) {
      cat(sprintf("  - %s (%s) [%d resources]\n", 
                  substr(sample_data$title[i], 1, 50),
                  sample_data$organization[i],
                  sample_data$n_resources[i]))
    }
    
    if (x$summary$n_datasets > 5) {
      cat(sprintf("  ... and %d more datasets\n", x$summary$n_datasets - 5))
    }
  }
  
  cat("\nAccess data with: $datasets, $resources, $summary\n")
  invisible(x)
}

#' Smart getter for Peru open data
#'
#' @description
#' Intelligently fetches data or metadata based on the identifier(s) provided.
#' Works with dataset names, dataset IDs, resource IDs, or tibbles from po_catalog/po_search.
#'
#' @param identifier Dataset name, dataset ID, resource ID(s), or tibble of resources
#' @param what What to get: "data" (default), "info", or "all"
#' @param format Preferred format when multiple are available (e.g., "CSV", "XLSX")
#' @param clean_names Logical. Clean column names for R compatibility (default TRUE)
#' @param save_to Directory path to save downloaded files (default NULL = no saving)
#' @param encoding Character encoding: "UTF-8" (default), "Latin1", "Windows-1252", "ISO-8859-1", or "auto"
#' @param use_cache Logical. Use cached files if available (default TRUE)
#' @param verbose Logical. Show download progress (default TRUE)
#'
#' @return Depending on input:
#' \describe{
#'   \item{Single resource}{A data.frame/tibble or metadata depending on 'what'}
#'   \item{Multiple resources}{A named list of data/metadata for each resource}
#'   \item{Dataset}{Data/metadata from the best available resource}
#' }
#'
#' @export
#' @examples
#' \dontrun{
#' # Get data using dataset name
#' malaria <- po_get("malaria-2024")
#' 
#' # Get metadata only
#' info <- po_get("malaria-2024", what = "info")
#' 
#' # Get specific resource by ID
#' data <- po_get("abc-123-resource-id")
#' 
#' # Get multiple resources
#' resources <- po_get(c("resource-id-1", "resource-id-2"))
#' 
#' # Get resources from catalog/search results
#' catalog <- po_catalog()
#' csv_resources <- catalog$resources %>% filter(format == "CSV")
#' all_csv_data <- po_get(csv_resources)
#' 
#' # Save files locally with original names
#' data <- po_get("dataset-name", save_to = "data/peru/")
#' 
#' # Force specific encoding for Spanish characters
#' data <- po_get("resource-id", encoding = "Windows-1252")
#' data <- po_get("resource-id", encoding = "ISO-8859-1")
#' 
#' # Cache control examples
#' data <- po_get("resource-id")  # Uses cache if available
#' data <- po_get("resource-id", use_cache = FALSE)  # Force fresh download
#' }
po_get <- function(identifier, 
                   what = c("data", "info", "all"),
                   format = NULL,
                   clean_names = TRUE,
                   save_to = NULL,
                   encoding = c("UTF-8", "Latin1", "Windows-1252", "ISO-8859-1", "auto"),
                   use_cache = TRUE,
                   verbose = TRUE) {
  
  what <- match.arg(what)
  encoding <- match.arg(encoding)
  
  # Get catalog for lookups
  catalog <- po_catalog(verbose = FALSE)
  
  # Check if identifier is a tibble/data.frame
  if (is.data.frame(identifier)) {
    # Extract resource IDs from tibble
    if ("resource_id" %in% names(identifier)) {
      resource_ids <- identifier$resource_id
      resource_names <- if ("resource_name" %in% names(identifier)) {
        identifier$resource_name
      } else {
        resource_ids
      }
    } else {
      stop("Tibble must contain a 'resource_id' column")
    }
    
    # Process multiple resources
    return(process_multiple_resources(resource_ids, resource_names, what, clean_names, 
                                     save_to, encoding, use_cache, verbose, catalog))
  }
  
  # Check if multiple identifiers provided as vector
  if (length(identifier) > 1) {
    # Process multiple resources
    return(process_multiple_resources(identifier, identifier, what, clean_names, 
                                     save_to, encoding, use_cache, verbose, catalog))
  }
  
  # Single identifier processing (original logic)
  identifier <- identifier[1]
  
  # Check if it's a resource ID
  is_resource <- identifier %in% catalog$resources$resource_id
  
  if (is_resource) {
    # Handle resource directly
    resource_info <- catalog$resources[catalog$resources$resource_id == identifier, ]
    
    if (what == "info") {
      return(as.list(resource_info))
    }
    
    # Load the resource
    if (verbose) message("Loading resource: ", resource_info$resource_name)
    
    # Prepare save path if requested
    save_path <- NULL
    if (!is.null(save_to)) {
      if (!dir.exists(save_to)) {
        dir.create(save_to, recursive = TRUE)
      }
      # Use original filename if available from resource name
      filename <- gsub("[^a-zA-Z0-9._-]", "_", resource_info$resource_name)
      if (!grepl("\\.", filename)) {
        # Add extension if not present
        filename <- paste0(filename, ".", tolower(resource_info$format))
      }
      save_path <- file.path(save_to, filename)
    }
    
    data <- tryCatch({
      po_load_resource(identifier, clean_names = clean_names, encoding = encoding, path = save_path, use_cache = use_cache)
    }, error = function(e) {
      stop("Failed to load resource: ", e$message)
    })
    
    if (what == "data") {
      return(data)
    } else {  # what == "all"
      return(list(
        data = data,
        info = as.list(resource_info),
        dataset_info = as.list(catalog$datasets[catalog$datasets$dataset_id == resource_info$dataset_id, ])
      ))
    }
  }
  
  # It's a dataset identifier (name or ID)
  # Try to find by name first, then by ID
  dataset <- catalog$datasets[catalog$datasets$dataset_name == identifier, ]
  if (nrow(dataset) == 0) {
    dataset <- catalog$datasets[catalog$datasets$dataset_id == identifier, ]
  }
  
  if (nrow(dataset) == 0) {
    # Try partial matching on name
    matches <- grep(identifier, catalog$datasets$dataset_name, ignore.case = TRUE, value = FALSE)
    if (length(matches) == 1) {
      dataset <- catalog$datasets[matches, ]
      if (verbose) message("Found dataset by partial match: ", dataset$dataset_name)
    } else if (length(matches) > 1) {
      stop("Multiple datasets match '", identifier, "'. Please be more specific.\nMatches: ", 
           paste(catalog$datasets$dataset_name[matches], collapse = ", "))
    } else {
      stop("No dataset found with identifier: ", identifier)
    }
  }
  
  # Get resources for this dataset
  dataset_resources <- catalog$resources[catalog$resources$dataset_id == dataset$dataset_id, ]
  
  if (nrow(dataset_resources) == 0) {
    stop("Dataset has no resources available: ", dataset$title)
  }
  
  if (what == "info") {
    return(list(
      dataset = as.list(dataset),
      resources = dataset_resources
    ))
  }
  
  # Select best resource for download
  selected_resource <- select_best_resource(dataset_resources, format, verbose)
  
  if (verbose) {
    message("Dataset: ", dataset$title)
    message("Downloading: ", selected_resource$resource_name, " (", selected_resource$format, ", ", 
            selected_resource$size_mb, " MB)")
  }
  
  # Prepare save path if requested
  save_path <- NULL
  if (!is.null(save_to)) {
    if (!dir.exists(save_to)) {
      dir.create(save_to, recursive = TRUE)
    }
    # Use original filename if available from resource name
    filename <- gsub("[^a-zA-Z0-9._-]", "_", selected_resource$resource_name)
    if (!grepl("\\.", filename)) {
      # Add extension if not present
      filename <- paste0(filename, ".", tolower(selected_resource$format))
    }
    save_path <- file.path(save_to, filename)
  }
  
  # Load the data
  data <- tryCatch({
    po_load_resource(selected_resource$resource_id, clean_names = clean_names, 
                    encoding = encoding, path = save_path, use_cache = use_cache)
  }, error = function(e) {
    stop("Failed to load data: ", e$message)
  })
  
  if (what == "data") {
    return(data)
  } else {  # what == "all"
    return(list(
      data = data,
      dataset_info = as.list(dataset),
      resource_info = as.list(selected_resource),
      all_resources = dataset_resources
    ))
  }
}

# Helper function to process multiple resources
process_multiple_resources <- function(resource_ids, resource_names, what, clean_names, 
                                     save_to, encoding, use_cache, verbose, catalog) {
  n_resources <- length(resource_ids)
  
  if (n_resources == 0) {
    stop("No resource IDs provided")
  }
  
  if (verbose) {
    message("Processing ", n_resources, " resources...")
  }
  
  # Initialize results list
  results <- list()
  
  # Process each resource
  for (i in seq_along(resource_ids)) {
    resource_id <- resource_ids[i]
    resource_name <- resource_names[i]
    
    if (verbose && n_resources > 1) {
      message("\n[", i, "/", n_resources, "] Processing: ", resource_name)
    }
    
    # Get resource info
    resource_info <- catalog$resources[catalog$resources$resource_id == resource_id, ]
    
    if (nrow(resource_info) == 0) {
      warning("Resource not found in catalog: ", resource_id)
      results[[resource_name]] <- list(error = "Resource not found")
      next
    }
    
    if (what == "info") {
      results[[resource_name]] <- as.list(resource_info)
      next
    }
    
    # Prepare save path if requested
    save_path <- NULL
    if (!is.null(save_to)) {
      if (!dir.exists(save_to)) {
        dir.create(save_to, recursive = TRUE)
      }
      # Use original filename if available from resource name
      filename <- gsub("[^a-zA-Z0-9._-]", "_", resource_info$resource_name)
      if (!grepl("\\.", filename)) {
        # Add extension if not present
        filename <- paste0(filename, ".", tolower(resource_info$format))
      }
      save_path <- file.path(save_to, filename)
    }
    
    # Try to load the resource
    result <- tryCatch({
      data <- po_load_resource(resource_id, clean_names = clean_names, 
                              encoding = encoding, path = save_path, use_cache = use_cache)
      
      if (what == "data") {
        data
      } else {  # what == "all"
        list(
          data = data,
          info = as.list(resource_info),
          dataset_info = as.list(catalog$datasets[catalog$datasets$dataset_id == resource_info$dataset_id, ])
        )
      }
    }, error = function(e) {
      warning("Failed to load resource '", resource_name, "': ", e$message)
      list(error = e$message)
    })
    
    results[[resource_name]] <- result
  }
  
  if (verbose && n_resources > 1) {
    successful <- sum(sapply(results, function(x) is.null(x[["error"]])))
    message("\nCompleted: ", successful, "/", n_resources, " resources loaded successfully")
  }
  
  # If only one resource, return it directly (not as a list)
  if (n_resources == 1) {
    return(results[[1]])
  }
  
  return(results)
}

# Helper function to select best resource
select_best_resource <- function(resources, preferred_format = NULL, verbose = TRUE) {
  
  # If specific format requested
  if (!is.null(preferred_format)) {
    format_matches <- resources[toupper(resources$format) == toupper(preferred_format), ]
    if (nrow(format_matches) > 0) {
      # Return the most recent one
      return(format_matches[order(format_matches$last_modified, decreasing = TRUE)[1], ])
    } else {
      if (verbose) warning("Format '", preferred_format, "' not available. Selecting best alternative.")
    }
  }
  
  # Priority order for formats
  format_priority <- c("CSV", "XLSX", "XLS", "JSON", "ZIP", "TXT")
  
  # Find best format available
  for (fmt in format_priority) {
    matches <- resources[toupper(resources$format) == fmt, ]
    if (nrow(matches) > 0) {
      # Return the most recent one
      return(matches[order(matches$last_modified, decreasing = TRUE)[1], ])
    }
  }
  
  # If no preferred formats found, return the most recent resource
  return(resources[order(resources$last_modified, decreasing = TRUE)[1], ])
}

#' Interactive exploration of Peru's open data
#'
#' @description
#' Provides structured views of the data catalog to help discover
#' relevant datasets. Returns organized summaries by different dimensions.
#'
#' @param query Optional search term to focus exploration
#' @param verbose Logical. Show exploration progress (default TRUE)
#'
#' @return A list with multiple views of the data:
#' \describe{
#'   \item{by_organization}{Datasets grouped by publishing organization}
#'   \item{by_format}{Resources grouped by file format}
#'   \item{by_year}{Temporal distribution of datasets}
#'   \item{by_size}{Size distribution and largest datasets}
#'   \item{recent}{Recently updated datasets}
#'   \item{popular}{Datasets with most resources}
#'   \item{recommendations}{Suggested starting points based on query}
#' }
#'
#' @export
#' @examples
#' \dontrun{
#' # General exploration
#' explore <- po_explore()
#' 
#' # Explore health-related data
#' health <- po_explore("salud")
#' 
#' # See what MINSA publishes
#' explore$by_organization$MINSA
#' 
#' # Find all available Excel files
#' explore$by_format$XLSX
#' }
po_explore <- function(query = NULL, verbose = TRUE) {
  
  # Get catalog
  if (!is.null(query) && query != "") {
    if (verbose) message("Exploring data related to: ", query)
    search_results <- po_search(query, use_cache = TRUE, verbose = FALSE)
    catalog_datasets <- search_results$datasets
    catalog_resources <- search_results$resources
  } else {
    if (verbose) message("Exploring complete Peru open data catalog...")
    catalog <- po_catalog(verbose = FALSE)
    catalog_datasets <- catalog$datasets
    catalog_resources <- catalog$resources
  }
  
  if (nrow(catalog_datasets) == 0) {
    stop("No datasets found", if (!is.null(query)) paste0(" for query: ", query) else "")
  }
  
  # 1. By Organization
  if (verbose) message("Analyzing by organization...")
  by_org <- split(catalog_datasets, catalog_datasets$organization)
  by_org <- lapply(by_org, function(org_data) {
    list(
      n_datasets = nrow(org_data),
      n_resources = sum(org_data$n_resources),
      total_size_mb = sum(org_data$total_size_mb),
      datasets = org_data[order(org_data$last_updated, decreasing = TRUE), c("dataset_name", "title", "n_resources", "formats_available")]
    )
  })
  by_org <- by_org[order(sapply(by_org, function(x) x$n_datasets), decreasing = TRUE)]
  
  # 2. By Format
  if (verbose) message("Analyzing by format...")
  by_format <- split(catalog_resources, catalog_resources$format)
  by_format <- lapply(by_format, function(fmt_data) {
    list(
      n_resources = nrow(fmt_data),
      total_size_mb = sum(fmt_data$size_mb, na.rm = TRUE),
      avg_size_mb = round(mean(fmt_data$size_mb, na.rm = TRUE), 2),
      resources = fmt_data[order(fmt_data$size_mb, decreasing = TRUE), 
                          c("resource_name", "dataset_title", "size_mb", "last_modified")]
    )
  })
  by_format <- by_format[order(sapply(by_format, function(x) x$n_resources), decreasing = TRUE)]
  
  # 3. By Year
  if (verbose) message("Analyzing temporal distribution...")
  catalog_datasets$year_updated <- substr(catalog_datasets$last_updated, 1, 4)
  year_summary <- table(catalog_datasets$year_updated)
  by_year <- list(
    summary = as.data.frame(year_summary),
    recent_years = names(tail(sort(year_summary), 5))
  )
  
  # 4. By Size
  if (verbose) message("Analyzing size distribution...")
  size_breaks <- c(0, 1, 10, 100, 1000, Inf)
  size_labels <- c("< 1 MB", "1-10 MB", "10-100 MB", "100 MB - 1 GB", "> 1 GB")
  catalog_datasets$size_category <- cut(catalog_datasets$total_size_mb, 
                                        breaks = size_breaks, 
                                        labels = size_labels)
  by_size <- list(
    distribution = table(catalog_datasets$size_category),
    largest = catalog_datasets[order(catalog_datasets$total_size_mb, decreasing = TRUE), 
                              c("title", "organization", "total_size_mb", "n_resources")][1:10, ]
  )
  
  # 5. Recent updates
  recent <- catalog_datasets[order(catalog_datasets$last_updated, decreasing = TRUE), 
                            c("title", "organization", "last_updated", "n_resources", "formats_available")][1:20, ]
  
  # 6. Popular (most resources)
  popular <- catalog_datasets[order(catalog_datasets$n_resources, decreasing = TRUE), 
                             c("title", "organization", "n_resources", "formats_available", "total_size_mb")][1:20, ]
  
  # 7. Recommendations
  recommendations <- list()
  
  if (!is.null(query)) {
    # Query-specific recommendations
    if (nrow(catalog_datasets) > 0) {
      # Datasets with CSV files (easy to work with)
      csv_datasets <- catalog_datasets[grepl("CSV", catalog_datasets$formats_available), ]
      if (nrow(csv_datasets) > 0) {
        recommendations$csv_available <- csv_datasets[1:min(5, nrow(csv_datasets)), c("dataset_name", "title", "organization")]
      }
      
      # Recently updated
      recommendations$recently_updated <- catalog_datasets[1:min(5, nrow(catalog_datasets)), c("dataset_name", "title", "last_updated")]
      
      # Most comprehensive (most resources)
      recommendations$comprehensive <- catalog_datasets[order(catalog_datasets$n_resources, decreasing = TRUE), ][1:min(5, nrow(catalog_datasets)), c("dataset_name", "title", "n_resources")]
    }
  } else {
    # General recommendations
    recommendations$getting_started <- list(
      health_data = "Try: po_explore('salud') or po_explore('MINSA')",
      covid_data = "Try: po_explore('covid') or po_explore('vacuna')",
      economic_data = "Try: po_explore('economico') or po_explore('MEF')",
      education_data = "Try: po_explore('educacion') or po_explore('MINEDU')"
    )
    
    recommendations$popular_organizations <- names(head(by_org, 5))
    recommendations$common_formats <- names(head(by_format, 5))
  }
  
  # Create result
  result <- list(
    query = query,
    summary = list(
      n_datasets = nrow(catalog_datasets),
      n_resources = nrow(catalog_resources),
      n_organizations = length(unique(catalog_datasets$organization)),
      date_range = range(catalog_datasets$last_updated, na.rm = TRUE)
    ),
    by_organization = by_org,
    by_format = by_format,
    by_year = by_year,
    by_size = by_size,
    recent = recent,
    popular = popular,
    recommendations = recommendations
  )
  
  class(result) <- c("po_explore_result", "list")
  
  if (verbose) message("Exploration complete!")
  
  return(result)
}

#' Print method for po_explore results
#' @export
print.po_explore_result <- function(x, ...) {
  cat("Peru Open Data Exploration\n")
  if (!is.null(x$query)) cat("Query:", x$query, "\n")
  cat("=====================================\n\n")
  
  cat("SUMMARY\n")
  cat("  Datasets:", x$summary$n_datasets, "\n")
  cat("  Resources:", x$summary$n_resources, "\n")
  cat("  Organizations:", x$summary$n_organizations, "\n")
  cat("  Date range:", paste(x$summary$date_range, collapse = " to "), "\n\n")
  
  cat("TOP ORGANIZATIONS\n")
  top_orgs <- head(x$by_organization, 5)
  for (i in seq_along(top_orgs)) {
    org <- names(top_orgs)[i]
    info <- top_orgs[[i]]
    cat(sprintf("  %d. %s: %d datasets, %d resources\n", 
                i, org, info$n_datasets, info$n_resources))
  }
  
  cat("\nTOP FORMATS\n")
  top_formats <- head(x$by_format, 5)
  for (i in seq_along(top_formats)) {
    fmt <- names(top_formats)[i]
    info <- top_formats[[i]]
    cat(sprintf("  %s: %d files (avg %.1f MB)\n", 
                fmt, info$n_resources, info$avg_size_mb))
  }
  
  cat("\nRECENT UPDATES\n")
  for (i in 1:min(5, nrow(x$recent))) {
    cat(sprintf("  - %s (%s)\n", 
                substr(x$recent$title[i], 1, 50),
                x$recent$last_updated[i]))
  }
  
  if (!is.null(x$recommendations$csv_available)) {
    cat("\nRECOMMENDED DATASETS (with CSV)\n")
    for (i in 1:nrow(x$recommendations$csv_available)) {
      cat(sprintf("  - %s\n", x$recommendations$csv_available$dataset_name[i]))
    }
  }
  
  cat("\nExplore further with: $by_organization, $by_format, $recent, $popular\n")
  invisible(x)
}
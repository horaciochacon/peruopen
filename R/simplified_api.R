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
po_catalog <- function(refresh = FALSE,
                       verbose = TRUE,
                       target_size = NULL,
                       extend_existing = FALSE) {
  cache_result <- check_and_handle_cache(refresh, extend_existing, target_size, verbose)
  if (!is.null(cache_result$return_early)) {
    return(cache_result$return_early)
  }

  start_offset <- cache_result$start_offset
  target_size <- cache_result$target_size
  extend_existing <- cache_result$extend_existing

  if (verbose) {
    display_catalog_progress(start_offset, target_size)
  }

  all_datasets <- fetch_datasets(start_offset, target_size, verbose)

  if (extend_existing && start_offset > 0) {
    all_datasets <- merge_with_cached_datasets(all_datasets, verbose)
  }

  if (length(all_datasets) == 0) {
    stop("Could not fetch any datasets - API may be temporarily unavailable")
  }

  result <- process_datasets_to_catalog(all_datasets, verbose)
  cache_catalog(result)

  if (verbose) {
    display_catalog_summary(result$summary)
  }

  return(result)
}

check_and_handle_cache <- function(refresh, extend_existing, target_size, verbose) {
  if (!refresh && !extend_existing) {
    cached <- get_cached_catalog()
    if (!is.null(cached)) {
      if (verbose) {
        message("Using cached catalog (",
                format(attr(cached, "cached_at"), "%Y-%m-%d %H:%M"),
                ")")
      }

      if (!is.null(target_size) && cached$summary$n_datasets < target_size) {
        if (verbose) {
          message("Cached catalog has ", cached$summary$n_datasets,
                  " datasets, extending to ", target_size)
        }
        extend_existing <- TRUE
      } else {
        return(list(return_early = cached))
      }
    }
  }

  start_offset <- 0
  if (extend_existing) {
    cached <- get_cached_catalog()
    if (!is.null(cached)) {
      if (verbose) {
        message("Extending existing catalog from ", cached$summary$n_datasets, " datasets")
      }
      start_offset <- cached$summary$n_datasets
    }
  }

  if (is.null(target_size)) {
    target_size <- 3954
  }

  list(
    return_early = NULL,
    start_offset = start_offset,
    target_size = target_size,
    extend_existing = extend_existing
  )
}

display_catalog_progress <- function(start_offset, target_size) {
  if (start_offset > 0) {
    message("Extending catalog from ", start_offset, " to ", target_size, " datasets...")
  } else {
    message("Building catalog with target: ", target_size, " datasets...")
  }
}

fetch_datasets <- function(start_offset, target_size, verbose) {
  if (start_offset == 0 && target_size <= 1000) {
    all_datasets <- try_single_request(target_size, verbose)
    if (!is.null(all_datasets)) {
      return(all_datasets)
    }
  }

  fetch_datasets_chunked(start_offset, target_size, verbose)
}

try_single_request <- function(target_size, verbose) {
  if (verbose) {
    message("Attempting single request for ", target_size, " datasets...")
  }

  result <- tryCatch(
    {
      data <- ckan_get_packages_resources(limit = target_size, offset = 0)
      list(success = TRUE, data = data)
    },
    error = function(e) {
      if (verbose) {
        message("Single request failed: ", e$message)
      }
      list(success = FALSE)
    }
  )

  if (result$success) {
    if (verbose) {
      message("✓ Single request succeeded: ", length(result$data), " datasets")
    }
    return(result$data)
  }

  NULL
}

fetch_datasets_chunked <- function(start_offset, target_size, verbose) {
  if (verbose) {
    message("Using chunked loading approach...")
  }

  all_datasets <- list()
  current_offset <- start_offset
  batch_size <- calculate_batch_size(current_offset)
  max_batches <- ceiling((target_size - start_offset) / batch_size)
  successful_batches <- 0
  failed_attempts <- 0

  for (batch_num in 1:max_batches) {
    if (length(all_datasets) + start_offset >= target_size) {
      break
    }

    remaining <- target_size - (length(all_datasets) + start_offset)
    current_batch_size <- min(batch_size, remaining)

    if (verbose) {
      cat("Batch", batch_num, ": fetching", current_batch_size,
          "datasets (offset:", current_offset, ")\n")
    }

    apply_api_delay(current_offset, batch_num, verbose)

    batch_result <- fetch_single_batch(current_batch_size, current_offset, verbose)

    if (batch_result$success) {
      all_datasets <- append(all_datasets, batch_result$data)
      current_offset <- current_offset + length(batch_result$data)
      successful_batches <- successful_batches + 1
      failed_attempts <- 0

      if (length(batch_result$data) < current_batch_size) {
        if (verbose) {
          cat("  → Reached end of available data\n")
        }
        break
      }
    } else {
      failed_attempts <- failed_attempts + 1

      if (failed_attempts >= 2) {
        if (verbose) {
          cat("  → Too many failures, stopping with", length(all_datasets), "new datasets\n")
        }
        break
      }

      retry_result <- retry_with_smaller_batch(current_batch_size, current_offset, verbose)
      if (retry_result$success) {
        all_datasets <- append(all_datasets, retry_result$data)
        current_offset <- current_offset + length(retry_result$data)
      }
    }
  }

  if (verbose) {
    display_chunked_summary(all_datasets, start_offset, successful_batches, max_batches)
  }

  all_datasets
}

calculate_batch_size <- function(current_offset) {
  if (current_offset < 500) {
    250
  } else if (current_offset < 1000) {
    200
  } else {
    150
  }
}

apply_api_delay <- function(current_offset, batch_num, verbose) {
  if (current_offset > 0 && batch_num > 1) {
    delay <- min(2 + (current_offset / 500), 10)
    if (verbose) {
      cat("  Waiting", round(delay, 1), "seconds for API stability...\n")
    }
    Sys.sleep(delay)
  }
}

fetch_single_batch <- function(batch_size, offset, verbose) {
  start_time <- Sys.time()

  tryCatch(
    {
      batch_data <- ckan_get_packages_resources(limit = batch_size, offset = offset)
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))

      if (verbose) {
        cat("  ✓ SUCCESS: Got", length(batch_data), "datasets in",
            round(elapsed, 1), "seconds\n")
      }

      list(success = TRUE, data = batch_data)
    },
    error = function(e) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))

      if (verbose) {
        cat("  ✗ FAILED after", round(elapsed, 1), "seconds\n")
      }

      list(success = FALSE, error = e$message)
    }
  )
}

retry_with_smaller_batch <- function(current_batch_size, current_offset, verbose) {
  if (verbose) {
    cat("  → Retrying with smaller batch...\n")
  }
  Sys.sleep(5)

  tryCatch(
    {
      smaller_data <- ckan_get_packages_resources(
        limit = max(50, current_batch_size / 2),
        offset = current_offset
      )
      if (verbose) {
        cat("  ✓ Retry succeeded with", length(smaller_data), "datasets\n")
      }
      list(success = TRUE, data = smaller_data)
    },
    error = function(e) {
      list(success = FALSE)
    }
  )
}

display_chunked_summary <- function(all_datasets, start_offset, successful_batches, max_batches) {
  total_new <- length(all_datasets)
  total_with_existing <- total_new + start_offset

  message("\nChunked loading complete:")
  message("  New datasets fetched: ", total_new)
  message("  Total datasets: ", total_with_existing)
  message("  Coverage: ~", round(100 * total_with_existing / 3954), "% of Peru open data")
  message("  Successful batches: ", successful_batches, "/", max_batches)
}

merge_with_cached_datasets <- function(all_datasets, verbose) {
  cached <- get_cached_catalog()
  if (!is.null(cached) && length(all_datasets) > 0) {
    if (verbose) {
      message("Merging new data with existing catalog...")
    }

    cached_raw <- convert_cached_to_raw_format(cached)
    combined_datasets <- c(cached_raw, all_datasets)

    if (verbose) {
      message("Combined ", length(cached_raw), " cached + ",
              length(all_datasets), " new = ", length(combined_datasets), " total datasets")
    }

    return(combined_datasets)
  }
  all_datasets
}

convert_cached_to_raw_format <- function(cached) {
  cached_raw <- list()

  for (i in seq_len(nrow(cached$datasets))) {
    dataset_row <- cached$datasets[i, ]
    dataset_resources <- cached$resources[cached$resources$dataset_id == dataset_row$dataset_id, ]

    resources_list <- convert_resources_to_raw(dataset_resources)
    tags_list <- convert_tags_to_raw(dataset_row$tags)
    groups_list <- convert_organization_to_raw(dataset_row$organization)

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

  cached_raw
}

convert_resources_to_raw <- function(dataset_resources) {
  if (nrow(dataset_resources) == 0) {
    return(list())
  }

  resources_list <- list()
  for (j in seq_len(nrow(dataset_resources))) {
    res_row <- dataset_resources[j, ]
    resources_list[[j]] <- list(
      id = res_row$resource_id,
      name = res_row$resource_name,
      format = res_row$format,
      size = res_row$size_mb * 1024 * 1024,
      url = res_row$url,
      description = res_row$description,
      created = res_row$created,
      last_modified = res_row$last_modified
    )
  }
  resources_list
}

convert_tags_to_raw <- function(tags_string) {
  if (is.na(tags_string) || tags_string == "") {
    return(list())
  }

  tag_names <- strsplit(tags_string, ", ")[[1]]
  lapply(tag_names, function(tag) list(name = tag))
}

convert_organization_to_raw <- function(organization) {
  if (is.na(organization)) {
    return(list())
  }
  list(list(title = organization))
}

process_datasets_to_catalog <- function(all_datasets, verbose) {
  if (verbose) {
    message("Processing catalog structure...")
  }

  datasets_list <- vector("list", length(all_datasets))
  resources_list <- vector("list", length(all_datasets))

  for (i in seq_along(all_datasets)) {
    pkg <- all_datasets[[i]]

    processed_dataset <- process_single_dataset(pkg)
    datasets_list[[i]] <- processed_dataset$dataset

    if (processed_dataset$n_resources > 0) {
      resources_list[[i]] <- process_dataset_resources(pkg)
    }
  }

  datasets <- dplyr::bind_rows(datasets_list)
  resources <- dplyr::bind_rows(unlist(resources_list, recursive = FALSE))

  class(datasets) <- c("po_datasets", class(datasets))
  class(resources) <- c("po_resources", class(resources))

  summary <- calculate_catalog_summary(datasets, resources)

  list(datasets = datasets, resources = resources, summary = summary)
}

process_single_dataset <- function(pkg) {
  org_name <- extract_organization_name(pkg)
  tags <- extract_tags_string(pkg)

  resources <- pkg$resources %||% list()
  n_resources <- length(resources)

  if (n_resources > 0) {
    formats <- unique(sapply(resources, function(r) r$format %||% "Unknown"))
    formats_str <- paste(sort(formats), collapse = ", ")
    total_size_mb <- calculate_total_size_mb(resources)
  } else {
    formats_str <- ""
    total_size_mb <- 0
  }

  list(
    dataset = list(
      dataset_id = pkg$id %||% NA_character_,
      dataset_name = pkg$name %||% NA_character_,
      title = clean_string(pkg$title %||% NA_character_, TRUE),
      organization = org_name,
      tags = tags,
      n_resources = n_resources,
      formats_available = formats_str,
      total_size_mb = total_size_mb,
      created = format_peru_date(pkg$metadata_created %||% NA_character_),
      last_updated = format_peru_date(pkg$metadata_modified %||% NA_character_),
      notes = clean_html_text(pkg$notes %||% NA_character_, TRUE)
    ),
    n_resources = n_resources
  )
}

extract_organization_name <- function(pkg) {
  if (!is.null(pkg$groups) && length(pkg$groups) > 0) {
    clean_string(pkg$groups[[1]]$title %||% NA_character_, TRUE)
  } else {
    NA_character_
  }
}

extract_tags_string <- function(pkg) {
  if (!is.null(pkg$tags) && length(pkg$tags) > 0) {
    paste(sapply(pkg$tags, function(x) x$name %||% ""), collapse = ", ")
  } else {
    ""
  }
}

calculate_total_size_mb <- function(resources) {
  sizes <- sapply(resources, function(r) parse_size_to_bytes(r$size))
  round(sum(sizes) / 1024 / 1024, 2)
}

parse_size_to_bytes <- function(size) {
  size <- size %||% 0

  if (is.character(size)) {
    size <- parse_formatted_size(size)
  } else if (is.numeric(size)) {
    size <- as.numeric(size)
  }

  if (is.na(size)) {
    size <- 0
  }
  size
}

process_dataset_resources <- function(pkg) {
  resources <- pkg$resources %||% list()
  res_list <- vector("list", length(resources))

  for (j in seq_along(resources)) {
    res <- resources[[j]]
    size_mb <- round(parse_size_to_bytes(res$size) / 1024 / 1024, 2)

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
  res_list
}

calculate_catalog_summary <- function(datasets, resources) {
  list(
    n_datasets = nrow(datasets),
    n_resources = nrow(resources),
    total_size_gb = round(sum(datasets$total_size_mb, na.rm = TRUE) / 1024, 2),
    organizations = sort(unique(datasets$organization[!is.na(datasets$organization)])),
    n_organizations = length(unique(datasets$organization[!is.na(datasets$organization)])),
    formats = sort(table(resources$format), decreasing = TRUE),
    last_updated = Sys.time(),
    catalog_date = max(datasets$last_updated, na.rm = TRUE)
  )
}

display_catalog_summary <- function(summary) {
  message("\nCatalog summary:")
  message("  - Datasets: ", format(summary$n_datasets, big.mark = ","))
  message("  - Resources: ", format(summary$n_resources, big.mark = ","))
  message("  - Total size: ", summary$total_size_gb, " GB")
  message("  - Organizations: ", summary$n_organizations)
  message("  - Formats: ", paste(names(head(summary$formats, 5)), collapse = ", "))
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
#' @param search_tags_only Logical. If TRUE and query provided, search only in tag
#'   fields (default FALSE)
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

  catalog <- get_search_catalog(use_cache, verbose, query)

  if (is.null(catalog)) {
    return(handle_fallback_search(query))
  }

  datasets <- catalog$datasets
  resources <- catalog$resources

  if (!is.null(query) && query != "") {
    matching_ids <- search_by_query(datasets, resources, query, search_tags_only, type)
    search_result <- filter_by_matching_ids(datasets, resources, matching_ids, type)
    datasets <- search_result$datasets
    resources <- search_result$resources
  }

  if (!is.null(tags) && length(tags) > 0) {
    filter_result <- filter_by_tags(datasets, resources, tags)
    datasets <- filter_result$datasets
    resources <- filter_result$resources
  }

  if (!is.null(formats)) {
    filter_result <- filter_by_formats(datasets, resources, formats)
    datasets <- filter_result$datasets
    resources <- filter_result$resources
  }

  if (!is.null(organizations)) {
    filter_result <- filter_by_organizations(datasets, resources, organizations)
    datasets <- filter_result$datasets
    resources <- filter_result$resources
  }

  create_search_result(datasets, resources, query, tags, search_tags_only)
}

get_search_catalog <- function(use_cache, verbose, query) {
  if (verbose) {
    message("Building complete catalog for complex search...")
  }

  tryCatch(
    {
      po_catalog(refresh = !use_cache, verbose = verbose)
    },
    error = function(e) {
      if (verbose) {
        message("Catalog building failed, using simple search")
      }
      NULL
    }
  )
}

handle_fallback_search <- function(query) {
  if (is.null(query)) {
    stop("Unable to search: API connection failed and no query provided")
  }

  simple_result <- po_search_datasets(
    query = query,
    limit = 100,
    clean_text = TRUE
  )

  summary <- list(
    query = query,
    n_datasets = nrow(simple_result),
    n_resources = sum(simple_result$num_resources %||% 0),
    total_size_gb = 0,
    formats_found = table(character(0)),
    organizations_found = unique(simple_result$organization[!is.na(simple_result$organization)])
  )

  resources <- tibble::tibble(
    resource_id = character(0),
    dataset_id = character(0),
    dataset_name = character(0),
    resource_name = character(0),
    format = character(0),
    size_mb = numeric(0)
  )

  class(simple_result) <- c("po_datasets", class(simple_result))
  class(resources) <- c("po_resources", class(resources))

  result <- list(
    datasets = simple_result,
    resources = resources,
    summary = summary
  )

  class(result) <- c("po_search_result", "list")
  result
}

search_by_query <- function(datasets, resources, query, search_tags_only, type) {
  pattern <- create_search_pattern(query)

  if (search_tags_only) {
    dataset_matches <- datasets$dataset_id[grepl(pattern, datasets$tags, perl = TRUE)]
    resource_dataset_matches <- character(0)
  } else {
    dataset_matches <- search_datasets_all_fields(datasets, pattern)
    resource_dataset_matches <- search_resources_all_fields(resources, pattern)
  }

  list(
    dataset_matches = dataset_matches,
    resource_dataset_matches = resource_dataset_matches,
    all_matches = unique(c(dataset_matches, resource_dataset_matches))
  )
}

create_search_pattern <- function(query) {
  paste0("(?i)", gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", query))
}

search_datasets_all_fields <- function(datasets, pattern) {
  datasets$dataset_id[
    grepl(pattern, datasets$title, perl = TRUE) |
      grepl(pattern, datasets$notes, perl = TRUE) |
      grepl(pattern, datasets$organization, perl = TRUE) |
      grepl(pattern, datasets$tags, perl = TRUE) |
      grepl(pattern, datasets$dataset_name, perl = TRUE)
  ]
}

search_resources_all_fields <- function(resources, pattern) {
  unique(resources$dataset_id[
    grepl(pattern, resources$resource_name, perl = TRUE) |
      grepl(pattern, resources$description, perl = TRUE) |
      grepl(pattern, resources$dataset_title, perl = TRUE)
  ])
}

filter_by_matching_ids <- function(datasets, resources, matching_ids, type) {
  if (type == "datasets") {
    datasets <- datasets[datasets$dataset_id %in% matching_ids$dataset_matches, ]
    resources <- resources[resources$dataset_id %in% matching_ids$dataset_matches, ]
  } else if (type == "resources") {
    datasets <- datasets[datasets$dataset_id %in% matching_ids$all_matches, ]
    resources <- resources[resources$dataset_id %in% matching_ids$all_matches, ]
  } else {
    datasets <- datasets[datasets$dataset_id %in% matching_ids$all_matches, ]
    resources <- resources[resources$dataset_id %in% matching_ids$all_matches, ]
  }

  list(datasets = datasets, resources = resources)
}

filter_by_tags <- function(datasets, resources, tags) {
  tags_pattern <- paste0("(?i)(", paste(sapply(tags, function(x) {
    gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", x)
  }), collapse = "|"), ")")

  tag_matching_ids <- datasets$dataset_id[grepl(tags_pattern, datasets$tags, perl = TRUE)]

  list(
    datasets = datasets[datasets$dataset_id %in% tag_matching_ids, ],
    resources = resources[resources$dataset_id %in% tag_matching_ids, ]
  )
}

filter_by_formats <- function(datasets, resources, formats) {
  format_pattern <- paste0("(?i)(", paste(formats, collapse = "|"), ")")
  matching_resources <- resources[grepl(format_pattern, resources$format, perl = TRUE), ]
  matching_dataset_ids <- unique(matching_resources$dataset_id)

  list(
    datasets = datasets[datasets$dataset_id %in% matching_dataset_ids, ],
    resources = matching_resources
  )
}

filter_by_organizations <- function(datasets, resources, organizations) {
  org_pattern <- paste0("(?i)(", paste(organizations, collapse = "|"), ")")
  filtered_datasets <- datasets[grepl(org_pattern, datasets$organization, perl = TRUE), ]

  list(
    datasets = filtered_datasets,
    resources = resources[resources$dataset_id %in% filtered_datasets$dataset_id, ]
  )
}

create_search_result <- function(datasets, resources, query, tags, search_tags_only) {
  search_description <- build_search_description(query, tags, search_tags_only)

  class(datasets) <- c("po_datasets", class(datasets))
  class(resources) <- c("po_resources", class(resources))

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

  result <- list(datasets = datasets, resources = resources, summary = summary)
  class(result) <- c("po_search_result", "list")
  result
}

build_search_description <- function(query, tags, search_tags_only) {
  if (!is.null(query) && !is.null(tags)) {
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
}

#' Print method for po_search results
#' @param x A po_search_result object
#' @param ... Additional arguments passed to print methods
#' @export
print.po_search_result <- function(x, ...) {
  cli::cli_h1("Peru Open Data Search Results")
  cli::cli_text("Query: {cli::col_cyan(x$summary$query)}")
  cli::cli_text(paste0(
    "Found: ", cli::col_green(x$summary$n_datasets), " datasets with ",
    cli::col_green(x$summary$n_resources), " resources"
  ))
  cli::cli_text("Total size: {cli::col_yellow(paste0(x$summary$total_size_gb, ' GB'))}")

  if (length(x$summary$formats_found) > 0) {
    cli::cli_h2("Top formats")
    top_formats <- head(x$summary$formats_found, 5)
    for (i in seq_along(top_formats)) {
      cli::cli_text(paste0(
        "  ", cli::col_blue(names(top_formats)[i]), ": ",
        top_formats[i], " files"
      ))
    }
  }

  if (x$summary$n_datasets > 0) {
    cli::cli_h2("Sample datasets")
    sample_data <- head(x$datasets[, c("title", "organization", "n_resources")], 5)
    for (i in seq_len(nrow(sample_data))) {
      title_truncated <- truncate_text(sample_data$title[i], 50)
      cli::cli_text(paste0(
        "  ", cli::symbol$bullet, " ", cli::style_bold(title_truncated),
        " (", cli::col_magenta(sample_data$organization[i]), ") [",
        cli::col_green(sample_data$n_resources[i]), " resources]"
      ))
    }

    if (x$summary$n_datasets > 5) {
      cli::cli_text(paste0(
        "  ", cli::col_silver("... and "), cli::col_green(x$summary$n_datasets - 5),
        " ", cli::col_silver("more datasets")
      ))
    }
  }

  cli::cli_rule()
  cli::cli_text(paste0(cli::col_blue("Access data with:"),
                       " $datasets, $resources, $summary"))
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
#' @param encoding Character encoding: "UTF-8" (default), "Latin1", "Windows-1252",
#'   "ISO-8859-1", or "auto"
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
#' data <- po_get("resource-id") # Uses cache if available
#' data <- po_get("resource-id", use_cache = FALSE) # Force fresh download
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

  catalog <- po_catalog(verbose = FALSE)

  if (is.data.frame(identifier)) {
    return(handle_tibble_input(
      identifier, what, clean_names, save_to, encoding, use_cache, verbose, catalog
    ))
  }

  if (length(identifier) > 1) {
    return(handle_multiple_identifiers(
      identifier, what, clean_names, save_to, encoding, use_cache, verbose, catalog
    ))
  }

  handle_single_identifier(
    identifier[1], what, format, clean_names, save_to, encoding, use_cache,
    verbose, catalog
  )
}

handle_tibble_input <- function(identifier, what, clean_names, save_to,
                                encoding, use_cache, verbose, catalog) {
  if (!"resource_id" %in% names(identifier)) {
    stop("Tibble must contain a 'resource_id' column")
  }

  resource_ids <- identifier$resource_id
  resource_names <- if ("resource_name" %in% names(identifier)) {
    identifier$resource_name
  } else {
    resource_ids
  }

  process_multiple_resources(
    resource_ids, resource_names, what, clean_names, save_to,
    encoding, use_cache, verbose, catalog
  )
}

handle_multiple_identifiers <- function(identifier, what, clean_names, save_to,
                                        encoding, use_cache, verbose, catalog) {
  process_multiple_resources(
    identifier, identifier, what, clean_names, save_to,
    encoding, use_cache, verbose, catalog
  )
}

handle_single_identifier <- function(identifier, what, format, clean_names,
                                     save_to, encoding, use_cache, verbose,
                                     catalog) {
  is_resource <- identifier %in% catalog$resources$resource_id

  if (is_resource) {
    return(handle_resource_identifier(
      identifier, what, clean_names, save_to, encoding, use_cache, verbose, catalog
    ))
  }

  handle_dataset_identifier(identifier, what, format, clean_names, save_to,
                            encoding, use_cache, verbose, catalog)
}

handle_resource_identifier <- function(identifier, what, clean_names, save_to,
                                       encoding, use_cache, verbose, catalog) {
  resource_info <- catalog$resources[catalog$resources$resource_id == identifier, ]

  if (what == "info") {
    return(as.list(resource_info))
  }

  if (verbose) {
    message("Loading resource: ", resource_info$resource_name)
  }

  save_path <- prepare_save_path(
    save_to, resource_info$resource_name, resource_info$format
  )

  data <- tryCatch(
    {
      po_load_resource(
        identifier,
        clean_names = clean_names,
        encoding = encoding,
        path = save_path,
        use_cache = use_cache
      )
    },
    error = function(e) {
      stop("Failed to load resource: ", e$message)
    }
  )

  if (what == "data") {
    return(data)
  }

  list(
    data = data,
    info = as.list(resource_info),
    dataset_info = as.list(
      catalog$datasets[catalog$datasets$dataset_id == resource_info$dataset_id, ]
    )
  )
}

handle_dataset_identifier <- function(identifier, what, format, clean_names,
                                      save_to, encoding, use_cache, verbose,
                                      catalog) {
  dataset <- find_dataset(identifier, catalog, verbose)

  dataset_resources <- catalog$resources[catalog$resources$dataset_id == dataset$dataset_id, ]

  if (nrow(dataset_resources) == 0) {
    stop("Dataset has no resources available: ", dataset$title)
  }

  if (what == "info") {
    return(list(dataset = as.list(dataset), resources = dataset_resources))
  }

  selected_resource <- select_best_resource(dataset_resources, format, verbose)

  if (verbose) {
    message("Dataset: ", dataset$title)
    message("Downloading: ", selected_resource$resource_name, " (",
            selected_resource$format, ", ", selected_resource$size_mb, " MB)")
  }

  save_path <- prepare_save_path(
    save_to, selected_resource$resource_name, selected_resource$format
  )

  data <- tryCatch(
    {
      po_load_resource(
        selected_resource$resource_id,
        clean_names = clean_names,
        encoding = encoding,
        path = save_path,
        use_cache = use_cache
      )
    },
    error = function(e) {
      stop("Failed to load data: ", e$message)
    }
  )

  if (what == "data") {
    return(data)
  }

  list(
    data = data,
    dataset_info = as.list(dataset),
    resource_info = as.list(selected_resource),
    all_resources = dataset_resources
  )
}

find_dataset <- function(identifier, catalog, verbose) {
  dataset <- catalog$datasets[catalog$datasets$dataset_name == identifier, ]
  if (nrow(dataset) == 0) {
    dataset <- catalog$datasets[catalog$datasets$dataset_id == identifier, ]
  }

  if (nrow(dataset) == 0) {
    matches <- grep(
      identifier, catalog$datasets$dataset_name, ignore.case = TRUE, value = FALSE
    )

    if (length(matches) == 1) {
      dataset <- catalog$datasets[matches, ]
      if (verbose) {
        message("Found dataset by partial match: ", dataset$dataset_name)
      }
    } else if (length(matches) > 1) {
      stop(
        "Multiple datasets match '", identifier, "'. Please be more specific.\nMatches: ",
        paste(catalog$datasets$dataset_name[matches], collapse = ", ")
      )
    } else {
      stop("No dataset found with identifier: ", identifier)
    }
  }

  dataset
}

prepare_save_path <- function(save_to, resource_name, format) {
  if (is.null(save_to)) {
    return(NULL)
  }

  if (!dir.exists(save_to)) {
    dir.create(save_to, recursive = TRUE)
  }

  filename <- gsub("[^a-zA-Z0-9._-]", "_", resource_name)
  if (!grepl("\\.", filename)) {
    filename <- paste0(filename, ".", tolower(format))
  }

  file.path(save_to, filename)
}

# Helper function to process multiple resources
process_multiple_resources <- function(resource_ids,
                                       resource_names,
                                       what,
                                       clean_names,
                                       save_to,
                                       encoding,
                                       use_cache,
                                       verbose,
                                       catalog) {
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
    result <- tryCatch(
      {
        data <- po_load_resource(
          resource_id,
          clean_names = clean_names,
          encoding = encoding,
          path = save_path,
          use_cache = use_cache
        )

        if (what == "data") {
          data
        } else {
          # what == "all"
          list(
            data = data,
            info = as.list(resource_info),
            dataset_info = as.list(
              catalog$datasets[catalog$datasets$dataset_id == resource_info$dataset_id, ]
            )
          )
        }
      },
      error = function(e) {
        warning("Failed to load resource '", resource_name, "': ", e$message)
        list(error = e$message)
      }
    )

    results[[resource_name]] <- result
  }

  if (verbose && n_resources > 1) {
    successful <- sum(sapply(results, function(x) {
      is.null(x[["error"]])
    }))
    message(
      "\nCompleted: ",
      successful,
      "/",
      n_resources,
      " resources loaded successfully"
    )
  }

  # If only one resource, return it directly (not as a list)
  if (n_resources == 1) {
    return(results[[1]])
  }

  return(results)
}

# Helper function to select best resource
select_best_resource <- function(resources,
                                 preferred_format = NULL,
                                 verbose = TRUE) {
  # If specific format requested
  if (!is.null(preferred_format)) {
    format_matches <- resources[toupper(resources$format) == toupper(preferred_format), ]
    if (nrow(format_matches) > 0) {
      # Return the most recent one
      return(format_matches[order(format_matches$last_modified, decreasing = TRUE)[1], ])
    } else {
      if (verbose) {
        warning(
          "Format '",
          preferred_format,
          "' not available. Selecting best alternative."
        )
      }
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
    if (verbose) {
      message("Exploring data related to: ", query)
    }
    search_results <- po_search(query, use_cache = TRUE, verbose = FALSE)
    catalog_datasets <- search_results$datasets
    catalog_resources <- search_results$resources
  } else {
    if (verbose) {
      message("Exploring complete Peru open data catalog...")
    }
    catalog <- po_catalog(verbose = FALSE)
    catalog_datasets <- catalog$datasets
    catalog_resources <- catalog$resources
  }

  if (nrow(catalog_datasets) == 0) {
    stop("No datasets found", if (!is.null(query)) {
      paste0(" for query: ", query)
    } else {
      ""
    })
  }

  # 1. By Organization
  if (verbose) {
    message("Analyzing by organization...")
  }
  by_org <- split(catalog_datasets, catalog_datasets$organization)
  by_org <- lapply(by_org, function(org_data) {
    list(
      n_datasets = nrow(org_data),
      n_resources = sum(org_data$n_resources),
      total_size_mb = sum(org_data$total_size_mb),
      datasets = org_data[order(org_data$last_updated, decreasing = TRUE), c(
        "dataset_name",
        "title",
        "n_resources",
        "formats_available"
      )]
    )
  })
  by_org <- by_org[order(sapply(by_org, function(x) {
    x$n_datasets
  }), decreasing = TRUE)]

  # 2. By Format
  if (verbose) {
    message("Analyzing by format...")
  }
  by_format <- split(catalog_resources, catalog_resources$format)
  by_format <- lapply(by_format, function(fmt_data) {
    list(
      n_resources = nrow(fmt_data),
      total_size_mb = sum(fmt_data$size_mb, na.rm = TRUE),
      avg_size_mb = round(mean(fmt_data$size_mb, na.rm = TRUE), 2),
      resources = fmt_data[order(fmt_data$size_mb, decreasing = TRUE), c(
        "resource_name",
        "dataset_title",
        "size_mb",
        "last_modified"
      )]
    )
  })
  by_format <- by_format[order(sapply(by_format, function(x) {
    x$n_resources
  }), decreasing = TRUE)]

  # 3. By Year
  if (verbose) {
    message("Analyzing temporal distribution...")
  }
  catalog_datasets$year_updated <- substr(catalog_datasets$last_updated, 1, 4)
  year_summary <- table(catalog_datasets$year_updated)
  by_year <- list(
    summary = as.data.frame(year_summary),
    recent_years = names(tail(sort(year_summary), 5))
  )

  # 4. By Size
  if (verbose) {
    message("Analyzing size distribution...")
  }
  size_breaks <- c(0, 1, 10, 100, 1000, Inf)
  size_labels <- c("< 1 MB", "1-10 MB", "10-100 MB", "100 MB - 1 GB", "> 1 GB")
  catalog_datasets$size_category <- cut(catalog_datasets$total_size_mb,
    breaks = size_breaks,
    labels = size_labels
  )
  by_size <- list(
    distribution = table(catalog_datasets$size_category),
    largest = catalog_datasets[
      order(catalog_datasets$total_size_mb, decreasing = TRUE),
      c("title", "organization", "total_size_mb", "n_resources")
    ][1:10, ]
  )

  # 5. Recent updates
  recent <- catalog_datasets[order(catalog_datasets$last_updated, decreasing = TRUE), c(
    "title",
    "organization",
    "last_updated",
    "n_resources",
    "formats_available"
  )][1:20, ]

  # 6. Popular (most resources)
  popular <- catalog_datasets[order(catalog_datasets$n_resources, decreasing = TRUE), c(
    "title",
    "organization",
    "n_resources",
    "formats_available",
    "total_size_mb"
  )][1:20, ]

  # 7. Recommendations
  recommendations <- list()

  if (!is.null(query)) {
    # Query-specific recommendations
    if (nrow(catalog_datasets) > 0) {
      # Datasets with CSV files (easy to work with)
      csv_datasets <- catalog_datasets[grepl("CSV", catalog_datasets$formats_available), ]
      if (nrow(csv_datasets) > 0) {
        n_csv <- min(5, nrow(csv_datasets))
        recommendations$csv_available <- csv_datasets[
          seq_len(n_csv), c("dataset_name", "title", "organization")
        ]
      }

      # Recently updated
      n_recent <- min(5, nrow(catalog_datasets))
      recommendations$recently_updated <- catalog_datasets[
        seq_len(n_recent), c("dataset_name", "title", "last_updated")
      ]

      # Most comprehensive (most resources)
      ordered_datasets <- catalog_datasets[order(catalog_datasets$n_resources, decreasing = TRUE), ]
      n_comp <- min(5, nrow(catalog_datasets))
      recommendations$comprehensive <- ordered_datasets[
        seq_len(n_comp), c("dataset_name", "title", "n_resources")
      ]
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

  if (verbose) {
    message("Exploration complete!")
  }

  return(result)
}

#' Print method for po_explore results
#' @param x A po_explore_result object
#' @param ... Additional arguments passed to print methods
#' @export
print.po_explore_result <- function(x, ...) {
  cli::cli_h1("Peru Open Data Exploration")
  if (!is.null(x$query)) {
    cli::cli_text("Query: {cli::col_cyan(x$query)}")
  }

  cli::cli_h2("Summary")
  cli::cli_text("  Datasets: {cli::col_green(x$summary$n_datasets)}")
  cli::cli_text("  Resources: {cli::col_green(x$summary$n_resources)}")
  cli::cli_text("  Organizations: {cli::col_green(x$summary$n_organizations)}")
  cli::cli_text("  Date range: {cli::col_yellow(paste(x$summary$date_range, collapse = ' to '))}")

  cli::cli_h2("Top Organizations")
  top_orgs <- head(x$by_organization, 5)
  for (i in seq_along(top_orgs)) {
    org <- names(top_orgs)[i]
    info <- top_orgs[[i]]
    cli::cli_text(paste0(
      "  ", cli::col_blue(i), ". ", cli::style_bold(org), ": ",
      cli::col_green(info$n_datasets), " datasets, ",
      cli::col_green(info$n_resources), " resources"
    ))
  }

  cli::cli_h2("Top Formats")
  top_formats <- head(x$by_format, 5)
  for (i in seq_along(top_formats)) {
    fmt <- names(top_formats)[i]
    info <- top_formats[[i]]
    cli::cli_text(paste0(
      "  ", cli::col_blue(fmt), ": ",
      cli::col_green(info$n_resources), " files (avg ",
      cli::col_yellow(paste0(info$avg_size_mb, " MB")), ")"
    ))
  }

  cli::cli_h2("Recent Updates")
  for (i in seq_len(min(5, nrow(x$recent)))) {
    title_truncated <- truncate_text(x$recent$title[i], 50)
    cli::cli_text(paste0(
      "  ", cli::symbol$bullet, " ", cli::style_bold(title_truncated),
      " (", cli::col_cyan(x$recent$last_updated[i]), ")"
    ))
  }

  if (!is.null(x$recommendations$csv_available)) {
    cli::cli_h2("Recommended Datasets (with CSV)")
    for (i in seq_len(nrow(x$recommendations$csv_available))) {
      cli::cli_text(paste0(
        "  ", cli::symbol$bullet, " ",
        cli::col_green(x$recommendations$csv_available$dataset_name[i])
      ))
    }
  }

  cli::cli_rule()
  cli::cli_text(paste0(
    cli::col_blue("Explore further with:"),
    " $by_organization, $by_format, $recent, $popular"
  ))
  invisible(x)
}

truncate_text <- function(text, max_length = 400, suffix = "...") {
  if (is.na(text) || text == "" || nchar(text) <= max_length) {
    return(text)
  }
  paste0(substr(text, 1, max_length - nchar(suffix)), suffix)
}

wrap_text <- function(text, prefix = "", width = 80, indent = 3) {
  if (is.na(text) || text == "") {
    return("")
  }

  available_width <- width - nchar(prefix) - indent
  words <- strsplit(text, "\\s+")[[1]]

  if (length(words) == 0) {
    return("")
  }

  lines <- character()
  current_line <- character()
  current_length <- 0

  for (word in words) {
    word_length <- nchar(word)
    space_needed <- if (length(current_line) == 0) word_length else word_length + 1

    if (current_length + space_needed <= available_width) {
      current_line <- c(current_line, word)
      current_length <- current_length + space_needed
    } else {
      if (length(current_line) > 0) {
        lines <- c(lines, paste(current_line, collapse = " "))
      }
      current_line <- word
      current_length <- word_length
    }
  }

  if (length(current_line) > 0) {
    lines <- c(lines, paste(current_line, collapse = " "))
  }

  if (length(lines) == 0) {
    return("")
  }

  result <- paste0(prefix, lines[1])

  if (length(lines) > 1) {
    indent_str <- paste(rep(" ", nchar(prefix) + indent), collapse = "")
    for (i in 2:length(lines)) {
      result <- paste0(result, "\n", indent_str, lines[i])
    }
  }

  result
}

#' Print method for datasets tibbles
#' @param x A po_datasets object
#' @param max_items Maximum number of items to display
#' @param ... Additional arguments passed to print methods
#' @export
print.po_datasets <- function(x,
                              max_items = getOption("peruopen.print_max", 10),
                              ...) {
  n_total <- nrow(x)
  n_show <- min(max_items, n_total)

  cli::cli_h1("Peru Open Data - Datasets ({n_total} total)")

  if (n_total == 0) {
    cli::cli_text("{cli::col_red('No datasets found.')}")
    return(invisible(x))
  }

  for (i in seq_len(n_show)) {
    row <- x[i, ]

    cli::cli_text("{cli::col_blue(i)}. {cli::style_bold(row$title)}")

    # Organization info
    org_text <- if (is.na(row$organization) ||
                      row$organization == "") {
      "[Unknown]"
    } else {
      row$organization
    }

    # Size info
    size_text <- if (is.na(row$total_size_mb)) {
      "[Unknown]"
    } else if (row$total_size_mb < 0.01) {
      "< 0.01 MB"
    } else {
      sprintf("%.1f MB", row$total_size_mb)
    }

    # Format info
    formats_text <- if (is.na(row$formats_available) ||
                          row$formats_available == "") {
      "[Unknown]"
    } else {
      row$formats_available
    }

    cli::cli_text(paste0(
      "   Organization: ", cli::col_magenta(org_text),
      " | Resources: ", cli::col_green(row$n_resources),
      " (", cli::col_blue(formats_text), ") | Size: ", cli::col_yellow(size_text)
    ))

    # Date info
    updated_text <- if (is.na(row$last_updated)) {
      "[Unknown]"
    } else {
      row$last_updated
    }
    created_text <- if (is.na(row$created)) {
      "[Unknown]"
    } else {
      row$created
    }

    cli::cli_text(paste0(
      "   Last updated: ", cli::col_cyan(updated_text),
      " | Created: ", cli::col_cyan(created_text)
    ))

    # Notes (wrapped with proper indentation)
    if (!is.na(row$notes) && row$notes != "") {
      notes_text <- truncate_text(row$notes)
      notes_formatted <- wrap_text(notes_text, "   Notes: ", width = 90, indent = 0)
      cli::cli_text(notes_formatted)
    }

    if (i < n_show) {
      cli::cli_text("")
    }
  }

  if (n_total > n_show) {
    cli::cli_text("")
    cli::cli_text(
      paste0(cli::col_silver("... (showing "), cli::col_green(n_show),
             cli::col_silver(" of "), cli::col_green(n_total),
             cli::col_silver(" datasets)"))
    )
  }

  cli::cli_rule()
  cli::cli_text(
    paste0(cli::col_blue("Access individual datasets:"), " x[1], x[2], etc. | ",
           cli::col_blue("Full tibble:"), " as_tibble(x)")
  )
  invisible(x)
}

#' Print method for resources tibbles
#' @param x A po_resources object
#' @param max_items Maximum number of items to display
#' @param ... Additional arguments passed to print methods
#' @export
print.po_resources <- function(x,
                               max_items = getOption("peruopen.print_max", 10),
                               ...) {
  n_total <- nrow(x)
  n_show <- min(max_items, n_total)

  cli::cli_h1("Peru Open Data - Resources ({n_total} total)")

  if (n_total == 0) {
    cli::cli_text("{cli::col_red('No resources found.')}")
    return(invisible(x))
  }

  for (i in seq_len(n_show)) {
    row <- x[i, ]

    # Resource name and size
    size_text <- if (is.na(row$size_mb)) {
      "[Unknown]"
    } else if (row$size_mb < 0.01) {
      "< 0.01 MB"
    } else {
      sprintf("%.1f MB", row$size_mb)
    }
    cli::cli_text(
      "{cli::col_blue(i)}. {cli::style_bold(row$resource_name)} ({cli::col_yellow(size_text)})"
    )

    # Dataset info
    cli::cli_text("   Dataset: {cli::style_italic(row$dataset_title)}")

    # Format and date info
    format_text <- if (is.na(row$format)) {
      "[Unknown]"
    } else {
      toupper(row$format)
    }
    modified_text <- if (is.na(row$last_modified)) {
      "[Unknown]"
    } else {
      row$last_modified
    }

    cli::cli_text(paste0(
      "   Format: ", cli::col_blue(format_text),
      " | Last modified: ", cli::col_cyan(modified_text)
    ))

    # URL (wrapped with proper indentation)
    if (!is.na(row$url) && row$url != "") {
      url_text <- truncate_text(row$url, 300)
      url_formatted <- wrap_text(url_text, "   URL: ", width = 90, indent = 0)
      cli::cli_text(url_formatted)
    }

    # Description (wrapped with proper indentation)
    if (!is.na(row$description) &&
          row$description != "" &&
          row$description != "Resource description") {
      desc_text <- truncate_text(row$description)
      desc_formatted <- wrap_text(desc_text, "   Description: ", width = 90, indent = 0)
      cli::cli_text(desc_formatted)
    }

    if (i < n_show) {
      cli::cli_text("")
    }
  }

  if (n_total > n_show) {
    cli::cli_text("")
    cli::cli_text(
      paste0(cli::col_silver("... (showing "), cli::col_green(n_show),
             cli::col_silver(" of "), cli::col_green(n_total),
             cli::col_silver(" resources)"))
    )
  }

  cli::cli_rule()
  cli::cli_text(
    paste0(cli::col_blue("Access individual resources:"), " x[1], x[2], etc. | ",
           cli::col_blue("Full tibble:"), " as_tibble(x)")
  )
  invisible(x)
}

#' Convert po_datasets back to regular tibble
#' @param x A po_datasets object
#' @param ... Additional arguments passed to as_tibble
#' @export
as_tibble.po_datasets <- function(x, ...) {
  class(x) <- class(x)[!class(x) %in% "po_datasets"]
  x
}

#' Convert po_resources back to regular tibble
#' @param x A po_resources object
#' @param ... Additional arguments passed to as_tibble
#' @export
as_tibble.po_resources <- function(x, ...) {
  class(x) <- class(x)[!class(x) %in% "po_resources"]
  x
}

#' Knit print method for po_search results (for Quarto/markdown)
#' @param x A po_search_result object
#' @param ... Additional arguments passed to knit_print methods
#' @importFrom knitr knit_print asis_output
#' @export
knit_print.po_search_result <- function(x, ...) {
  output <- character()

  output <- c(output, "# Peru Open Data Search Results")
  output <- c(output, "")
  output <- c(output, paste0("**Query:** ", x$summary$query))
  output <- c(
    output,
    paste0(
      "**Found:** ",
      x$summary$n_datasets,
      " datasets with ",
      x$summary$n_resources,
      " resources"
    )
  )
  output <- c(
    output,
    paste0("**Total size:** ", x$summary$total_size_gb, " GB")
  )
  output <- c(output, "")

  if (length(x$summary$formats_found) > 0) {
    output <- c(output, "## Top formats")
    output <- c(output, "")
    top_formats <- head(x$summary$formats_found, 5)
    for (i in seq_along(top_formats)) {
      output <- c(
        output,
        paste0("- **", names(top_formats)[i], "**: ", top_formats[i], " files")
      )
    }
    output <- c(output, "")
  }

  if (x$summary$n_datasets > 0) {
    output <- c(output, "## Sample datasets")
    output <- c(output, "")
    sample_data <- head(x$datasets[, c("title", "organization", "n_resources")], 5)
    for (i in seq_len(nrow(sample_data))) {
      title_truncated <- truncate_text(sample_data$title[i], 50)
      output <- c(
        output,
        paste0(
          "- **",
          title_truncated,
          "** (",
          sample_data$organization[i],
          ") [",
          sample_data$n_resources[i],
          " resources]"
        )
      )
    }

    if (x$summary$n_datasets > 5) {
      output <- c(
        output,
        paste0("- ... and ", x$summary$n_datasets - 5, " more datasets")
      )
    }
    output <- c(output, "")
  }

  output <- c(output, "---")
  output <- c(output, "")
  output <- c(
    output,
    "**Access data with:** `$datasets`, `$resources`, `$summary`"
  )

  knitr::asis_output(paste(output, collapse = "\n"))
}

#' Knit print method for po_explore results (for Quarto/markdown)
#' @param x A po_explore_result object
#' @param ... Additional arguments passed to knit_print methods
#' @importFrom knitr knit_print asis_output
#' @export
knit_print.po_explore_result <- function(x, ...) {
  output <- character()

  output <- c(output, "# Peru Open Data Exploration")
  if (!is.null(x$query)) {
    output <- c(output, paste0("**Query:** ", x$query))
  }
  output <- c(output, "")

  output <- c(output, "## Summary")
  output <- c(output, "")
  output <- c(output, paste0("- **Datasets:** ", x$summary$n_datasets))
  output <- c(output, paste0("- **Resources:** ", x$summary$n_resources))
  output <- c(
    output,
    paste0("- **Organizations:** ", x$summary$n_organizations)
  )
  output <- c(output, paste0(
    "- **Date range:** ",
    paste(x$summary$date_range, collapse = " to ")
  ))
  output <- c(output, "")

  output <- c(output, "## Top Organizations")
  output <- c(output, "")
  top_orgs <- head(x$by_organization, 5)
  for (i in seq_along(top_orgs)) {
    org <- names(top_orgs)[i]
    info <- top_orgs[[i]]
    output <- c(
      output,
      paste0(
        i,
        ". **",
        org,
        "**: ",
        info$n_datasets,
        " datasets, ",
        info$n_resources,
        " resources"
      )
    )
  }
  output <- c(output, "")

  output <- c(output, "## Top Formats")
  output <- c(output, "")
  top_formats <- head(x$by_format, 5)
  for (i in seq_along(top_formats)) {
    fmt <- names(top_formats)[i]
    info <- top_formats[[i]]
    output <- c(
      output,
      paste0(
        "- **",
        fmt,
        "**: ",
        info$n_resources,
        " files (avg ",
        info$avg_size_mb,
        " MB)"
      )
    )
  }
  output <- c(output, "")

  output <- c(output, "## Recent Updates")
  output <- c(output, "")
  for (i in seq_len(min(5, nrow(x$recent)))) {
    title_truncated <- truncate_text(x$recent$title[i], 50)
    output <- c(
      output,
      paste0("- **", title_truncated, "** (", x$recent$last_updated[i], ")")
    )
  }
  output <- c(output, "")

  if (!is.null(x$recommendations$csv_available)) {
    output <- c(output, "## Recommended Datasets (with CSV)")
    output <- c(output, "")
    for (i in seq_len(nrow(x$recommendations$csv_available))) {
      output <- c(
        output,
        paste0("- ", x$recommendations$csv_available$dataset_name[i])
      )
    }
    output <- c(output, "")
  }

  output <- c(output, "---")
  output <- c(output, "")
  output <- c(
    output,
    "**Explore further with:** `$by_organization`, `$by_format`, `$recent`, `$popular`"
  )

  knitr::asis_output(paste(output, collapse = "\n"))
}

#' Knit print method for po_datasets (for Quarto/markdown)
#' @param x A po_datasets object
#' @param max_items Maximum number of items to display
#' @param ... Additional arguments passed to knit_print methods
#' @importFrom knitr knit_print asis_output
#' @export
knit_print.po_datasets <- function(x,
                                   max_items = getOption("peruopen.print_max", 10),
                                   ...) {
  n_total <- nrow(x)
  n_show <- min(max_items, n_total)

  output <- character()

  output <- c(
    output,
    paste0("# Peru Open Data - Datasets (", n_total, " total)")
  )
  output <- c(output, "")

  if (n_total == 0) {
    output <- c(output, "**No datasets found.**")
    return(knitr::asis_output(paste(output, collapse = "\n")))
  }

  for (i in seq_len(n_show)) {
    row <- x[i, ]

    output <- c(output, paste0(i, ". **", row$title, "**"))
    output <- c(output, "")

    # Organization info
    org_text <- if (is.na(row$organization) ||
                      row$organization == "") {
      "[Unknown]"
    } else {
      row$organization
    }

    # Size info
    size_text <- if (is.na(row$total_size_mb)) {
      "[Unknown]"
    } else if (row$total_size_mb < 0.01) {
      "< 0.01 MB"
    } else {
      sprintf("%.1f MB", row$total_size_mb)
    }

    # Format info
    formats_text <- if (is.na(row$formats_available) ||
                          row$formats_available == "") {
      "[Unknown]"
    } else {
      row$formats_available
    }

    output <- c(output, paste0("   - **Organization:** ", org_text))
    output <- c(
      output,
      paste0("   - **Resources:** ", row$n_resources, " (", formats_text, ")")
    )
    output <- c(output, paste0("   - **Size:** ", size_text))

    # Date info
    updated_text <- if (is.na(row$last_updated)) {
      "[Unknown]"
    } else {
      row$last_updated
    }
    created_text <- if (is.na(row$created)) {
      "[Unknown]"
    } else {
      row$created
    }

    output <- c(output, paste0("   - **Last updated:** ", updated_text))
    output <- c(output, paste0("   - **Created:** ", created_text))

    # Notes
    if (!is.na(row$notes) && row$notes != "") {
      notes_text <- truncate_text(row$notes)
      output <- c(output, paste0("   - **Notes:** ", notes_text))
    }

    output <- c(output, "")
  }

  if (n_total > n_show) {
    output <- c(
      output,
      paste0("... (showing ", n_show, " of ", n_total, " datasets)")
    )
    output <- c(output, "")
  }

  output <- c(output, "---")
  output <- c(output, "")
  output <- c(
    output,
    "**Access individual datasets:** `x[1]`, `x[2]`, etc. | **Full tibble:** `as_tibble(x)`"
  )

  knitr::asis_output(paste(output, collapse = "\n"))
}

#' Knit print method for po_resources (for Quarto/markdown)
#' @param x A po_resources object
#' @param max_items Maximum number of items to display
#' @param ... Additional arguments passed to knit_print methods
#' @importFrom knitr knit_print asis_output
#' @export
knit_print.po_resources <- function(x,
                                    max_items = getOption("peruopen.print_max", 10),
                                    ...) {
  n_total <- nrow(x)
  n_show <- min(max_items, n_total)

  output <- character()

  output <- c(
    output,
    paste0("# Peru Open Data - Resources (", n_total, " total)")
  )
  output <- c(output, "")

  if (n_total == 0) {
    output <- c(output, "**No resources found.**")
    return(knitr::asis_output(paste(output, collapse = "\n")))
  }

  for (i in seq_len(n_show)) {
    row <- x[i, ]

    # Resource name and size
    size_text <- if (is.na(row$size_mb)) {
      "[Unknown]"
    } else if (row$size_mb < 0.01) {
      "< 0.01 MB"
    } else {
      sprintf("%.1f MB", row$size_mb)
    }
    output <- c(
      output,
      paste0(i, ". **", row$resource_name, "** (", size_text, ")")
    )
    output <- c(output, "")

    # Dataset info
    output <- c(output, paste0("   - **Dataset:** *", row$dataset_title, "*"))

    # Format and date info
    format_text <- if (is.na(row$format)) {
      "[Unknown]"
    } else {
      toupper(row$format)
    }
    modified_text <- if (is.na(row$last_modified)) {
      "[Unknown]"
    } else {
      row$last_modified
    }

    output <- c(output, paste0("   - **Format:** ", format_text))
    output <- c(output, paste0("   - **Last modified:** ", modified_text))

    # URL
    if (!is.na(row$url) && row$url != "") {
      url_text <- truncate_text(row$url, 300)
      output <- c(output, paste0("   - **URL:** ", url_text))
    }

    # Description
    if (!is.na(row$description) &&
          row$description != "" &&
          row$description != "Resource description") {
      desc_text <- truncate_text(row$description)
      output <- c(output, paste0("   - **Description:** ", desc_text))
    }

    output <- c(output, "")
  }

  if (n_total > n_show) {
    output <- c(
      output,
      paste0("... (showing ", n_show, " of ", n_total, " resources)")
    )
    output <- c(output, "")
  }

  output <- c(output, "---")
  output <- c(output, "")
  output <- c(
    output,
    "**Access individual resources:** `x[1]`, `x[2]`, etc. | **Full tibble:** `as_tibble(x)`"
  )

  knitr::asis_output(paste(output, collapse = "\n"))
}

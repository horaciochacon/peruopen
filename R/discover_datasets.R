#' List all available datasets from Peru's CKAN portal
#'
#' @param use_cache Logical. Whether to use cached results (default TRUE)
#' @return A tibble with dataset names
#' @keywords internal
# Internal function - use po_catalog() instead
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows filter mutate select
#' @importFrom rlang abort warn

po_list_datasets <- function(use_cache = TRUE) {
  if (use_cache) {
    packages <- get_packages_cached()
  } else {
    packages <- ckan_get_packages()
  }

  if (length(packages) == 0) {
    rlang::warn("No datasets found in CKAN portal")
    return(tibble::tibble(name = character(0)))
  }

  tibble::tibble(name = packages)
}

#' Search datasets with filters (FAST - using efficient API endpoint)
#'
#' @param query Character string for text search
#' @param tags Character vector of tags to filter by
#' @param organizations Character vector of organizations to filter by
#' @param limit Integer. Maximum number of results (default 50)
#' @param use_cache Logical. Whether to use cached results (default TRUE)
#' @param clean_text Logical. Whether to clean HTML and formatting (default TRUE)
#' @return A tibble with filtered dataset information
#' @keywords internal
# Internal function - use po_search() instead
po_search_datasets <- function(query = NULL, tags = NULL, organizations = NULL,
                               limit = 50, use_cache = TRUE, clean_text = TRUE) {
  if (use_cache) {
    packages_data <- get_current_packages_cached(limit = limit)
  } else {
    packages_data <- ckan_get_packages_resources(limit = limit)
  }

  if (length(packages_data) == 0) {
    return(tibble::tibble(
      name = character(0),
      title = character(0),
      notes = character(0),
      tags = list(),
      organization = character(0),
      num_resources = integer(0),
      last_modified = character(0)
    ))
  }

  metadata_list <- vector("list", length(packages_data))

  for (i in seq_along(packages_data)) {
    pkg_data <- packages_data[[i]]

    # Extract tags properly
    pkg_tags <- character(0)
    if (!is.null(pkg_data$tags) && length(pkg_data$tags) > 0) {
      pkg_tags <- sapply(pkg_data$tags, function(x) {
        if (is.list(x) && !is.null(x$name)) x$name else x
      })
    }

    # Extract organization properly
    org_title <- NA_character_
    if (!is.null(pkg_data$groups) && length(pkg_data$groups) > 0) {
      org_title <- pkg_data$groups[[1]]$title %||% NA_character_
    }

    # Apply text cleaning if requested
    clean_title <- clean_string(pkg_data$title %||% NA_character_, clean_text)
    clean_notes <- clean_html_text(pkg_data$notes %||% NA_character_, clean_text)
    clean_org <- clean_string(org_title, clean_text)

    metadata_list[[i]] <- list(
      name = pkg_data$name %||% NA_character_,
      title = clean_title,
      notes = clean_notes,
      tags = list(pkg_tags),
      organization = clean_org,
      num_resources = length(pkg_data$resources %||% list()),
      last_modified = pkg_data$metadata_modified %||% NA_character_
    )
  }

  result <- dplyr::bind_rows(metadata_list)

  if (!is.null(query) && query != "") {
    query_pattern <- paste0("(?i)", gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", query))
    result <- result |>
      dplyr::filter(
        grepl(query_pattern, name, perl = TRUE) |
          grepl(query_pattern, title, perl = TRUE) |
          grepl(query_pattern, notes, perl = TRUE)
      )
  }

  if (!is.null(tags) && length(tags) > 0) {
    search_tags <- tags # Store search terms to avoid confusion
    result <- result |>
      dplyr::filter(
        sapply(tags, function(pkg_tags) {
          if (length(pkg_tags) == 0) {
            return(FALSE)
          }
          any(sapply(search_tags, function(tag) any(grepl(tag, pkg_tags, ignore.case = TRUE))))
        })
      )
  }

  if (!is.null(organizations) && length(organizations) > 0) {
    result <- result |>
      dplyr::filter(organization %in% organizations)
  }

  result
}

#' Get available organizations/groups from CKAN portal
#'
#' @param use_cache Logical. Whether to use cached results (default TRUE)
#' @return A tibble with organization information
#' @keywords internal
# Internal function - organization info available in po_catalog()
po_list_organizations <- function(use_cache = TRUE) {
  if (use_cache) {
    groups <- get_groups_cached()
  } else {
    groups <- ckan_get_groups()
  }

  if (length(groups) == 0) {
    rlang::warn("No organizations found in CKAN portal")
    return(tibble::tibble(name = character(0), title = character(0)))
  }

  tibble::tibble(
    name = sapply(groups, function(x) x$name %||% NA_character_),
    title = sapply(groups, function(x) x$title %||% NA_character_)
  )
}

#' Search datasets with pagination support for large result sets
#'
#' @param query Character string for text search
#' @param tags Character vector of tags to filter by
#' @param organizations Character vector of organizations to filter by
#' @param max_results Integer. Maximum total results to return (default 500)
#' @param page_size Integer. Results per API call (default 100)
#' @param clean_text Logical. Whether to clean HTML and formatting (default TRUE)
#' @return A tibble with filtered dataset information
#' @keywords internal
# Internal function - use po_search() instead
po_search_datasets_paginated <- function(query = NULL, tags = NULL, organizations = NULL,
                                         max_results = 500, page_size = 100, clean_text = TRUE) {
  all_results <- list()
  current_offset <- 0
  total_fetched <- 0

  while (total_fetched < max_results) {
    remaining <- max_results - total_fetched
    current_limit <- min(page_size, remaining)

    packages_data <- ckan_get_packages_resources(
      limit = current_limit,
      offset = current_offset
    )

    if (length(packages_data) == 0) break

    page_results <- list()
    for (i in seq_along(packages_data)) {
      pkg_data <- packages_data[[i]]

      # Extract tags properly
      pkg_tags <- character(0)
      if (!is.null(pkg_data$tags) && length(pkg_data$tags) > 0) {
        pkg_tags <- sapply(pkg_data$tags, function(x) {
          if (is.list(x) && !is.null(x$name)) x$name else x
        })
      }

      # Extract organization properly
      org_title <- NA_character_
      if (!is.null(pkg_data$groups) && length(pkg_data$groups) > 0) {
        org_title <- pkg_data$groups[[1]]$title %||% NA_character_
      }

      # Apply text cleaning if requested
      clean_title <- clean_string(pkg_data$title %||% NA_character_, clean_text)
      clean_notes <- clean_html_text(pkg_data$notes %||% NA_character_, clean_text)
      clean_org <- clean_string(org_title, clean_text)

      page_results[[i]] <- list(
        name = pkg_data$name %||% NA_character_,
        title = clean_title,
        notes = clean_notes,
        tags = list(pkg_tags),
        organization = clean_org,
        num_resources = length(pkg_data$resources %||% list()),
        last_modified = pkg_data$metadata_modified %||% NA_character_
      )
    }

    all_results <- append(all_results, page_results)
    total_fetched <- total_fetched + length(packages_data)
    current_offset <- current_offset + current_limit

    if (length(packages_data) < current_limit) break
  }

  if (length(all_results) == 0) {
    return(tibble::tibble(
      name = character(0),
      title = character(0),
      notes = character(0),
      tags = list(),
      organization = character(0),
      num_resources = integer(0),
      last_modified = character(0)
    ))
  }

  result <- dplyr::bind_rows(all_results)

  # Apply filters
  if (!is.null(query) && query != "") {
    query_pattern <- paste0("(?i)", gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", query))
    result <- result |>
      dplyr::filter(
        grepl(query_pattern, name, perl = TRUE) |
          grepl(query_pattern, title, perl = TRUE) |
          grepl(query_pattern, notes, perl = TRUE)
      )
  }

  if (!is.null(tags) && length(tags) > 0) {
    search_tags <- tags
    result <- result |>
      dplyr::filter(
        sapply(tags, function(pkg_tags) {
          if (length(pkg_tags) == 0) {
            return(FALSE)
          }
          any(sapply(search_tags, function(tag) any(grepl(tag, pkg_tags, ignore.case = TRUE))))
        })
      )
  }

  if (!is.null(organizations) && length(organizations) > 0) {
    result <- result |>
      dplyr::filter(organization %in% organizations)
  }

  result
}

#' Get detailed tabular metadata including all resources
#'
#' @param query Character string for text search
#' @param tags Character vector of tags to filter by
#' @param organizations Character vector of organizations to filter by
#' @param limit Integer. Maximum number of datasets (default 50)
#' @param use_cache Logical. Whether to use cached results (default TRUE)
#' @param clean_text Logical. Whether to clean HTML and formatting (default TRUE)
#' @return A list with datasets tibble and resources tibble
#' @keywords internal
# Internal function - use po_catalog() instead
po_get_structured_metadata <- function(query = NULL, tags = NULL, organizations = NULL,
                                       limit = 50, use_cache = TRUE, clean_text = TRUE) {
  if (use_cache) {
    packages_data <- get_current_packages_cached(limit = limit)
  } else {
    packages_data <- ckan_get_packages_resources(limit = limit)
  }

  if (length(packages_data) == 0) {
    return(list(
      datasets = tibble::tibble(
        dataset_id = character(0), name = character(0), title = character(0),
        notes = character(0), organization = character(0), author = character(0),
        license_id = character(0), state = character(0), created = character(0),
        modified = character(0), num_resources = integer(0)
      ),
      resources = tibble::tibble(
        dataset_id = character(0), dataset_name = character(0), resource_id = character(0),
        resource_name = character(0), description = character(0), format = character(0),
        mimetype = character(0), url = character(0), size = character(0),
        created = character(0), last_modified = character(0)
      )
    ))
  }

  datasets_list <- vector("list", length(packages_data))
  resources_list <- vector("list", length(packages_data))

  for (i in seq_along(packages_data)) {
    pkg_data <- packages_data[[i]]

    # Extract tags properly
    pkg_tags <- character(0)
    if (!is.null(pkg_data$tags) && length(pkg_data$tags) > 0) {
      pkg_tags <- sapply(pkg_data$tags, function(x) {
        if (is.list(x) && !is.null(x$name)) x$name else x
      })
    }

    # Extract organization properly
    org_title <- NA_character_
    if (!is.null(pkg_data$groups) && length(pkg_data$groups) > 0) {
      org_title <- pkg_data$groups[[1]]$title %||% NA_character_
    }

    # Dataset metadata with proper date formatting
    created_date <- pkg_data$metadata_created %||% NA_character_
    modified_date <- pkg_data$metadata_modified %||% NA_character_

    # Parse dates if they exist
    if (!is.na(created_date) && created_date != "") {
      created_date <- format_peru_date(created_date)
    }
    if (!is.na(modified_date) && modified_date != "") {
      modified_date <- format_peru_date(modified_date)
    }

    # Apply text cleaning to datasets
    clean_title <- clean_string(pkg_data$title %||% NA_character_, clean_text)
    clean_notes <- clean_html_text(pkg_data$notes %||% NA_character_, clean_text)
    clean_org <- clean_string(org_title, clean_text)
    clean_author <- clean_string(pkg_data$author %||% NA_character_, clean_text)

    datasets_list[[i]] <- list(
      dataset_id = pkg_data$id %||% NA_character_,
      name = pkg_data$name %||% NA_character_,
      title = clean_title,
      notes = clean_notes,
      organization = clean_org,
      author = clean_author,
      license_id = pkg_data$license_title %||% NA_character_,
      state = pkg_data$state %||% NA_character_,
      created = created_date,
      modified = modified_date,
      num_resources = length(pkg_data$resources %||% list())
    )

    # Resources metadata
    if (!is.null(pkg_data$resources) && length(pkg_data$resources) > 0) {
      pkg_resources <- vector("list", length(pkg_data$resources))
      for (j in seq_along(pkg_data$resources)) {
        res <- pkg_data$resources[[j]]

        # Format resource dates
        res_created <- res$created %||% NA_character_
        res_modified <- res$last_modified %||% NA_character_

        if (!is.na(res_created) && res_created != "") {
          res_created <- format_peru_date(res_created)
        }
        if (!is.na(res_modified) && res_modified != "") {
          res_modified <- format_peru_date(res_modified)
        }

        # Apply text cleaning to resources
        clean_res_name <- clean_string(res$name %||% NA_character_, clean_text)
        clean_res_desc <- clean_html_text(res$description %||% NA_character_, clean_text)

        pkg_resources[[j]] <- list(
          dataset_id = pkg_data$id %||% NA_character_,
          dataset_name = pkg_data$name %||% NA_character_,
          resource_id = res$id %||% NA_character_,
          resource_name = clean_res_name,
          description = clean_res_desc,
          format = res$format %||% NA_character_,
          mimetype = res$mimetype %||% NA_character_,
          url = res$url %||% NA_character_,
          size = res$size %||% NA_character_,
          created = res_created,
          last_modified = res_modified
        )
      }
      resources_list[[i]] <- pkg_resources
    } else {
      resources_list[[i]] <- list()
    }
  }

  datasets <- dplyr::bind_rows(datasets_list)

  # Flatten resources list
  flat_resources <- unlist(resources_list, recursive = FALSE)
  if (length(flat_resources) > 0) {
    resources <- dplyr::bind_rows(flat_resources)
  } else {
    resources <- tibble::tibble(
      dataset_id = character(0), dataset_name = character(0), resource_id = character(0),
      resource_name = character(0), description = character(0), format = character(0),
      mimetype = character(0), url = character(0), size = character(0),
      created = character(0), last_modified = character(0)
    )
  }

  # Apply filters to datasets
  if (!is.null(query) && query != "") {
    query_pattern <- paste0("(?i)", gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", query))
    filtered_ids <- datasets |>
      dplyr::filter(
        grepl(query_pattern, name, perl = TRUE) |
          grepl(query_pattern, title, perl = TRUE) |
          grepl(query_pattern, notes, perl = TRUE)
      ) |>
      dplyr::pull(dataset_id)

    datasets <- datasets |> dplyr::filter(dataset_id %in% filtered_ids)
    resources <- resources |> dplyr::filter(dataset_id %in% filtered_ids)
  }

  if (!is.null(organizations) && length(organizations) > 0) {
    filtered_ids <- datasets |>
      dplyr::filter(organization %in% organizations) |>
      dplyr::pull(dataset_id)

    datasets <- datasets |> dplyr::filter(dataset_id %in% filtered_ids)
    resources <- resources |> dplyr::filter(dataset_id %in% filtered_ids)
  }

  list(datasets = datasets, resources = resources)
}

#' Get total number of datasets
#'
#' @param use_cache Logical. Whether to use cached results (default TRUE)
#' @return Integer count of total datasets
#' @keywords internal
# Internal function - count available in po_catalog()$summary
po_get_dataset_count <- function(use_cache = TRUE) {
  packages <- po_list_datasets(use_cache = use_cache)
  nrow(packages)
}

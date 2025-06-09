#' Get complete dataset metadata in nested list format with data cleaning
#'
#' @param query Character string for text search
#' @param tags Character vector of tags to filter by
#' @param organizations Character vector of organizations to filter by
#' @param max_results Integer. Maximum number of datasets (default 1000)
#' @param page_size Integer. Results per API call (default 200)
#' @param clean_text Logical. Whether to clean HTML and formatting (default TRUE)
#' @return A nested list structure with complete metadata per dataset
#' @keywords internal
# Internal function - use po_catalog() instead
#' @importFrom tibble tibble
#' @importFrom dplyr filter
#' @importFrom rlang abort

po_get_metadata_nested <- function(query = NULL, tags = NULL, organizations = NULL,
                                   max_results = 1000, page_size = 200,
                                   clean_text = TRUE) {
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

    for (i in seq_along(packages_data)) {
      pkg_data <- packages_data[[i]]

      # Extract and clean basic dataset info
      dataset_id <- pkg_data$id %||% NA_character_
      dataset_name <- clean_string(pkg_data$name %||% NA_character_, clean_text)
      dataset_title <- clean_string(pkg_data$title %||% NA_character_, clean_text)
      dataset_notes <- clean_html_text(pkg_data$notes %||% NA_character_, clean_text)
      dataset_url <- pkg_data$url %||% NA_character_

      # Extract organization info
      organization <- extract_organization_info(pkg_data$groups)

      # Extract and clean author/maintainer info
      author_info <- list(
        author = clean_string(pkg_data$author %||% NA_character_, clean_text),
        author_email = clean_email(pkg_data$author_email %||% NA_character_),
        maintainer = clean_string(pkg_data$maintainer %||% NA_character_, clean_text),
        maintainer_email = clean_email(pkg_data$maintainer_email %||% NA_character_)
      )

      # Extract license info
      license_info <- list(
        license_id = pkg_data$license_id %||% NA_character_,
        license_title = clean_string(pkg_data$license_title %||% NA_character_, clean_text),
        license_url = pkg_data$license_url %||% NA_character_
      )

      # Extract and format dates
      dates_info <- list(
        created = format_peru_date(pkg_data$metadata_created %||% NA_character_),
        modified = format_peru_date(pkg_data$metadata_modified %||% NA_character_),
        revision_timestamp = format_peru_date(pkg_data$revision_timestamp %||% NA_character_)
      )

      # Extract tags with cleaning
      tags_cleaned <- extract_and_clean_tags(pkg_data$tags, clean_text)

      # Extract and clean resources with complete metadata
      resources_list <- extract_complete_resources(pkg_data$resources, clean_text)

      # Extract additional metadata fields
      additional_metadata <- list(
        type = pkg_data$type %||% NA_character_,
        state = pkg_data$state %||% NA_character_,
        private = pkg_data$private %||% FALSE,
        version = clean_string(pkg_data$version %||% NA_character_, clean_text),
        creator_user_id = pkg_data$creator_user_id %||% NA_character_,
        revision_id = pkg_data$revision_id %||% NA_character_,
        isopen = pkg_data$isopen %||% NA,
        num_tags = length(tags_cleaned),
        num_resources = length(resources_list)
      )

      # Create complete nested structure
      dataset_complete <- list(
        # Basic identification
        id = dataset_id,
        name = dataset_name,
        title = dataset_title,
        description = dataset_notes,
        url = dataset_url,

        # Organization
        organization = organization,

        # Authorship
        authorship = author_info,

        # Legal
        license = license_info,

        # Temporal
        dates = dates_info,

        # Classification
        tags = tags_cleaned,

        # Content
        resources = resources_list,

        # Administrative
        metadata = additional_metadata
      )

      all_results[[dataset_id]] <- dataset_complete
    }

    total_fetched <- total_fetched + length(packages_data)
    current_offset <- current_offset + current_limit

    if (length(packages_data) < current_limit) break
  }

  # Apply filters
  if (!is.null(query) || !is.null(tags) || !is.null(organizations)) {
    all_results <- apply_filters_to_nested(all_results, query, tags, organizations)
  }

  return(all_results)
}

# Helper functions for data cleaning and extraction

clean_string <- function(text, clean_text = TRUE) {
  if (!clean_text || is.na(text) || text == "") {
    return(text)
  }

  # Very conservative cleaning - only fix spacing issues
  text <- trimws(text) # Remove leading/trailing whitespace
  text <- gsub("\\s+", " ", text) # Fix multiple spaces to single space
  text <- gsub("[\r\n\t]", " ", text) # Replace actual line breaks with spaces (FIXED!)

  return(text)
}

clean_html_text <- function(html_text, clean_text = TRUE) {
  if (!clean_text || is.na(html_text) || html_text == "") {
    return(html_text)
  }

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
  if (is.na(email) || email == "") {
    return(NA_character_)
  }

  # Basic email validation
  if (grepl("^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$", email)) {
    return(tolower(trimws(email)))
  }

  return(NA_character_)
}

extract_organization_info <- function(groups_data) {
  if (is.null(groups_data) || length(groups_data) == 0) {
    return(list(
      id = NA_character_,
      name = NA_character_,
      title = NA_character_,
      description = NA_character_
    ))
  }

  org <- groups_data[[1]]
  return(list(
    id = org$id %||% NA_character_,
    name = clean_string(org$name %||% NA_character_),
    title = clean_string(org$title %||% NA_character_),
    description = clean_string(org$description %||% NA_character_)
  ))
}

extract_and_clean_tags <- function(tags_data, clean_text = TRUE) {
  if (is.null(tags_data) || length(tags_data) == 0) {
    return(character(0))
  }

  tags <- sapply(tags_data, function(x) {
    tag_name <- if (is.list(x) && !is.null(x$name)) x$name else x
    if (clean_text) {
      clean_string(tag_name)
    } else {
      tag_name
    }
  })

  return(tags[!is.na(tags) & tags != ""])
}

extract_complete_resources <- function(resources_data, clean_text = TRUE) {
  if (is.null(resources_data) || length(resources_data) == 0) {
    return(list())
  }

  resources_list <- list()

  for (i in seq_along(resources_data)) {
    res <- resources_data[[i]]

    # Parse file size properly
    size_info <- parse_file_size(res$size %||% NA_character_)

    resource_complete <- list(
      # Basic identification
      id = res$id %||% NA_character_,
      name = clean_string(res$name %||% NA_character_, clean_text),
      description = clean_html_text(res$description %||% NA_character_, clean_text),

      # File information
      format = clean_string(res$format %||% NA_character_, clean_text),
      mimetype = res$mimetype %||% NA_character_,
      url = res$url %||% NA_character_,

      # Size information
      size = list(
        original = res$size %||% NA_character_,
        parsed = size_info
      ),

      # Temporal
      dates = list(
        created = format_peru_date(res$created %||% NA_character_),
        last_modified = format_peru_date(res$last_modified %||% NA_character_),
        revision_timestamp = format_peru_date(res$revision_timestamp %||% NA_character_)
      ),

      # Administrative
      metadata = list(
        position = res$position %||% NA_integer_,
        revision_id = res$revision_id %||% NA_character_,
        resource_group_id = res$resource_group_id %||% NA_character_,
        package_id = res$package_id %||% NA_character_,
        state = res$state %||% NA_character_,
        hash = res$hash %||% NA_character_
      )
    )

    resources_list[[res$id %||% paste0("resource_", i)]] <- resource_complete
  }

  return(resources_list)
}

parse_file_size <- function(size_string) {
  if (is.na(size_string) || size_string == "") {
    return(list(bytes = NA_integer_, unit = NA_character_, formatted = NA_character_))
  }

  # Extract number and unit
  size_pattern <- "([0-9.,]+)\\s*([A-Za-z]+)"
  matches <- regmatches(size_string, regexec(size_pattern, size_string))

  if (length(matches[[1]]) == 3) {
    number_str <- gsub(",", "", matches[[1]][2])
    unit <- toupper(matches[[1]][3])
    number <- as.numeric(number_str)

    # Convert to bytes
    multipliers <- list("B" = 1, "KB" = 1024, "MB" = 1024^2, "GB" = 1024^3, "TB" = 1024^4)
    bytes <- if (!is.na(number) && unit %in% names(multipliers)) {
      round(number * multipliers[[unit]])
    } else {
      NA_integer_
    }

    return(list(
      bytes = bytes,
      unit = unit,
      formatted = size_string,
      number = number
    ))
  }

  return(list(bytes = NA_integer_, unit = NA_character_, formatted = size_string))
}

apply_filters_to_nested <- function(results_list, query = NULL, tags = NULL, organizations = NULL) {
  if (length(results_list) == 0) {
    return(results_list)
  }

  filtered_results <- list()

  for (dataset_id in names(results_list)) {
    dataset <- results_list[[dataset_id]]
    include_dataset <- TRUE

    # Query filter
    if (!is.null(query) && query != "") {
      query_pattern <- paste0("(?i)", gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", query))
      searchable_text <- paste(
        dataset$name, dataset$title, dataset$description,
        collapse = " "
      )
      if (!grepl(query_pattern, searchable_text, perl = TRUE)) {
        include_dataset <- FALSE
      }
    }

    # Tags filter
    if (include_dataset && !is.null(tags) && length(tags) > 0) {
      dataset_tags <- dataset$tags
      if (length(dataset_tags) == 0) {
        include_dataset <- FALSE
      } else {
        tag_match <- any(sapply(tags, function(tag) {
          any(grepl(tag, dataset_tags, ignore.case = TRUE))
        }))
        if (!tag_match) include_dataset <- FALSE
      }
    }

    # Organization filter
    if (include_dataset && !is.null(organizations) && length(organizations) > 0) {
      org_title <- dataset$organization$title
      if (is.na(org_title) || !org_title %in% organizations) {
        include_dataset <- FALSE
      }
    }

    if (include_dataset) {
      filtered_results[[dataset_id]] <- dataset
    }
  }

  return(filtered_results)
}

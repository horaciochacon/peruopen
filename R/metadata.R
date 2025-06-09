#' Get detailed metadata for a dataset
#'
#' @param dataset_id Character. The dataset ID
#' @param use_cache Logical. Whether to use cached results (default TRUE)
#' @param clean_text Logical. Whether to clean HTML and formatting (default TRUE)
#' @return A list with comprehensive dataset metadata
#' @keywords internal
# Internal function - use po_get() instead
#' @importFrom tibble tibble
#' @importFrom dplyr mutate select
#' @importFrom rlang abort

po_get_dataset_metadata <- function(dataset_id, use_cache = TRUE, clean_text = TRUE) {
  if (missing(dataset_id) || is.null(dataset_id) || dataset_id == "") {
    rlang::abort("Dataset ID is required", class = "po_input_error")
  }

  tryCatch(
    {
      if (use_cache) {
        pkg_data <- get_package_cached(dataset_id)
      } else {
        pkg_data <- ckan_get_package(dataset_id)
      }

      # Peru CKAN returns result as single-element list
      if (is.list(pkg_data) && length(pkg_data) == 1 && is.list(pkg_data[[1]])) {
        pkg_data <- pkg_data[[1]]
      }

      resources <- if (length(pkg_data$resources) > 0) {
        do.call(dplyr::bind_rows, lapply(pkg_data$resources, function(r) {
          tibble::tibble(
            resource_id = r$id %||% NA_character_,
            name = clean_string(r$name %||% NA_character_, clean_text),
            description = clean_html_text(r$description %||% NA_character_, clean_text),
            format = r$format %||% NA_character_,
            mimetype = r$mimetype %||% NA_character_,
            url = r$url %||% NA_character_,
            size = r$size %||% NA_integer_,
            created = r$created %||% NA_character_,
            last_modified = r$last_modified %||% NA_character_
          )
        }))
      } else {
        tibble::tibble(
          resource_id = character(0),
          name = character(0),
          description = character(0),
          format = character(0),
          mimetype = character(0),
          url = character(0),
          size = integer(0),
          created = character(0),
          last_modified = character(0)
        )
      }

      tags <- if (length(pkg_data$tags) > 0) {
        sapply(pkg_data$tags, function(x) x$name %||% NA_character_)
      } else {
        character(0)
      }

      list(
        id = pkg_data$id %||% NA_character_,
        name = pkg_data$name %||% NA_character_,
        title = clean_string(pkg_data$title %||% NA_character_, clean_text),
        notes = clean_html_text(pkg_data$notes %||% NA_character_, clean_text),
        url = pkg_data$url %||% NA_character_,
        version = pkg_data$version %||% NA_character_,
        author = clean_string(pkg_data$author %||% NA_character_, clean_text),
        author_email = clean_email(pkg_data$author_email %||% NA_character_),
        maintainer = clean_string(pkg_data$maintainer %||% NA_character_, clean_text),
        maintainer_email = clean_email(pkg_data$maintainer_email %||% NA_character_),
        license_id = pkg_data$license_id %||% NA_character_,
        license_title = clean_string(pkg_data$license_title %||% NA_character_, clean_text),
        state = pkg_data$state %||% NA_character_,
        type = pkg_data$type %||% NA_character_,
        created = format_peru_date(pkg_data$metadata_created %||% NA_character_),
        modified = format_peru_date(pkg_data$metadata_modified %||% NA_character_),
        organization = clean_string(pkg_data$organization$title %||% NA_character_, clean_text),
        organization_id = pkg_data$organization$id %||% NA_character_,
        tags = list(tags),
        resources = list(resources),
        num_resources = nrow(resources)
      )
    },
    error = function(e) {
      if (inherits(e, "ckan_api_error")) {
        rlang::abort(paste("Failed to get metadata for dataset", dataset_id, ":", e$message),
          class = "po_metadata_error"
        )
      }
      rlang::abort(paste("Error retrieving dataset metadata:", e$message),
        class = "po_metadata_error"
      )
    }
  )
}

#' Get metadata for a specific resource
#'
#' @param resource_id Character. The resource ID
#' @return A list with resource metadata
#' @keywords internal
# Internal function - use po_get() instead
po_get_resource_metadata <- function(resource_id) {
  if (missing(resource_id) || is.null(resource_id) || resource_id == "") {
    rlang::abort("Resource ID is required", class = "po_input_error")
  }

  tryCatch(
    {
      resource_data <- ckan_get_resource(resource_id)

      # Peru CKAN returns result as single-element list
      if (is.list(resource_data) && length(resource_data) == 1 && is.list(resource_data[[1]])) {
        resource_data <- resource_data[[1]]
      }

      list(
        id = resource_data$id %||% NA_character_,
        name = resource_data$name %||% NA_character_,
        description = resource_data$description %||% NA_character_,
        format = resource_data$format %||% NA_character_,
        mimetype = resource_data$mimetype %||% NA_character_,
        url = resource_data$url %||% NA_character_,
        size = resource_data$size %||% NA_integer_,
        created = resource_data$created %||% NA_character_,
        last_modified = resource_data$last_modified %||% NA_character_,
        package_id = resource_data$package_id %||% NA_character_,
        position = resource_data$position %||% NA_integer_,
        revision_id = resource_data$revision_id %||% NA_character_,
        hash = resource_data$hash %||% NA_character_
      )
    },
    error = function(e) {
      if (inherits(e, "ckan_api_error")) {
        rlang::abort(paste("Failed to get metadata for resource", resource_id, ":", e$message),
          class = "po_metadata_error"
        )
      }
      rlang::abort(paste("Error retrieving resource metadata:", e$message),
        class = "po_metadata_error"
      )
    }
  )
}

#' List resources for a dataset
#'
#' @param dataset_id Character. The dataset ID
#' @param format Character. Filter by resource format (optional)
#' @param use_cache Logical. Whether to use cached results (default TRUE)
#' @param clean_text Logical. Whether to clean HTML and formatting (default TRUE)
#' @return A tibble with resource information
#' @keywords internal
# Internal function - use po_get() or po_catalog() instead
po_list_resources <- function(dataset_id, format = NULL, use_cache = TRUE, clean_text = TRUE) {
  metadata <- po_get_dataset_metadata(dataset_id, use_cache = use_cache, clean_text = clean_text)
  resources <- metadata$resources[[1]]

  if (!is.null(format) && nrow(resources) > 0) {
    format_pattern <- paste0("(?i)", gsub("([.*+?^${}()|\\[\\]\\\\])", "\\\\\\1", format))
    resources <- resources |>
      dplyr::filter(grepl(format_pattern, format, perl = TRUE))
  }

  resources
}

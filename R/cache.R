#' @importFrom memoise memoise

cache_timeout <- 3600

get_packages_cached <- memoise::memoise(ckan_get_packages)

get_package_cached <- memoise::memoise(ckan_get_package)

get_groups_cached <- memoise::memoise(ckan_get_groups)

get_current_packages_cached <- memoise::memoise(ckan_get_packages_resources)

#' Clear package cache
#'
#' @param type Character. Type of cache to clear: "all" (default), "api", "catalog", or "files"
#' @return Invisible TRUE
#' @export
clear_cache <- function(type = c("all", "api", "catalog", "files")) {
  type <- match.arg(type)

  if (type %in% c("all", "api")) {
    memoise::forget(get_packages_cached)
    memoise::forget(get_package_cached)
    memoise::forget(get_groups_cached)
    memoise::forget(get_current_packages_cached)
  }

  if (type %in% c("all", "catalog")) {
    cache_dir <- rappdirs::user_cache_dir("peruopen", "R")
    catalog_file <- file.path(cache_dir, "complete_catalog.rds")
    if (file.exists(catalog_file)) {
      unlink(catalog_file)
    }
  }

  if (type %in% c("all", "files")) {
    clear_resource_cache()
  }

  invisible(TRUE)
}

#' Clear downloaded resource file cache
#'
#' @return Invisible TRUE
#' @export
clear_resource_cache <- function() {
  cache_dir <- rappdirs::user_cache_dir("peruopen", "R")
  resource_cache_dir <- file.path(cache_dir, "resources")

  if (dir.exists(resource_cache_dir)) {
    files_to_remove <- list.files(resource_cache_dir, full.names = TRUE)
    if (length(files_to_remove) > 0) {
      unlink(files_to_remove)
    }
  }

  invisible(TRUE)
}

#' Get cache information
#'
#' @return List with cache status information
#' @export
cache_info <- function() {
  # API cache info
  api_info <- list(
    packages_cached = memoise::has_cache(get_packages_cached)(),
    timeout_seconds = cache_timeout
  )

  # Catalog cache info
  cache_dir <- rappdirs::user_cache_dir("peruopen", "R")
  catalog_file <- file.path(cache_dir, "complete_catalog.rds")
  catalog_info <- list(
    exists = file.exists(catalog_file),
    size_mb = if (file.exists(catalog_file))
      round(file.size(catalog_file) / 1024 / 1024, 2)
    else
      0,
    age_hours = if (file.exists(catalog_file)) {
      round(as.numeric(difftime(
        Sys.time(), file.info(catalog_file)$mtime, units = "hours"
      )), 1)
    } else {
      NA
    }
  )

  # Resource cache info
  resource_cache_dir <- file.path(cache_dir, "resources")
  if (dir.exists(resource_cache_dir)) {
    resource_files <- list.files(resource_cache_dir, pattern = "^resource_", full.names = TRUE)
    # Exclude .meta files from count
    data_files <- resource_files[!grepl("\\.meta$", resource_files)]
    total_size <- sum(file.size(data_files), na.rm = TRUE)

    resource_info <- list(
      n_files = length(data_files),
      total_size_mb = round(total_size / 1024 / 1024, 2),
      cache_dir = resource_cache_dir
    )
  } else {
    resource_info <- list(
      n_files = 0,
      total_size_mb = 0,
      cache_dir = resource_cache_dir
    )
  }

  list(
    api = api_info,
    catalog = catalog_info,
    resources = resource_info
  )
}

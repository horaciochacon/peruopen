#' @importFrom memoise memoise

cache_timeout <- 3600

get_packages_cached <- memoise::memoise(ckan_get_packages)

get_package_cached <- memoise::memoise(ckan_get_package)

get_groups_cached <- memoise::memoise(ckan_get_groups)

get_current_packages_cached <- memoise::memoise(ckan_get_current_packages_with_resources)

#' Clear package cache
#'
#' @return Invisible TRUE
#' @export
clear_cache <- function() {
  memoise::forget(get_packages_cached)
  memoise::forget(get_package_cached)
  memoise::forget(get_groups_cached)
  memoise::forget(get_current_packages_cached)
  invisible(TRUE)
}

#' Get cache information
#'
#' @return List with cache status information
#' @export
cache_info <- function() {
  list(
    packages_cached = memoise::has_cache(get_packages_cached)(),
    timeout_seconds = cache_timeout
  )
}
#' @importFrom httr2 request req_user_agent req_timeout req_retry req_perform req_error resp_body_json resp_status req_url_query
#' @importFrom rlang abort

CKAN_BASE_URL <- "https://www.datosabiertos.gob.pe/api/3/action/"

ckan_request <- function(endpoint, params = list(), ...) {
  url <- paste0(CKAN_BASE_URL, endpoint)
  
  req <- httr2::request(url) |>
    httr2::req_user_agent("peruopen R package (https://github.com/user/peruopen)") |>
    httr2::req_timeout(30) |>
    httr2::req_retry(max_tries = 3)
  
  if (length(params) > 0) {
    req <- httr2::req_url_query(req, !!!params)
  }
  
  tryCatch({
    resp <- httr2::req_perform(req)
    
    if (httr2::resp_status(resp) != 200) {
      rlang::abort(
        paste("CKAN API request failed with status", httr2::resp_status(resp)),
        class = "ckan_api_error"
      )
    }
    
    result <- httr2::resp_body_json(resp)
    
    if (!isTRUE(result$success)) {
      rlang::abort(
        paste("CKAN API returned error:", result$error$message %||% "Unknown error"),
        class = "ckan_api_error"
      )
    }
    
    result$result
    
  }, error = function(e) {
    if (inherits(e, "ckan_api_error")) {
      stop(e)
    }
    rlang::abort(
      paste("Failed to connect to CKAN API:", e$message),
      class = "ckan_connection_error"
    )
  })
}

ckan_get_packages <- function() {
  result <- ckan_request("package_list")
  unlist(result)
}

ckan_get_package <- function(id) {
  if (missing(id) || is.null(id) || id == "") {
    rlang::abort("Package ID cannot be empty", class = "ckan_input_error")
  }
  
  ckan_request("package_show", params = list(id = id))
}

ckan_get_resource <- function(id) {
  if (missing(id) || is.null(id) || id == "") {
    rlang::abort("Resource ID cannot be empty", class = "ckan_input_error")
  }
  
  ckan_request("resource_show", params = list(id = id))
}

ckan_get_groups <- function() {
  result <- ckan_request("group_list", params = list(all_fields = "true"))
  if (is.list(result) && length(result) == 1 && is.list(result[[1]])) {
    result <- result[[1]]
  }
  result
}

ckan_get_current_packages_with_resources <- function(limit = 50, offset = 0) {
  params <- list(limit = limit)
  if (offset > 0) {
    params$offset <- offset
  }
  
  result <- ckan_request("current_package_list_with_resources", params = params)
  if (is.list(result) && length(result) == 1 && is.list(result[[1]])) {
    result <- result[[1]]
  }
  result
}

ckan_check_availability <- function() {
  tryCatch({
    result <- ckan_request("site_read")
    TRUE
  }, error = function(e) {
    FALSE
  })
}
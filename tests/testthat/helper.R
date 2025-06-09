skip_if_offline <- function() {
  testthat::skip_if_not(curl::has_internet(), "No internet connection")
}

skip_if_no_ckan <- function() {
  skip_if_offline()

  tryCatch(
    {
      resp <- httr2::request("https://www.datosabiertos.gob.pe/api/3/action/site_read") |>
        httr2::req_timeout(10) |>
        httr2::req_perform()
      testthat::skip_if_not(httr2::resp_status(resp) == 200, "CKAN portal not available")
    },
    error = function(e) {
      testthat::skip("CKAN portal not available")
    }
  )
}

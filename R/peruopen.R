#' peruopen: Access Peru's Open Data Portal (CKAN)
#'
#' This package provides a comprehensive interface to Peru's CKAN data portal 
#' (datosabiertos.gob.pe), enabling users to query and search datasets, 
#' extract metadata, and download resources directly into R.
#'
#' @section Main functions:
#' \describe{
#'   \item{\code{\link{po_catalog}}}{Get complete data catalog}
#'   \item{\code{\link{po_search}}}{Search datasets and resources}
#'   \item{\code{\link{po_get}}}{Smart getter for datasets/resources}
#'   \item{\code{\link{po_explore}}}{Interactive data exploration}
#' }
#'
#' @keywords internal
#' @importFrom cli cli_h1 cli_h2 cli_text cli_rule col_blue col_cyan col_green col_magenta col_yellow style_bold symbol
#' @importFrom dplyr bind_rows filter select mutate arrange desc slice_head
#' @importFrom tibble tibble as_tibble
#' @importFrom readr read_csv cols col_character
#' @importFrom readxl read_excel
#' @importFrom httr2 request req_url_path_append req_url_query req_perform resp_body_json req_timeout req_retry req_user_agent
#' @importFrom jsonlite fromJSON
#' @importFrom rlang .data
#' @importFrom fs path_home
#' @importFrom memoise memoise
#' @importFrom rappdirs user_cache_dir
#' @importFrom utils head tail read.csv
#' @importFrom graphics title
"_PACKAGE"

# Global variable definitions to avoid R CMD check NOTEs
utils::globalVariables(c("name", "title", "notes", "dataset_id", "organization", "format"))
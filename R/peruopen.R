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
#' @docType package
#' @name peruopen
NULL

# Removed po_datasets alias to avoid loading order issues
# Users should use po_catalog() directly
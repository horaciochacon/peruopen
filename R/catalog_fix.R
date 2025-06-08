# TEMPORARY WORKAROUND for catalog extension caching issue

#' Clear cache and rebuild complete catalog with target size
#'
#' @description
#' Workaround for extension caching issue. Clears cache and rebuilds
#' catalog from scratch with target size.
#'
#' @param target_size Number of datasets to load
#' @param verbose Show progress
# Internal utility function
po_catalog_rebuild <- function(target_size = 3000, verbose = TRUE) {
  
  if (verbose) {
    cat("REBUILDING CATALOG FROM SCRATCH\n")
    cat("Target size:", target_size, "datasets\n")
    cat("This avoids extension caching issues\n")
    cat("=====================================\n\n")
  }
  
  # Clear cache to start fresh
  clear_cache()
  
  # Build catalog with target size
  catalog <- po_catalog(target_size = target_size, verbose = verbose)
  
  if (verbose) {
    cat("\n✅ REBUILD COMPLETE\n")
    cat("Final size:", catalog$summary$n_datasets, "datasets\n")
    cat("Coverage:", round(100 * catalog$summary$n_datasets / 3954), "%\n")
  }
  
  return(catalog)
}
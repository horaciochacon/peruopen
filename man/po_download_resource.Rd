% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/download.R
\name{po_download_resource}
\alias{po_download_resource}
\title{Download a resource file}
\usage{
po_download_resource(
  resource_id,
  path = NULL,
  overwrite = FALSE,
  use_cache = TRUE
)
}
\arguments{
\item{resource_id}{Character. The resource ID}

\item{path}{Character. Local file path (optional, uses temp directory if NULL)}

\item{overwrite}{Logical. Whether to overwrite existing files (default FALSE)}

\item{use_cache}{Logical. Use cached files if available (default TRUE)}
}
\value{
Character path to downloaded file
}
\description{
Download a resource file
}
\keyword{internal}

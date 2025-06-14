% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/download.R
\name{po_load_resource}
\alias{po_load_resource}
\title{Load a resource directly into R}
\usage{
po_load_resource(
  resource_id,
  format = NULL,
  clean_names = TRUE,
  encoding = "UTF-8",
  path = NULL,
  use_cache = TRUE,
  ...
)
}
\arguments{
\item{resource_id}{Character. The resource ID}

\item{format}{Character. Resource format (auto-detected if NULL)}

\item{clean_names}{Logical. Clean column names for R compatibility (default TRUE)}

\item{encoding}{Character. Encoding to use: "UTF-8", "Latin1", "Windows-1252", "ISO-8859-1", or "auto" (default "UTF-8")}

\item{path}{Character. Custom save path for the downloaded file (optional)}

\item{use_cache}{Logical. Use cached files if available (default TRUE)}

\item{...}{Additional arguments passed to reading functions}
}
\value{
Data frame or file path for unsupported formats
}
\description{
Load a resource directly into R
}
\keyword{internal}

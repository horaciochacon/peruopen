# peruopen 0.0.0.9000

## New Features

* **Complete data catalog access**: `po_catalog()` provides access to 3,954+ datasets and 10,000+ resources from Peru's open data portal
* **Smart search functionality**: `po_search()` enables flexible searching across datasets with support for:
  - Full-text search across titles, descriptions, and metadata
  - Filtering by organizations, formats, and tags
  - Tag-only search mode
* **Interactive data exploration**: `po_explore()` offers structured views of the catalog organized by:
  - Publishing organizations
  - File formats
  - Publication years
  - File sizes
  - Recent updates
* **Intelligent data downloads**: `po_get()` provides smart data access with:
  - Automatic best format selection
  - Support for multiple resources
  - Local file saving with original names
  - Encoding detection for Spanish characters
* **Beautiful console output**: Enhanced print methods with color-coded information for easy data browsing
* **Robust caching system**: Automatic caching of catalog data and resources for improved performance
* **Comprehensive metadata access**: Full access to dataset and resource metadata including:
  - Organization information
  - Creation and modification dates
  - File formats and sizes
  - Descriptions and tags

## API Coverage

* **Core CKAN endpoints**: Support for `package_list`, `package_show`, `resource_show`, `group_list`, and `current_package_list_with_resources`
* **Pagination support**: Efficient handling of large datasets through intelligent chunked loading
* **Error handling**: Robust retry logic and graceful degradation for API reliability

## Data Processing

* **Multiple format support**: CSV, Excel (XLS/XLSX), JSON, PDF, and ZIP files
* **Smart encoding detection**: Automatic handling of Spanish characters with support for UTF-8, Latin1, Windows-1252, and ISO-8859-1 encodings
* **Clean data output**: Optional column name cleaning for R compatibility

## Performance Features

* **Intelligent caching**: 6-hour cache TTL for catalog data with cache management functions
* **Efficient API usage**: Optimized API calls with appropriate delays and batch sizing
* **Memory management**: Streamlined data structures for large datasets

This is the initial release providing comprehensive access to Peru's open data ecosystem.
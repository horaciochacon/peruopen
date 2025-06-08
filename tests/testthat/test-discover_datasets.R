test_that("po_list_datasets returns tibble with name column", {
  skip_if_no_ckan()
  
  datasets <- po_list_datasets()
  expect_s3_class(datasets, "tbl_df")
  expect_true("name" %in% names(datasets))
  expect_gt(nrow(datasets), 0)
})

test_that("po_get_dataset_count returns positive integer", {
  skip_if_no_ckan()
  
  count <- po_get_dataset_count()
  expect_type(count, "integer")
  expect_gt(count, 0)
})

test_that("po_search_datasets returns properly structured tibble", {
  skip_if_no_ckan()
  
  result <- po_search_datasets(limit = 5)
  expect_s3_class(result, "tbl_df")
  
  expected_cols <- c("name", "title", "notes", "tags", "organization")
  expect_true(all(expected_cols %in% names(result)))
})

test_that("po_search_datasets respects limit parameter", {
  skip_if_no_ckan()
  
  result <- po_search_datasets(limit = 3)
  expect_lte(nrow(result), 3)
})
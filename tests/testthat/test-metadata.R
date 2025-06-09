test_that("po_get_dataset_metadata requires valid dataset ID", {
  expect_snapshot(po_get_dataset_metadata(""), error = TRUE)
  expect_snapshot(po_get_dataset_metadata(NULL), error = TRUE)
})

test_that("po_get_resource_metadata requires valid resource ID", {
  expect_snapshot(po_get_resource_metadata(""), error = TRUE)
  expect_snapshot(po_get_resource_metadata(NULL), error = TRUE)
})

test_that("po_list_resources works with valid dataset", {
  skip_if_no_ckan()

  datasets <- po_list_datasets()
  if (nrow(datasets) > 0) {
    first_dataset <- datasets$name[1]

    expect_no_error({
      resources <- po_list_resources(first_dataset)
    })

    expect_s3_class(resources, "tbl_df")
  }
})

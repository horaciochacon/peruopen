test_that("validate_dataset_id works correctly", {
  expect_snapshot(validate_dataset_id(""), error = TRUE)
  expect_snapshot(validate_dataset_id(NULL), error = TRUE)
  expect_snapshot(validate_dataset_id(c("a", "b")), error = TRUE)
  
  expect_true(validate_dataset_id("valid-id"))
})

test_that("validate_resource_id works correctly", {
  expect_snapshot(validate_resource_id(""), error = TRUE)
  expect_snapshot(validate_resource_id(NULL), error = TRUE)
  expect_snapshot(validate_resource_id(c("a", "b")), error = TRUE)
  
  expect_true(validate_resource_id("valid-id"))
})

test_that("format_file_size works correctly", {
  expect_equal(format_file_size(500), "500 B")
  expect_equal(format_file_size(1500), "1.5 KB")
  expect_equal(format_file_size(1500000), "1.4 MB")
  expect_equal(format_file_size(1500000000), "1.4 GB")
  expect_true(is.na(format_file_size(NA)))
})

test_that("filter_resources_by_format works", {
  sample_resources <- tibble::tibble(
    name = c("file1", "file2", "file3"),
    format = c("CSV", "xlsx", "json")
  )
  
  csv_filtered <- filter_resources_by_format(sample_resources, "csv")
  expect_equal(nrow(csv_filtered), 1)
  expect_equal(csv_filtered$name, "file1")
  
  multi_filtered <- filter_resources_by_format(sample_resources, c("csv", "json"))
  expect_equal(nrow(multi_filtered), 2)
})
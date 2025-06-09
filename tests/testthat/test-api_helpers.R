test_that("ckan_request handles basic requests", {
  skip_if_no_ckan()

  expect_no_error({
    result <- ckan_request("site_read")
  })
})

test_that("ckan_get_packages returns character vector", {
  skip_if_no_ckan()

  packages <- ckan_get_packages()
  expect_type(packages, "character")
  expect_gt(length(packages), 0)
})

test_that("ckan_get_package requires valid ID", {
  expect_snapshot(ckan_get_package(""), error = TRUE)
  expect_snapshot(ckan_get_package(NULL), error = TRUE)
})

test_that("ckan_get_resource requires valid ID", {
  expect_snapshot(ckan_get_resource(""), error = TRUE)
  expect_snapshot(ckan_get_resource(NULL), error = TRUE)
})

test_that("ckan_check_availability returns logical", {
  result <- ckan_check_availability()
  expect_type(result, "logical")
  expect_length(result, 1)
})

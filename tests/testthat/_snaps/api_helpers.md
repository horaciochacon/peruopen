# ckan_get_package requires valid ID

    Code
      ckan_get_package("")
    Condition
      Error in `ckan_get_package()`:
      ! Package ID cannot be empty

---

    Code
      ckan_get_package(NULL)
    Condition
      Error in `ckan_get_package()`:
      ! Package ID cannot be empty

# ckan_get_resource requires valid ID

    Code
      ckan_get_resource("")
    Condition
      Error in `ckan_get_resource()`:
      ! Resource ID cannot be empty

---

    Code
      ckan_get_resource(NULL)
    Condition
      Error in `ckan_get_resource()`:
      ! Resource ID cannot be empty


library(here)
library(purrr)
library(stringr)

# where zipped files are stored
in_dir <- here("clean-and-process", "input")
zip_files <- list.files(in_dir)
zip_in_paths <- file.path(in_dir, zip_files)

# where to write zipped files
zip_out_paths <- file.path(here("clean-and-process", "output", "rds"),
                           str_replace(zip_files, ".zip", ""))

# unzip all the files
pwalk(list(zip_in_paths, zip_out_paths),
      function(x, y) {unzip(zipfile = x, exdir = y)})

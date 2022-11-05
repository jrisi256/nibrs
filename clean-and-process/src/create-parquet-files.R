library(here)
library(arrow)
library(purrr)
library(stringr)

# Take in command line arguments
args <- commandArgs(trailingOnly = T)

if(length(args) != 1)
    {stop("Only one arg can be supplied: the path to the rds files.")}

# Create output directory for parquet files
out_dir <- here("clean-and-process",
                "output",
                "parquet",
                str_replace(args[1], "rds", "parquet"))
if(!dir.exists(out_dir)) {dir.create(out_dir)}

# Directory of the data
in_dir <- here("clean-and-process", "output", "rds", args[1])

# Set the name of each file to be its corresponding year
files <- list.files(here(in_dir))
files <- files[str_detect(files, "segment|header")]

# Change RDS to Parquet so we can use Arrow to analyze larger than RAM data
Convert_To_Parquet <- function(in_path, file_name, out_path) {
    
    # Read in RDS file
    df <- readRDS(file.path(in_path, file_name))
    
    # Write out as a parquet file
    new_file_name <- str_replace(file_name, ".rds", "")
    write_parquet(df, file.path(out_path, new_file_name))
    gc()
}

walk(files, Convert_To_Parquet, in_path = in_dir, out_path = out_dir)

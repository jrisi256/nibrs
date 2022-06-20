library(here)
library(arrow)
library(purrr)
library(stringr)

# Create output directory for parquet files
out_dir <- str_c("clean-and-process", "output", "parquet", sep = "/")
if(!dir.exists(here(out_dir))) {dir.create(here(out_dir))}

# Directory of the data
in_dir <- str_c("clean-and-process",
                "input",
                "rds",
                "nibrs_1991_2020_victim_segment_rds",
                sep = "/")

# Set the name of each file to be its corresponding year
victim_files <- list.files(here(in_dir))
year_names <- paste0("victim_", str_match(victim_files, "[12][09][0-9]{2}"))
names(victim_files) <- year_names

# Change RDS to Parquet so we can use Arrow to analyze larger than RAM data
Convert_To_Parquet <- function(in_path, file_name, out_path) {
    
    # Read in RDS file
    df <- readRDS(here(in_path, file_name))
    
    # Write out as a parquet file
    new_file_name <- str_replace(file_name, ".rds", "")
    write_parquet(df, here(out_path, new_file_name))
    gc()
}

walk(victim_files, Convert_To_Parquet, in_path = in_dir, out_path = out_dir)

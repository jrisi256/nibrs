################################################ Creating Parquet files

CLEAN = clean-and-process/
CLEAN_OUTPUT = $(CLEAN)output/parquet/
CLEAN_SRC = $(CLEAN)src/

$(CLEAN_OUTPUT)nibrs_victim_segment_%: $(CLEAN_SRC)Create_Parquet_Files.R
	Rscript $(CLEAN_SRC)Create_Parquet_Files.R
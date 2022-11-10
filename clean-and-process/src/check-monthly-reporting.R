library(here)
library(dplyr)
library(tidyr)
library(purrr)
library(arrow)
library(ggplot2)
library(lubridate)

# Create output directory for parquet files
out_dir <- file.path(here("clean-and-process", "output", "parquet"))

# Create Schema for NIBRS victim data
schema <-
    schema(ori = string(),
           year = double(),
           state = string(),
           state_abb = string(),
           incident_number = string(),
           incident_date = string(),
           victim_sequence_number = double(),
           ucr_offense_code_1 = string(),
           ucr_offense_code_2 = string(),
           ucr_offense_code_3 = string(),
           ucr_offense_code_4 = string(),
           ucr_offense_code_5 = string(),
           ucr_offense_code_6 = string(),
           ucr_offense_code_7 = string(),
           ucr_offense_code_8 = string(),
           ucr_offense_code_9 = string(),
           ucr_offense_code_10 = string(),
           type_of_victim = string(),
           age_of_victim = string(),
           sex_of_victim = string(),
           race_of_victim = string(),
           ethnicity_of_victim = string(),
           resident_status_of_victim = string(),
           agg_assault_homicide_circumsta1 = string(),
           agg_assault_homicide_circumsta2 = string(),
           addit_just_homicide_circumsta = string(),
           type_of_injury_1 = string(),
           type_of_injury_2 = string(),
           type_of_injury_3 = string(),
           type_of_injury_4 = string(),
           type_of_injury_5 = string(),
           offender_number_to_be_related1 = double(),
           offender_number_to_be_related2 = double(),
           offender_number_to_be_related3 = double(),
           offender_number_to_be_related4 = double(),
           offender_number_to_be_related5 = double(),
           offender_number_to_be_related6 = double(),
           offender_number_to_be_related7 = double(),
           offender_number_to_be_related8 = double(),
           offender_number_to_be_related9 = double(),
           offender_number_to_be_related10 = double(),
           relation_of_vict_to_offender1 = string(),
           relation_of_vict_to_offender2 = string(),
           relation_of_vict_to_offender3 = string(),
           relation_of_vict_to_offender4 = string(),
           relation_of_vict_to_offender5 = string(),
           relation_of_vict_to_offender6 = string(),
           relation_of_vict_to_offender7 = string(),
           relation_of_vict_to_offender8 = string(),
           relation_of_vict_to_offender9 = string(),
           relation_of_vict_to_offender10 = string(),
           unique_incident_id = string())

# Read in data as a series of parquet files
nibrs <- open_dataset(out_dir, schema = schema)

# Combine sexual and non sexual crimes
monthly_reporting_agencies <-
    nibrs %>%
    mutate(incident_date = strptime(incident_date, "%Y-%m-%d"),
           year = arrow_year(incident_date),
           month = arrow_month(incident_date)) %>%
    distinct(ori, year, month) %>%
    collect()

Find_Agencies_Reporting <- function(agencies_df, start_year, end_year) {
    
    unique_agencies <-
        agencies_df %>%
        filter(year == start_year) %>%
        summarise(w = unique(ori)) %>%
        pull(w)
    
    count <-
        agencies_df %>%
        filter(ori %in% unique_agencies & year >= start_year & year <= end_year) %>%
        group_by(ori) %>%
        summarise(months_reported = n()) %>%
        ungroup()
    
    total_months <- (end_year - start_year + 1) * 12
    nr_complete <- count %>% filter(months_reported == total_months) %>% nrow()
    
    title <- paste0("PDF and CDF for the number of months each police agency reported to NIBRS\n",
                    "Total Number of Police Agencies: ", length(unique_agencies), "\n",
                    "Total Number of Months: ", total_months, "\n",
                    "Total Number of Police Agencies Reporting All Months: ", nr_complete, "\n",
                    "Total Number of Complete Observations: ", total_months * nr_complete, "\n",
                    "Years: ", start_year, " - ", end_year)
    
    g <-
        ggplot(count, aes(x = months_reported)) +
        stat_ecdf(geom = "line") +
        theme_bw() +
        geom_histogram(aes(y = ..count.. / sum(..count..)), alpha = 0.5) +
        scale_x_continuous(breaks = ~ pretty(.x)) + 
        labs(x = "Number of Months Reported",
             y = "Proportion of Agencies Reporting",
             title = title)
    
    return(list(df = count, graph = g))
}

start_years <- 1991:2021
names(start_years) <- paste0("year_", start_years)

months_reporting <- map(start_years,
                        Find_Agencies_Reporting,
                        agencies_df = monthly_reporting_agencies,
                        end_year = 2021)

pdf(here("clean-and-process", "reports", "monthly_reporting_ori.pdf"), onefile = T)
map(months_reporting, 2)
dev.off()

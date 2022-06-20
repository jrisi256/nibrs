library(here)
library(dplyr)
library(tidyr)
library(arrow)
library(stringr)
library(lubridate)

# Create output directory for parquet files
out_dir <- str_c("clean-and-process", "output", "parquet", sep = "/")

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
nibrs <- open_dataset(here(out_dir), schema = schema)

# Crimes defined as sexual
sex_crimes <- c("rape", "sodomy", "incest", "statutory rape",
                "sexual assault with an object",
                "human trafficking - commercial sex acts",
                "fondling (incident liberties/child molest)")

# Read in sexual crimes and aggregate to the incident date level
sexual_crimes <-
    nibrs %>%
    filter(ucr_offense_code_1 %in% sex_crimes |
               ucr_offense_code_2 %in% sex_crimes |
               ucr_offense_code_3 %in% sex_crimes |
               ucr_offense_code_4 %in% sex_crimes |
               ucr_offense_code_5 %in% sex_crimes |
               ucr_offense_code_6 %in% sex_crimes |
               ucr_offense_code_7 %in% sex_crimes |
               ucr_offense_code_8 %in% sex_crimes |
               ucr_offense_code_9 %in% sex_crimes |
               ucr_offense_code_10 %in% sex_crimes) %>%
    group_by(incident_date, state, ori) %>%
    summarise(count = n()) %>%
    collect() %>%
    ungroup()

# Read in non-sexual crimes and aggregate to the incident date level
non_sexual_crimes <-
    nibrs %>%
    filter(!(ucr_offense_code_1 %in% sex_crimes) &
               !(ucr_offense_code_2 %in% sex_crimes) &
               !(ucr_offense_code_3 %in% sex_crimes) &
               !(ucr_offense_code_4 %in% sex_crimes) &
               !(ucr_offense_code_5 %in% sex_crimes) &
               !(ucr_offense_code_6 %in% sex_crimes) &
               !(ucr_offense_code_7 %in% sex_crimes) &
               !(ucr_offense_code_8 %in% sex_crimes) &
               !(ucr_offense_code_9 %in% sex_crimes) &
               !(ucr_offense_code_10 %in% sex_crimes)) %>%
    group_by(incident_date, state, ori) %>%
    summarise(count = n()) %>%
    collect() %>%
    ungroup()

sexual_crimes_mem <-
    sexual_crimes %>%
    mutate(incident_date = ymd(incident_date),
           year = year(incident_date),
           month = month(incident_date),
           day = day(incident_date),
           round_month = floor_date(incident_date, "month")) %>%
    group_by(state, ori, round_month) %>%
    summarise(count = sum(count),
              nr_agencies = length(unique(ori))) %>%
    ungroup()

non_sexual_crimes_mem <-
    non_sexual_crimes %>%
    mutate(incident_date = ymd(incident_date),
           year = year(incident_date),
           month = month(incident_date),
           day = day(incident_date),
           round_month = floor_date(incident_date, "month")) %>%
    group_by(state, ori, round_month) %>%
    summarise(count = sum(count),
              nr_agencies = length(unique(ori))) %>%
    ungroup()

all_dates <- seq(ymd("1991-01-01"), ymd("2020-12-31"), by = "month")
all_agencies <- unique(c(unique(sexual_crimes_mem$ori),
                         unique(non_sexual_crimes_mem$ori)))
cross_dates_agencies <- crossing(all_dates, all_agencies)

sexual_crimes_mem <- full_join(sexual_crimes_mem, cross_dates_agencies,
                               by = c("ori" = "all_agencies", 
                                      "round_month" = "all_dates"))

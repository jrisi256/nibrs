library(here)
library(arrow)
library(dplyr)
library(tidyr)
library(ggplot2)

# Directory for parquet files
arrestees_dir <- here("clean-and-process",
                      "output",
                      "parquet",
                      "nibrs_1991_2021_arrestee_segment_parquet")

victims_dir <- here("clean-and-process",
                    "output",
                    "parquet",
                    "nibrs_1991_2021_victim_segment_parquet")
  
######################################################## Schema for victims
victim_schema <- schema(ori = string(),
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

################################################### Schema for those arrestees
arrestee_schema <- schema(ori = string(),
                          year = string(),
                          state = string(),
                          state_abb = string(),
                          incident_number = string(),
                          incident_date = string(),
                          arrestee_sequence_number = double(),
                          arrest_transaction_number = string(),
                          arrest_date = string(),
                          type_of_arrest = string(),
                          multiple_arrestee_indicator = string(),
                          ucr_arrest_offense_code = string(),
                          arrestee_weapon_1 = string(),
                          automatic_weapon_indicator_1 = string(),
                          arrestee_weapon_2 = string(),
                          automatic_weapon_indicator_2 = string(),
                          age_of_arrestee = string(),
                          sex_of_arrestee = string(),
                          race_of_arrestee = string(),
                          ethnicity_of_arrestee = string(),
                          resident_status_of_arrestee = string(),
                          disposition_of_arrestee_under18 = string(),
                          unique_incident_id = string())
  
arrestees <- open_dataset(here(arrestees_dir), schema = arrestee_schema)
victims <- open_dataset(here(victims_dir), schema = victim_schema)

arrestees_df <-
    arrestees %>%
    select(ori, year, state, incident_number, incident_date, arrest_date,
           ucr_arrest_offense_code, age_of_arrestee, sex_of_arrestee,
           race_of_arrestee, ethnicity_of_arrestee, resident_status_of_arrestee,
           unique_incident_id) %>%
    count(resident_status_of_arrestee) %>%
    collect() %>%
    rename(arrestee = resident_status_of_arrestee) %>%
    mutate(arrestee = case_when(!(arrestee %in% c("resident", "nonresident")) ~ "unknown",
                                is.na(arrestee) ~ "unknown",
                                T ~ arrestee)) %>%
    count(arrestee, wt = n) %>%
    mutate(prcnt = n / sum(n)) %>%
    pivot_longer(cols = arrestee, names_to = "actor", values_to = "status")
    
victims_df <-
    victims %>%
    filter(type_of_victim %in% c("individual", "law enforcement officer")) %>%
    count(resident_status_of_victim) %>%
    collect() %>%
    rename(victim = resident_status_of_victim) %>%
    mutate(victim = if_else(is.na(victim), "unknown", victim)) %>%
    count(victim, wt = n) %>%
    mutate(prcnt = n / sum(n)) %>%
    pivot_longer(cols = victim, names_to = "actor", values_to = "status")

resident_status <- bind_rows(arrestees_df, victims_df)

ggplot(resident_status, aes(x = status, y = prcnt)) +
    geom_bar(stat = "identity") +
    facet_wrap(~actor) +
    theme_bw()

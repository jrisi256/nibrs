library(here)
library(arrow)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(ggplot2)
library(forcats)

print('test')

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
                        year = string(),
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
# ori, year, state, incident_number, incident_date, arrest_date,
# ucr_arrest_offense_code, age_of_arrestee, sex_of_arrestee, race_of_arrestee,
# ethnicity_of_arrestee, resident_status_of_arrestee, unique_incident_id
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
  
############################################################# Clean arrest data
arrestees <-
    open_dataset(here(arrestees_dir), schema = arrestee_schema) %>%
    mutate(resident_status_of_arrestee = case_when(!(resident_status_of_arrestee %in% c("resident", "nonresident")) ~ "unknown",
                                                   is.na(resident_status_of_arrestee) ~ "unknown",
                                                   T ~ resident_status_of_arrestee),
           race_of_arrestee = if_else(race_of_arrestee == "F",
                                      "unknown",
                                      race_of_arrestee),
           ethnicity_of_arrestee = case_when(ethnicity_of_arrestee == "hispanic origin" ~ "hispanic",
                                             ethnicity_of_arrestee == "not of hispanic origin" ~ "not hispanic",
                                             is.na(ethnicity_of_arrestee) ~ "unknown",
                                             T ~ "unknown"),
           sex_of_arrestee = if_else(!(sex_of_arrestee %in% c("female", "male")),
                                     "unknown",
                                     sex_of_arrestee),
           age_of_arrestee = case_when(str_sub(age_of_arrestee, 1, 1) == "0" ~ str_replace(age_of_arrestee, "0", ""),
                                       age_of_arrestee == "over 98 years old" ~ "99",
                                       age_of_arrestee == "unknown" ~ "-1",
                                       T ~ age_of_arrestee),
           age_of_arrestee = as.numeric(age_of_arrestee),
           age_cat_a = case_when(age_of_arrestee >= 0 & age_of_arrestee <= 12 ~ "0 - 12",
                                 age_of_arrestee >= 13 & age_of_arrestee <= 17 ~ "13 - 17",
                                 age_of_arrestee >= 18 & age_of_arrestee <= 22 ~ "18 - 22",
                                 age_of_arrestee >= 23 & age_of_arrestee <= 26 ~ "23 - 26",
                                 age_of_arrestee >= 27 & age_of_arrestee <= 32 ~ "27 - 32",
                                 age_of_arrestee >= 33 & age_of_arrestee <= 43 ~ "33 - 43",
                                 age_of_arrestee >= 44 & age_of_arrestee <= 54 ~ "44 - 54",
                                 age_of_arrestee >= 55 & age_of_arrestee <= 65 ~ "55 - 65",
                                 age_of_arrestee >= 66 ~ "66 and older",
                                 age_of_arrestee == -1 ~ "unknown"))
       
########################################################### Clean victim data
# only keep individuals and officers, only ones with residential status.
victims <-
    open_dataset(here(victims_dir), schema = victim_schema) %>%
    filter(type_of_victim %in% c("individual", "law enforcement officer")) %>%
    mutate(resident_status_of_victim = if_else(is.na(resident_status_of_victim),
                                               "unknown",
                                               resident_status_of_victim),
           ethnicity_of_victim = case_when(ethnicity_of_victim == "hispanic origin" ~ "hispanic",
                                           ethnicity_of_victim == "not of hispanic origin" ~ "not hispanic",
                                           is.na(ethnicity_of_victim) ~ "unknown",
                                           T ~ "unknown"),
           age_of_victim = case_when(str_sub(age_of_victim, 1, 1) == "0" ~ str_replace(age_of_victim, "0", ""),
                                     age_of_victim %in% c("1-6 days old", "7-364 days old", "under 24 hours (neonate)") ~ "0",
                                     age_of_victim == "over 98 years old" ~ "99",
                                     age_of_victim == "unknown" ~ "-1",
                                     T ~ age_of_victim),
           age_of_victim = as.numeric(age_of_victim),
           age_cat_v = case_when(age_of_victim >= 0 & age_of_victim <= 12 ~ "0 - 12",
                                 age_of_victim >= 13 & age_of_victim <= 17 ~ "13 - 17",
                                 age_of_victim >= 18 & age_of_victim <= 22 ~ "18 - 22",
                                 age_of_victim >= 23 & age_of_victim <= 26 ~ "23 - 26",
                                 age_of_victim >= 27 & age_of_victim <= 32 ~ "27 - 32",
                                 age_of_victim >= 33 & age_of_victim <= 43 ~ "33 - 43",
                                 age_of_victim >= 44 & age_of_victim <= 54 ~ "44 - 54",
                                 age_of_victim >= 55 & age_of_victim <= 65 ~ "55 - 65",
                                 age_of_victim >= 66 ~ "66 and older",
                                 age_of_victim == -1 ~ "unknown"))

Query_Data_Arrestee <- function(.arrow_obj, ...) {
    
    .arrow_obj %>%
        count(resident_status_of_arrestee, ...) %>%
        collect() %>%
        group_by(...) %>%
        mutate(prcnt = n / sum(n)) %>%
        ungroup() %>%
        pivot_longer(cols = resident_status_of_arrestee,
                     names_to = "actor",
                     values_to = "status") %>%
        mutate(actor = if_else(actor == "resident_status_of_arrestee",
                               "arrestee",
                               NA_character_))
}

Query_Data_Victim <- function(.arrow_obj, ...) {
    
    .arrow_obj %>%
        count(resident_status_of_victim, ...) %>%
        collect() %>%
        group_by(...) %>%
        mutate(prcnt = n / sum(n)) %>%
        ungroup() %>%
        pivot_longer(cols = resident_status_of_victim,
                     names_to = "actor",
                     values_to = "status") %>%
        mutate(actor = if_else(actor == "resident_status_of_victim",
                               "victim",
                               NA_character_))
}

arrestees_df <- Query_Data_Arrestee(arrestees)
arrestees_time_df <- Query_Data_Arrestee(arrestees, year)
arrestees_race_df <- Query_Data_Arrestee(arrestees, race_of_arrestee, year)
arrestees_ethnicity_df <- Query_Data_Arrestee(arrestees, ethnicity_of_arrestee, year)
arrestees_sex_df <- Query_Data_Arrestee(arrestees, sex_of_arrestee, year)
arrestees_age_df <- Query_Data_Arrestee(arrestees, age_cat_a, year)
arrestees_state_time_df <- Query_Data_Arrestee(arrestees, state, year)
arrestees_crime_df <-
    arrestees %>%
    filter(ucr_arrest_offense_code %in% c("drug/narcotic violations",
                                          "simple assault",
                                          "shoplifting",
                                          "aggravated assault",
                                          "all other larceny",
                                          "destruction/damage/vandalism of property",
                                          "burglary/breaking and entering",
                                          "motor vehicle theft",
                                          "robbery",
                                          "disorderly conduct",
                                          "theft from building",
                                          "theft from motor vehicle",
                                          "driving under the influence",
                                          "rape",
                                          "murder/nonnegligent manslaughter")) %>%
    Query_Data_Arrestee(ucr_arrest_offense_code) %>%
    filter(status == "resident") %>%
    mutate(ucr_arrest_offense_code = fct_reorder(ucr_arrest_offense_code, prcnt))

victims_df <- Query_Data_Victim(victims)
victims_time_df <- Query_Data_Victim(victims, year)
victims_race_df <- Query_Data_Victim(victims, race_of_victim, year)
victims_ethnicity_df <- Query_Data_Victim(victims, ethnicity_of_victim, year)
victims_sex_df <- Query_Data_Victim(victims, sex_of_victim, year)
victims_age_df <- Query_Data_Victim(victims, age_cat_v, year)
victims_state_df <- Query_Data_Victim(victims, state, year)
victims_crime_df <-
    victims %>%
    filter(ucr_offense_code_1 %in% c("drug/narcotic violations",
                                     "simple assault",
                                     "shoplifting",
                                     "aggravated assault",
                                     "all other larceny",
                                     "destruction/damage/vandalism of property",
                                     "burglary/breaking and entering",
                                     "motor vehicle theft",
                                     "robbery",
                                     "disorderly conduct",
                                     "theft from building",
                                     "theft from motor vehicle",
                                     "driving under the influence",
                                     "rape",
                                     "murder/nonnegligent manslaughter")) %>%
    Query_Data_Victim(ucr_offense_code_1) %>%
    filter(status == "resident") %>%
    mutate(ucr_offense_code_1 = fct_reorder(ucr_offense_code_1, prcnt))
ggplot(victims_crime_df, aes(x = ucr_offense_code_1, y = prcnt)) + geom_point(aes(color = status)) +
    theme(axis.text.x = element_text(hjust = 0, angle = 340))

##################################### arrested vs. victim residential status
################ inner join captures incidents where the offender was arrested
Join_Victim_Arrestee <- function(victims_arrow, arrestees_arrow, year_i) {
    
    year_i <- as.character(year_i)
    print(year_i)
    
    v <-
        victims %>%
        select(resident_status_of_victim, unique_incident_id, year) %>%
        filter(year == year_i)
    
    a <-
        arrestees %>%
        select(resident_status_of_arrestee, unique_incident_id, year) %>%
        filter(year == year_i)
    
    join <-
        inner_join(v, a, by = c("unique_incident_id", "year")) %>%
        count(resident_status_of_victim, resident_status_of_arrestee, year) %>%
        collect()
    
    gc()
    return(join)
}

join <-
    map_dfr(1991:2021,
            Join_Victim_Arrestee,
            victims_arrow = victims,
            arrestees_arrow = arrestees) %>%
    group_by(year) %>%
    mutate(prcnt = n / sum(n)) %>%
    ungroup()

library(here)
library(dplyr)
library(tidyr)
library(arrow)
library(purrr)
library(stringr)
library(ggplot2)
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

# Read in sexual crime victims and aggregate to the incident date level
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
    mutate(incident_date = strptime(incident_date, "%Y-%m-%d"),
           year = arrow_year(incident_date),
           month = arrow_month(incident_date)) %>%
    group_by(year, month, state, ori) %>%
    summarise(monthly_reported_crime = n()) %>%
    ungroup() %>%
    mutate(crime_type = "sexual") %>%
    collect()

# Read in non-sexual crime victims and aggregate to the incident date level
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
    mutate(incident_date = strptime(incident_date, "%Y-%m-%d"),
           year = arrow_year(incident_date),
           month = arrow_month(incident_date)) %>%
    group_by(year, month, state, ori) %>%
    summarise(monthly_reported_crime = n()) %>%
    ungroup() %>%
    mutate(crime_type = "non-sexual") %>%
    collect()

# Combine sexual and non sexual crimes
monthly_reporting_agencies <-
    nibrs %>%
    mutate(incident_date = strptime(incident_date, "%Y-%m-%d"),
           year = arrow_year(incident_date),
           month = arrow_month(incident_date)) %>%
    distinct(ori, year, month) %>%
    collect()

# 
# group_by(ori) %>%
#     summarise(months_reported = n(),
#               earliest_year = min(year)) %>%
#     ungroup() %>%
#     collect()

# Create_All_Dates <- function(start_year, end_year) {
#     
#     seq(ymd(paste0(start_year, "-01-01")),
#         ymd(paste0(end_year, "-12-31")),
#         by = "month")
# }

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

start_years <- 1991:2020
names(start_years) <- paste0("year_", start_years)

months_reporting <- map(start_years,
                        Find_Agencies_Reporting,
                        agencies_df = monthly_reporting_agencies,
                        end_year = 2020)

pdf(here("src", "plots.pdf"), onefile = T)
map(months_reporting, 2)
dev.off()

# Based on PDF, I am going to use years 2005 and 2014.
agencies_2005 <-
    months_reporting$year_2005$df %>%
    filter(months_reported == max(months_reported)) %>%
    pull(ori)

agencies_2014 <-
    months_reporting$year_2014$df %>%
    filter(months_reported == max(months_reported)) %>%
    pull(ori)

sexual_crimes_2005 <-
    sexual_crimes %>%
    filter(ori %in% agencies_2005) %>%
    mutate(year_month = ym(paste0(year, "-", month))) %>%
    filter(year >= 2005) %>%
    count(ori, state, year_month, crime_type, wt = monthly_reported_crime)

non_sexual_crimes_2005 <-
    non_sexual_crimes %>%
    filter(ori %in% agencies_2005) %>%
    mutate(year_month = ym(paste0(year, "-", month))) %>%
    filter(year >= 2005) %>%
    count(ori, state, year_month, crime_type, wt = monthly_reported_crime)

all_dates_2005 <-
    tibble(year_month = seq(ymd("2005-01-01"), ymd("2020-12-31"), by = "month"),
           temp = 1)

all_dates_2014 <-
    tibble(year_month = seq(ymd("2014-01-01"), ymd("2020-12-31"), by = "month"),
           temp = 1)

cross_join_2005 <-
    tibble(ori = agencies_2005, temp = 1, sexual = 1) %>%
    full_join(all_dates_2005, by = "temp") %>%
    dplyr::select(-temp)

cross_join_2014 <-
    tibble(ori = agencies_2014, temp = 1, sexual = 1) %>%
    full_join(all_dates_2014, by = "temp") %>%
    dplyr::select(-temp)

crimes_2005 <-
    bind_rows(sexual_crimes_2005, non_sexual_crimes_2005) %>%
    mutate(dummy = 1) %>%
    pivot_wider(names_from = crime_type,
                values_from = dummy,
                values_fill = list(dummy = 0)) %>%
    mutate(month = as.factor(month(year_month)),
           post_metoo = if_else(year_month >= "2017-10-01", 0, 1)) %>%
    full_join(cross_join_2005, by = c("ori", "year_month", "sexual")) %>%
    mutate(if_else(is.na(n), 0L, n))

# indexed_crimes_2005 <-
#     crimes_2005 %>%
#     filter(year_month == "2005-01-01") %>%
#     rename(index = n) %>%
#     select(-year_month) %>%
#     full_join(crimes_2005, by = c("state", "crime_type")) %>%
#     mutate(new_n = n / index)
# 
# ggplot(indexed_crimes_2005, aes(x = year_month, y = new_n)) +
#     geom_line(aes(group = crime_type, color = crime_type)) +
#     theme_bw() +
#     geom_vline(xintercept = as.numeric(ymd("2017-10-01"))) +
#     facet_wrap(~state, scales = "free")

##############################
sexual_crimes_2014 <-
    sexual_crimes %>%
    filter(ori %in% agencies_2014) %>%
    mutate(year_month = ym(paste0(year, "-", month))) %>%
    filter(year >= 2014) %>%
    count(ori, state, year_month, crime_type, wt = monthly_reported_crime)

non_sexual_crimes_2014 <-
    non_sexual_crimes %>%
    filter(ori %in% agencies_2014) %>%
    mutate(year_month = ym(paste0(year, "-", month))) %>%
    filter(year >= 2014) %>%
    count(ori, state, year_month, crime_type, wt = monthly_reported_crime)

crimes_2014 <-
    bind_rows(sexual_crimes_2014, non_sexual_crimes_2014) %>%
    mutate(dummy = 1) %>%
    pivot_wider(names_from = crime_type,
                values_from = dummy,
                values_fill = list(dummy = 0)) %>%
    mutate(month = as.factor(month(year_month)),
           post_metoo = if_else(year_month >= "2017-10-01", 0, 1)) %>%
    full_join(cross_join_2005, by = c("ori", "year_month", "sexual")) %>%
    mutate(if_else(is.na(n), 0L, n))

# indexed_crimes_2014 <-
#     crimes_2014 %>%
#     filter(year_month == "2014-01-01") %>%
#     rename(index = n) %>%
#     select(-year_month) %>%
#     full_join(crimes_2014, by = c("state", "crime_type")) %>%
#     mutate(dummy = 1) %>%
#     pivot_wider(names_from = crime_type,
#                 values_from = dummy,
#                 values_fill = list(dummy = 0)) %>%
#     mutate(new_n = n / index,
#            year = year(year_month),
#            month = month(year_month),
#            post_metoo = if_else(year_month >= "2017-10-01", 0, 1))
#     
# 
# ggplot(indexed_crimes_2014, aes(x = year_month, y = new_n)) +
#     geom_line(aes(group = crime_type, color = crime_type)) +
#     theme_bw() +
#     geom_vline(xintercept = as.numeric(ymd("2017-10-01"))) +
#     facet_wrap(~state, scales = "free")

ols_2005 <-
    lm(n ~ sexual + post_metoo + sexual * post_metoo + state + month,
       data = crimes_2005)

ols_2014 <-
    lm(n ~ sexual + post_metoo + sexual * post_metoo + state + month,
       data = crimes_2014)

library(MASS)
negbn_2005 <-
    glm.nb(n ~ sexual + post_metoo + sexual * post_metoo + state + month,
           data = crimes_2005)

negbn_2014 <-
    glm.nb(n ~ sexual + post_metoo + sexual * post_metoo + state + month,
           data = crimes_2014)

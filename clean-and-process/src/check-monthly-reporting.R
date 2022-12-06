library(here)
library(dplyr)
library(tidyr)
library(purrr)
library(arrow)
library(ggplot2)
library(stringr)
library(lubridate)
library(tidycensus)

# Read in parquet files
batch <- open_dataset(here("clean-and-process",
                           "output",
                           "parquet",
                           "nibrs_1991_2021_batch_header_parquet"))

administrative <- open_dataset(here("clean-and-process",
                               "output",
                               "parquet",
                               "nibrs_1991_2021_administrative_segment_parquet"))

# Get number of reported crimes each month in each police agency
monthly_crime_count <-
    administrative %>%
    mutate(incident_date = strptime(incident_date, "%Y-%m-%d"),
           year = arrow_year(incident_date),
           month = arrow_month(incident_date)) %>%
    count(ori, year, month) %>%
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
    
    title <- paste0("PDF and CDF for the # of months each police agency reported to NIBRS\n",
                    "Total # of Police Agencies Reporting At Least One Month: ", length(unique_agencies), "\n",
                    "Total # of Months: ", total_months, "\n",
                    "Total # of Police Agencies Reporting All Months: ", nr_complete, "\n",
                    "Total # of Complete Observations: ", total_months * nr_complete, "\n",
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

# Graph PDF and CDFs of the percentage of police agencies which report every
# month for a continuous set of years.
start_years <- 1991:2021
names(start_years) <- paste0("year_", start_years)

months_reporting_cnt <- map(start_years,
                            Find_Agencies_Reporting,
                            agencies_df = monthly_crime_count,
                            end_year = 2021)

pdf(here("clean-and-process",
         "reports",
         "monthly_reporting_ori_cnt.pdf"), onefile = T)
map(months_reporting_cnt, 2)
dev.off()

# Graph PDF and CDFs of the percentage of police agencies which report every
# month for a discrete set of years.
end_years <- 1991:2021
names(end_years) <- paste0("year_", end_years)

months_reporting_dscrt <- pmap(list(start_years, end_years),
                               Find_Agencies_Reporting,
                               agencies_df = monthly_crime_count)

pdf(here("clean-and-process",
         "reports",
         "monthly_reporting_ori_dscrt.pdf"), onefile = T)
map(months_reporting_dscrt, 2)
dev.off()

# Count number of months each police agency reports each year
full_year_reporting_count <-
    monthly_crime_count %>%
    count(ori, year, name = "nr_month_reporting")

############################################################ Crosswalk file
data(fips_codes)
fips_codes <-
    fips_codes %>%
    distinct(state_name, state_code) %>%
    rename(state = state_name)

crosswalk <-
    batch %>%
    select(ori, year, population, state, fips_county_code_1) %>%
    filter(!is.na(fips_county_code_1)) %>%
    collect() %>%
    # need to pad the county codes because they do not always have 3 digits
    mutate(fips_county_code_1 = str_pad(fips_county_code_1,
                                        width = 3,
                                        side = "left",
                                        pad = 0)) %>%
    # join with census data to get state FIPS codes
    inner_join(fips_codes, by = "state") %>%
    mutate(fips = paste0(state_code, fips_county_code_1)) %>%
    select(ori, year, population, fips) %>%
    # connect counties to the police agencies in those counties
    inner_join(full_year_reporting_count, by = c("ori", "year"))

# total number of police agencies in each county each year
total_nr_agencies_per_county <-
    crosswalk %>%
    count(year, fips, name = "total_nr_agencies")

# police agencies which report every month in a given year in a given county
total_nr_agencies_reporting_per_county <-
    crosswalk %>%
    filter(nr_month_reporting == 12) %>%
    count(year, fips, name = "nr_agencies_full_year_report")

# which counties had every police agency report every month in a given year
total_nr_agencies_complete_per_county <-
    inner_join(total_nr_agencies_per_county,
               total_nr_agencies_reporting_per_county,
               by = c("year", "fips")) %>%
    mutate(prcnt_coverage = nr_agencies_full_year_report / total_nr_agencies)

pdf(here("clean-and-process",
         "reports",
         "agency_coverage_per_county.pdf"), onefile = T)

map(2013:2021,
    function(df, year_var) {
    
        df <- df %>% filter(year == year_var)
        
        title <- paste0("PDF and CDF for the % of counties in which every police agency\n",
                        "in that county reported every month\n",
                        "Total # of counties: ", length(unique(df$fips)), "\n",
                        "Year: ", year_var)
        
        df %>%
            ggplot(aes(x = prcnt_coverage)) +
            stat_ecdf(geom = "line") +
            theme_bw() +
            geom_histogram(aes(y = ..count.. / sum(..count..)), alpha = 0.5) +
            scale_x_continuous(breaks = ~ pretty(.x)) +
            labs(x = "Percentage of police agencies which reported every month",
                 y = "% of counties with \'x\' percentage of police agencies reporting every month",
                 title = title)},
    
    df = total_nr_agencies_complete_per_county)

dev.off()

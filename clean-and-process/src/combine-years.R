library(here)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(ggplot2)

input_dir <- str_c("clean-and-process",
                   "input",
                   "rds",
                   "nibrs_1991_2020_victim_segment_rds",
                   sep = "/")

victim_files <- list.files(here(input_dir), full.names = T)
year_names <- str_replace(str_match(victim_files, "[12][09][0-9]{2}.rds"),
                          ".rds",
                          "")
names(victim_files) <- year_names

sex_crimes <- c("rape", "sodomy", "incest", "statutory rape",
                "sexual assault with an object",
                "human trafficking - commercial sex acts",
                "fondling (incident liberties/child molest)")

Summarise_By_State <- function(file_path, crimes) {
    
    df <- readRDS(here(file_path))
    
    sex_crimes <-
        df %>%
        filter(if_any(matches("ucr_offense_code"), ~ .x %in% crimes))
    
    summ <-
        df %>%
        group_by(year) %>%
        summarise(nr_agencies = length(unique(ori)),
                  nr_states = length(unique(state)))
    
    return(list(sex_crimes = sex_crimes, summ = summ))
}

sex_crimes <- map(victim_files, Summarise_By_State, crimes = sex_crimes)

sex_crimes_all <-
    sex_crimes %>%
    map(1) %>%
    map(function(df) {df %>% mutate(across(everything(), ~as.character(.)))}) %>%
    bind_rows()

sex_crimes_count <-
    sex_crimes_all %>%
    group_by(year, state) %>%
    summarise(count = n(),
              nr_agencies = length(unique(ori))) %>%
    ungroup() %>%
    mutate(count_rate = count / nr_agencies * 10) %>%
    filter(nr_agencies >= median(nr_agencies)) %>%
    mutate(year = as.numeric(year))

ggplot(sex_crimes_count, aes(x = year, y = count_rate)) +
    geom_point() +
    geom_line(aes(group = state)) +
    facet_wrap(~state, scales = "free") +
    theme_bw() +
    scale_x_continuous(breaks = seq(1991, 2020, 5), limits = c(1991, 2020))

ggplot(sex_crimes_count, aes(x = year, y = count)) +
    geom_point() +
    geom_line(aes(group = state)) +
    facet_wrap(~state, scales = "free") +
    theme_bw() +
    scale_x_continuous(breaks = seq(1991, 2020, 5), limits = c(1991, 2020))

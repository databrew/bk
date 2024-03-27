library(readr)
library(dplyr)
library(ggplot2)

# Load the data object generated in scripts/metadata/generate_metadata.R
load('../../scripts/metadata/pre_safety.RData')

# Get the population of each cluster
population_by_cluster <- v0demography %>%
  group_by(cluster) %>%
  summarise(population = sum(num_hh_members))

# Join to assignments
population_by_cluster <- population_by_cluster %>%
  left_join(assignments, by = c('cluster' = 'cluster_number')) %>%
  mutate(cluster = as.numeric(cluster))

# Get population by arm
population_by_arm <- population_by_cluster %>%
  group_by(assignment) %>%
  summarise(population = sum(population))

# Get number of treatments per date
treatment_by_date <- safety_repeat_individual %>%
  left_join(safety %>% dplyr::select(KEY, cluster, todays_date, visit), by = c('PARENT_KEY' = 'KEY')) %>%
  mutate(took_drug =  participant_take_drug == 'yes' | participant_take_drug_2 == 'yes') %>%
  filter(took_drug) %>%
  group_by(cluster, todays_date) %>%
  summarise(population_treated = n()) %>%
  left_join(assignments %>%
              mutate(cluster = as.numeric(cluster_number)) %>%
              dplyr::select(cluster, assignment)) %>%
  group_by(assignment, date = todays_date) %>%
  summarise(population_treated = sum(population_treated)) %>%
  ungroup 

# Join treatment by date with population
joined <- left_join(
  treatment_by_date,
  population_by_arm
) %>%
  mutate(percentage = population_treated / population * 100) %>%
  filter(date >= as.Date('2023-07-01')) %>%
  mutate(assignment = paste0('Arm ', assignment)) %>%
  mutate(visit = ifelse(date <= as.Date('2023-10-25'),
                        1,
                        ifelse(date <= as.Date('2023-11-25'),
                               2, 3))) %>%
  group_by(assignment, visit) %>%
  mutate(cumulative_percentage = cumsum(percentage)) %>%
  ungroup

ggplot(data = joined,
       aes(x = date,
           y = percentage)) +
  geom_bar(stat = 'identity', color = 'black', fill = 'blue', alpha = 0.4) +
  facet_wrap(~assignment, ncol = 1) +
  labs(x = 'Date', y = 'Percent of residents treated on that day') +
  theme_bw()

ggplot(data = joined,
       aes(x = date,
           y = cumulative_percentage,
           group = visit)) +
  geom_line(stat = 'identity', color = 'black', alpha = 0.9) +
  facet_wrap(~assignment, ncol = 1) +
  labs(x = 'Date', y = 'Cumulative percentage of residents treated during visit block') +
  theme_bw()


# Read in some Mozambique data
moz <- read_csv('~/Documents/bohemia/analyses/treatment_time/agg_arm.csv') %>%
  mutate(assignment = paste0('Arm ', intervention)) %>%
  dplyr::select(assignment, date,
                population_treated = treated_today,
                population = total_people)

# Combine moz and kenya
combined <- 
  bind_rows(
    moz %>% mutate(country = 'Mozambique'),
    joined %>% mutate(country = 'Kenya')
  ) %>%
  dplyr::select(assignment, date, population_treated, population, country) %>%
  mutate(percentage = population_treated / population * 100) %>%
  group_by(country) %>%
  mutate(day = date - min(date)) %>%
  ungroup

# Plot
ggplot(data = combined,
       aes(x = day,
           y = percentage)) +
  geom_bar(stat = 'identity', width = 1,
           color = 'black', alpha = 0.6,
           position = position_stack(),
           aes(fill = assignment)) +
  facet_wrap(~country, ncol = 1) +
  theme_bw() +
  labs(x = 'Day',
       y = 'Percentage of population treated that day') +
  theme(legend.position = 'bottom') +
  scale_fill_manual(name = '',
                    values = c('red', 'blue', 'black')) +
  xlim(0, 100)

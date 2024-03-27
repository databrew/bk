library(readr)
library(dplyr)
library(ggplot2)

# Read in NOAA daily summary data
# ken <- read_csv('data/ken.csv') %>% mutate(country = 'Kenya')
# eldo shared kwale data
ken <- read_csv('data/kwale_2022-01-01_to_2022-12-31.csv') %>%
  bind_rows(read_csv('data/kwale_2023-01-01_to_2023-12-31.csv')) %>%
  mutate(PRCP = precip) %>%
  mutate(DATE = datetime) %>% mutate(country = 'Kenya')
# moz <- read_csv('data/moz.csv') %>% mutate(country = 'Mozambique')
# https://www.visualcrossing.com/weather/weather-data-services
moz <- read_csv('data/Quelimane 2021-10-01 to 2024-04-10.csv') %>%
  mutate(PRCP = precip) %>%
  mutate(DATE = datetime) %>% mutate(country = 'Mozambique')

# Combine
weather <- 
  bind_rows(
    ken, moz
  )

# Read in the time of treatments
load('../population_treated/combined.RData')

# Join the "day" concept to weather
day0 <- combined %>%
  filter(day == 0) %>%
  dplyr::select(country, 
                day0 = date) %>%
  dplyr::distinct(country, .keep_all = TRUE)
right <- combined %>%
  filter(population_treated > 0) %>%
  group_by(country) %>%
  summarise(daymax = max(date))
weather <- weather %>%
  mutate(date = DATE) %>%
  left_join(day0) %>%
  mutate(day = date - day0)
# Get rolling average
ma <- function(x, n = 7){as.numeric(stats::filter(x, rep(1 / n, n), sides = 2))}

weather <- weather %>%
  arrange(country, day) %>%
  mutate(PRCP = ifelse(is.na(PRCP), 0, PRCP)) %>%
  group_by(country) %>%
  mutate(precip = ma(PRCP)) %>%
  ungroup

# add the approx end date
weather <- weather %>%
  mutate(end_day = ifelse(country == 'Kenya', 75, 100))
  

# Plot
ggplot(data = weather,
       aes(x = day,
           y = PRCP)) +
  geom_bar(stat = 'identity',
           color = 'black',
           fill = 'blue',
           alpha = 0.6) +
  theme_bw() +
  facet_wrap(~country, ncol = 1) +
  xlim(-100, 200) +
  labs(x = 'Days since project start',
       y = 'Daily precipitation') +
  geom_vline(xintercept = 0, col = 'red', alpha = 0.8, lty = 2) +
  geom_vline(aes(xintercept = end_day),
             col = 'red', apha = 0.01,
             lty = 2)

# Aggregate by month
monthly_weather <- 
  weather %>%
  mutate(month = lubridate::floor_date(date, 'month')) %>%
  group_by(country, month) %>%
  summarise(precipitation = sum(PRCP))

ggplot(data = monthly_weather,
       aes(x = month,
           y = precipitation/100)) +
  geom_bar(stat = 'identity',
           color = 'black',
           fill = 'blue',
           alpha = 0.6) +
  theme_bw() +
  facet_wrap(~country, ncol = 1) +
  labs(x = 'Date',
       y = 'Monthly precipitation (cm)') +
  geom_bar(stat = 'identity', width = 1,
           color = NA, alpha = 0.6,
           fill = 'red',
           position = position_stack(),
           # aes(fill = assignment),
           data = combined,
           aes(x = date,
               y = percentage)) 
  # geom_vline(data = day0,
  #            aes(xintercept = day0),
  #            color = 'red',
  #            size = 3,
  #            alpha = 0.6) +
  # geom_vline(data = right,
  #            aes(xintercept = daymax),
  #            color = 'red',
  #            size = 3,
  #            alpha = 0.6) 


ggplot(data = weather,
       aes(x = date,
           y = PRCP/10)) +
  geom_bar(stat = 'identity',
           color = 'black',
           fill = 'blue',
           alpha = 0.6) +
  theme_bw() +
  facet_wrap(~country, ncol = 1) +
  labs(x = 'Date',
       y = 'Daily precipitation') +
  geom_bar(stat = 'identity', width = 1,
           color = NA, alpha = 0.6,
           fill = 'red',
           position = position_stack(),
           # aes(fill = assignment),
           data = combined,
           aes(x = date,
               y = percentage)) 

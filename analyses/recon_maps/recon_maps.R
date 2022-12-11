library(paws)
library(dplyr)
library(readr)
library(ggplot2)
library(rgdal)
library(raster)
library(ggthemes)
ken3 <- raster::getData(name = 'GADM', download = TRUE, country = 'KEN', level = 3)
ken3_fortified <- fortify(ken3, regions = ken3@data$NAME_3)
kwale <- ken3[ken3@data$NAME_1 == 'Kwale',]
kwale_fortified <- fortify(kwale, regions = ken3@data$NAME_3)
pongwe_kikoneni <- ken3[ken3@data$NAME_3 == 'Pongwe/Kikoneni',]
pongwe_kikoneni_fortified <- fortify(pongwe_kikoneni, regions = ken3@data$NAME_3)
Sys.setenv('AWS_PROFILE' = 'dbrew-prod')
# aws sso login --profile dbrew-prod
s3obj <- paws::s3()
# s3obj$list_buckets()

# Read in recon functions
recon_functions_dir <- '../../../recon-monitoring/R/'
recon_functions <- dir(recon_functions_dir)
for(i in 1:length(recon_functions)){
  source(paste0(recon_functions_dir, '/', recon_functions[i]))
}

households <- get_household_forms()
households <- households %>%
  mutate(date = as.Date(substr(SubmissionDate, 1, 10)))

ggplot() +
  geom_polygon(data = pongwe_kikoneni_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               color = 'black',
               fill = 'beige',
               alpha = 0.5) +
  geom_point(data = households %>% filter(date >= as.Date('2022-12-01')),
             aes(x = Longitude,
                 y = Latitude),
             alpha = 0.3,
             size = 0.01,
             color = 'red') +
  ggthemes::theme_map() +
  coord_map() +
  facet_wrap(~date)

# Satellite map
library(leaflet)
l <- leaflet() %>%
  addProviderTiles(providers$Esri.WorldImagery) %>%
  addPolylines(data = pongwe_kikoneni) %>%
  addCircleMarkers(data = households, radius = 3, fillOpacity = 1, weight = 3, color = 'yellow', fillColor = 'yellow')
l
htmlwidgets::saveWidget(l, file = '~/Desktop/satmap.html', selfcontained = TRUE)
# htmltools::save_html(l, file = '~/Desktop/satmap.html')

# Charts
pd <- households %>%
  group_by(roof_type) %>%
  tally %>% arrange(desc(n)) %>%
  mutate(roof_type = factor(roof_type, levels = rev(roof_type))) %>%
  mutate(p = n / sum(n) * 100)
ggplot(data = pd,
       aes(x = roof_type,
           y = n)) +
  geom_segment(aes(yend = 0,
                   xend = roof_type)) +
  geom_point() +
  coord_flip() +
  theme_bw() +
  labs(x = 'Households',
       y = 'Roof type') +
  geom_text(aes(label = paste0(round(p, digits = 2), '%')),nudge_y = 400)

pd <- households %>%
  group_by(house_wall_material) %>%
  tally %>% arrange(desc(n)) %>%
  mutate(house_wall_material = factor(house_wall_material, levels = rev(house_wall_material))) %>%
  mutate(p = n / sum(n) * 100)
ggplot(data = pd,
       aes(x = house_wall_material,
           y = n)) +
  geom_segment(aes(yend = 0,
                   xend = house_wall_material)) +
  geom_point() +
  coord_flip() +
  theme_bw() +
  labs(x = 'Households',
       y = 'Wall material') +
  geom_text(aes(label = paste0(round(p, digits = 2), '%')),nudge_y = 400)

pd <- households %>%
  summarise(`<5` = sum(num_hh_members_lt_5),
            `5-15` = sum(num_hh_members_bt_5_15),
            `>15` = sum(num_hh_members_gt_15)) %>%
  tidyr::gather(key, value, `<5` : `>15`) %>%
  mutate(p = value / sum(value) * 100) %>%
  mutate(key = factor(key, levels = c('<5', '5-15', '>15')))
ggplot(data = pd,
       aes(x = key,
           y = value)) +
  geom_col() +
  geom_text(aes(label = paste0(round(p, digits = 2), '%')),nudge_y = 1000) +
  theme_bw() +
  labs(x = 'Age group',
       y = 'Inhabitants')

ggplot(data = households,
       aes(x = num_hh_members)) +
  geom_histogram() +
  theme_bw()
quantile(households$num_hh_members)

# Form completion time
options(scipen = 999)
pd <- households %>%
  mutate(start_time = lubridate::as_datetime(start_time)) %>%
  mutate(end_time = lubridate::as_datetime(end_time)) %>%
  mutate(completion = difftime(end_time, start_time, units = 'secs')) %>%
  mutate(completion = as.numeric(completion))
ggplot(data = pd,
       aes(x = completion)) +
  geom_histogram() +
  theme_bw()
quantile(pd$completion)
quantile(pd$completion, 0.9)


# time between forms
pd <- households %>%
  mutate(start_time = lubridate::as_datetime(start_time)) %>%
  mutate(end_time = lubridate::as_datetime(end_time)) %>%
  arrange(start_time) %>%
  group_by(wid) %>%
  mutate(previous_end = dplyr::lag(end_time, 1)) %>%
  mutate(between_time = difftime(start_time, previous_end, units = 'secs')) %>%
  ungroup %>%
  mutate(between_time = as.numeric(between_time))
quantile(pd$between_time, na.rm = T)

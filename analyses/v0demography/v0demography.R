

library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(sp)
library(lubridate)
library(readr)

# Define production
is_production <- TRUE
Sys.setenv(PIPELINE_STAGE = ifelse(is_production, 'production', 'develop')) # change to production
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")

# Log in
tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = env_pipeline_stage)
  
}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})


# Define datasets for which I'm retrieving data
datasets <- c('v0demography', 'sev0rab', 'sev0ra')
datasets_names <- datasets
# Loop through each dataset and retrieve
# bucket <- 'databrew.org'
# folder <- 'kwale'
bucket <- 'databrew.org'
if(is_production){
  folder <- 'kwale'
} else {
  folder <- 'kwale_testing'
}

for(i in 1:length(datasets)){
  this_dataset <- datasets[i]
  object_keys <- glue::glue('/{folder}/clean-form/{this_dataset}',
                            folder = folder,
                            this_dataset = this_dataset)
  output_dir <- glue::glue('{folder}/clean-form/{this_dataset}',
                           folder = folder,
                           this_dataset = this_dataset)
  dir.create(object_keys, recursive = TRUE, showWarnings = FALSE)
  print(object_keys)
  cloudbrewr::aws_s3_bulk_get(
    bucket = bucket,
    prefix = as.character(object_keys),
    output_dir = output_dir
  )
}

v0demography <- read_csv('kwale/clean-form/v0demography/v0demography.csv')
v0demography_repeat_individual <- read_csv('kwale/clean-form/v0demography/v0demography-repeat_individual.csv')
ra <- read_csv('kwale/clean-form/sev0ra/sev0ra.csv') %>% dplyr::select(Longitude, Latitude, cluster)
rab <- read_csv('kwale/clean-form/sev0rab/sev0rab.csv') %>% dplyr::select(Longitude, Latitude, cluster)
rx <- bind_rows(ra, rab)

# Load in clusters
library(ggplot2)
library(rgdal)
library(sp)
library(ggthemes)
clusters <- rgdal::readOGR('../../data_public/spatial/clusters/', 'clusters')
clusters_fortified <- fortify(clusters, id = 'cluster_nu')
load('../../data_public/spatial/pongwe_kikoneni_ramisi.RData')
pongwe_kikoneni_ramisi_fortified <- fortify(pongwe_kikoneni_ramisi, id = 'NAME_3')
ggplot() +
  geom_polygon(data = pongwe_kikoneni_ramisi_fortified,
               aes(x = long, y = lat, group = group),
               fill = 'beige',
               color = 'black') +
  geom_polygon(data = clusters_fortified,
               aes(x = long, y = lat, group = group),
               fill = 'lightblue',
               color = 'blue') +
  geom_point(data = v0demography,
             aes(x = `Longitude`,
                 y = `Latitude`),
             size = 0.3) +
  geom_point(data = rx,
             aes(x = `Longitude`,
                 y = `Latitude`),
             size = 0.5, color = 'red') +
  theme_map()

# forms and refusals by cluster
left <- v0demography %>%
  group_by(cluster) %>%
  summarise(successes = n())
right <- rx %>%
  group_by(cluster) %>%
  summarise(refusals = n())
joined <- left_join(left, right) %>%
  mutate(n_hh = refusals + successes) %>%
  mutate(p_refusal = refusals /n_hh * 100)

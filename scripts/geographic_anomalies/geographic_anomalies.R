

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
datasets <- c('v0demography')#, 'sev0rab', 'sev0ra')
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

# Load in clusters
library(ggplot2)
library(rgdal)
library(sp)
library(ggthemes)
if(!file.exists('clusters.RData')){
  clusters <- rgdal::readOGR('../../data_public/spatial/clusters/', 'clusters')
  save(clusters, file = 'clusters.RData')
} else {
  load('clusters.RData')
}

# Check each submission to see if in clusters or no
v0 <- v0demography
coordinates(v0) <- ~Longitude+Latitude
proj4string(v0) <- proj4string(clusters)

# Add a 100 meter buffer
# Convert objects to projected UTM reference system
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)
clusters_projected <- spTransform(clusters, crs)
clusters_projected_buffered <- rgeos::gBuffer(clusters_projected, byid = TRUE, width = 100)
v0_projected <- spTransform(v0, crs)
o <- sp::over(v0_projected, polygons(clusters_projected_buffered))
o_strict <- sp::over(v0_projected, polygons(clusters_projected))
anom <- v0_projected@data
anom$outside_cluster <- is.na(o_strict)
anom$outside_cluster_by_more_than_100_m <- is.na(o)

anom <- anom %>% filter(outside_cluster_by_more_than_100_m | outside_cluster)
anom <- anom %>% dplyr::select(
  instanceID,
  SubmissionDate,
  start_time,
  todays_date,
  wid, fa_id, cluster,
  # Latitude, Longitude,
  recon_hhid_map, hhid,
  outside_cluster_by_more_than_100_m,
  outside_cluster)
write_csv(anom, 'anom_households.csv')


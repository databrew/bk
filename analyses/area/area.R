

library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(sp)
library(lubridate)
library(readr)
library(ggplot2)
library(rgdal)
library(sp)
library(ggthemes)

# Define production
is_production <- TRUE
Sys.setenv(PIPELINE_STAGE = ifelse(is_production, 'production', 'develop')) # change to production
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
start_fresh <- FALSE

if(start_fresh){
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
  ra <- read_csv('kwale/clean-form/sev0ra/sev0ra.csv') %>% dplyr::select(todays_date, Longitude, Latitude, cluster, refusal_or_absence, form_version = FormVersion, hhid, recon_hhid_map_manual, recon_hhid_painted_manual) %>% mutate(version = 'a')
  rab <- read_csv('kwale/clean-form/sev0rab/sev0rab.csv') %>% dplyr::select(todays_date, Longitude, Latitude, cluster, refusal_or_absence, form_version = FormVersion, hhid, recon_hhid_map_manual, recon_hhid_painted_manual) %>% mutate(version = 'b')
  rx <- bind_rows(ra, rab) %>%
    mutate(id = ifelse(is.na(hhid), recon_hhid_map_manual, hhid)) %>%
    mutate(id = ifelse(is.na(id), recon_hhid_painted_manual, id)) %>%
    mutate(recon_id = ifelse(!is.na(recon_hhid_map_manual), recon_hhid_map_manual, recon_hhid_painted_manual))
  save(ra, rab, rx, v0demography, v0demography_repeat_individual, file = 'data.RData')
} else {
  load('data.RData')
}
  
# Load in spatial data
load('../../data_public/spatial/clusters.RData')
load('../../data_public/spatial/new_clusters.RData')
load('../../data_public/spatial/new_cores.RData')

# clusters_fortified <- fortify(clusters, id = 'cluster_nu')
# load('../../data_public/spatial/pongwe_kikoneni_ramisi.RData')
# pongwe_kikoneni_ramisi_fortified <- fortify(pongwe_kikoneni_ramisi, id = 'NAME_3')
cluster_coordinates <- data.frame(coordinates(clusters))
names(cluster_coordinates) <- c('x', 'y')
# cluster_coordinates$cluster <- clusters@data$cluster_nu
# cores <- rgdal::readOGR('../../data_public/spatial/cores/', 'cores')
# buffers <- rgdal::readOGR('../../data_public/spatial/buffers/', 'buffers')
# Make a spatial version of households
v0demography_spatial <- v0demography %>% mutate(x = Longitude, y = Latitude)
coordinates(v0demography_spatial) <- ~x+y
proj4string(v0demography_spatial) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)
v0demography_spatial_projected <- spTransform(v0demography_spatial, crs)
# cores_projected <- spTransform(cores, crs)
# cores_projected_buffered <- rgeos::gBuffer(cores_projected, byid = TRUE, width = 50)
# clusters_projected <- spTransform(clusters, crs)
# clusters_projected_buffered <- rgeos::gBuffer(clusters_projected, byid = TRUE, width = 50)

new_clusters_projected <- spTransform(new_clusters, crs)
new_cores_projected <- spTransform(new_cores, crs)

# areas <- rgeos::gArea(new_clusters_projected, byid = TRUE)  /1000
areas <- geosphere::areaPolygon(new_clusters)
out <- new_clusters_projected@data %>%
  mutate(area = areas)

write_csv(out, 'areas.csv')


# Calculate the distance in meters from the edge of each cluster core (ie, ignore the buffer) to the edge of the nearest cluster's core (generating one value - which should be >400 meters - for each cluster). Thereafter, calculate the average of all the values. 
library(sp)
out_list <- list()
for(i in 1:nrow(new_cores_projected)){
  this_shp <- new_cores_projected[i,]
  coords <- this_shp@polygons[[1]]@Polygons[[1]]@coords
  out <- SpatialPointsDataFrame(coords, data = tibble(cluster_nu = rep(this_shp@data %>% pull(cluster_nu), nrow(coords))))
  out_list[[i]] <- out
}
done <- do.call('rbind', out_list)
proj4string(done) <- proj4string(new_cores_projected)
distance_list <- list()
cluster_nus <- sort(unique(done$cluster_nu))
for(i in 1:length(cluster_nus)){
  this_cluster_nu <- cluster_nus[i]
  these_points <- done[done@data$cluster_nu == this_cluster_nu,]
  other_points <- done[done@data$cluster_nu != this_cluster_nu,]
  distances <- spDists(these_points, other_points)
  distances <- apply(distances, 1, min)
  distance_list[[i]] <- tibble(cluster_nu = this_cluster_nu,
                               meters_to_nearest_other_core = min(distances))
}
distances <- bind_rows(distance_list)
mean(distances$meters_to_nearest_other_core)

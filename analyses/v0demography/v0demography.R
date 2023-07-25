

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
clusters <- rgdal::readOGR('../../data_public/spatial/clusters/', 'clusters')
clusters_fortified <- fortify(clusters, id = 'cluster_nu')
load('../../data_public/spatial/pongwe_kikoneni_ramisi.RData')
pongwe_kikoneni_ramisi_fortified <- fortify(pongwe_kikoneni_ramisi, id = 'NAME_3')
cluster_coordinates <- data.frame(coordinates(clusters))
names(cluster_coordinates) <- c('x', 'y')
cluster_coordinates$cluster <- clusters@data$cluster_nu
cores <- rgdal::readOGR('../../data_public/spatial/cores/', 'cores')
buffers <- rgdal::readOGR('../../data_public/spatial/buffers/', 'buffers')
# Make a spatial version of households
v0demography_spatial <- v0demography %>% mutate(x = Longitude, y = Latitude)
coordinates(v0demography_spatial) <- ~x+y
proj4string(v0demography_spatial) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)
v0demography_spatial_projected <- spTransform(v0demography_spatial, crs)
cores_projected <- spTransform(cores, crs)
cores_projected_buffered <- rgeos::gBuffer(cores_projected, byid = TRUE, width = 50)
clusters_projected <- spTransform(clusters, crs)
clusters_projected_buffered <- rgeos::gBuffer(clusters_projected, byid = TRUE, width = 50)

o <- sp::over(v0demography_spatial_projected, polygons(cores_projected_buffered))
v0demography_spatial@data$in_core <- !is.na(o)
v0demography_spatial$core_cluster_number <- as.numeric(cores@data$cluster_nu[o])

o <- sp::over(v0demography_spatial_projected, polygons(clusters_projected_buffered))
v0demography_spatial@data$in_cluster <- !is.na(o)
v0demography_spatial$cluster_cluster_number <- as.numeric(clusters@data$cluster_nu[o])

people <- v0demography_repeat_individual %>%
  left_join(v0demography_spatial@data %>%
              dplyr::select(KEY,
                            lng = Longitude,
                            lat = Latitude,
                            cluster,
                            in_core,
                            in_cluster,
                            cluster_cluster_number,
                            core_cluster_number),
            by = c('PARENT_KEY' = 'KEY'))
date_of_enrollment <- as.Date('2023-10-01')
# See who is eligible or not
people <- people %>%
  mutate(age_at_enrollment  = date_of_enrollment - dob) %>%
  mutate(age_at_enrollment = age_at_enrollment / 365.25) %>%
  mutate(efficacy_eligible = age_at_enrollment >= 5.0 & age_at_enrollment < 15.0)
# Get a dataframe of only efficacy eligible children
eligibles <- people %>% filter(efficacy_eligible) %>%
  filter(in_cluster)
pd <- eligibles %>% group_by(cluster) %>% tally
# Make spatial and project
eligibles_sp <- eligibles %>% mutate(x = lng, y = lat)
coordinates(eligibles_sp) <- ~x+y
proj4string(eligibles_sp) <- proj4string(clusters)
eligibles_sp_projected <- spTransform(eligibles_sp, crs)


# July 24 2023, cluster cores do not have enough efficacy-eligible children
# We need to expand outwards until enough are reached
# This means making the cluster grow all the way out to 400 meters (buffer) or less until 35 are reached
cluster_numbers <- sort(unique(clusters$cluster_nu))
out_list <- list()
if(!dir.exists('expansion_charts')){
  dir.create('expansion_charts')
}
for(i in 1:length(cluster_numbers)){
  this_cluster_number <- cluster_numbers[i]
  this_core_projected <- cores_projected[cores_projected@data$cluster_nu == this_cluster_number, ]
  this_cluster_projected <- clusters_projected[clusters_projected@data$cluster_nu == this_cluster_number,]
  expanded_core <- this_core_projected
  ok <- FALSE
  expansion_meters <- 0
  in_cluster_kids <- eligibles_sp_projected[eligibles_sp_projected@data$cluster_cluster_number == this_cluster_number,]
  n_original_core <- length(which(!is.na(sp::over(in_cluster_kids, polygons(expanded_core)))))
  n_eligibles_full_cluster <- length(which(!is.na(sp::over(in_cluster_kids, polygons(clusters_projected_buffered[clusters_projected_buffered@data$cluster_nu == this_cluster_number,])))))
  
  while(!ok){
    n_eligibles <- length(which(!is.na(sp::over(in_cluster_kids, polygons(expanded_core)))))
    if(n_eligibles >= 35 | expansion_meters >= 400){
      ok <- TRUE
      message('Cluster ', this_cluster_number, ' got ', n_eligibles, ' kids at an expansion of ', 
              expansion_meters)
      out_list[[i]] <- tibble(cluster_number = this_cluster_number,
                              n_original_core,
                              n_eligibles,
                              n_eligibles_full_cluster,
                              expansion_meters)
      png(filename = paste0('expansion_charts/', this_cluster_number, '.png'))
      xlims <- bbox(expanded_core)[1,] * c(0.9999, 1.0001)
      ylims <- bbox(expanded_core)[2,] * c(0.9999, 1.0001)
      plot(this_cluster_projected,
           xlim = xlims, ylim = ylims)
      title(main = this_cluster_number, sub = paste0('From ', n_original_core, ' to ', n_eligibles, ' efficacy-aged children with a ',expansion_meters, ' meters expansion of the core'))

      
      plot(expanded_core, add = T, col = adjustcolor('red', alpha.f = 0.6), lty = 2)
      plot(this_core_projected, add = T, col = adjustcolor('red', alpha.f = 0.7))
      points(eligibles_sp_projected, pch = 16, cex = 0.3)
      other_clusters <- clusters_projected[clusters_projected@data$cluster_nu != this_cluster_number,]
      plot(other_clusters, add = T, lty = 3)
      text(coordinates(other_clusters), labels = other_clusters@data$cluster_nu)
      # plot(clusters_projected_buffered, add = T, lty = 2)
      dev.off()
      
    } else {
      expansion_meters <- expansion_meters + 1
      # message('Adding a meter because only ', n_eligibles, ' kids: ', expansion_meters)
      expanded_core <- rgeos::gBuffer(this_core_projected, width = expansion_meters)
      n_eligibles <- length(which(!is.na(sp::over(in_cluster_kids, polygons(expanded_core)))))
      
    }
  }
}
out <- bind_rows(out_list)
write_csv(out, '~/Desktop/slack/table_of_results.csv')
# the above is a dataframe of the necessary expansion to reach X kids
summary(out$expansion_meters)
table(out$n_eligibles >= 35)
table(out$n_eligibles >= 25)

library(ggrepel)
ggplot(data = out,
       aes(x = expansion_meters,
           y = n_eligibles)) +
  geom_hline(yintercept = 35, lty = 2, col = 'red') +
  geom_hline(yintercept = 25, lty = 2, col = 'blue') +
  
  geom_point(size = 3, alpha = 0.7) +
  geom_text_repel(aes(label = cluster_number), 
                  max.overlaps = 10,
                  size = 3) +
  labs(x = 'Expansion of core (in meters)',
       y = 'Number of efficacy-eligible kids in expanded core')


# Expand the buffers by expansion_meters
cores_projected@data <- left_join(cores_projected@data,
                                  out %>% dplyr::select(cluster_nu = cluster_number,
                                                        expansion_meters) %>%
                                    mutate(expand_by = expansion_meters + 20) %>%
                                    mutate(expand_by = ifelse(expand_by >= 400, 400, expand_by)))
new_cores_projected <- rgeos::gBuffer(cores_projected, byid = TRUE, id = cores_projected@data$cluster_nu,
                              width = cores_projected@data$expand_by)
plot(clusters_projected, col = adjustcolor('red', alpha.f = 0.3), lwd = 0.2, lty = 3)
plot(cores_projected, lty = 1, add = T)
plot(new_cores_projected, lty = 2, add = T)
text(coordinates(new_cores_projected), labels = new_cores_projected@data$cluster_nu, cex = 1,
     col = adjustcolor('black', alpha.f = 0.5))
points(eligibles_sp_projected, pch = '.')

# Get aggregate statistics using 20 meter additional buffer, etc.
o <- sp::over(eligibles_sp_projected, polygons(new_cores_projected))
eligibles_sp_projected@data$in_expanded_core <- !is.na(o)
eligibles_sp_projected@data$expanded_core_number <- new_cores_projected@data$cluster_nu[o]

# Assign each child a priority number based on location within cluster
st_eligibles <- sf::st_as_sf(eligibles_sp_projected)
st_new_cores_projected <- sf::st_as_sf(new_cores_projected)
st_clusters_projected <- sf::st_as_sf(clusters_projected)

dist_for_edge <- st_geometry(obj = st_clusters_projected) %>%
  st_cast(to = 'MULTILINESTRING') %>%
  st_distance(y=st_eligibles)
distances <- apply(dist_for_edge, 2, min)
st_eligibles$distance_to_edge <- distances
eligibles_sp_projected@data$distance_to_edge <- distances
plot(clusters_projected)
points(eligibles_sp_projected, add = T, pch = '.')
ggplot(data = eligibles_sp_projected@data,
       aes(x = lng,
           y = lat,
           color = distance_to_edge)) +
  geom_point(size = 0.6, alpha = 0.6) +
  theme_bw() +
  scale_color_gradient(name = 'Distance\nto edge', low = 'yellow',
                       high = 'black')

# Assign number to each child
priority_numbers <- eligibles_sp_projected@data %>%
  # filter(!is.na(expanded_core_number)) %>%
  arrange(desc(distance_to_edge)) %>%
  mutate(dummy = 1) %>%
  group_by(cluster_cluster_number) %>%
  mutate(cs = cumsum(dummy)) %>%
  ungroup %>%
  dplyr::select(KEY, priority_number = cs)
eligibles_sp_projected@data <- 
  left_join(eligibles_sp_projected@data,
            priority_numbers)

# Plot relationship between priority number and distance to contamination
ggplot(data = eligibles_sp_projected@data,
       aes(x = priority_number,
           y = distance_to_edge)) +
  geom_point(alpha = 0.2, size = 2) +
  labs(x = 'Priority number',
       y = 'Distance to edge of cluster') 

library(ggplot2)
# refusal type of form by day
pd <- rx %>%
  group_by(date = todays_date, version) %>%
  tally
ggplot(data = pd,
       aes(x = date, y = n, fill = version)) +
  geom_bar(stat = 'identity', position = position_stack())

# Second absences before first absences
ab2 <- rx %>% filter(refusal_or_absence == 'Absence 2nd attempt') %>% dplyr::select(id, ab2 = todays_date)
ab1 <- rx %>% filter(refusal_or_absence == 'Absence 1st attempt') %>% dplyr::select(id, ab1 = todays_date)
ab <- full_join(ab1, ab2)
abx <- ab %>% filter(is.na(ab1), !is.na(ab2))
write_csv(abx, 'abx.csv')


# Submissions by daqy
pd <- v0demography %>%
  group_by(date = todays_date) %>%
  tally
ggplot(data = pd,
       aes(x = date, y = n)) +
  geom_bar(stat = 'identity', position = position_stack()) +
  labs(x = 'Date', y = 'Household submissions') +
  geom_text(aes(label = n),
            nudge_y = 30)

# Get list of households not yet visited
if(FALSE){
  recon <- rgdal::readOGR('../../data_public/spatial/households/', 'households')
  recon <- recon[recon$cluster_n0 > 0,]
  v0 <- v0demography %>%
    # mutate(recon_id = ifelse(!is.na(recon_hhid_painted_manual), recon_hhid_painted_manual, recon_hhid_painted_select))
    mutate(recon_id = recon_hhid_map)
  # recon_clean <- read_csv('~/Downloads/curated_recon_household_data.csv')
  done_ids <- c(v0$recon_id, rx$recon_hhid_map_manual)
  done_ids <- done_ids[!is.na(done_ids)]
  x <- recon[!recon$hh_id %in% done_ids,]
  x <- x@data %>% dplyr::select(cluster = cluster_n0,
                           hh_id, 
                           ward, 
                           community = community0,
                           village,
                           longitude = Longitude,
                           latitude = Latitude,
                           wall = house_wal0,
                           roof = roof_type)
  write_csv(x, '~/Desktop/recon_households_not_visited.csv')
}


# Load in clusters

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
  theme_map() +
  geom_text(data = cluster_coordinates,
            aes(x = x, y = y, label = cluster))

# Get distance between clusters
library(sp)
library(rgeos)
library(Rfast)
clusters_projected <- spTransform(clusters, CRS("+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"))
# x <- clusters_projected[clusters_projected@data$cluster_nu %in% 88:93,]
x <- clusters_projected
plot(x)
text(coordinates(x), labels = x$cluster_nu)
distances <- rgeos::gDistance(x, byid = TRUE)

out_list <- list()
for(i in 1:nrow(distances)){
  this_cluster <- x$cluster_nu[i]
  sub_distances <- distances[i,]
  # Get the furthest one
  furthest_index <- as.numeric(which.max(sub_distances))
  furthest_cluster <- x$cluster_nu[furthest_index]
  distance_to_furthest_cluster <- sub_distances[furthest_index]
  # Get the nearest one (excluding the 0, which is self)
  distance_to_nearest_cluster <- Rfast::nth(sub_distances, 2, descending = F)
  nearest_cluster <- x$cluster_nu[sub_distances == distance_to_nearest_cluster]
  # Dump the results into a dataframe
  out <- 
    tibble(cluster = this_cluster,
           distance_to_nearest_cluster,
           nearest_cluster,
           distance_to_furthest_cluster,
           furthest_cluster)
  out_list[[i]] <- out
}
out <- bind_rows(out_list)

ggplot(data = out,
       aes(x = distance_to_nearest_cluster)) +
  geom_histogram(fill = 'beige', color = 'black') + theme_bw()
summary(out$distance_to_nearest_cluster)

# forms and refusals by cluster
left <- v0demography %>%
  group_by(cluster) %>%
  summarise(successes = n())
right <- rx %>%
  group_by(cluster) %>%
  summarise(refusals = n())
joined <- left_join(left, right) %>%
  mutate(n_hh = refusals + successes) %>%
  mutate(p_refusal = refusals /n_hh * 100) %>%
  arrange(desc(p_refusal))

# refusals vs absences
pd_right <- rx %>%
  group_by(cluster, refusal_or_absence) %>%
  tally %>%
  tidyr::spread(key = refusal_or_absence, value = n)
pd <- left_join(left, pd_right) %>%
  mutate_all(~replace(., is.na(.), 0))
write_csv(pd, 'refusals_and_absences_by_type_and_cluster.csv')

# Examine results for efficacy eligibility
# See which households are in cluster core or not
cores <- rgdal::readOGR('../../data_public/spatial/cores/', 'cores')
buffers <- rgdal::readOGR('../../data_public/spatial/buffers/', 'buffers')

v0demography_spatial <- v0demography %>% mutate(x = Longitude, y = Latitude)
coordinates(v0demography_spatial) <- ~x+y
proj4string(v0demography_spatial) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)
v0demography_spatial_projected <- spTransform(v0demography_spatial, crs)
cores_projected <- spTransform(cores, crs)
cores_projected_buffered <- rgeos::gBuffer(cores_projected, byid = TRUE, width = 50)
clusters_projected <- spTransform(clusters, crs)
clusters_projected_buffered <- rgeos::gBuffer(clusters_projected, byid = TRUE, width = 50)

o <- sp::over(v0demography_spatial_projected, polygons(cores_projected_buffered))
v0demography_spatial@data$in_core <- !is.na(o)
v0demography_spatial$core_cluster_number <- as.numeric(cores@data$cluster_nu[o])

o <- sp::over(v0demography_spatial_projected, polygons(clusters_projected_buffered))
v0demography_spatial@data$in_cluster <- !is.na(o)
v0demography_spatial$cluster_cluster_number <- as.numeric(clusters@data$cluster_nu[o])

people <- v0demography_repeat_individual %>%
  left_join(v0demography_spatial@data %>%
              dplyr::select(KEY,
                            cluster,
                            in_core,
                            in_cluster,
                            cluster_cluster_number,
                            core_cluster_number),
            by = c('PARENT_KEY' = 'KEY'))
date_of_enrollment <- as.Date('2023-10-01')
# https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1689847361003619?thread_ts=1689845516.823389&cid=C042KSRLYUA
people <- people %>%
  mutate(age_at_enrollment  = date_of_enrollment - dob) %>%
  mutate(age_at_enrollment = age_at_enrollment / 365.25) %>%
  mutate(efficacy_eligible = age_at_enrollment >= 5.0 & age_at_enrollment < 15.0)

# Recruitment from cors
pd <- people %>%
  group_by(cluster, in_core, efficacy_eligible) %>%
  tally %>%
  ungroup %>%
  filter(in_core, efficacy_eligible)
right <- people %>%
  group_by(cluster, efficacy_eligible) %>%
  summarise(full_area = n()) %>%
  ungroup %>%
  filter(efficacy_eligible)
pd <- pd %>% dplyr::select(-in_core) %>%
  dplyr::mutate(in_core = n) %>% dplyr::select(-in_core) %>%
  left_join(right)
write_csv(pd, '~/Desktop/carlos.csv')

pd$problematic <- pd$n < 35
x <- clusters
x@data$cluster <- as.numeric(x@data$cluster_nu)
x@data <- left_join(x@data, pd)
x@data$problematic <- ifelse(is.na(x@data$problematic), TRUE, x@data$problematic)
plot(x)
assignments <- read_csv('../randomization/outputs/assignments.csv')
x@data <- left_join(x@data, assignments, by = c('cluster' = 'cluster_number'))
plot(x)
x35 <- x
plot(x[x@data$assignment == 1,], col = 'green', add = T)
plot(x[x@data$assignment == 2,], col = 'orange', add = T)
text(coordinates(x), labels = x@data$cluster)
# plot(x[x@data$problematic, ], col = adjustcolor('red', alpha.f = 0.5), add = T)
points(coordinates(x[x@data$problematic, ]), cex = 4, col = adjustcolor('red', alpha.f = 0.9), pch = 12)
# Read in ento clusters
ento <- read_csv('../randomization/outputs/table_1_ento_clusters.csv') %>% mutate(ento = TRUE)
x@data$cluster_number <- x@data$cluster
x@data <- left_join(x@data, ento)
x@data$ento <- ifelse(is.na(x@data$ento), FALSE, x@data$ento)
x@data[x@data$problematic & x@data$ento,]
summary(pd$n)
table(pd$n >= 35)
table(pd$n >= 30)
table(pd$n >= 25)
table(pd$n >= 15)
hh <- rgdal::readOGR('../../data_public/spatial/households/', 'households')
points(hh, pch = '.')

# Recruitment from entire cluster (including buffers)
pd <- people %>%
  dplyr::select(-cluster) %>% # not using the cluster the fw said, instead using actual location
  group_by(cluster = cluster_cluster_number, in_cluster, efficacy_eligible) %>%
  tally %>%
  ungroup %>%
  filter(in_cluster, efficacy_eligible)

pd %>% filter(cluster %in% x@data$cluster[x@data$problematic]) %>% View

# Get distance to any line
kids <- people %>% filter(efficacy_eligible, in_cluster) %>%
  left_join(v0demography %>% dplyr::select(KEY, lng = Longitude, lat = Latitude),
            by = c('PARENT_KEY' = 'KEY')) %>%
  mutate(x = lng, y = lat)
coordinates(kids) <- ~x+y
proj4string(kids) <- proj4string(clusters)
kids_projected <- spTransform(kids, crs)

# convert SPDF to SLDF
bound <- as(clusters, 'SpatialLinesDataFrame')
bound <- raster::intersect(bound, clusters)
# # remove 'self' overlays (polygon boundary over the same polygon)
# bound <- bound[bound@data$ID_poly.1!=bound@data$ID_poly.2,]
# remove 'duplicated' results (as each boundary occurs twice after the overlay)
bound@data$key <- apply(bound@data, 1, function(s) paste0(sort(s), collapse=''))
bound <- bound[!duplicated(bound@data$key),]
bound_projected <- spTransform(bound, crs)

# Get distance to nearest line for each kid
distances <- rgeos::gDistance(kids_projected, bound_projected, byid = TRUE)
d <- as.numeric(apply(distances, 2, min))
kids$distance_to_contamination <- d

# Number each kid in the cluster
out <- kids@data %>%
  arrange(desc(distance_to_contamination)) %>%
  mutate(dummy = 1) %>%
  group_by(cluster) %>%
  mutate(randomization_number = cumsum(dummy)) %>%
  ungroup
kids@data <- left_join(kids@data,
                       out %>% dplyr::select(KEY, randomization_number))

dir.create('outy')
cluster_numbers <- sort(unique(clusters@data$cluster_nu))
for(i in 1:length(cluster_numbers)){
  cn <- cluster_numbers[i]
  png(paste0('outy/', i, '.png'))
  plot(clusters[clusters@data$cluster_nu == cn,])
  plot(cores[cores@data$cluster_nu == cn,],
       col = adjustcolor('red', alpha.f = 0.6), add = TRUE)
  # points(kids[kids@data$cluster == cn,])
  text(coordinates(kids[kids@data$cluster == cn,]), labels = kids@data$randomization_number[kids@data$cluster == cn], cex = 0.7)
  dev.off()
}
# cn <- 44
# plot(clusters[clusters@data$cluster_nu == cn,])
# points(kids[kids@data$cluster == cn,])
# text(coordinates(kids[kids@data$cluster == cn,]), labels = kids@data$randomization_number[kids@data$cluster == cn])
# 
summary(pd$n)
table(pd$n >= 35)
table(pd$n >= 30)
table(pd$n >= 25)
table(pd$n >= 15)


out_list <- list()
for(i in 1:40){
  out <- tibble(n = i,
                clusters = length(which(pd$n >= i)))
  out_list[[i]] <- out
}
out <- bind_rows(out_list)
ggplot(data = out,
       aes(x = n,
           y = clusters)) +
  geom_bar(stat = 'identity') +
  labs(x = 'Number of efficacy-eligible children in BUFFER',
       y = 'Number of clusters which have X children or more') +
  geom_vline(xintercept = 35, lty = 2) +
  geom_hline(yintercept = out$clusters[out$n == 35], lty = 2)


# Tesselation ############################################################
# convert SPDF to SLDF
plot(cores)
text(coordinates(cores), labels = cores@data$cluster_nu, cex = 0.5)
bound <- as(cores, 'SpatialLinesDataFrame')
bound <- raster::intersect(bound, cores)
# # remove 'self' overlays (polygon boundary over the same polygon)
# bound <- bound[bound@data$ID_poly.1!=bound@data$ID_poly.2,]
# remove 'duplicated' results (as each boundary occurs twice after the overlay)
bound@data$key <- apply(bound@data, 1, function(s) paste0(sort(s), collapse=''))
bound <- bound[!duplicated(bound@data$key),]
# bound <- spTransform(bound, crs)
# bound <- gBuffer(bound, byid = TRUE, id = bound@data$cluster_nu, width = 0.00001)

# Turn clusters into a points dataframe
out_list <- list()
for(i in 1:nrow((bound))){
  this_data <- coordinates(bound)[[i]]
  this_data <- bind_rows(lapply(this_data, data.frame))
  this_data$cluster <- bound@data$cluster_nu[i]
  out_list[[i]] <- this_data
}
cluster_points <- bind_rows(out_list)
# names(cluster_points)[1:2] <- c('x',)
cluster_points <- cluster_points %>% dplyr::distinct(x,y,cluster)  #%>%
  # arrange((cluster))
coordinates(cluster_points) <- ~x+y
proj4string(cluster_points) <- proj4string(clusters)
row.names(cluster_points) <- 1:nrow(cluster_points)

plot(cluster_points$x, cluster_points$y, pch = '.')
setScale(70000)
v <- dismo::voronoi(cluster_points)
x = gUnaryUnion(v, id = v$cluster, checkValidity = 2)
plot(x)
# # Match row names
row.names(cores) <- as.character(cores@data$cluster_nu)
# o <- sp::over(cores, polygons(x))
# row.names(cores@data) <- 1:nrow(cores@data)
# row.names(x) <- as.character(o)
# row.names(x) <- bound@data$cluster_nu;# row.names(bound@data) <- bound@data$cluster_nu
cn <- 25
dfv <- SpatialPolygonsDataFrame(Sr = x, data = cores@data, match.ID = TRUE)
plot(dfv); plot(dfv[dfv@data$cluster_nu == cn,], col = 'red', add = T); plot(cores[cores@data$cluster_nu == cn, ], col = adjustcolor('blue', alpha.f = 0.6), add = TRUE)

plot(buffers[buffers@data$cluster_nu == cn,])
plot(clusters)
text(coordinates(clusters), labels = clusters@data$cluster_nu, cex =0.7)
plot(clusters[clusters@data$cluster_nu == cn,], add = TRUE, col = 'red')

cols <- sample(rainbow(96), 96)
cols_df <- tibble(cluster_nu = dfv@data$cluster_nu, col = cols)
dfv@data <- left_join(dfv@data, cols_df)
row.names(clusters) <- clusters@data$cluster_nu
clusters@data <- left_join(clusters@data, cols_df)
plot(dfv, col = adjustcolor(dfv$col, alpha.f = 0.7))
plot(clusters, col = clusters$col, add = T)
# plot(buffers[buffers@data$cluster_nu == cn,])
plot(clusters[clusters@data$cluster_nu == cn,], add = T, col = 'red')
plot(dfv[dfv@data$cluster_nu == cn,], add = T, col = adjustcolor('blue', alpha.f = 0.4))
text(coordinates(clusters), labels = clusters@data$cluster_nu, cex =0.7)

# Narrow down so as to only keep those areas which are IN Magude
clusters@data$x <- 1
boundaries <- gConvexHull(clusters)
ll_proj <- proj4string(boundaries)
boundaries_projected <- spTransform(boundaries, crs)
boundaries_projected_buffered <- gBuffer(boundaries_projected, width = 400)
boundaries <- spTransform(boundaries_projected_buffered, ll_proj)

out <- gIntersection(dfv, boundaries, byid=TRUE)


##################
row.names(out) <- row.names(dfv@data)
out <- SpatialPolygonsDataFrame(Sr = out, data = dfv@data)
plot(out)
text(coordinates(out), labels = out@data$cluster_nu)

plot(out, col = out@data$col)
plot(clusters, add = T, col = 'grey')
text(coordinates(out), labels = out@data$cluster_nu)
hh <- rgdal::readOGR('../../data_public/spatial/households/', 'households')
o <- sp::over(hh, polygons(clusters))
hhx <- hh[is.na(o),]
o2 <- sp::over(hhx, polygons(out))
hhx <- hhx[!is.na(o2),]
points(hhx, col = 'red', pch = '.')

o3 <- sp::over(hh, polygons(clusters))
hhz <- hh[!is.na(o3),]


x35 <- x35[order(x35@data$cluster_nu),]
out <- out[order(out@data$cluster_nu),]

row.names(x35) <- x35@data$cluster_nu
row.names(out) <- out@data$cluster_nu



hybrid_list <- list()
for(i in 1:nrow(x35)){
  this_cluster <- x35@data$cluster[i]
  is_problematic <- x35@data$problematic[i]
  message(is_problematic, ' ', this_cluster)
  if(is_problematic){
    out_poly <- out[out@data$cluster_nu == this_cluster,]
  } else {
    out_poly <- x35[x35@data$cluster == this_cluster,]
  }
  out_poly@data <- tibble(cluster = this_cluster)
  hybrid_list[[i]] <- out_poly
}
hybrid <- rbind(hybrid_list[[1]], 
                hybrid_list[[2]],
                makeUniqueIDs = TRUE)
for(i in 3:length(hybrid_list)){
  hybrid <- rbind(hybrid, hybrid_list[[i]], makeUniqueIDs = TRUE)
}
save(hybrid, file = 'hybrid.RData')
plot(hybrid, col = 'darkorange')
plot(clusters, col = adjustcolor('green', alpha.f = 0.5), add = T)
text(coordinates(hybrid), labels = hybrid@data$cluster, cex = 0.5)

# See how many houses are in hybrid
o <- sp::over(hh, polygons(hybrid))
o2 <- sp::over(hh, polygons(clusters))
hhx <- hh[!is.na(o) & is.na(o2),]
points(hhx, pch = '.')

# Plot again
plot(mag2, col = adjustcolor('red', alpha.f = 0.2))
plot(out, add = T, lwd = 0.2)

# Join in info on bairros
dict$full_name <- paste0(dict$posto_administrativo,
                         ': ',
                         dict$bairro_name)
out@data <- left_join(out@data,
                      dict,
                      by = 'bairro_number')
bairros <- left_join(bairros,
                     dict, 
                     by = 'bairro_number')


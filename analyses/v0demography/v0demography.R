

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
ra <- read_csv('kwale/clean-form/sev0ra/sev0ra.csv') %>% dplyr::select(todays_date, Longitude, Latitude, cluster, refusal_or_absence, form_version = FormVersion, hhid, recon_hhid_map_manual, recon_hhid_painted_manual) %>% mutate(version = 'a')
rab <- read_csv('kwale/clean-form/sev0rab/sev0rab.csv') %>% dplyr::select(todays_date, Longitude, Latitude, cluster, refusal_or_absence, form_version = FormVersion, hhid, recon_hhid_map_manual, recon_hhid_painted_manual) %>% mutate(version = 'b')
rx <- bind_rows(ra, rab) %>%
  mutate(id = ifelse(is.na(hhid), recon_hhid_map_manual, hhid)) %>%
  mutate(id = ifelse(is.na(id), recon_hhid_painted_manual, id)) %>%
  mutate(recon_id = ifelse(!is.na(recon_hhid_map_manual), recon_hhid_map_manual, recon_hhid_painted_manual))
  
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
library(ggplot2)
library(rgdal)
library(sp)
library(ggthemes)
clusters <- rgdal::readOGR('../../data_public/spatial/clusters/', 'clusters')
clusters_fortified <- fortify(clusters, id = 'cluster_nu')
load('../../data_public/spatial/pongwe_kikoneni_ramisi.RData')
pongwe_kikoneni_ramisi_fortified <- fortify(pongwe_kikoneni_ramisi, id = 'NAME_3')
cluster_coordinates <- data.frame(coordinates(clusters))
names(cluster_coordinates) <- c('x', 'y')
cluster_coordinates$cluster <- clusters@data$cluster_nu
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

summary(pd$n)
table(pd$n >= 35)
table(pd$n >= 25)
table(pd$n >= 15)

# Recruitment from entire cluster (including buffers)
pd <- people %>%
  dplyr::select(-cluster) %>% # not using the cluster the fw said, instead using actual location
  group_by(cluster = cluster_cluster_number, in_cluster, efficacy_eligible) %>%
  tally %>%
  ungroup %>%
  filter(in_cluster, efficacy_eligible)

summary(pd$n)
table(pd$n >= 35)
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

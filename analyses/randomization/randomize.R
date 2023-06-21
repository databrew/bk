# Libraries
library(dplyr)
library(readr)
library(leaflet)
library(rgdal)
library(sp)

# Based on plot, define each cluster as north or south
clusters <- tibble(
  cluster_number = 1:96,
  location = c(rep('North', 48),
               'South', 'South', 'South', 'North', 'North', 'South',
               'North', 'North',
               rep('South', 40)))


# Load in spatial files
load('../recon_clustering/final/cores.RData')
load('../recon_clustering/final/buffers.RData')

# Plot to see what is above/below the highway
if(FALSE){
  leaflet() %>%
    addTiles() %>%
    addPolygons(data = cores,
                label = cores@data$cluster_number,
                labelOptions = list('permanent' = TRUE,
                                    'autclose' = FALSE))  
  # Inspect
  cores@data <- left_join(cores@data, clusters)
  leaflet() %>%
    addTiles() %>%
    addPolygons(data = cores[cores@data$location == 'North',],
                label = cores@data$cluster_number[cores@data$location == 'North'],
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'blue', color = 'blue'
    ) %>%
    addPolygons(data = cores[cores@data$location == 'South',],
                label = cores@data$cluster_number[cores@data$location == 'South'],
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'red', color = 'red'
    )
}

# North / South  randomization
if('assignments.csv' %in% dir('outputs')){
  assignments <- read_csv('outputs/assignments.csv')
} else {
  # Set a randomization seed
  set.seed(123)
  # Define a list of As and Bs
  assignment_options_north <- c(rep(1, 26), rep(2, 26))
  assignment_options_south <- c(rep(1, 22), rep(2, 22))
  assignment_options_north <- sample(assignment_options_north, size = length(assignment_options_north), replace = FALSE)
  assignment_options_south <- sample(assignment_options_south, size = length(assignment_options_south), replace = FALSE)
  assignments <- tibble(cluster_number = 1:96) %>%
    left_join(clusters) %>%
    arrange(location) %>%
    mutate(assignment = c(assignment_options_north, assignment_options_south))
  # Write a csv
  write_csv(assignments, 'outputs/assignments.csv')
  file.copy('outputs/assignments.csv', '../../data_public/randomization/assignments.csv')
}

# See results
if(FALSE){
  
  # Inspect
  cores@data <- left_join(cores@data, assignments)
  l <- leaflet() %>%
    addTiles() %>%
    addPolygons(data = buffers, fillColor = 'grey', color = 'grey', fillOpacity = 0.5, weight = 0,
                label = buffers@data$cluster_number) %>%
    addPolygons(data = cores[cores@data$assignment == 1,],
                label = cores@data$cluster_number[cores@data$assignment == 1],
                weight = 1,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'blue', color = 'blue'
    ) %>%
    addPolygons(data = cores[cores@data$assignment == 2,],
                label = cores@data$cluster_number[cores@data$assignment == 2],
                weight = 1,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'red', color = 'red'
    )
  
  # See some social science stuff
  # hh <- readOGR('../../data_public/spatial/households/', 'households')
  # hhx <- hh[hh@data$village == 'Kilulu',]
  # l %>% addCircleMarkers(data = hhx, radius = 1, col = 'green') %>%
  #   addCircleMarkers(data = inclusion, radius = 1, col = 'orange')
  # load('../../analyses/recon_clustering/inclusion.RData')
}

###########################
# ENTOMOLOGY
###########################

# https://docs.google.com/document/d/1MNiDi-Jln-CrNrFMgzgReJqSGkIJ1FCSaagQbxcmjOk/edit#heading=h.vo9rz17me9hc

# Start by reading in the "curated" recon data
# https://s3.console.aws.amazon.com/s3/object/databrew.org?region=us-east-1&prefix=kwale/recon/clean-form/reconbhousehold/reconbhousehold.csv
recon <- read_csv('inputs/reconbhousehold.csv')
# https://s3.console.aws.amazon.com/s3/object/databrew.org?region=us-east-1&prefix=kwale/recon/clean-form/reconbhousehold/curated_recon_household_data.csv
recon_curated <- read_csv('inputs/curated_recon_household_data.csv')

# Process households for preparation for household-specific deliverables
# Get sub-counties for use in final output
# load('../recon_clustering/final/clusters.RData')
load('../../data_public/spatial/clusters.RData')

# Get sub-counties
sub_counties <- recon %>%
  group_by(ward, community_health_unit, village, sub_county) %>%
  tally %>%
  ungroup %>%
  arrange(desc(n)) %>%
  dplyr::distinct(ward, .keep_all = TRUE) %>%
  dplyr::select(ward, sub_county) %>%
  filter(!is.na(ward))

# Read in and organize recon data
hhsp <- readOGR('../../data_public/spatial/households/', 'households')
hhsp@data <- left_join(hhsp@data %>% dplyr::rename(hh_id_clean = hh_id) %>% dplyr::select(-village, ward),
                       recon_curated %>% dplyr::select(hh_id_raw, hh_id_clean,
                                                       community_health_unit,
                                                       village,
                                                       ward))
# See if in cluster core / buffer
o <- sp::over(hhsp, polygons(clusters))
hhsp@data$cluster <- clusters@data$cluster_number[o]
o <- sp::over(hhsp, polygons(buffers))
hhsp@data$buffer <- buffers@data$cluster_number[o]
o <- sp::over(hhsp, polygons(cores))
hhsp@data$core <- cores@data$cluster_number[o]
# Keep only those which are in clusters
hhsp <- hhsp[!is.na(hhsp@data$cluster),]
# Define buffer / core status
hhsp@data$core_buffer <- ifelse(!is.na(hhsp@data$core), 'Core',
                                ifelse(!is.na(hhsp@data$buffer), 'Buffer', NA))
hhsp@data$core_buffer <- factor(hhsp@data$core_buffer, levels = c('Core', 'Buffer'))
hhsp <- hhsp[!is.na(hhsp@data$core_buffer),]

# Deliverable 1 ################    
# select 6 clusters per arm, a total of 12 clusters. ALL the hh from these clusters will be metadata for the three Entomology data collection tools. Select only from clusters that have more than 15 hh in the core.
# Deliverable 1: a table named “Table 1_ento_clusters.csv” in which one row is an Ento cluster with the column: 
# Cluster#
# Arm (just the code (1 or 2), not the intervention)
set.seed(18)
if('table_1_ento_clusters.csv' %in% dir('outputs/')){
  ento_clusters <- read_csv('outputs/table_1_ento_clusters.csv')
  if(FALSE){
    # Manual replacement of cluster 47 as per project request, May 31 2023
    # Options are 
    #  2  3  4  9 10 12 15 18 20 24 25 27 28 30 31 34 35 38 41 47 48 52 55
    # sample(c(2, 3, 4, 9, 10, 12, 15, 18, 20, 24, 25, 27, 28, 30, 31, 34, 
    #          35, 38, 41, 47, 48, 52, 55), 1)
    # 52
    ento_clusters$cluster_number[ento_clusters$cluster_number == 47] <- 52
    write_csv(ento_clusters, 'outputs/table_1_ento_clusters.csv')
  }
} else {
  # Get the number of households per core
  hh_per_core <- hhsp@data %>%
    filter(core_buffer == 'Core') %>%
    group_by(cluster) %>%
    tally %>%
    ungroup %>%
    filter(n >= 15)
  # Select 6 clusters per arm
  ento_clusters <- assignments %>%
    # keep only those households which have at least 15 households in the core
    filter(cluster_number %in% hh_per_core$cluster) %>%
    group_by(assignment) %>%
    dplyr::sample_n(6) %>%
    ungroup
  write_csv(ento_clusters, 'outputs/table_1_ento_clusters.csv')
}

# Examine a bit
if(FALSE){
  ento_sp <- buffers
  ento_sp <- ento_sp[ento_sp@data$cluster_number %in% ento_clusters$cluster_number,]
  cores@data <- left_join(cores@data, assignments)
  
  l <- leaflet() %>%
    addTiles() %>%
    addPolygons(data = buffers, fillColor = 'grey', color = 'grey', fillOpacity = 0.2, weight = 1,
                label = buffers@data$cluster_number) %>%
    addPolygons(data = cores[cores@data$assignment == 1,],
                label = cores@data$cluster_number[cores@data$assignment == 1],
                weight = 0,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillOpacity = 0.2,
                fillColor = 'blue', color = 'blue'
    ) %>%
    addPolygons(data = cores[cores@data$assignment == 2,],
                label = cores@data$cluster_number[cores@data$assignment == 2],
                weight = 0,
                fillOpacity = 0.2,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'red', color = 'red') %>%
    addPolygons(data = ento_sp,
                label = ento_sp@data$cluster_number,
                weight = 1,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillOpacity = 0.6,
                fillColor = 'yellow', color = 'purple'
    )
  l
  
  # Write a shapefile
  owd <- getwd()
  dir.create('../../data_public/spatial/ento_clusters')
  setwd('../../data_public/spatial/ento_clusters')
  raster::shapefile(x = ento_sp, file = "ento_clusters.shp", overwrite = TRUE)
  # writeOGR(cores, dsn = '.', layer = 'clusters', driver = "ESRI Shapefile")
  setwd(owd)
  
  # htmltools::save_html(html = l, file = '~/Desktop/kenyaentoclusters.html')
  htmlwidgets::saveWidget(widget = l,
                          file = '~/Desktop/kenyaentoclusters.html',
                          selfcontained = TRUE)
  plot(cores)
  plot(ento_sp, add = T, col = 'red')
  plot(buffers, add = T)
  library(sp)
  raster::text(x = (ento_sp), ento_sp@data$cluster_number)
}


# SPATIAL OBJECTS FOR LOCUS GIS #######################
if(!dir.exists('outputs/general_spatial')){
  dir.create('outputs/general_spatial')
}
if(!file.exists('outputs/general_spatial/clusters.shp')){
  message('Writing shapefile')
  raster::shapefile(clusters, 'outputs/general_spatial/clusters.shp')
} else {
  message('Shapefile already written')
}
if(!file.exists('outputs/general_spatial/households.shp')){
  message('Writing shapefile')
  all_study_households <- hhsp
  all_study_households@data <- all_study_households@data %>%
    dplyr::select(hh_id_clean, hh_id_raw,
                  cluster, Longitude, Latitude, core_buffer)
  raster::shapefile(all_study_households, 'outputs/general_spatial/households.shp')
} else {
  message('Shapefile already written')
}

# Assign cluster number
hhsp@data$cluster_number <- hhsp@data$cluster

# Get list of cleaned IDs for Xing
xing <- hhsp@data %>% dplyr::select(cluster_number, hh_id_clean,
                                    hh_id_raw, core_buffer) %>%
  arrange(cluster_number, hh_id_clean)
# write csv
write_csv(xing, '/tmp/recon_clean_ids.csv')

# Filter down to only those in ento clusters
hhsp <- hhsp[hhsp@data$cluster_number %in% ento_clusters$cluster_number,]
xing <- hhsp@data %>% dplyr::select(cluster_number, hh_id_clean,
                                    hh_id_raw, core_buffer) %>%
  arrange(cluster_number, hh_id_clean)

write_csv(xing, '/tmp/ento_recon_clean_ids.csv')


# Deliverable 2 ################    
#  	Deliverable 2: “Table2_cdc_resting_households_core”: One table for each of the 12 entomology clusters from deliverable 1, in which each row is one household. Randomly order all hh in the core of the cluster. 

# Each table will contain the Cluster # at the top. The columns of the tables will be: 
# 
# Randomization number (row number)
# Painted Recon Hh_id (i.e.; from the Raw Reconnaissance)
# Map Recon Hh_id (i.e.; from the Clean Reconnaissance)
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type

set.seed(17)
if('table_2_cdc_resting_households_core.csv' %in% dir('outputs/')){
  cdc_resting_households_core <- read_csv('outputs/table_2_cdc_resting_households_core.csv')
  if(FALSE){
    # Replacement of cluster 47 by 52 as per entomology team's may 2023 request
    # Done via interactive scripting
  }
} else {
  cdc_resting_households_core <- hhsp@data %>%
    mutate(dummy = 1) %>%
    # keep just core
    filter(core_buffer == 'Core') %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
    arrange(cluster_number, randomization_number)
  write_csv(cdc_resting_households_core, 'outputs/table_2_cdc_resting_households_core.csv')
  # Create a supporting document of entomology table 2 (ie, 1 table per cluster, formatted, etc.)
  rmarkdown::render('rmds/entomology_table_2_cdc_resting_households_core.Rmd')
}

# # # Deliverable 3 ################   
# One table for each of the 12 entomology clusters from deliverable 1 in which each row is one household. Randomly order all hh in the buffer of the cluster. 
# 
# Each table will contain the Cluster # at the top. The columns of the table will be:
# 
# Randomization number (row number)
# Painted Recon Hh_id (i.e.; from the Raw Reconnaissance)
# Map Recon Hh_id (i.e.; from the Clean Reconnaissance)
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type
set.seed(17)
if('table_3_cdc_resting_households_buffer.csv' %in% dir('outputs/')){
  cdc_resting_households_buffer <- read_csv('outputs/table_3_cdc_resting_households_buffer.csv')
} else {
  cdc_resting_households_buffer <- hhsp@data %>%
    mutate(dummy = 1) %>%
    # keep just core
    filter(core_buffer == 'Buffer') %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
    arrange(cluster_number, randomization_number)
  write_csv(cdc_resting_households_buffer, 'outputs/table_3_cdc_resting_households_buffer.csv')
  # Create a supporting document of entomology table 2 (ie, 1 table per cluster, formatted, etc.)
  rmarkdown::render('rmds/entomology_table_3_cdc_resting_households_buffer.Rmd')
}


# # Deliverable 4 ################   
# Table 4 resting household pit shelter

if('table_4_resting_household_pit_shelter.csv' %in% dir('outputs')){
  resting_household_pit_shelter <- read_csv('outputs/table_4_resting_household_pit_shelter.csv')
} else {
  resting_household_pit_shelter <-  hhsp@data %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    # Keep only core
    filter(core_buffer == 'Core') %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep just 3 per cluster (ie, 1 selection plus 2 backups)
    filter(randomization_number <= 3) %>%
    # left_join(sub_counties) %>%
    # # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  # sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
    arrange(cluster_number, randomization_number)
  
  resting_household_pit_shelter_b <-  hhsp@data %>%
    filter(!hh_id_raw %in% resting_household_pit_shelter$painted_recon_hh_id,
           !hh_id_clean %in% resting_household_pit_shelter$map_recon_hh_id) %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    # Keep only core
    filter(core_buffer == 'Core') %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy) + 3) %>%
    ungroup %>%
    # Keep just 3 per cluster (ie, 1 selection plus 2 backups)
    filter(randomization_number <= 10 & randomization_number >= 4) %>%
    # left_join(sub_counties) %>%
    # # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  # sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
    arrange(cluster_number, randomization_number)
  resting_household_pit_shelter <- resting_household_pit_shelter %>%
    bind_rows(resting_household_pit_shelter_b) %>%
    arrange(cluster_number, randomization_number)
  
  write_csv(resting_household_pit_shelter, 'outputs/table_4_resting_household_pit_shelter.csv')
  rmarkdown::render('rmds/resting_household_pit_shelter.Rmd')
}

# # Deliverable 5 ################   
# Deliverable 5: Table5_cdc_light_trap_livestock_enclosure: one table per cluster in which one row is one household and ALL the households of the cluster are ordered randomly. Each table will contain at the top: 
#   Cluster #
# L(cluster #)-11
#   E.g. Cluster 76, L76-11, Cluster 05, L05-11
if('table_5_cdc_light_trap_livestock_enclosures.csv' %in% dir('outputs')){
  cdc_light_trap_livestock_enclosures <- read_csv('outputs/table_5_cdc_light_trap_livestock_enclosures.csv')
} else {
  cdc_light_trap_livestock_enclosures <-  hhsp@data %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # left_join(sub_counties) %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  # core_buffer,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  # sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
    arrange(cluster_number, randomization_number)
  
  write_csv(cdc_light_trap_livestock_enclosures, 'outputs/table_5_cdc_light_trap_livestock_enclosures.csv')
  rmarkdown::render('rmds/cdc_light_trap_livestock_enclosures.Rmd')
}

# Deliverable 9, geographic files
#####################################################################
# Generate spatial files and then manually copy-paste to this google drive:
# https://drive.google.com/drive/folders/1pVEcZzPevVCcHe4Sc4lAn5Gmri1xR_RB?usp=sharing
if(!dir.exists('outputs/ento_households_shp')){
  dir.create('outputs/ento_households_shp')
}
if(!dir.exists('outputs/cores_shp')){
  dir.create('outputs/cores_shp')
}
if(!dir.exists('outputs/buffers_shp')){
  dir.create('outputs/buffers_shp')
}

if(!file.exists('outputs/ento_households_shp/households.shp')){
  message('Writing shapefile')  
  households <- hhsp
  # households <- households[households@data$cluster_number %in% ento_clusters$cluster_number,]
  households@data <- households@data %>% dplyr::select(cluster_number, core_buffer, hh_id_clean, hh_id_raw, Longitude, Latitude)
  raster::shapefile(households, 'outputs/ento_households_shp/households.shp', overwrite = TRUE)
} else {
  message('Shapefile already written')
}

if(!file.exists('outputs/buffers_shp/buffers_shp.shp')){
  message('Writing shapefile')  
  buffers_shp <- buffers[buffers@data$cluster_number %in% ento_clusters$cluster_number,]
  raster::shapefile(buffers_shp, 'outputs/buffers_shp/buffers_shp.shp', overwrite = TRUE)
} else {
  message('Shapefile already written')
}

if(!file.exists('outputs/cores_shp/cores_shp.shp')){
  message('Writing shapefile')
  cores_shp <- cores[cores@data$cluster_number %in% ento_clusters$cluster_number,]
  raster::shapefile(cores_shp, 'outputs/cores_shp/cores_shp.shp', overwrite = TRUE)
} else {
  message('Shapefile already written')
}

# Assign arms 1 and 2 to control / intervention
set.seed(321)
if('intervention_assignment.csv' %in% dir('outputs')){
  intervention_assignment <- read_csv('outputs/intervention_assignment.csv')
} else {
  intervention_assignment <- tibble(arm = sample(1:2, 2),
                                    intervention = c('Control', 'Treatment'))
  write_csv(intervention_assignment, 'outputs/intervention_assignment.csv')
}

##########################
# DELIVERABLE 7
##########################
# Deliverable 7: “Ento_enrolled_households” Table of all houses enrolled for Entomology after round 1 of Ento work (from round 2 onwards Caroline Kiuru will generate this list herself if any hh has changed): One table that includes all clusters in which one row is one household. This table is generated using the data collected from the Screening form where the answer to “What are you screening?” == household and “Has the household head or household head substitute agreed to participate to mosquito collections“  == Yes
# 
# The columns of the list will be: 
#   Cluster
# Collection method (□ CDC-light trap □ Resting household indoor □ Resting household pit-shelter)
# Recon Hh_id entered in the Screening form (Map HhID: “Write the Recon5-character household ID of the pin that you see in the map”)
# New Hh ID assigned
# Core/buffer
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type

# Read in the cleaned ento screening results (generated in scripts/ento_screening)
es <- read_csv('inputs/entoscreening_households_cleaned.csv')
if(!file.exists('outputs/table_7_ento_enrolled_households.csv')){
  out <- es %>%
    mutate(longitude = `hh_geolocation-Longitude`,
           latitude = `hh_geolocation-Latitude`) %>%
    mutate(x = longitude, y = latitude)
  coordinates(out) <- ~x+y
  proj4string(out) <- proj4string(clusters)
  o <- sp::over(out, polygons(clusters))
  out@data$cluster <- clusters@data$cluster_number[o]
  # Examine those not in cluster
  if(FALSE){
    bad <- out[is.na(out@data$cluster),]
    right <- recon[recon$hh_id %in% bad$recon_hhid_map,]
    combined <- bind_rows(
      bad@data %>%
        dplyr::rename(new_hhid = hhid) %>%
        dplyr::select(hhid = recon_hhid_map, longitude, latitude) %>% mutate(type = 'Ento screening'),
      right %>% dplyr::select(hhid = hh_id, longitude = Longitude,
                              latitude = Latitude) %>% mutate(type = 'Recon')
    ) %>%
      mutate(color_number = as.numeric(factor(hhid)))
    cols <- rainbow(length(unique(combined$color_number)))
    combined$col <- cols[combined$color_number]
    leaflet() %>%
      addTiles() %>%
      addPolygons(data = clusters, label = clusters$cluster_number) %>%
      addCircleMarkers(data = combined, col = combined$col, 
                       popup = combined$hhid,
                       label = combined$hhid)
    library(ggplot2)
    clusters_fortified <- fortify(clusters[clusters$cluster_number %in% c(43,47),], id = clusters$cluster_number)
    
    ggplot() +
      geom_polygon(data = clusters_fortified,
                   aes(x = long,
                       y =lat,
                       group = group),
                   color = 'black',
                   alpha = 0.5) +
      geom_point(data = combined,
                 aes(x = longitude,
                     y = latitude,
                     group = hhid,
                     color = hhid),
                 size = 3) +
      geom_path(data = combined,
                 aes(x = longitude,
                     y = latitude,
                     group = hhid,
                     color = hhid)) +
      theme_bw() +
      theme(legend.position = 'bottom')
  }
  # REMOVE THOSE NOT IN A CLUSTER
  outside <- out[is.na(out@data$cluster),]
  plot(outside, pch = 2, cex = 2, col = 'blue')
  plot(clusters, add = T, col = 'red')
  text(coordinates(clusters), label = clusters@data$cluster_number)
  text(coordinates(outside), label = outside@data$hhid)
  outside@data %>% dplyr::select(recon_hhid_map, hhid, wid) %>% arrange(hhid)
  out <- out[!is.na(out@data$cluster),]
  # See whether in core or buffer
  o <- sp::over(out, polygons(cores))
  out@data$in_core <- !is.na(o)
  out@data$core_or_buffer <- ifelse(out@data$in_core, 'Core', 'Buffer')
  # Create the requested columns
  ento_enrolled_households <- out@data %>%
    dplyr::select(cluster,
                  hhid,
                  collection_method = hh_collection,
                  hh_id = recon_hhid_map,
                  core_or_buffer,
                  ward,
                  community_health_unit,
                  village,
                  longitude,
                  latitude) %>%
    left_join(recon %>% dplyr::select(hh_id,
                                      wall_type = house_wall_material,
                                      roof_type)) %>%
    # remove the recon id
    dplyr::rename(recon_id = hh_id) %>%
    arrange(cluster, hhid)
  # Remove (invalid) cluster 47
  ento_enrolled_households <- ento_enrolled_households %>%
    filter(cluster != 47)
  write_csv(ento_enrolled_households, 'outputs/table_7_ento_enrolled_households.csv')
} else {
  ento_enrolled_households <- read_csv('outputs/table_7_ento_enrolled_households.csv')
}


##########################
# DELIVERABLE 8
##########################
# Deliverable 8: “Ento_enrolled_livestock enclosures” Table of all livestock enclosures enrolled for Entomology. One table that includes all clusters in which one row is one household. This table is generated using the data collected from the Screening form where the answer to “What are you screening?” == Livestock enclosure and “Has the owner of the livestock enclosure or representative given informed consent?” == yes
# The columns of the list will be: 
#   Cluster
# Hh_id (i.e.; Clean Recon HhId of the closest hh from the LE GPS registered)
# Livestock enclosure ID
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type
# Download entoscreeningke.zip from databrew.org
# unzip('entoscreeningke.zip')
# Read
if(file.exists('outputs/table_8_entomology_enrolled_livestock_enclosures.csv')){
  le <- read_csv('outputs/table_8_entomology_enrolled_livestock_enclosures.csv')
} else {
  # entoscreeningke <- read_csv('../../scripts/ento_screening/entoscreeningke.csv')
  entoscreeningke <- read_csv('../../scripts/ento_screening/kwale/clean-form/entoscreeningke/entoscreeningke.csv')
  
  out <- entoscreeningke %>%
    filter(site == 'Livestock enclosure',
           le_owner_consent == 'yes') %>%
    mutate(longitude = `Longitude`,
           latitude = `Latitude`) %>%
    mutate(x = longitude, y = latitude)
  # Get which cluster each is in
  out_sp <- out
  coordinates(out_sp) <- ~x+y
  proj4string(out_sp) <- proj4string(hhsp)
  o <- sp::over(out_sp, polygons(clusters))
  out_sp@data$cluster <- clusters@data$cluster_number[o]
  # Get nearest household
  recon_curated_sp <- recon_curated %>%
    left_join(recon %>% dplyr::select(instanceID, latitude = Latitude,
                                      longitude = Longitude,
                                      ward,
                                      community_unit = community_health_unit,
                                      village,
                                      wall_type = house_wall_material,
                                      roof_type)) %>%
    mutate(x = longitude, y = latitude)
  coordinates(recon_curated_sp) <- ~x+y
  proj4string(recon_curated_sp) <- proj4string(hhsp)
  distances <- rgeos::gDistance(out_sp, recon_curated_sp, byid = TRUE)
  min_distances <- apply(distances, 2, which.min)
  out_sp@data$nearest_household <- recon_curated_sp$hh_id_clean[min_distances]
  out <- out_sp@data %>%
    dplyr::select(cluster,
                  hh_id = nearest_household,
                  livestock_enclosure_id = leid,
                  ward,
                  community_unit = community_health_unit,
                  village,
                  longitude,
                  latitude) %>%
    # get wall type and roof type based on nearest household
    left_join(recon_curated_sp@data %>% dplyr::select(hh_id = hh_id_clean,
                                                      roof_type,
                                                      wall_type))
  le <- out
  write_csv(le, 'outputs/table_8_entomology_enrolled_livestock_enclosures.csv')
}

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
  text(ento_sp@data$cluster_number)
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

# Filter down to only those in ento clusters
hhsp@data$cluster_number <- hhsp@data$cluster
hhsp <- hhsp[hhsp@data$cluster_number %in% ento_clusters$cluster_number,]

# Get list of cleaned IDs for Xing
xing <- hhsp@data %>% dplyr::select(cluster_number, hh_id_clean,
                                    hh_id_raw, core_buffer) %>%
  arrange(cluster_number, hh_id_clean)
write_csv(xing, '/tmp/ento_recon_clean_ids.csv')


# Deliverable 2 ################    
#  One table for each of the 12 entomology per clusters from deliverable 1, in which each row is one household. The team will enroll a total of 4 hh per cluster. Randomly select: 
set.seed(17)
if('table_2_cdc_light_trap_households.csv' %in% dir('outputs/')){
  cdc_light_trap_households <- read_csv('outputs/table_2_cdc_light_trap_households.csv')
} else {

  # "The team will enroll a total of 4 hh per cluster. Randomly select: 
  # 2 hh in the core + 2 hh extra in the core
  # 2 hh in the buffer + 2 hh extra in the buffer"
  cdc_light_trap_households <- hhsp@data %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(core_buffer, cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep just 4 per category
    filter(randomization_number <= 4) %>%
    left_join(sub_counties) %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  core_buffer,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
    arrange(cluster_number, core_buffer, randomization_number)
  write_csv(cdc_light_trap_households, 'outputs/table_2_cdc_light_trap_households.csv')
  # Create a supporting document of entomology table 2 (ie, 1 table per cluster, formatted, etc.)
  rmarkdown::render('rmds/entomology_cdc_light_trap_households.Rmd')
}



# # Deliverable 3 ################   
# Resting household indoor
# One table per entomology cluster in which each row is one household. The team will enroll a total of 6 hh per cluster. Exclude households randomized to Deliverable 2: “Table2_cdc_light_trap_household” Randomly select: 
# 4 hh in the core + 4 hh extra in the core
# 2 hh in the buffer + 2 hh extra in the buffer
# Each table will contain the Cluster # at the top.

set.seed(25)
if('table_3_resting_household_indoor_households.csv' %in% dir('outputs/')){
  resting_household_indoor_households <- read_csv('outputs/table_3_resting_household_indoor_households.csv')
} else {
  resting_household_indoor_households <- hhsp@data %>%
    # Remove those in table 2
    filter(!hh_id_raw %in% cdc_light_trap_households$painted_recon_hh_id,
           !hh_id_clean %in% cdc_light_trap_households$map_recon_hh_id) %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(core_buffer, cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep just 8 per category if core
    # and 4 per category if buffer
    filter(
      (randomization_number <= 8 & core_buffer == 'Core') |
        (randomization_number <= 4 & core_buffer == 'Buffer')
    ) %>%
    left_join(sub_counties) %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  core_buffer,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
    arrange(cluster_number, core_buffer, randomization_number)  
  write_csv(resting_household_indoor_households, 'outputs/table_3_resting_household_indoor_households.csv')
  rmarkdown::render('rmds/resting_household_indoor_households.Rmd')
  
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
    left_join(sub_counties) %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wal0,
                  roof_type) %>%
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
    # Keep only core
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    left_join(sub_counties) %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  # core_buffer,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  sub_county,
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
  raster::shapefile(buffers_shp, 'outputs/buffers_shp/buffers_shp.shp')
} else {
  message('Shapefile already written')
}

if(!file.exists('outputs/cores_shp/cores_shp.shp')){
  message('Writing shapefile')
  cores_shp <- cores[cores@data$cluster_number %in% ento_clusters$cluster_number,]
  raster::shapefile(cores_shp, 'outputs/cores_shp/cores_shp.shp')
} else {
  message('Shapefile already written')
}


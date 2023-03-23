# Libraries
library(dplyr)
library(readr)
library(leaflet)
library(rgdal)
library(sp)
# # Load in village shapefiles
# villages <- readOGR('../../data_public/spatial/village_shapefile/', 'village')


  # Load in clusters
load('../recon_clustering/final/cores.RData')
load('../recon_clustering/final/buffers.RData')
load('../recon_clustering/final/clusters.RData')

# Plot to see what is above/below the highway
leaflet() %>%
  addTiles() %>%
  addPolygons(data = cores,
              label = cores@data$cluster_number,
              labelOptions = list('permanent' = TRUE,
                                  'autclose' = FALSE))

# # See shapefile versions
# villages <- readOGR('../../data_public/spatial/village_shapefile/', 'village')
# cores2 <- readOGR('../../data_public/spatial/clusters/', 'clusters')
# plot(cores2); text(coordinates(cores2), label = cores2@data$cluster_nu)
# cores3 <- readOGR('~/Desktop/cores/', 'cores')
# plot(cores3); text(coordinates(cores3), label = cores2@data$cluster_nu)
# buffers2 <- readOGR('../../data_public/spatial/buffers/', 'buffers')
# plot(buffers2); text(coordinates(buffers2), label = buffers2@data$cluster_nu)
# buffers3 <- readOGR('~/Desktop/buffers/', 'buffers')
# plot(buffers3); text(coordinates(buffers3), label = buffers3@data$clstr_n)
# clusters2 <- readOGR('../../data_public/spatial/clusters/', 'clusters')
# plot(clusters2); text(coordinates(clusters2), label = clusters2@data$cluster_nu)


# Based on plot, define each cluster as north or south
clusters <- tibble(
  cluster_number = 1:96,
  location = c(rep('North', 48),
               'South', 'South', 'South', 'North', 'North', 'South',
               'North', 'North',
               rep('South', 40))
)


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



if('assignments.csv' %in% dir()){
  assignments <- read_csv('assignments.csv')
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
  write_csv(assignments, 'assignments.csv')
  file.copy('assignments.csv', '../../data_public/randomization/assignments.csv')
}

# See results
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

# hh <- readOGR('../../data_public/spatial/households/', 'households')
# hhx <- hh[hh@data$village == 'Kilulu',]
# l %>% addCircleMarkers(data = hhx, radius = 1, col = 'green') %>%
#   addCircleMarkers(data = inclusion, radius = 1, col = 'orange')
# load('../../analyses/recon_clustering/inclusion.RData')

###########################
# ENTOMOLOGY
###########################

# https://docs.google.com/document/d/1MNiDi-Jln-CrNrFMgzgReJqSGkIJ1FCSaagQbxcmjOk/edit#heading=h.vo9rz17me9hc

# Start by reading in the "curated" recon data
# https://s3.console.aws.amazon.com/s3/object/databrew.org?region=us-east-1&prefix=kwale/recon/clean-form/reconbhousehold/reconbhousehold.csv
recon <- read_csv('inputs/reconbhousehold.csv')
# https://s3.console.aws.amazon.com/s3/object/databrew.org?region=us-east-1&prefix=kwale/recon/clean-form/reconbhousehold/curated_recon_household_data.csv
recon_curated <- read_csv('inputs/curated_recon_household_data.csv')

# Deliverable 1 ################    
# select 6 clusters per arm, a total of 12 clusters. ALL the hh from these clusters will be metadata for the three Entomology data collection tools
# Deliverable 1: a table named “Table 1_ento_clusters.csv” in which one row is an Ento cluster with the column: 
# Cluster#
# Arm (just the code (1 or 2), not the intervention)
set.seed(17)
if('table_1_ento_clusters.csv' %in% dir('outputs/')){
  ento_clusters <- read_csv('outputs/table_1_ento_clusters.csv')
} else {
  # Select 6 clusters per arm
  ento_clusters <- assignments %>%
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

# Deliverable 2 ################    
#  One table for each of the 12 entomology per clusters from deliverable 1, in which each row is one household. The team will enroll a total of 4 hh per cluster. Randomly select: 
set.seed(17)
if('table_2_cdc_light_trap_households.csv' %in% dir('outputs/')){
  cdc_light_trap_households <- read_csv('outputs/table_2_cdc_light_trap_households.csv')
} else {
  # Get sub-counties for use in final output
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
  # Filter down to only those in ento clusters
  hhsp@data <- left_join(
    hhsp@data %>% dplyr::rename(cluster_number = cluster),
    ento_clusters %>% dplyr::select(cluster_number) %>% mutate(ento = TRUE)
  ) %>%
    filter(!is.na(ento))
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
}

# Create a supporting document of entomology table 2 (ie, 1 table per cluster, formatted, etc.)
rmarkdown::render('rmds/entomology_cdc_light_trap_households.Rmd', envir = new.nev())


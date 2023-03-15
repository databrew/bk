# Libraries
library(dplyr)
library(readr)
library(leaflet)
library(rgdal)

# # Load in village shapefiles
# villages <- readOGR('../../data_public/spatial/village_shapefile/', 'village')


  # Load in clusters
load('../recon_clustering/final/cores.RData')
load('../recon_clustering/final/buffers.RData')

# Plot to see what is above/below the highway
leaflet() %>%
  addTiles() %>%
  addPolygons(data = cores,
              label = cores@data$cluster_number,
              labelOptions = list('permanent' = TRUE,
                                  'autclose' = FALSE))

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

# Deliverable 1 ################    
# select 6 clusters per arm, a total of 12 clusters. ALL the hh from these clusters will be metadata for the three Entomology data collection tools
# Deliverable 1: a table named “Table 1_ento_clusters.csv” in which one row is an Ento cluster with the column: 
# Cluster#
# Arm (just the code (1 or 2), not the intervention)
set.seed(17)
if('ento_clusters.csv' %in% dir()){
  ento_clusters <- read_csv('ento_clusters.csv')
} else {
  # Select 6 clusters per arm
  ento_clusters <- assignments %>%
    group_by(assignment) %>%
    dplyr::sample_n(6) %>%
    ungroup
  write_csv(ento_clusters, 'ento_clusters.csv')
  file.copy('ento_clusters.csv', '../../data_public/randomization/ento_clusters.csv')
}

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

plot(cores)
plot(ento_sp, add = T, col = 'red')
text(ento_sp@data$cluster_number)

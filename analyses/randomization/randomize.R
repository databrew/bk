# Libraries
library(dplyr)
library(readr)
library(leaflet)

  # Load in clusters
load('../recon_clustering/final/cores.RData')

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
              label = cores@data$cluster_number,
              # labelOptions = list('permanent' = TRUE,
              #                     'autclose' = FALSE),
              fillColor = 'blue', color = 'blue'
              ) %>%
  addPolygons(data = cores[cores@data$location == 'South',],
              label = cores@data$cluster_number,
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


###########################
# ENTOMOLOGY
###########################

# https://docs.google.com/document/d/1MNiDi-Jln-CrNrFMgzgReJqSGkIJ1FCSaagQbxcmjOk/edit#heading=h.vo9rz17me9hc

# Deliverable 1 ################    
# select 6 clusters per arm, a total of 12 clusters. ALL the hh from these clusters will be metadata for the three Entomology data collection tools
# Deliverable 1: a table named “Table 1_ento_clusters.csv” in which one row is an Ento cluster with the column: 
# Cluster#
# Arm (just the code (1 or 2), not the intervention)
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

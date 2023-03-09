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
}

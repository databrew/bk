# Libraries
library(dplyr)
library(readr)
library(leaflet)
library(rgdal)
library(sp)

# Read in randomization outputs
assignments <- read_csv('../randomization/outputs/assignments.csv')

# Read in spatial clusters
load('../../data_public/spatial/clusters.RData')

# Read in recon results
recon <- read_csv('../randomization/inputs/reconbhousehold.csv')
# https://s3.console.aws.amazon.com/s3/object/databrew.org?region=us-east-1&prefix=kwale/recon/clean-form/reconbhousehold/curated_recon_household_data.csv
recon_curated <- read_csv('../randomization/inputs/curated_recon_household_data.csv')

# Read in and organize recon spatial data
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

# Get the assignment for each household
hhsp@data <- left_join(hhsp@data,
                       assignments,
                       by = c('cluster' = 'cluster_number'))

# Bring in demographic information
demographic <- left_join(recon_curated,
                         recon %>%
                           dplyr::select(num_hh_members, num_hh_members_lt_5,
                                         num_hh_members_gt_15,
                                         num_hh_members_5_15 = `15`,
                                         instanceID)) %>%
  dplyr::select(hh_id_clean, num_hh_members, num_hh_members_lt_5,
                num_hh_members_gt_15,
                num_hh_members_5_15)
hhsp@data <- left_join(hhsp@data,
                       demographic)

# Number of people per arm
hhsp@data %>%
  group_by(assignment) %>%
  summarise(people = sum(num_hh_members)) %>%
  knitr::kable()

hhsp@data %>%
  group_by(assignment) %>%
  summarise(people_five_plus = sum(num_hh_members_5_15) + sum(num_hh_members_gt_15)) %>%
  knitr::kable()

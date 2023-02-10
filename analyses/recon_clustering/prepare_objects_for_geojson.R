library(sf)
library(dplyr)
library(sp)
load('households_spatial.RData')
load('final/clusters.RData')


# households = st_as_sf(households_spatial)
# st_write(households, "~/Desktop/households.geojson") 

clusters@data$id <- clusters@data
clusters = st_as_sf(clusters)
st_write(clusters, '~/Desktop/clusters.geojson')

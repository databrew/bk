library(readr)
library(dplyr)
library(usethis)
library(raster)
library(rgdal)
library(sp)
library(tidyverse)
library(RColorBrewer)
library(raster)
library(sf)
library(readxl)

ken0 <- raster::getData(name = 'GADM', country = 'KEN', level = 0)

# Load the study area
load('../recon_clustering/final/pongwe_kikoneni_ramisi.RData')
shps <- pongwe_kikoneni_ramisi
# Download shapefile from https://download.geofabrik.de/africa/kenya.html

setwd(paste0('kenya-latest-free'))
# Get OSM data
files <- dir()
files <- files[grepl('.shp', files, fixed = TRUE)]
files <- files[!grepl('zip', files)]
files <- gsub('.shp', '', files, fixed = TRUE)
files <- files[grepl('roads|water', files)]

this_shp <- shps
this_shp <- sf::st_as_sf(this_shp)
for(i in 1:length(files)){
  this_file <- files[i]
  message(i, ' of ', length(files), ': ', this_file)
  
  processed_file <- paste0('osm/processed_osms/',
                           this_file,
                           '.RData')
  try({
    if(!dir.exists('osm')){
      dir.create('osm')
      setwd('osm')
      dir.create('processed_osms')
      setwd('..')  
    }
    
    # x <- readOGR('osm', this_file)
    y <- st_read('.', this_file)
    valid <- sf::st_is_valid(y)
    y <- y[valid,]
    st_crs(y) <- st_crs(this_shp)
    keep <- st_within(y, this_shp)
    k <- !is.na(as.numeric(keep))
    x <- y[k,]
    x <- as_Spatial(x)
    object_name <- paste0(gsub('gis_osm_|_free_1', '', this_file))
    assign(object_name,
           x,
           envir = .GlobalEnv)
  })
}
setwd('../..')

save(roads, file = '../analyses/recon_clustering/final/roads.RData')


library(dplyr)
library(readr)
library(ggplot2)
library(sp)
library(raster)
library(rgdal)
library(geodata)
library(ggthemes)

# Africa
afr <- readOGR('africa', 'afr_g2014_2013_0')
afr_fortified <- fortify(afr, id = 'ADM0_CODE')

# Kenya
kenya <- geodata::gadm(country = 'KEN', level = 0, path = '.')
kenya <- as(kenya, 'Spatial')
kenya_fortified <- fortify(kenya, id = 'GID_0')

# Plot Africa + Kenya
ggplot() +
  geom_polygon(data = afr_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', 
               color = 'black',
               size = 0.5) +
  theme_map() +
  geom_polygon(data = kenya_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'darkred', alpha = 0.7)

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
kenya3 <- geodata::gadm(country = 'KEN', level = 3, path = '.')
kenya3 <- as(kenya3, 'Spatial')
kenya3_fortified <- fortify(kenya3, id = 'NAME_3')
kenya2 <- geodata::gadm(country = 'KEN', level = 2, path = '.')
kenya2 <- as(kenya2, 'Spatial')
kenya2_fortified <- fortify(kenya2, id = 'NAME_2')
kenya1 <- geodata::gadm(country = 'KEN', level = 1, path = '.')
kenya1 <- as(kenya1, 'Spatial')
kenya1_fortified <- fortify(kenya1, id = 'NAME_1')

# Kwale
kwale <- kenya1[kenya1@data$NAME_1 == 'Kwale',]
kwale_fortified <- fortify(kwale, id = 'NAME_1')

# pongwe kikoneni
pw <- kenya3[kenya3@data$NAME_3 == 'Pongwe/Kikoneni',]
ramisi <- kenya3[kenya3@data$NAME_3 == 'Ramisi',]
pw_fortified <- fortify(pw, id = 'NAME_3')
ramisi_fortified <- fortify(ramisi, id = 'NAME_3')

# Clusters
load('../../data_public/spatial/clusters.RData')
old_clusters <- clusters
# New (reduced) clusters
load('../../data_public/spatial/new_clusters.RData')
new_clusters_foritifed <- fortify(new_clusters, id = 'cluster_nu')
old_clusters_fortified <- fortify(old_clusters, id = 'cluster_number')

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

# Plot Kwale in Kenya
ggplot() +
  theme_map() +
  geom_polygon(data = kenya_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', color = 'black', size = 0.5) +
  geom_polygon(data = kwale_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'darkred', alpha = 0.7, color = 'black')


# Clusters in kwale
ggplot() +
  theme_map() +
  geom_polygon(data = pw_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', color = 'black', size = 0.5) +
  geom_polygon(data = ramisi_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', color = 'black', size = 0.5) +
  geom_polygon(data = new_clusters_foritifed,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'darkred', alpha = 0.7, color = 'black')


# New vs old in kwale
ggplot() +
  theme_map() +
  geom_polygon(data = pw_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', color = 'black', size = 0.5) +
  geom_polygon(data = ramisi_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', color = 'black', size = 0.5) +
  geom_polygon(data = new_clusters_foritifed,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'darkred', alpha = 0.7, color = 'black')

# New vs old in kwale
ggplot() +
  theme_map() +
  geom_polygon(data = pw_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', color = 'black', size = 0.5) +
  geom_polygon(data = ramisi_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'grey', color = 'black', size = 0.5) +
  geom_polygon(data = old_clusters_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'white', alpha = 1, color = 'black') +
  geom_polygon(data = new_clusters_foritifed,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'darkred', alpha = 0.7, color = 'black')

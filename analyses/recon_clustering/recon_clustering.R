library(paws)
library(dplyr)
library(readr)
library(ggplot2)
library(rgdal)
library(raster)
library(ggthemes)
library(sp)
library(rgeos)
library(maptools)
Sys.setenv('AWS_PROFILE' = 'dbrew-prod')

fresh_data <- FALSE

if(fresh_data){
  ken3 <- raster::getData(name = 'GADM', download = TRUE, country = 'KEN', level = 3)
  ken3_fortified <- fortify(ken3, regions = ken3@data$NAME_3)
  kwale <- ken3[ken3@data$NAME_1 == 'Kwale',]
  kwale_fortified <- fortify(kwale, regions = ken3@data$NAME_3)
  pongwe_kikoneni <- ken3[ken3@data$NAME_3 == 'Pongwe/Kikoneni',]
  pongwe_kikoneni_fortified <- fortify(pongwe_kikoneni, regions = ken3@data$NAME_3)
  pongwe_kikoneni_ramisi <- ken3[ken3@data$NAME_3 %in% c('Pongwe/Kikoneni', 'Ramisi'),]
  pongwe_kikoneni_ramisi_fortified <- fortify(pongwe_kikoneni_ramisi, regions = pongwe_kikoneni_ramisi@data$NAME_3)
  
  # aws sso login --profile dbrew-prod
  s3obj <- paws::s3()
  # s3obj$list_buckets()
  
  # Read in recon functions
  recon_functions_dir <- '../../../recon-monitoring/R/'
  recon_functions <- dir(recon_functions_dir)
  for(i in 1:length(recon_functions)){
    source(paste0(recon_functions_dir, '/', recon_functions[i]))
  }
  households <- get_household_forms()  
  save(households,
       pongwe_kikoneni_fortified,
       pongwe_kikoneni,
       pongwe_kikoneni_ramisi,
       pongwe_kikoneni_ramisi_fortified,
       kwale_fortified,
       kwale,
       ken3_fortified,
       ken3,
       file = 'data.RData')
} else {
  load('data.RData')
}
load('wards/kenya_wards.RData')

# Read in geographic data sent from Zinde for zones of explicit 
# exclusion and inclusion
# exclusion <- sf::st_read('from_zinde/RamisiMines/RamisiMines.shp')
exclusion <- readOGR('from_zinde/RamisiMines/', 'RamisiMines')

# Excluding urban areas per Carlos' instruction
ramisi_urban <- readxl::read_excel('from_zinde/RamisiUrban.xlsx')

# Inclusion areas
# Per Carlos Chaccour, no need to include all households in the village: https://bohemiakenya.slack.com/archives/C042P3A05UP/p1674731483701879?thread_ts=1674730627.631939&cid=C042P3A05UP
mkonga <- readOGR('from_zinde/Mkonga/', 'Mkonga')
mkonga@data$src <- 'Mkonga'
mwabandari <- readOGR('from_zinde/Mwabandari/', 'Mwabandari')
mwabandari@data$src <- 'Mwabandari'
nguzo_a <- readOGR('from_zinde/Nguzo_a/', 'Nguzo_a')
nguzo_a@data$src <- 'Nguzo_a'
inclusion <- rbind(mkonga, mwabandari)
inclusion <- rbind(inclusion, nguzo_a)
# Get centroids
coords <- coordinates(inclusion)
coords_df <- data.frame(coords)
names(coords_df) <- c('lat', 'lng')
coords_df$src <- inclusion@data$src
inclusion_centroids <- coords_df %>%
  group_by(src) %>%
  summarise(lng = mean(lng),
            lat = mean(lat))

ggplot() +
  geom_point(data = coords_df,
             aes(x = lng,
                 y = lat,
                 color = src),
             alpha = 0.2) +
  geom_point(data = inclusion_centroids,
             aes(x = lng,
                 y = lat,
                 color = src),
             pch = 5,
             size = 5) +
  theme(legend.position = 'bottom') +
  scale_color_manual(name = '', values = c('red', 'orange', 'black'))

  


# Define function for adding zeros prefix to strings
add_zero <- function (x, n) {
  x <- as.character(x)
  adders <- n - nchar(x)
  adders <- ifelse(adders < 0, 0, adders)
  for (i in 1:length(x)) {
    if (!is.na(x[i])) {
      x[i] <- paste0(paste0(rep("0", adders[i]), collapse = ""), 
                     x[i], collapse = "")
    }
  }
  return(x)
}

# Total residents
sum(households$num_hh_members, na.rm = TRUE)
# 5 to 15 year-olds
sum(households$num_hh_members_bt_5_15, na.rm = TRUE)
pd <- households %>%
  group_by(wid) %>%
  summarise(x = n())
library(leaflet)
leaflet() %>% addProviderTiles(providers$OpenStreetMap) %>% addPolygons(data = kwale, label = kwale@data$NAME_3, weight = 1, fillOpacity = 0) %>% addPolygons(data = pongwe_kikoneni, fillColor = 'red')

# Convert household locations to spatial
households <- households %>%
  filter(!is.na(Longitude)) %>%
  mutate(x = Longitude, y = Latitude) %>%
  dplyr::distinct(hh_id, .keep_all = TRUE)
households_spatial <- households
coordinates(households_spatial) <- ~x+y
proj4string(households_spatial) <- proj4string(ken3)

# Convert objects to projected UTM reference system
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)

households_spatial_projected <- spTransform(households_spatial, crs)
pongwe_kikoneni_projected <- spTransform(pongwe_kikoneni, crs)
pongwe_kikoneni_ramisi_projected <- spTransform(pongwe_kikoneni_ramisi, crs)
study_area_projected <- gUnaryUnion(pongwe_kikoneni_ramisi_projected, id = NULL)

# Sanity test plot
plot(pongwe_kikoneni_ramisi_projected)
points(households_spatial_projected)


# EXCLUDE MINING VILLAGES
# mining_villages <-sort(unique((exclusion$village)))
mining_villages <- c('Mafisini-A', 'Mafisini-B', 'Nguluku') # differently formatted than above
mining_households <- households_spatial[households_spatial$village %in% mining_villages,]
# (not done yet)
plot(households_spatial, col = adjustcolor('black', alpha.f = 0.1),
     cex = 0.1)
# points(ramisi_urban$x, ramisi_urban$y, col = 'red', pch = '*', cex = 3)
plot(mining_households, col = 'red', add = T, pch = '.', cex = 2)
plot(pongwe_kikoneni_ramisi, add = T)
households <- households[!households$village %in% mining_villages,]
households_spatial <- households_spatial[!households_spatial$village %in% mining_villages,]
households_spatial_projected <- households_spatial_projected[!households_spatial_projected$village %in% mining_villages,]

# EXCLUDE URBAN AREAS
# Just excluding a block based on Carlos' instructions
# https://bohemiakenya.slack.com/archives/C042P3A05UP/p1674914277260639?thread_ts=1674913831.106159&cid=C042P3A05UP
urban_coords <- 
  matrix(c(
    -320.5036354, -4.4636862,
    -320.5317192, -4.4473249,
    -320.5438728, -4.4534861,
    -320.5569191, -4.4625909,
    -320.5663948, -4.4806118,
    -320.5377617, -4.5337419,
    -320.5432892, -4.5365588,
    -320.5651245, -4.5408606,
    -320.5733986, -4.5266632,
    -320.5830460, -4.5142008,
    -320.5929337, -4.5126301,
    -320.5960922, -4.5181411,
    -320.5999374, -4.5223509,
    -320.6018600, -4.5286483,
    -320.6049843, -4.5399425,
    -320.6041260, -4.5462056,
    -320.6010704, -4.5504493,
    -320.5829773, -4.5706070,
    -320.5679741, -4.5765276,
    -320.5638199, -4.5938833,
    -320.5465508, -4.5903239,
    -320.5301399, -4.5247168,
    -320.5174370, -4.4902156,
    -320.4980736, -4.4677251,
    -320.5036354, -4.4636862
  ),
  ncol = 2, byrow = TRUE)
urban_coords[,1] <- -1 * (-360 - urban_coords[,1])
urban_poly <- Polygon(coords = urban_coords)
urban_polys <- Polygons(list(urban_poly), ID = 1)
urban_polys <- SpatialPolygons(list(urban_polys),proj4string = CRS(proj4string(ken3)))
urbansp <- SpatialPolygonsDataFrame(Sr = urban_polys, data = data.frame(a = 1))
urbansp_projected <- spTransform(urbansp, crs)
plot(households_spatial, col = adjustcolor('black', alpha.f = 0.1))
points(ramisi_urban$x, ramisi_urban$y, col = 'red')     
plot(urbansp, add = T, border = 'red', fill = NA)
# Remove them
o <- sp::over(households_spatial, polygons(urbansp))
households_spatial@data$remove_urban <- !is.na(o)
right <- households_spatial@data %>%
  dplyr::select(hh_id, remove_urban) %>%
  dplyr::distinct(hh_id, .keep_all = TRUE)
households <- households %>%
  left_join(right) %>%
  filter(is.na(remove_urban) | !remove_urban)
households_spatial <- households_spatial[!households_spatial@data$remove_urban,]
households_spatial_projected@data <-
  households_spatial_projected@data %>%
  left_join(right)
households_spatial_projected <- households_spatial_projected[is.na(households_spatial_projected@data$remove_urban) | !households_spatial_projected@data$remove_urban,]

# Remove wasini island
households_spatial_projected <- households_spatial_projected[coordinates(households_spatial_projected)[,2] >= 9485543 & coordinates(households_spatial_projected)[,2] <= 9517543,]
plot(households_spatial_projected)
households_spatial <- households_spatial[households_spatial@data$hh_id %in% households_spatial_projected@data$hh_id,]
households <- households[households$hh_id %in% households_spatial_projected@data$hh_id,]

# Sanity check number of households
nrow(households)
nrow(households_spatial)
nrow(households_spatial_projected)

# Sanity check on total number of kids
n_needed <- 96 * 35
n_actual <- sum(households_spatial_projected@data$num_hh_members_bt_5_15)
message('Need at least ', n_needed, '. Have ', n_actual)

#######################################
# Begin clustering
#######################################
# Adhere to these rules: https://docs.google.com/document/d/1tFFpx3ho7lqVuGWNlW5yL6PqdFnEdeic_7Mlphkj3QQ/edit
hh <- households_spatial_projected
hh$id <- 1:nrow(hh)

# get the distances between all houses
hh_distances <- gDistance(hh, byid = TRUE)

# ARBITRARY SHAPE METHOD
# Create one arbitrarily shaped polygon for each household, containing
# the minimum number of children
arbi_poly_list <- list()
arbi_out_list <- list()
for(i in 1:nrow(hh@data)){
  message('Household ', i, ' of ', nrow(hh@data))
  these_hh <- hh[i,]
  the_id <- these_hh$hh_id
  n_kids <- sum(these_hh$num_hh_members_bt_5_15)
  # calculate the ordered nearest households
  neighbors_by_order <- hh@data %>%
    mutate(distance_from_starter = as.numeric(hh_distances[i,])) %>%
    arrange(distance_from_starter)
  expansion_counter <- 1
  while(n_kids < 35){
    # message('---Expanding to ', expansion_counter, ' households')
    expansion_counter <- expansion_counter + 1
    these_hh <- neighbors_by_order[1:expansion_counter,]
    n_kids <- sum(these_hh$num_hh_members_bt_5_15)
  }
  # Now that we've got enough kids, save a polygon
  these_hh <- hh[hh@data$id %in% these_hh$id,]
  this_poly <- gConvexHull(these_hh, byid = FALSE)
  # Buffer by 5 meters to account for error in measurement, people on the line
  this_poly <- gBuffer(this_poly, width = 5)
  this_buffer <- gBuffer(this_poly, width = 400)
  # Calcualte the households in core / buffer
  hhs_in_core <- hh[!is.na(over(hh, this_poly)),]
  hhs_in_buffer <- hh[!is.na(sp::over(hh, polygons(this_buffer))),]
  out <- tibble(
    i = i,
    hh_id = the_id,
    n_hhs_core = nrow(hhs_in_core),
    n_hhs_buffer = nrow(hhs_in_buffer),
    num_hh_members_core = sum(hhs_in_core$num_hh_members),
    num_hh_members_lt_5_core = sum(hhs_in_core$num_hh_members_lt_5),
    num_hh_members_bt_5_15_core = sum(hhs_in_core$num_hh_members_bt_5_15),
    num_hh_members_gt_15_core = sum(hhs_in_core$num_hh_members_gt_15),
    num_hh_members_buffer = sum(hhs_in_buffer$num_hh_members),
    num_hh_members_lt_5_buffer = sum(hhs_in_buffer$num_hh_members_lt_5),
    num_hh_members_bt_5_15_buffer = sum(hhs_in_buffer$num_hh_members_bt_5_15),
    num_hh_members_gt_15_buffer = sum(hhs_in_buffer$num_hh_members_gt_15)
  )
  arbi_out_list[[i]] <- out
  arbi_poly_list[[i]] <- this_poly
}
arbi_out <- bind_rows(arbi_out_list)
# Add buffers to arbi_poly_list
arbi_buffer_list <- arbi_poly_list
for(i in 1:length(arbi_buffer_list)){
  x <- gBuffer(arbi_buffer_list[[i]], width = 400)
  xdf <- SpatialPolygonsDataFrame(Sr = x, data = arbi_out[i,], match.ID = FALSE)
  arbi_buffer_list[[i]] <- xdf
}
arbi_buffer_df <- do.call('rbind', arbi_buffer_list)
# plot(arbi_buffer_df, border = adjustcolor('black', alpha.f = 0.1))
# Arrange arbi_buffer_df by area so as to select small ones first
arbi_buffer_df$area <- gArea(arbi_buffer_df, byid = TRUE)
order_vec <- order(arbi_buffer_df$area)
arbi_buffer_df <- arbi_buffer_df[order_vec,]

# Turn the arbi_poly list into a dataframe too
for(i in 1:length(arbi_poly_list)){
  x <- arbi_poly_list[[i]]
  xdf <- SpatialPolygonsDataFrame(Sr = x, data = arbi_out[i,], match.ID = FALSE)
  arbi_poly_list[[i]] <- xdf
}
arbi_poly_df <- do.call('rbind', arbi_poly_list)
# plot(arbi_poly_df, border = adjustcolor('black', alpha.f = 0.1))
# order identically to arbi_buffer_df
arbi_poly_df <- arbi_poly_df[order_vec,]



# RADIAL METHOD
# Create one circular polygon for each household
out_list <- poly_list  <- list()
for(i in 1:nrow(hh@data)){
  message('Household ', i, ' of ', nrow(hh@data))
  this_hh <- these_hh <- hh[i,]
  the_id <- this_hh$hh_id
  n_kids <- sum(this_hh$num_hh_members_bt_5_15)
  r <- 10
  this_poly <- gBuffer(this_hh, width = r)
  while(n_kids < 35){
    this_poly <- gBuffer(this_hh, width = r)
    these_hh <- hh[!is.na(over(hh, this_poly)),]
    n_kids <- sum(these_hh$num_hh_members_bt_5_15)
    r <- r + 5
  }
  
  # Buffer by 5 meters to account for error in measurement, people on the line
  this_poly <- gBuffer(this_poly, width = 5)
  this_buffer <- gBuffer(this_poly, width = 400)
  # Calcualte the households in core / buffer
  hhs_in_core <- hh[!is.na(over(hh, this_poly)),]
  hhs_in_buffer <- hh[!is.na(sp::over(hh, polygons(this_buffer))),]
  out <- tibble(
    i = i,
    hh_id = the_id,
    n_hhs_core = nrow(hhs_in_core),
    n_hhs_buffer = nrow(hhs_in_buffer),
    num_hh_members_core = sum(hhs_in_core$num_hh_members),
    num_hh_members_lt_5_core = sum(hhs_in_core$num_hh_members_lt_5),
    num_hh_members_bt_5_15_core = sum(hhs_in_core$num_hh_members_bt_5_15),
    num_hh_members_gt_15_core = sum(hhs_in_core$num_hh_members_gt_15),
    num_hh_members_buffer = sum(hhs_in_buffer$num_hh_members),
    num_hh_members_lt_5_buffer = sum(hhs_in_buffer$num_hh_members_lt_5),
    num_hh_members_bt_5_15_buffer = sum(hhs_in_buffer$num_hh_members_bt_5_15),
    num_hh_members_gt_15_buffer = sum(hhs_in_buffer$num_hh_members_gt_15)
  )
  out_list[[i]] <- out
  poly_list[[i]] <- this_poly
}

out <- bind_rows(out_list)

# Sanity test
plot(hh, pch = '.')
for(i in 1:length(poly_list)){
  plot(poly_list[[i]], add = T, border = adjustcolor('black', alpha.f = 0.2))
}

# Add buffers to poly_list
buffer_list <- poly_list
for(i in 1:length(buffer_list)){
  x <- gBuffer(buffer_list[[i]], width = 400)
  xdf <- SpatialPolygonsDataFrame(Sr = x, data = out[i,], match.ID = FALSE)
  buffer_list[[i]] <- xdf
}
buffer_df <- do.call('rbind', buffer_list)
plot(buffer_df, border = adjustcolor('black', alpha.f = 0.1))
# Arrange buffer_df by area so as to select small ones first
buffer_df$area <- gArea(buffer_df, byid = TRUE)
order_vec <- order(buffer_df$area)
buffer_df <- buffer_df[order_vec,]

# Turn the poly list into a dataframe too
for(i in 1:length(poly_list)){
  x <- poly_list[[i]]
  xdf <- SpatialPolygonsDataFrame(Sr = x, data = out[i,], match.ID = FALSE)
  poly_list[[i]] <- xdf
}
poly_df <- do.call('rbind', poly_list)
plot(poly_df, border = adjustcolor('black', alpha.f = 0.1))
# order identically to buffer_df
poly_df <- poly_df[order_vec,]

save.image('image.RData')

# Remove clusters whose buffer overlaps the border (since we don't know about their contamination status)
# We don't want to remove coastline though
# Do this by getting distance to nearest non pongwe kikoneni area
if(TRUE){
  not_pongwe_kikoneni_ramisi <- ken3[!ken3@data$NAME_3 %in% c('Pongwe/Kikoneni', 'Ramisi'),]
  not_pongwe_kikoneni_ramisi_projected <- spTransform(not_pongwe_kikoneni_ramisi, crs)
  not_pongwe_kikoneni_ramisi_projected <- gUnaryUnion(not_pongwe_kikoneni_ramisi_projected, id = not_pongwe_kikoneni_ramisi_projected@data$NAME_0)
  
  # Remove the arbitrarily shaped polygons which overlap borders
  overlapping <- over(arbi_buffer_df, polygons(not_pongwe_kikoneni_ramisi_projected))
  plot(arbi_buffer_df[!is.na(overlapping),])
  plot(pongwe_kikoneni_ramisi_projected, add = T, border = 'blue')
  arbi_buffer_df <- arbi_buffer_df[is.na(overlapping),]
  arbi_poly_df <- arbi_poly_df[is.na(overlapping),]
  
  # Remove the arbitrarily shaped polygons which overlap urban areas
  overlapping <- over(arbi_buffer_df, polygons(urbansp_projected))
  plot(urbansp_projected, border = 'blue')
  plot(arbi_buffer_df[!is.na(overlapping),], add = T)
  arbi_buffer_df <- arbi_buffer_df[is.na(overlapping),]
  arbi_poly_df <- arbi_poly_df[is.na(overlapping),]
  
  # Remove the cirular polygons which overlap borders
  overlapping <- over(buffer_df, polygons(not_pongwe_kikoneni_ramisi_projected))
  plot(buffer_df[!is.na(overlapping),])
  plot(pongwe_kikoneni_ramisi_projected, add = T, border = 'blue')
  buffer_df <- buffer_df[is.na(overlapping),]
  poly_df <- poly_df[is.na(overlapping),]
  
  # Remove the arbitrarily shaped polygons which overlap urban areas
  overlapping <- over(buffer_df, polygons(urbansp_projected))
  plot(urbansp_projected, border = 'blue')
  plot(buffer_df[!is.na(overlapping),], add = T)
  buffer_df <- buffer_df[is.na(overlapping),]
  poly_df <- poly_df[is.na(overlapping),]
  
  # # OLD, DEPRECATED WAY OF DOING THIS
  # distances <- gDistance(not_pongwe_kikoneni_ramisi_projected, households_spatial_projected, byid = TRUE)
  # distances <- apply(distances, 1, min)
  # # Removals
  # removal_indices <- which(distances < 400)
  # households_spatial_projected$remove <- FALSE
  # households_spatial_projected$remove[removal_indices] <- TRUE
  # 
  # # Plot the removals
  # plot(gBuffer(study_area_projected, width = -400), col = adjustcolor('black', alpha.f = 0.1))
  # plot(study_area_projected, add = TRUE)
  # points(households_spatial_projected, pch = '.', cex = 3)
  # points(households_spatial_projected[households_spatial_projected$remove,], col = 'red', pch = '.', cex = 3)
  # # Carry out the removals
  # nr <- nrow(households_spatial_projected)
  # households_spatial_projected <- households_spatial_projected[!households_spatial_projected$remove,]
  # message(nrow(households_spatial_projected) - nr, ' households removed due to proximity with neighboring ward.')
}

# Remove clusters which overlap with the urban areas or the borders


# Now randomly pick some without overlapping
if(!dir.exists('arbi_plots')){
  dir.create('arbi_plots')
}
arbi_buffer_df$eligible <- TRUE
arbi_buffer_df$cluster_number <- arbi_poly_df$cluster_number <- 0
arbi_buffer_df$rn <- 1:nrow(arbi_buffer_df)
done <- FALSE
counter <- 0
while(!done){
  counter <- counter + 1
  message('Counter ', counter)
  # Define which buffers are still eligible (ie, not already overlapping)
  eligible_indices <- which(arbi_buffer_df$eligible)
  # Next cluster selection -------------------------
  ## Random method
  # this_i <- sample(eligible_indices, 1)
  ## First method
  this_i <- eligible_indices[1]
  # # Nearest method
  # current_shape <- arbi_buffer_df[arbi_buffer_df$cluster_number > 0,]
  # # if nothing yet, just pick the smallest one
  # if(nrow(current_shape) == 0){
  #   # northernmost point
  #   this_i <- which.max(coordinates(arbi_buffer_df)[,2]) # eligible_indices[1]
  # } else {
  #   # # pick the one which is nearest to the current_shape
  #   current_shape$id <- 1
  #   current_shape <- gUnaryUnion(current_shape, id = current_shape$id)
  #   # ## Nearest to last method
  #   # current_shape <- last_buffer
  #   distance_df <- arbi_buffer_df[arbi_buffer_df$eligible,]
  #   gd <- gDistance(current_shape, distance_df, byid = TRUE)
  #   # # nearest
  #   # nearest <- distance_df$rn[which.min(gd)[1]]
  #   # this_i <- which(arbi_buffer_df$rn == nearest)
  #   # # furthest
  #   # furthest <- distance_df$rn[which.max(gd)[1]]
  #   # this_i <- which(arbi_buffer_df$rn == furthest)
  #   # # northernmost
  #   northernmost <- distance_df$rn[which.min(coordinates(distance_df)[,2])[1]]
  #   this_i <- which(arbi_buffer_df$rn == northernmost)
  # }
  # ------------------------------------------------
  # Make the selection and mark it as selected
  this_buffer <- arbi_buffer_df[this_i,]
  this_core <- arbi_poly_df[this_i,]
  arbi_buffer_df$eligible[this_i] <- FALSE
  arbi_buffer_df$cluster_number[this_i] <- counter
  arbi_poly_df$cluster_number[this_i] <- counter
  # Identify all the overlaps
  go <- as.logical(gIntersects(this_buffer, arbi_buffer_df, byid = T))
  # The overlappers are all now ineligible, mark them as such
  arbi_buffer_df$eligible[go] <- FALSE
  # Make a plot of progress
  clusters <- arbi_buffer_df[arbi_buffer_df$cluster_number > 0,]
  cores <- arbi_poly_df[arbi_poly_df$cluster_number > 0,]
  png(filename = paste0('arbi_plots/', add_zero(counter, n = 3), '.png'),
      height = 720,
      width = 600)
  plot(pongwe_kikoneni_ramisi_projected, col = 'beige')
  points(hh, pch = '.')
  plot(clusters, add = T, col = adjustcolor('yellow', alpha.f = 0.6))
  plot(cores, add = T, col = adjustcolor('darkorange', alpha.f = 0.8))
  plot(this_buffer, col = 'grey', add = TRUE)
  plot(this_core, col = 'red', add = TRUE)
  title(main = paste0('Clusters created: ', nrow(clusters)), sub = paste0('Remaining households eligible: ', length(which(arbi_buffer_df$eligible))))
  dev.off()
  message('Clusters selected so far: ', nrow(clusters))
  message('Remaining eligible: ', length(which(arbi_buffer_df$eligible)))
  # Identify the overlaps and mark as eligible
  # Sys.sleep(3)
  last_buffer <- this_buffer
}
owd <- getwd()
setwd('arbi_plots')
system("convert -delay 50 -loop 0 *.png clustering_northernmost.gif")
setwd(owd)


done <- arbi_buffer_df@data %>% filter(cluster_number > 0)
plot(arbi_buffer_df[arbi_buffer_df@data$cluster_number > 0,])
plot(arbi_poly_df[arbi_poly_df@data$cluster_number > 0,], add = T, col = 'red')
plot(pongwe_kikoneni_ramisi_projected, add = T)

nrow(done)
sum(done$n_hhs_buffer)
sum(done$n_hhs_core)
sum(done$num_hh_members_bt_5_15_core)
sum(done$num_hh_members_bt_5_15_buffer) + sum(done$num_hh_members_gt_15_buffer)

# RADIAL APPROACH
# Now randomly pick some without overlapping
if(!dir.exists('plots')){
  dir.create('plots')
}
buffer_df$eligible <- TRUE
buffer_df$cluster_number <- poly_df$cluster_number <- 0
buffer_df$rn <- 1:nrow(buffer_df)
done <- FALSE
counter <- 0
while(!done){
  counter <- counter + 1
  message('Counter ', counter)
  # Define which buffers are still eligible (ie, not already overlapping)
  eligible_indices <- which(buffer_df$eligible)
  # Next cluster selection -------------------------
  ## Random method
  # this_i <- sample(eligible_indices, 1)
  ## First method
  this_i <- eligible_indices[1]
  # Nearest method
  current_shape <- buffer_df[buffer_df$cluster_number > 0,]
  # if nothing yet, just pick the smallest one
  # if(nrow(current_shape) == 0){
  #   # northernmost point
  #   this_i <- which.max(coordinates(buffer_df)[,2]) # eligible_indices[1]
  # } else {
  #   # # pick the one which is nearest to the current_shape
  #   current_shape$id <- 1
  #   current_shape <- gUnaryUnion(current_shape, id = current_shape$id)
  #   # ## Nearest to last method
  #   # current_shape <- last_buffer
  #   distance_df <- buffer_df[buffer_df$eligible,]
  #   gd <- gDistance(current_shape, distance_df, byid = TRUE)
  #   # # nearest
  #   # nearest <- distance_df$rn[which.min(gd)[1]]
  #   # this_i <- which(buffer_df$rn == nearest)
  #   # # furthest
  #   # furthest <- distance_df$rn[which.max(gd)[1]]
  #   # this_i <- which(buffer_df$rn == furthest)
  #   # northernmost
  #   northernmost <- distance_df$rn[which.max(coordinates(distance_df)[,2])[1]]
  #   this_i <- which(buffer_df$rn == northernmost)
  # }
  # ------------------------------------------------
  # Make the selection and mark it as selected
  this_buffer <- buffer_df[this_i,]
  this_core <- poly_df[this_i,]
  buffer_df$eligible[this_i] <- FALSE
  buffer_df$cluster_number[this_i] <- counter
  poly_df$cluster_number[this_i] <- counter
  # Identify all the overlaps
  go <- as.logical(gIntersects(this_buffer, buffer_df, byid = T))
  # The overlappers are all now ineligible, mark them as such
  buffer_df$eligible[go] <- FALSE
  # Make a plot of progress
  clusters <- buffer_df[buffer_df$cluster_number > 0,]
  cores <- poly_df[poly_df$cluster_number > 0,]
  png(filename = paste0('plots/', add_zero(counter, n = 3), '.png'),
      height = 720,
      width = 600)
  plot(pongwe_kikoneni_ramisi_projected, col = 'beige')
  points(hh, pch = '.')
  plot(clusters, add = T, col = adjustcolor('yellow', alpha.f = 0.6))
  plot(cores, add = T, col = adjustcolor('darkorange', alpha.f = 0.8))
  plot(this_buffer, col = 'grey', add = TRUE)
  plot(this_core, col = 'red', add = TRUE)
  title(main = paste0('Clusters created: ', nrow(clusters)), sub = paste0('Remaining households eligible: ', length(which(buffer_df$eligible))))
  dev.off()
  message('Clusters selected so far: ', nrow(clusters))
  message('Remaining eligible: ', length(which(buffer_df$eligible)))
  # Identify the overlaps and mark as eligible
  # Sys.sleep(3)
  last_buffer <- this_buffer
}
owd <- getwd()
setwd('plots')
system("convert -delay 50 -loop 0 *.png clustering_northernmost.gif")
setwd(owd)

done <- buffer_df@data %>% filter(cluster_number > 0)
plot(buffer_df[buffer_df@data$cluster_number > 0,])
plot(poly_df[poly_df@data$cluster_number > 0,], add = T, col = 'red')
plot(pongwe_kikoneni_ramisi_projected, add = T)

nrow(done)
sum(done$n_hhs_buffer)
sum(done$n_hhs_core)
sum(done$num_hh_members_bt_5_15_core)
sum(done$num_hh_members_bt_5_15_buffer) + sum(done$num_hh_members_gt_15_buffer)


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

radial_too <- FALSE
fresh_data <- FALSE

if(fresh_data){
  ken3 <- raster::getData(name = 'GADM', download = TRUE, country = 'KEN', level = 3)
  ken3_fortified <- fortify(ken3, regions = ken3@data$NAME_3)
  kwale <- ken3[ken3@data$NAME_1 == 'Kwale',]
  kwale_fortified <- fortify(kwale, regions = ken3@data$NAME_3)
  pongwe_kikoneni <- ken3[ken3@data$NAME_3 == 'Pongwe/Kikoneni',]
  pongwe_kikoneni_fortified <- fortify(pongwe_kikoneni, regions = ken3@data$NAME_3)
  
  ramisi <- ken3[ken3@data$NAME_3 %in% c('Ramisi'),]
  ramisi_fortified <- fortify(ramisi, regions = ramisi@data$NAME_3)
  
  pongwe_kikoneni_ramisi <- ken3[ken3@data$NAME_3 %in% c('Pongwe/Kikoneni', 'Ramisi'),]
  pongwe_kikoneni_ramisi_fortified <- fortify(pongwe_kikoneni_ramisi, regions = pongwe_kikoneni_ramisi@data$NAME_3)
  
  # Save for later
  save(ken3, ken3_fortified, file = 'final/ken3.RData')
  save(kwale, kwale_fortified, file = 'final/kwale.RData')
  save(pongwe_kikoneni, pongwe_kikoneni_fortified, file = 'final/pongwe_kikoneni.RData')
  save(ramisi, ramisi_fortified, file = 'final/ramisi.RData')
  save(pongwe_kikoneni_ramisi, pongwe_kikoneni_ramisi_fortified, file = 'final/pongwe_kikoneni_ramisi.RData')

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
  registrations <- get_registartion_forms()
  save(registrations, file = 'registrations.RData')
  save(households, file = 'households.RData')
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

# Convert objects to projected UTM reference system
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)

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
inclusion_projected <- spTransform(inclusion, crs)
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



households_spatial_projected <- spTransform(households_spatial, crs)
pongwe_kikoneni_projected <- spTransform(pongwe_kikoneni, crs)
pongwe_kikoneni_ramisi_projected <- spTransform(pongwe_kikoneni_ramisi, crs)
study_area_projected <- gUnaryUnion(pongwe_kikoneni_ramisi_projected, id = NULL)

# Sanity test plot
plot(pongwe_kikoneni_ramisi_projected)
points(households_spatial_projected, pch = '.', cex = 0.2)

# Remove those households which are outside of study area
o <- sp::over(households_spatial_projected, polygons(pongwe_kikoneni_ramisi_projected))
remove <- is.na(o)
households <- households[!remove,]
households_spatial <- households_spatial[!remove,]
households_spatial_projected <- households_spatial_projected[!remove,]

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

# Define the different distances we want to try
buffer_sizes <- c(400, 425, 450, 475, 500, 525, 550, 575, 600)
final_list_arbi <- final_list_arbi_buffer <- list()

# ARBITRARY SHAPE METHOD
# Create one arbitrarily shaped polygon for each household, containing
# the minimum number of children
arbi_poly_list <- list()
arbi_out_list <- list()
for(b in 1:length(buffer_sizes)){
  this_buffer_size <- buffer_sizes[b]
  
  for(i in 1:nrow(hh@data)){
    # for (i in 1:10){
    message('ARBITRARY. Buffer size ', this_buffer_size, '. Household ', i, ' of ', nrow(hh@data))
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
    this_buffer <- gBuffer(this_poly, width = this_buffer_size)
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
      num_hh_members_gt_15_buffer = sum(hhs_in_buffer$num_hh_members_gt_15),
      buffer_size = this_buffer_size
    )
    arbi_out_list[[i]] <- out
    arbi_poly_list[[i]] <- this_poly
  }
  arbi_out <- bind_rows(arbi_out_list)
  
  # Add buffers to arbi_poly_list
  arbi_buffer_list <- arbi_poly_list
  for(i in 1:length(arbi_buffer_list)){
    x <- gBuffer(arbi_buffer_list[[i]], width = this_buffer_size)
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
  arbi_poly_df@data$buffer_size <- this_buffer_size
  final_list_arbi[[b]] <- arbi_poly_df
  final_list_arbi_buffer[[b]] <- arbi_buffer_df
  
}
names(final_list_arbi) <- buffer_sizes
names(final_list_arbi_buffer) <- buffer_sizes


# RADIAL METHOD
if(radial_too){
  # Create one circular polygon for each household
  out_list <- poly_list  <- final_list <- final_list_buffer <- list()
  
  for(b in 1:length(buffer_sizes)){
    this_buffer_size <- buffer_sizes[b]
    
    for(i in 1:nrow(hh@data)){
      message('RADIAL. Buffer size ', this_buffer_size, '. Household ', i, ' of ', nrow(hh@data))
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
      this_buffer <- gBuffer(this_poly, width = this_buffer_size)
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
        num_hh_members_gt_15_buffer = sum(hhs_in_buffer$num_hh_members_gt_15),
        buffer_size = this_buffer_size
      )
      out_list[[i]] <- out
      poly_list[[i]] <- this_poly
    }
    
    out <- bind_rows(out_list)
    # # Sanity test
    # plot(hh, pch = '.')
    # for(i in 1:length(poly_list)){
    #   plot(poly_list[[i]], add = T, border = adjustcolor('black', alpha.f = 0.2))
    # }
    
    # Add buffers to poly_list
    buffer_list <- poly_list
    for(i in 1:length(buffer_list)){
      x <- gBuffer(buffer_list[[i]], width = this_buffer_size)
      xdf <- SpatialPolygonsDataFrame(Sr = x, data = out[i,], match.ID = FALSE)
      buffer_list[[i]] <- xdf
    }
    buffer_df <- do.call('rbind', buffer_list)
    # plot(buffer_df, border = adjustcolor('black', alpha.f = 0.1))
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
    poly_df@data$buffer_size <- this_buffer_size
    final_list[[b]] <- poly_df
    final_list_buffer[[b]] <- buffer_df
  }
  names(final_list) <- names(final_list_buffer) <- buffer_sizes
  
}

save.image('image.RData')

# Remove clusters whose buffer overlaps the border (since we don't know about their contamination status)
# We don't want to remove coastline though
# Do this by getting distance to nearest non pongwe kikoneni area
if(TRUE){
  not_pongwe_kikoneni_ramisi <- ken3[!ken3@data$NAME_3 %in% c('Pongwe/Kikoneni', 'Ramisi'),]
  not_pongwe_kikoneni_ramisi_projected <- spTransform(not_pongwe_kikoneni_ramisi, crs)
  not_pongwe_kikoneni_ramisi_projected <- gUnaryUnion(not_pongwe_kikoneni_ramisi_projected, id = not_pongwe_kikoneni_ramisi_projected@data$NAME_0)
  
  for(i in 1:length(buffer_sizes)){
    this_buffer_size <- buffer_sizes[i]
    message('Removing urban/mining clusters for buffer size ', this_buffer_size)
    
    arbi_poly_df <- final_list_arbi[[i]]
    arbi_buffer_df <- final_list_arbi_buffer[[i]]
    
    # Remove the arbitrarily shaped polygons which overlap borders
    overlapping <- over(arbi_buffer_df, polygons(not_pongwe_kikoneni_ramisi_projected))
    plot(arbi_buffer_df[!is.na(overlapping),])
    plot(pongwe_kikoneni_ramisi_projected, add = T, border = 'blue')
    arbi_buffer_df <- arbi_buffer_df[as.logical(is.na(overlapping)),]
    arbi_poly_df <- arbi_poly_df[as.logical(is.na(overlapping)),]
    
    # Remove the arbitrarily shaped polygons which overlap urban areas
    overlapping <- over(arbi_buffer_df, polygons(urbansp_projected))
    plot(urbansp_projected, border = 'blue')
    plot(arbi_buffer_df[!is.na(overlapping),], add = T)
    arbi_buffer_df <- arbi_buffer_df[is.na(overlapping),]
    arbi_poly_df <- arbi_poly_df[is.na(overlapping),]
    
    # Reassign after the srinking
    final_list_arbi[[i]] <- arbi_poly_df
    final_list_arbi_buffer[[i]] <- arbi_buffer_df
    
    
    if(radial_too){
      # THIS SECTION IS NOT YET REFACTORED TO HANDLE VARIABLE DISTANCES
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
    }
  }
}

stats_list <- finished_cores_list <- finished_buffers_list <- list()
for(b in 1:length(buffer_sizes)){
  this_buffer_size <- buffer_sizes[b]
  this_directory <- paste0('arbi_plots_', this_buffer_size, '/')
  
  # Now randomly pick some without overlapping
  unlink(this_directory, recursive = TRUE)
  if(!dir.exists(this_directory)){
    dir.create(this_directory)
  }
  arbi_buffer_df <- final_list_arbi_buffer[[b]]
  arbi_poly_df <- final_list_arbi[[b]]
  arbi_buffer_df$eligible <- TRUE
  arbi_buffer_df$cluster_number <- arbi_poly_df$cluster_number <- 0
  arbi_buffer_df$rn <- 1:nrow(arbi_buffer_df)
  done <- FALSE
  counter <- 0
  while(!done & counter < 150){
    try({
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
      # distance_df <- arbi_buffer_df[arbi_buffer_df$eligible,]
      #   gd <- gDistance(current_shape, distance_df, byid = TRUE)
      #   # # nearest
      #   # nearest <- distance_df$rn[which.min(gd)[1]]
      #   # this_i <- which(arbi_buffer_df$rn == nearest)
      #   # # furthest
      #   # furthest <- distance_df$rn[which.max(gd)[1]]
      #   # this_i <- which(arbi_buffer_df$rn == furthest)
      # # # westernmost
      # westernmost <- distance_df$rn[which.min(coordinates(distance_df)[,1])[1]]
      # this_i <- which(arbi_buffer_df$rn == westernmost)
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
      png(filename = paste0(this_directory, add_zero(counter, n = 3), '.png'),
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
    })
    }
    
  # Make GIF
  if(FALSE){
    owd <- getwd()
    setwd(this_directory)
    system(paste0("convert -delay 50 -loop 0 *.png ", this_directory, 'animation.gif'))
    setwd(owd)  
  }
  
  
  # Save results
  done <- arbi_buffer_df@data %>% filter(cluster_number > 0)
  png(filename = paste0('buffer_', this_buffer_size, '.png'))
  plot(arbi_buffer_df[arbi_buffer_df@data$cluster_number > 0,])
  plot(arbi_poly_df[arbi_poly_df@data$cluster_number > 0,], add = T, col = 'red')
  plot(pongwe_kikoneni_ramisi_projected, add = T)
  # Add the social science villages
  points(inclusion_projected, col = adjustcolor('green', alpha.f = 0.5), pch = '.', cex = 2)
  dev.off()
  
  finished_buffers_list[[b]] <- arbi_buffer_df[arbi_buffer_df@data$cluster_number > 0,]
  finished_cores_list[[b]] <- arbi_poly_df[arbi_poly_df@data$cluster_number > 0,]
  
  # Save final statistics
  # out <- tibble(
  #   buffer_size = this_buffer_size,
  #   n_clusters = nrow(done),
  #   n_hhs_buffer = sum(done$n_hhs_buffer),
  #   n_hhs_core = sum(done$n_hhs_core),
  #   num_hh_members_bt_5_15_core = sum(done$num_hh_members_bt_5_15_core),
  #   num_hh_members_gt_15_buffer = sum(done$num_hh_members_gt_15_buffer)
  # )
  out <- done %>%
    mutate(buffer_size = this_buffer_size)
  stats_list[[b]] <- out
}
stats_df <- bind_rows(stats_list)
save(stats_df, finished_buffers_list, finished_cores_list, file = 'finished.RData')
load('households_spatial.RData')
load('households.RData')

# load('image.RData')
# load('finished.RData')



agg <- stats_df %>%
  arrange(n_hhs_buffer) %>%
  mutate(dummy = 1) %>%
  group_by(buffer_size) %>%
  mutate(cs = cumsum(dummy)) %>%
  ungroup %>%
  mutate(eligible = cs <= 96) %>%
  group_by(buffer_size, eligible) %>%
  summarise(n_clusters = n(),
            n_hhs_core = sum(n_hhs_core),
            n_hhs_buffer = sum(n_hhs_buffer),
            num_hh_members_core = sum(num_hh_members_core),
            num_hh_members_lt_5_core = sum(num_hh_members_lt_5_core),
            num_hh_members_bt_5_15_core = sum(num_hh_members_bt_5_15_core),
            num_hh_members_gt_15_core = sum(num_hh_members_gt_15_core),
            num_hh_members_buffer = sum(num_hh_members_buffer),
            num_hh_members_lt_5_buffer = sum(num_hh_members_lt_5_buffer),
            num_hh_members_bt_5_15_buffer = sum(num_hh_members_bt_5_15_buffer),
            num_hh_members_gt_15_buffer = sum(num_hh_members_gt_15_buffer),
            area = sum(area)) %>%
  ungroup %>%
  mutate(n_5_or_older = num_hh_members_bt_5_15_buffer + num_hh_members_gt_15_buffer) %>%
  group_by(buffer_size) %>%
  mutate(extra_clusters = dplyr::first(n_clusters[!eligible])) %>%
  ungroup %>%
  filter(eligible) %>%
  dplyr::select(-eligible)

agg %>% dplyr::select(buffer_size, n_clusters,
                      extra_clusters, 
                      n_hhs_core,
                      n_hhs_total = n_hhs_buffer, 
                      num_hh_members_core, 
                      n_people_total = num_hh_members_buffer,
                      n_5_or_older,
                      n_5_to_15_year_olds_core = num_hh_members_bt_5_15_core) 

# Keep only the 400 meter approach
cores <- finished_cores_list[[1]]
buffers <- finished_buffers_list[[1]]
plot(buffers)
plot(cores, add = T)

# Get malaria info from bohemia/kenya/wards_map.Rmd
if(!file.exists('study_area_malaria.RData')){
  file.copy('../../../bohemia/analyses/kenya/study_area_malaria.RData',
            'study_area_malaria.RData')
}
load('study_area_malaria.RData')
if(!file.exists('hfj.RData')){
  file.copy('../../../bohemia/analyses/kenya/hfj.RData',
            'hfj.RData')
}
load('hfj.RData')
o <- sp::over(hfj, polygons(pongwe_kikoneni_ramisi))
hfj <- hfj[!as.logical(is.na(o)),]

# Reproject to lat long
cores_ll <- spTransform(cores, proj4string(wds_shp))
buffers_ll <- spTransform(buffers, proj4string(wds_shp))

# Create labels and popups
cores_ll@data <- cores_ll@data %>%
  mutate(label = paste0('Core of cluster ', cluster_number)) %>%
  mutate(popup = paste0('Cluster ', cluster_number, '<br>Households in core: ', n_hhs_core, '. Households total: ', n_hhs_buffer))
buffers_ll@data <- buffers_ll@data %>%
  mutate(label = paste0('Buffer of cluster ', cluster_number)) %>%
  mutate(popup = paste0('Cluster ', cluster_number, '<br>Households in core: ', n_hhs_core, '. Households total: ', n_hhs_buffer))


# Plot into leaflet map
library(leaflet)
library(leaflet.extras)
library(raster)
pal <- colorNumeric(c("blue", "white", "red"), values(study_area),
                    na.color = "transparent")
xpal <- colorNumeric("Spectral", hfj@data$cases, reverse = TRUE)

map <- leaflet() %>%
  addProviderTiles(providers$Stamen.Toner, group = "Basic") %>%
  addProviderTiles(providers$OpenStreetMap, group = "OSM") %>%
  addProviderTiles(providers$Esri.WorldImagery, group = "Satellite")

# Add social science households
ss <- inclusion

map <- map %>%
  addRasterImage(study_area, colors = pal, opacity = 0.8,
                 group = 'PAPfPR') %>%
  addLegend(pal = pal, values = values(study_area),
            title = "PAPfPR", group = 'PAPfPR')
map <- map %>%
  addCircleMarkers(data = hfj, color = xpal(hfj@data$cases),
                   label = paste0(hfj@data$Facility_Name,
                                  ": ", hfj@data$cases),
                   fillOpacity = 1,
                   group = 'Health facilities',
                   radius = 10) %>%
  addLegend("bottomleft", pal = xpal, values = hfj@data$cases,
            title = "2021 malaria cases", opacity = 1,
            group = 'Health facilities')

map <- map %>%
  addCircleMarkers(data = households_spatial,
                   opacity = 1,
                   color = 'black',
                   radius = 1,
                   fillOpacity = 1, 
                   weight = 1,
                   group = 'Households',
                   popup = households_spatial@data$hh_id,
                   label = paste0(households_spatial@data$hh_id, '<br>', households_spatial@data$num_hh_members, ' members'))
map <- map %>%
  addCircleMarkers(data = ss, color = 'orange',
                   label = paste0('Social science household ',
                                  ": ", ss@data$hh_id),
                   group = 'Social science households',
                   radius = 1.5,
                   fillOpacity = 1, 
                   weight = 1) 
mydrawPolylineOptions <- function (allowIntersection = TRUE, 
                                   drawError = list(color = "#b00b00", timeout = 2500), 
                                   guidelineDistance = 20, metric = TRUE, feet = FALSE, zIndexOffset = 2000, 
                                   shapeOptions = drawShapeOptions(fill = FALSE), repeatMode = FALSE) {
  leaflet::filterNULL(list(allowIntersection = allowIntersection, 
                           drawError = drawError, guidelineDistance = guidelineDistance, 
                           metric = metric, feet = feet, zIndexOffset = zIndexOffset,
                           shapeOptions = shapeOptions,  repeatMode = repeatMode)) }

map <- map %>%
  addLayersControl(
    baseGroups = c("Basic", "OSM", "Satellite"),
    overlayGroups = c(#"Hamlet centroids", 
      'PAPfPR',
      'Health facilities',
      'Households',
      'Social science households'),
    options = layersControlOptions(collapsed = FALSE)
  ) %>% addFullscreenControl() %>% 
  addDrawToolbar(
    polylineOptions = mydrawPolylineOptions(metric=TRUE, feet=FALSE),
    editOptions=editToolbarOptions(selectedPathOptions=selectedPathOptions())
  ) 

map <- map %>%
  addPolygons(data = buffers_ll, weight = 1, fillOpacity = 0.3, popup = buffers_ll@data$popup, label = buffers_ll@data$label) %>%
  addPolygons(data = cores_ll, fillColor = 'red', weight = 1, fillOpacity = 0.3, popup = cores_ll@data$popup, label = cores_ll@data$label)

map

# htmltools::save_html(map, file = 'map.html')
htmlwidgets::saveWidget(widget = map,
                        file = 'map.html',
                        selfcontained = TRUE)

# Remove clusters as per project instruction
# https://trello.com/c/yFGlLOBt/1718-cluster-removals
study <- list()
remove_clusters <- c(70, 68, 81, 114, 78, 55, 22, 94, 66, 61, 95, 2, 104, 112, 76, 52, 89, 102, 20, 16, 13)





# July 21 2023, adding some of these back
if(FALSE){
  new_cluster_numbers <- remove_clusters
  new_clusters <- buffers_ll[buffers_ll@data$cluster_number %in% new_cluster_numbers,]
  plot(buffers_ll)
  # Renumber
  new_clusters$new_number <- new_clusters$cluster_number + 500
  plot(new_clusters, col = 'red', add = TRUE)
  text(coordinates(new_clusters), labels = new_clusters@data$new_number, cex = 0.8)
}

remove_buffers <- buffers_ll[buffers_ll@data$cluster_number %in% remove_clusters,]
plot(buffers_ll)
plot(remove_buffers, add = T, col = 'red')
plot(buffers_ll[buffers_ll@data$cluster_number == 108,], add = TRUE, col = 'green')

study$buffers_ll <- buffers_ll[!buffers_ll@data$cluster_number %in% remove_clusters,]
study$cores_ll <- cores_ll[!cores_ll@data$cluster_number %in% remove_clusters,]

study$buffers_ll@data <- study$buffers_ll@data %>% dplyr::select(cluster_number)
study$cores_ll@data <- study$cores_ll@data %>% dplyr::select(cluster_number)

# Re-number each cluster 1-96
# get distance to hq in kwale
shp <- study$buffers_ll
hq <- data.frame(y = -4.178132774894949, x = 39.45980823316276) %>%
  mutate(lng = x, lat = y)
coordinates(hq) <- ~x+y
proj4string(hq) <- proj4string(pongwe_kikoneni_ramisi)
library(geosphere)
dd <- rep(NA, nrow(shp))
for(i in 1:length(dd)){
  d <- distm(c(hq@data$lat, hq@data$lng), c(coordinates(shp)[i,2], coordinates(shp)[i,1]), fun = distHaversine)
  dd[i] <- d
}
shp@data$distance <- dd

coords <- coordinates(shp)
coords <- data.frame(coords)
names(coords) <- c('x', 'y')
coords$cluster_number <- shp@data$cluster_number
coords$distance <- shp@data$distance
number_matcher <- coords %>%
  arrange(distance) %>%
  mutate(new_cluster_number = 1:nrow(.))
# plot for sanity
p <- study$buffers_ll
p@data <- left_join(p@data, number_matcher)
plot(pongwe_kikoneni_ramisi, xlim = c(39.22861, 39.46),
     ylim = c(-4.74999, -4.1))
points(hq, col = 'red', pch = '+', cex = 3)

plot(p, add = T)
text(p, labels = p@data$new_cluster_number, cex = 0.3,
     col = adjustcolor('black', alpha.f = 0.5))

# Re-number
number_matcher <- number_matcher %>% dplyr::select(cluster_number, new_cluster_number)
buffers <- study$buffers_ll
cores <- study$cores_ll
clusters <- buffers
buffers <- buffers - cores # removing cores buffers

buffers@data <- left_join(buffers@data, number_matcher) %>% dplyr::select(-cluster_number) %>% dplyr::rename(cluster_number = new_cluster_number) 
cores@data <- left_join(cores@data, number_matcher) %>% dplyr::select(-cluster_number) %>% dplyr::rename(cluster_number = new_cluster_number) 
clusters@data <- left_join(clusters@data, number_matcher) %>% dplyr::select(-cluster_number) %>% dplyr::rename(cluster_number = new_cluster_number) 

# Save
if(!dir.exists('final')){
  dir.create('final')
}
if(!dir.exists('final/buffers')){
  dir.create('final/buffers')
}
if(!dir.exists('final/cores')){
  dir.create('final/cores')
}
if(!dir.exists('final/clusters')){
  dir.create('final/clusters')
}

setwd('final/buffers')
shapefile(x = buffers, file = "buffers.shp", overwrite = TRUE)
# writeOGR(buffers, dsn = '.', layer = 'buffers', driver = "ESRI Shapefile")
setwd('..')

setwd('cores')
shapefile(x = cores, file = "cores.shp", overwrite = TRUE)
# writeOGR(cores, dsn = '.', layer = 'cores', driver = "ESRI Shapefile")
setwd('..')

setwd('clusters')
shapefile(x = clusters, file = "clusters.shp", overwrite = TRUE)
# writeOGR(cores, dsn = '.', layer = 'clusters', driver = "ESRI Shapefile")
setwd('../..')

# Define which households are in cluster
o <- sp::over(households_spatial, polygons(clusters))
households_spatial@data$cluster_number <- clusters@data$cluster_number[o]
households_spatial@data$cluster_number <- ifelse(is.na(households_spatial@data$cluster_number),
                                                 0, 
                                                 households_spatial@data$cluster_number)

setwd('final/households')
shapefile(x = households_spatial, file = "households.shp", overwrite = TRUE)
# writeOGR(cores, dsn = '.', layer = 'clusters', driver = "ESRI Shapefile")
setwd('../..')

save(clusters, file = 'final/clusters.RData')
save(buffers, file = 'final/buffers.RData')
save(cores, file = 'final/cores.RData')
save(households_spatial, file = 'households_spatial.RData')
save(inclusion, file = 'inclusion.RData')
save(pongwe_kikoneni,
     pongwe_kikoneni_fortified,
     pongwe_kikoneni_ramisi,
     pongwe_kikoneni_ramisi_fortified,
     file = 'shapefiles.RData')

#################################################

load('final/clusters.RData')
load('final/buffers.RData')
load('final/cores.RData')
# Get aggregate statistics ################################




# ########################################################################
# # Examine accuracy in gps data
# # Update with "flagged" households
# load('households.RData')
# households$suspect <- households$Accuracy >= 20
# households_spatial <- households %>% filter(!is.na(Longitude))
# coordinates(households_spatial) <- ~Longitude+Latitude
# proj4string(households_spatial) <- proj4string(wds_shp)
# # See which cluster each household is in
# o <- sp::over(households_spatial, polygons(study$buffers_ll))
# households_spatial@data$buffer_number <- study$buffers_ll$cluster_number[o]
# o <- sp::over(households_spatial, polygons(study$cores_ll))
# households_spatial@data$core_number <- study$cores_ll$cluster_number[o]
# 
# # See which "suspect" houses are in each area
# sus <- households_spatial@data %>%
#   filter(suspect) %>%
#   group_by(core_number) %>%
#   tally
# sus
# sus <- households_spatial@data %>%
#   filter(suspect) %>%
#   filter(!is.na(core_number)) %>%
#   dplyr::select(hh_id, core_number, num_hh_members_bt_5_15)
# sus
# rr = read_csv('~/Desktop/reconbhousehold.csv') # downloaded from site
# ggplot(data = rr %>% filter(`location_geocode-Accuracy` >= 50),
#        aes(x = `location_geocode-Longitude`,
#            y = `location_geocode-Latitude`,
#            color = `location_geocode-Accuracy`)) +
#   geom_point() +
#   scale_color_gradient2(name = '',
#                      low = 'white',
#                      high = 'red') +
#   labs(title = 'GPS accuracy')
# suspect <- rr %>% filter(`location_geocode-Accuracy` >= 50)
# plot(study$buffers_ll)
# points(suspect$`location_geocode-Longitude`, suspect$`location_geocode-Latitude`, col = 'red')
# suspect_sp <- suspect %>% mutate(lng = `location_geocode-Longitude`,
#                                  lat = `location_geocode-Latitude`)
# coordinates(suspect_sp) <- ~lng+lat
# proj4string(suspect_sp) <- proj4string(study$buffers_ll)
# o <- over(suspect_sp, polygons(study$buffers_ll))
# table(is.na(o))
# table(o)

# # Get the cluster of each household
# hh <- households_spatial
# hh@data$joiner <- paste0(hh@data$hh_id,
#                          hh@data$Longitude,
#                          hh@data$Latitude)
# hh@data <- left_join(hh@data, ss)
# o <- sp::over(households_spatial, polygons(study$buffers_ll))
# hh@data$cluster <- o
# hh@data %>% 
#   group_by(cluster, suspect) %>%
#   summarise(n = n(),
#             five_to_fifteens = sum(num_hh_members_bt_5_15)) %>%
#   filter(!is.na(suspect))

# Provokes problems with cluster 6 (12 five to 15 year olds) and maybe 89 (6)

# # Define suspect households
# rr$joiner <- paste0(rr$hh_id, rr$`location_geocode-Longitude`, rr$`location_geocode-Latitude`)
# ss <- tibble(joiner = rr$joiner[rr$`location_geocode-Accuracy` >= 100],
#              suspect = TRUE)

########################################################################
# RADIAL APPROACH
if(radial_too){
  # Now randomly pick some without overlapping
  unlink('plots', recursive = TRUE)
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
    # this_i <- eligible_indices[1]
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
    distance_df <- buffer_df[buffer_df$eligible,]
    #   gd <- gDistance(current_shape, distance_df, byid = TRUE)
    #   # # nearest
    #   # nearest <- distance_df$rn[which.min(gd)[1]]
    #   # this_i <- which(buffer_df$rn == nearest)
    #   # # furthest
    #   # furthest <- distance_df$rn[which.max(gd)[1]]
    #   # this_i <- which(buffer_df$rn == furthest)
    #   # westernmost
    northernmost <- distance_df$rn[which.min(coordinates(distance_df)[,1])[1]]
    this_i <- which(buffer_df$rn == northernmost)
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
  system("convert -delay 50 -loop 0 *.png clustering_west.gif")
  setwd(owd)
  
  done <- buffer_df@data %>% filter(cluster_number > 0)
  plot(buffer_df[buffer_df@data$cluster_number > 0,])
  plot(poly_df[poly_df@data$cluster_number > 0,], add = T, col = 'red')
  plot(pongwe_kikoneni_ramisi_projected, add = T)
  # Add the social science villages
  points(inclusion_projected, col = adjustcolor('green', alpha.f = 0.5), pch = '.', cex = 2)
  
  nrow(done)
  sum(done$n_hhs_buffer)
  sum(done$n_hhs_core)
  sum(done$num_hh_members_bt_5_15_core)
  sum(done$num_hh_members_bt_5_15_buffer) + sum(done$num_hh_members_gt_15_buffer)
  
  
}

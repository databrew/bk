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
       kwale_fortified,
       kwale,
       ken3_fortified,
       ken3,
       file = 'data.RData')
} else {
  load('data.RData')
}

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

# Convert household locations to spatial
households_spatial <- households %>%
  mutate(x = Longitude, y = Latitude)
coordinates(households_spatial) <- ~x+y
proj4string(households_spatial) <- proj4string(ken3)

# Convert objects to projected UTM reference system
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)

households_spatial_projected <- spTransform(households_spatial, crs)
pongwe_kikoneni_projected <- spTransform(pongwe_kikoneni, crs)

# Sanity test plot
plot(pongwe_kikoneni_projected)
points(households_spatial_projected)

# Remove households which are < 400 meters from the border (since we don't know about their contamination status)
# We don't want to remove coastline though
# Do this by getting distance to nearest non pongwe kikoneni area
not_pongwe_kikoneni <- ken3[ken3@data$NAME_3 != 'Pongwe/Kikoneni',]
not_pongwe_kikoneni_projected <- spTransform(not_pongwe_kikoneni, crs)
not_pongwe_kikoneni_projected <- gUnaryUnion(not_pongwe_kikoneni_projected, id = not_pongwe_kikoneni_projected@data$NAME_0)

distances <- gDistance(not_pongwe_kikoneni_projected, households_spatial_projected, byid = TRUE)
distances <- apply(distances, 1, min)
# Removals
removal_indices <- which(distances < 400)
households_spatial_projected$remove <- FALSE
households_spatial_projected$remove[removal_indices] <- TRUE

# Plot the removals
plot(gBuffer(pongwe_kikoneni_projected, width = -400), col = adjustcolor('black', alpha.f = 0.1))
plot(pongwe_kikoneni_projected, add = TRUE)
points(households_spatial_projected, pch = '.', cex = 3)
points(households_spatial_projected[households_spatial_projected$remove,], col = 'red', pch = '.', cex = 3)
# Carry out the removals
nr <- nrow(households_spatial_projected)
households_spatial_projected <- households_spatial_projected[!households_spatial_projected$remove,]
message(nrow(households_spatial_projected) - nr, ' households removed due to proximity with neighboring ward.')

# Sanity check on total number of kids
n_needed <- 96 * 35
n_actual <- sum(households_spatial_projected@data$num_hh_members_bt_5_15)

#######################################
# Begin clustering
#######################################
# Adhere to these rules: https://docs.google.com/document/d/1tFFpx3ho7lqVuGWNlW5yL6PqdFnEdeic_7Mlphkj3QQ/edit
hh <- households_spatial_projected

# Create one polygon for each household
out_list <- poly_list <- list()
for(i in 1:nrow(hh@data)){
  message('Household ', i, ' of ', nrow(hh@data))
  this_hh <- these_hh <- hh[i,]
  n_kids <- sum(this_hh$num_hh_members_bt_5_15)
  r <- 10
  this_poly <- gBuffer(this_hh, width = r)
  while(n_kids < 35){
    this_poly <- gBuffer(this_hh, width = r)
    these_hh <- hh[!is.na(over(hh, this_poly)),]
    n_kids <- sum(these_hh$num_hh_members_bt_5_15)
    r <- r + 10
  }
  out <- tibble(
    i = i,
    hh_id = hh@data$hh_id[i],
    r = r,
    n_hhs = nrow(these_hh)
  )
  out_list[[i]] <- out
  poly_list[[i]] <- this_poly
}
save.image('image.RData')

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
buffer_df <- buffer_df[order(buffer_df$area),]

# Turn the poly list into a dataframe too
for(i in 1:length(poly_list)){
  x <- gBuffer(poly_list[[i]], width = 400)
  xdf <- SpatialPolygonsDataFrame(Sr = x, data = out[i,], match.ID = FALSE)
  poly_list[[i]] <- xdf
}
poly_df <- do.call('rbind', poly_list)
plot(poly_df, border = adjustcolor('black', alpha.f = 0.1))
# order identically to buffer_df
poly_df <- poly_df[order(buffer_df$area),]

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
  # this_i <- eligible_indices[1]
  # Nearest method
  current_shape <- buffer_df[buffer_df$cluster_number > 0,]
  # if nothing yet, just pick the smallest one
  if(nrow(current_shape) == 0){
    # northernmost point
    this_i <- which.max(coordinates(buffer_df)[,2]) # eligible_indices[1]
  } else {
    # # pick the one which is nearest to the current_shape
    current_shape$id <- 1
    current_shape <- gUnaryUnion(current_shape, id = current_shape$id)
    # ## Nearest to last method
    # current_shape <- last_buffer
    distance_df <- buffer_df[buffer_df$eligible,]
    gd <- gDistance(current_shape, distance_df, byid = TRUE)
    # # nearest
    # nearest <- distance_df$rn[which.min(gd)[1]]
    # this_i <- which(buffer_df$rn == nearest)
    # # furthest
    # furthest <- distance_df$rn[which.max(gd)[1]]
    # this_i <- which(buffer_df$rn == furthest)
    # northernmost
    northernmost <- distance_df$rn[which.max(coordinates(distance_df)[,2])[1]]
    this_i <- which(buffer_df$rn == northernmost)
  }
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
  plot(pongwe_kikoneni_projected, col = 'beige')
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




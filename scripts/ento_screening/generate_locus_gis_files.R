library(readr)
library(dplyr)
library(rgdal)
library(sp)
library(cloudbrewr)

# Download entoscreeningke.zip from databrew.org
# unzip('entoscreeningke.zip')
# Read
entoscreeningke <- read_csv('entoscreeningke.csv')
# Keep only household
hh <- entoscreeningke %>% filter(site == 'Household')
# Keep only those with an appropriate respondent
hh <- hh %>% filter(appropriate_respondent == 'yes')
# Keep only those who are eligible per bednet rule
hh <- hh %>% filter(sleep_bednet == 'yes')
# Keep only those who has the household head in agreement
hh <- hh %>% filter(hh_head_sub_agree == 'yes')
# Keep only those with a new barcode (should be everyone by now)
hh <- hh %>% filter(!is.na(hhid))

# Identify duplicates
dups <- hh %>%
  mutate(dummy = 1) %>%
  group_by(recon_hhid_map) %>%
  mutate(n = n(),
         cs = cumsum(dummy)) %>%
  ungroup %>%
  filter(n > 1) %>%
  # dplyr::select(recon_hhid_manual, hhid_barcode) %>%
  arrange(recon_hhid_manual) %>%
  mutate(label = paste0(recon_hhid_map, '-', cs, '-'))

# View the duplicates and mark as invalid

dups %>% dplyr::select(instanceID, start_time, hhid, recon_hhid_map, recon_hhid_painted, hh_collection) %>% arrange(recon_hhid_map, start_time) %>% View

# TEMPORARY read in the google sheet with modifications so as to "fix" the raw data
# This assumes everything is a SET
modifications <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/1i98uVuSj3qETbrH7beC8BkFmKV80rcImGobBvUGuqbU/edit#gid=0')
entoscreeningke_modifications <- modifications %>%
  filter(Form == 'entoscreeningke')
for(i in 1:nrow(entoscreeningke_modifications)){
  message(i)
  try({
    this_modification <- entoscreeningke_modifications[i,]
    hh[hh$instanceID == this_modification$instanceID, this_modification$Column]  <- this_modification$`Set To`  
  })
  
}

library(ggplot2)
library(ggrepel)
ggplot(data = dups,
       aes(x = `hh_geolocation-Longitude`,
           y = `hh_geolocation-Latitude`,
           color = recon_hhid_map)) +
  geom_jitter() +
  geom_label_repel(aes(label = label))

# # Remove duplicates aribrarily
# hh <- hh %>%
#   arrange(desc(SubmissionDate)) %>%
#   dplyr::distinct(recon_hh_id, .keep_all = TRUE)

# # Load up original ento households from google drive
# https://drive.google.com/drive/u/0/folders/1PK4cBCCqiqtNZhY4vn0E0wXI0u2W1Yk2
ento_households <- rgdal::readOGR('ento_households_shp/', 'households')

# Get non-spatial versions
ento_households_flat <- ento_households@data

# Join the ento_households with the ento screening
out <- left_join(ento_households_flat,
                 hh %>% dplyr::select(recon_hhid_map, hhid_barcode),
                 by = c('hh_id_clea' = 'recon_hhid_map'),
                 multiple = 'all') %>%
  mutate(x = Longitude,
         y = Latitude)
coordinates(out) <- ~x+y
proj4string(out) <- proj4string(ento_households)

owd <- getwd()
setwd('../../analyses/randomization/outputs/ento_households_after_screening_shp/')
message('Writing shapefile')  
households <- out
# households <- households[households@data$cluster_number %in% ento_clusters$cluster_number,]
households@data <- households@data %>% dplyr::select(cluster_number = cluster_nu,
                                                     core_buffer = core_buffe, 
                                                     hh_id_clean = hh_id_clea, 
                                                     hh_id_raw, 
                                                     Longitude, 
                                                     Latitude, 
                                                     hhid_barcode)
raster::shapefile(households, 'households_after_screening.shp', overwrite = TRUE)
setwd(owd)

# Write a csv for use in entomology deliverables 7 and 8 (analyses/randomization)
write_csv(hh, '../../analyses/randomization/inputs/entoscreening_households_cleaned.csv')

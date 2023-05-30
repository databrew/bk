# Generate visit conrol sheets for v0
# slack request: https://bohemiakenya.slack.com/archives/C04R44ATLDR/p1681312566394949
# google sheet with specs: https://docs.google.com/spreadsheets/d/1tqD-Gi2GTlPDJB61oTc-mdcvlsToTtfjLwWikB9Vo50/edit#gid=0


library(readr)
library(dplyr)
library(rgdal)
library(sp)
library(cloudbrewr)

# login to aws via cloudbrewr
if(FALSE){
  tryCatch({
    # login to AWS - this will be bypassed if executed in CI/CD environment
    cloudbrewr::aws_login(
      role_name = 'cloudbrewr-aws-role',
      profile_name =  'cloudbrewr-aws-role',
      pipeline_stage = 'production')
    
  }, error = function(e){
    logger::log_error('AWS Login Failed')
    stop(e$message)
  })
  
}

# Read in cleaned / curated recon data: https://bohemiakenya.slack.com/archives/C042P3A05UP/p1679505186892229
if('recon_raw.RData' %in% dir()){
  load('recon_raw.RData')
} else {
  recon_raw <- cloudbrewr::aws_s3_get_table(
    bucket = 'databrew.org',
    key = 'raw-form/reconbhousehold.csv')
  # recon_raw <- read_csv('reconbhousehold.csv') # had to download from https://s3.console.aws.amazon.com/s3/buckets/databrew.org?region=us-east-1&tab=objects
  save(recon_raw, file = 'recon_raw.RData')
}

if('recon.RData' %in% dir()){
  load('recon.RData')
} else {
  recon <- cloudbrewr::aws_s3_get_table(
    bucket = 'databrew.org',
    key = 'clean-form/reconbhousehold/reconbhousehold.csv') %>%
    dplyr::select(hh_id_clean = hh_id,
                  ward,
                  community_health_unit,
                  village,
                  roof_type,
                  instanceID,
                  todays_date) %>%
    dplyr::left_join(recon_raw %>% dplyr::select(instanceID, hh_id_raw = hh_id)) %>%
    dplyr::select(instanceID, todays_date, hh_id_clean, hh_id_raw, ward,
                  community_health_unit, village, roof_type)
  # recon <- read_csv('reconbhousehold_clean.csv')
  save(recon, file = 'recon.RData')
}


# Get geographic data and randomization data
assignments <- read_csv('../../data_public/randomization/assignments.csv')
households <- rgdal::readOGR('../../data_public/spatial/households/', 'households')

# Get the raw/uncorrected ID into the households data
households@data <- left_join(households@data %>% dplyr::select(-village,
                                                               -ward,
                                                               -community0),
                             recon %>% dplyr::select(
                               hh_id = hh_id_clean,
                               hh_id_raw,
                               village,
                               ward,
                               community_unit = community_health_unit
                             ))

# Reformat columns
hh <- households@data %>%
  dplyr::select(cluster = cluster_n0,
                ward,
                community_unit,
                village,
                map_recon_HHID = hh_id,
                painted_recon_HHID = hh_id_raw,
                roof_material = roof_type,
                wall_material = house_wal0)

# Remove non-study households
hh <- hh %>% filter(cluster > 0)

# Identify mismatches in IDS
if(FALSE){
  mm <- hh
  mm$mismatch <- mm$map_recon_HHID != mm$painted_recon_HHID
  x <- mm %>%
    filter(mismatch) %>%
    group_by(cluster) %>%
    tally %>%
    arrange(desc(n))
  hh <- hh %>% filter(cluster %in% c(3, 40))
  mm %>% filter(cluster %in% c(3, 40)) %>% filter(mismatch) %>% dplyr::select(cluster, contains('HHID'))
}

# Read in the number of fieldworkers per cluster
if('fw_per_cluster.RData' %in% dir()){
  load('fw_per_cluster.RData')
} else {
  g <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/13nTDel4sRvUfMy1s_HyrLo9EhEn6Kj-XSr0llpfoRqk/edit#gid=0') 
  g <- g %>% dplyr::rename(cls = CLs_V0_in_cluster)
  fw_per_cluster <- g
  save(fw_per_cluster, file = 'fw_per_cluster.RData')
}

# Re order
hh <- hh %>%
  arrange(cluster, map_recon_HHID)

# Loop through each cluster and get a page number
clusters <- sort(unique(hh$cluster))

hh$pg <- NA
for(i in 1:length(clusters)){
  this_cluster <- clusters[i]
  # number of casual laborers
  n_cls <- fw_per_cluster %>% filter(cluster == this_cluster) %>% pull(cls)
  these_hh <- hh %>% filter(cluster == this_cluster)
  n_hh <- nrow(these_hh)
  n_per_cl <- ceiling(n_hh / n_cls)
  vec <- rep(1:n_cls, each = n_per_cl)
  vec <- vec[1:n_hh]
  if(length(vec) != length(hh$pg[hh$cluster == this_cluster]) ){
    message(i)
  }
  hh$pg[hh$cluster == this_cluster] <- vec
}
hh$let <- LETTERS[hh$pg]
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
hh$pg <- as.numeric(factor(paste0(add_zero(hh$cluster, 3), '-', add_zero(hh$pg, 3))))



# Loop through every pg and write a pdf
if(!dir.exists('rmds/pdfs')){
  dir.create('rmds/pdfs')
}

households <- hh

# Marta changes:
# no more community unit
households$community_unit <- NULL

pages <- sort(unique(households$pg))

for(i in 1:length(pages)){
  message(i)
  this_page <- pages[i]
  this_data <- households %>%
    filter(pg == this_page) %>%
    # filter(cluster == this_cluster) %>%
    arrange(map_recon_HHID) #%>%
    # dplyr::rename(`Map recon HHID (Locus GIS)` = map_recon_HHID) %>%
    # dplyr::rename(`Painted recon HHID (door)` = painted_recon_HHID)
  this_cluster <- this_data$cluster[1]
  this_letter <- this_data$let[1]
  # Write a csv
  write_csv(this_data, 'hh.csv')
  # Knit the visit control sheet
  rmarkdown::render('rmds/v0vcs.Rmd',
                    output_file = paste0('pdfs/', add_zero(this_cluster, 2), '-', this_letter, '.pdf'))
}



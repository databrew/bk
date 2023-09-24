# https://trello.com/c/QORpzA5d/1938-se-metadata-scripting
# https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=389444343
# libraries <- c('logger', 'purrr', 'dplyr', 'data.table', 'sf', 'sp', 'lubridate', 'readr', 
#                'pdftools', 'rmarkdown', 'knitr', 'kableExtra', 'readr')
# remove.packages(libraries)
# install.packages(libraries)

library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(sp)
library(lubridate)
library(readr)
library(pdftools)
library(rmarkdown)
library(knitr)
library(kableExtra)
library(readr)

# Define production
is_production <- TRUE
# folder <- 'kwale_testing'
real_preselections <- TRUE
folder <- 'kwale'
# folder <- 'health_economics_testing'
# folder <- 'test_of_test'
if(folder == 'kwale'){
  geo_filter <- TRUE
} else {
  geo_filter <- FALSE
}


if(folder %in% c('health_economics_testing', 'test_of_test', 'kwale')){
  real_preselections <- TRUE
  use_real_v0 <- TRUE
} else {
  use_real_v0 <- FALSE
}

if(is_production){
  Sys.setenv(PIPELINE_STAGE = 'production') 
  # raw_or_clean <- 'clean'
} else {
  Sys.setenv(PIPELINE_STAGE = 'develop')
  # raw_or_clean <- 'raw'
}
raw_or_clean <- 'clean'
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
start_fresh <- FALSE
save_empty_objects <- FALSE # for one-off creation of empty objects (so as to make script work in production before some forms have any submitted data)

rr <- function(x){
  message('removing ', nrow(x), ' rows')
  return(head(x, 0))
}

if(start_fresh){
  # Log in
  tryCatch({
    logger::log_info('Attempt AWS login')
    # login to AWS - this will be bypassed if executed in CI/CD environment
    cloudbrewr::aws_login(
      role_name = 'cloudbrewr-aws-role',
      profile_name =  'cloudbrewr-aws-role',
      pipeline_stage = env_pipeline_stage)
    
  }, error = function(e){
    logger::log_error('AWS Login Failed')
    stop(e$message)
  })
  
  
  # Define datasets for which I'm retrieving data
  datasets <- c('v0demography', 'safetynew', 'safety', 'efficacy', 'pfu',
                'pkday0', 'pkdays123',
                'pkfollowup',
                'healtheconbaseline', 'healtheconnew', 'healtheconmonthly')
  datasets_names <- datasets
  
  # Loop through each dataset and retrieve
  # bucket <- 'databrew.org'
  # folder <- 'kwale'
  bucket <- 'databrew.org'
  if(!dir.exists('kwale')){
    dir.create('kwale')
  }
  if(!dir.exists('kwale_testing')){
    dir.create('kwale_testing')
  }
  if(!dir.exists('test_of_tests')){
    dir.create('test_of_tests')
  }
  if(!dir.exists('health_economics_testing')){
    dir.create('health_economics_testing')
  }
  if(!dir.exists('empty_objects')){
    dir.create('empty_objects')
  }
  
  for(i in 1:length(datasets)){
    this_dataset <- datasets[i]
    object_keys <- glue::glue('/{folder}/{raw_or_clean}-form/{this_dataset}',
                              folder = folder,
                              this_dataset = this_dataset)
    output_dir <- glue::glue('{folder}/{raw_or_clean}-form/{this_dataset}',
                             folder = folder,
                             this_dataset = this_dataset)
    unlink(output_dir, recursive = TRUE)
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    print(object_keys)
    aws_s3_bulk_get(
      bucket = bucket,
      prefix = as.character(object_keys),
      output_dir = output_dir
    )
  }
  
  # Read in the datasets
  middle_path <- glue::glue('{folder}/{raw_or_clean}-form/')
  
  # Safety
  tryCatch({
    safety <- read_csv(paste0(middle_path, 'safety/safety.csv'))
    safety_repeat_drug <- read_csv(paste0(middle_path, 'safety/safety-repeat_drug.csv'))
    safety_repeat_individual <- read_csv(paste0(middle_path, 'safety/safety-repeat_individual.csv'))
    safety_repeat_ae_symptom <- read_csv(paste0(middle_path, 'safety/safety-repeat_ae_symptom.csv'))
    if(save_empty_objects){
      safety <- safety %>% rr()
      safety_repeat_drug <- safety_repeat_drug %>% rr()
      safety_repeat_individual <- safety_repeat_individual %>% rr()
      safety_repeat_ae_symptom <- safety_repeat_ae_symptom %>% rr()
      save(safety,safety_repeat_drug,safety_repeat_individual, safety_repeat_ae_symptom, file = 'empty_objects/safety.RData')
    }
  }, error = {
    load('empty_objects/safety.RData')
  })
  # Safety new
  tryCatch({
    safetynew <- read_csv(paste0(middle_path, 'safetynew/safetynew.csv'))
    safetynew_repeat_individual <- read_csv(paste0(middle_path, 'safetynew/safetynew-repeat_individual.csv'))
    if(save_empty_objects){
      safetynew <- safetynew %>% rr()
      safetynew_repeat_individual <- safetynew_repeat_individual %>% rr()
      save(safetynew, safetynew_repeat_individual, file = 'empty_objects/safetynew.RData')
    }
  }, error = {
    load('empty_objects/safetynew.RData')
  })
  #v0demography
  if(use_real_v0){
    # v0demography <- read_csv(paste0(middle_path, 'v0demography/v0demography.csv'))
    # v0demography_repeat_individual <- read_csv(paste0(middle_path, 'v0demography/v0demography-repeat_individual.csv'))
    # save(v0demography, v0demography_repeat_individual, file = 'real_v0.RData')
    load('real_v0.RData')
  } else {
    tryCatch({
      v0demography <- read_csv(paste0(middle_path, 'v0demography/v0demography.csv'))
      v0demography_repeat_individual <- read_csv(paste0(middle_path, 'v0demography/v0demography-repeat_individual.csv'))
      if(save_empty_objects){
        v0demography <- v0demography %>% rr()
        v0demography_repeat_individual <- v0demography_repeat_individual %>% rr()
        save(v0demography, v0demography_repeat_individual, file = 'empty_objects/v0demography.RData')
      }
    },
    error = {
      load('empty_objects/v0demography.RData')
    })
  }

  # efficacy
  tryCatch({
    efficacy <- read_csv(paste0(middle_path, 'efficacy/efficacy.csv'))
    if(save_empty_objects){
      efficacy <- efficacy %>% rr()
      save(efficacy, file = 'empty_objects/efficacy.RData')
    }
  },
  error = {
    load('empty_objects/efficacy.RData')
  })
  # pregnancy follow-up
  tryCatch({
    pfu <- read_csv(paste0(middle_path, 'pfu/pfu.csv'))
    pfu_repeat_preg_symptom <- read_csv(paste0(middle_path, 'pfu/pfu-repeat_preg_symptom.csv'))
    if(save_empty_objects){
      pfu <- pfu %>% rr()
      pfu_repeat_preg_symptom <- pfu_repeat_preg_symptom %>% rr()
      save(pfu, pfu_repeat_preg_symptom, file = 'empty_objects/pfu.RData')
    }
  },
  error = {
    load('empty_objects/pfu.RData')
  })
  # pkday0
  tryCatch({
    pkday0 <- read_csv(paste0(middle_path, 'pkday0/pkday0.csv'))
    if(save_empty_objects){
      pkday0 <- pkday0 %>% rr()
      save(pkday0, file = 'empty_objects/pkday0.RData')
    }
  }, 
  error = {
    load('empty_objects/pkday0.RData')
  })
  # pkdays123
  tryCatch({
    pkdays123 <- read_csv(paste0(middle_path, 'pkdays123/pkdays123.csv'))
    if(save_empty_objects){
      pkdays123 <- pkdays123 %>% rr()
      save(pkdays123, file = 'empty_objects/pkdays123.RData')
    }
  },
  error = {
    load('empty_objects/pkdays123.RData')
  })
  # pkfollowup
  tryCatch({
    pkfollowup <- read_csv(paste0(middle_path, 'pkfollowup/pkfollowup.csv'))
    if(save_empty_objects){
      pkfollowup <- pkfollowup %>% rr()
      save(pkfollowup, file = 'empty_objects/pkfollowup.RData')
    }
  },
  error = {
    load('empty_objects/pkfollowup.RData')
  })
  # healtheconnew
  tryCatch({
    healtheconnew <- read_csv(paste0(middle_path, 'healtheconnew/healtheconnew.csv'))
    healtheconnew_repeat_individual <- read_csv(paste0(middle_path, 'healtheconnew/healtheconnew-repeat_individual.csv'))
    healtheconnew_repeat_miss_work_school <- read_csv(paste0(middle_path, 'healtheconnew/healtheconnew-repeat_miss_work_school.csv'))
    healtheconnew_repeat_other_employment_details <- read_csv(paste0(middle_path, 'healtheconnew/healtheconnew-repeat_other_employment_details.csv'))
    if(save_empty_objects){
      healtheconnew <- healtheconnew %>% rr()
      healtheconnew_repeat_individual <- healtheconnew_repeat_individual %>% rr()
      healtheconnew_repeat_miss_work_school <- healtheconnew_repeat_miss_work_school %>% rr()
      healtheconnew_repeat_other_employment_details <- healtheconnew_repeat_other_employment_details %>% rr()
      save(healtheconnew, healtheconnew_repeat_individual, healtheconnew_repeat_miss_work_school, healtheconnew_repeat_other_employment_details, file = 'empty_objects/healtheconnew.RData')
    }
  },
  error = {
    load('empty_objects/healtheconnew.RData')
  })
  # healtheconbaseline
  tryCatch({
    healtheconbaseline <- read_csv(paste0(middle_path, 'healtheconbaseline/healtheconbaseline.csv'))
    healtheconbaseline_repeat_cattle <- read_csv(paste0(middle_path, 'healtheconbaseline/healtheconbaseline-repeat_cattle.csv'))
    healtheconbaseline_repeat_disease <- read_csv(paste0(middle_path, 'healtheconbaseline/healtheconbaseline-repeat_disease.csv'))
    healtheconbaseline_repeat_individual <- read_csv(paste0(middle_path, 'healtheconbaseline/healtheconbaseline-repeat_individual.csv'))
    healtheconbaseline_repeat_miss_work_school <- read_csv(paste0(middle_path, 'healtheconbaseline/healtheconbaseline-repeat_miss_work_school.csv'))
    healtheconbaseline_repeat_other_employment_details <- read_csv(paste0(middle_path, 'healtheconbaseline/healtheconbaseline-repeat_other_employment_details.csv'))
    if(save_empty_objects){
      healtheconbaseline <- healtheconbaseline %>% rr()
      healtheconbaseline_repeat_cattle <- healtheconbaseline_repeat_cattle %>% rr()
      healtheconbaseline_repeat_disease <- healtheconbaseline_repeat_disease %>% rr()
      healtheconbaseline_repeat_individual <- healtheconbaseline_repeat_individual %>% rr()
      healtheconbaseline_repeat_miss_work_school <- healtheconbaseline_repeat_miss_work_school %>% rr()
      healtheconbaseline_repeat_other_employment_details <- healtheconbaseline_repeat_other_employment_details %>% rr()
      save(healtheconbaseline, healtheconbaseline_repeat_cattle, healtheconbaseline_repeat_disease,
           healtheconbaseline_repeat_individual, healtheconbaseline_repeat_miss_work_school, healtheconbaseline_repeat_other_employment_details, file = 'empty_objects/healtheconbaseline.RData')
    }
  }, error = {
    load('empty_objects/healtheconbaseline.RData')
  })
  # healtheconmonthly
  tryCatch({
    healtheconmonthly <- read_csv(paste0(middle_path, 'healtheconmonthly/healtheconmonthly.csv'))
    healtheconmonthly_repeat_cattle <- read_csv(paste0(middle_path, 'healtheconmonthly/healtheconmonthly-repeat_cattle.csv'))
    healtheconmonthly_repeat_disease <- read_csv(paste0(middle_path, 'healtheconmonthly/healtheconmonthly-repeat_disease.csv'))
    healtheconmonthly_repeat_individual <- read_csv(paste0(middle_path, 'healtheconmonthly/healtheconmonthly-repeat_individual.csv'))
    healtheconmonthly_repeat_miss_work_school <- read_csv(paste0(middle_path, 'healtheconmonthly/healtheconmonthly-repeat_miss_work_school.csv'))
    healtheconmonthly_repeat_other_employment_details <- read_csv(paste0(middle_path, 'healtheconmonthly/healtheconmonthly-repeat_other_employment_details.csv'))
    if(save_empty_objects){
      healtheconmonthly <- healtheconmonthly %>% rr()
      healtheconmonthly_repeat_cattle <- healtheconmonthly_repeat_cattle %>% rr()
      healtheconmonthly_repeat_disease <- healtheconmonthly_repeat_disease %>% rr()
      healtheconmonthly_repeat_individual <- healtheconmonthly_repeat_individual %>% rr()
      healtheconmonthly_repeat_miss_work_school <- healtheconmonthly_repeat_miss_work_school %>% rr()
      healtheconmonthly_repeat_other_employment_details <- healtheconmonthly_repeat_other_employment_details %>% rr()
      save(healtheconmonthly, healtheconmonthly_repeat_cattle, healtheconmonthly_repeat_disease,
           healtheconmonthly_repeat_individual, healtheconmonthly_repeat_miss_work_school, healtheconmonthly_repeat_other_employment_details, file = 'empty_objects/healtheconmonthly.RData')
    }
  },
  error = {
    load('empty_objects/healtheconmonthly.RData')
  })

  # save(safety, safety_repeat_drug,
  #      safety_repeat_individual, safety_repeat_ae_symptom,
  #      safetynew, safetynew_repeat_individual,
  #      v0demography,
  #      v0demography_repeat_individual, efficacy,
  #      pfu, pfu_repeat_preg_symptom,
  #      pkday0, pkdays123, pkfollowup,
  #      healtheconnew,
  #      healtheconnew_repeat_individual,
  #      healtheconnew_repeat_miss_work_school,
  #      healtheconnew_repeat_other_employment_details,
  #      healtheconbaseline,
  #      healtheconbaseline_repeat_cattle,
  #      healtheconbaseline_repeat_disease,
  #      healtheconbaseline_repeat_individual,
  #      healtheconbaseline_repeat_miss_work_school,
  #      healtheconbaseline_repeat_other_employment_details,
  #      healtheconmonthly,
  #      healtheconmonthly_repeat_cattle,
  #      healtheconmonthly_repeat_disease ,
  #      healtheconmonthly_repeat_individual,
  #      healtheconmonthly_repeat_miss_work_school,
  #      healtheconmonthly_repeat_other_employment_details,
  #      file = 'data.RData'
  #      )
  save.image('data.RData')
} else {
  load('data.RData')
}

# Make household ID 5 characters
add_zero <- function (x, n) {
  if(length(x) > 0){
    x <- as.character(x)
    adders <- n - nchar(x)
    adders <- ifelse(adders < 0, 0, adders)
    for (i in 1:length(x)) {
      if (!is.na(x[i])) {
        x[i] <- paste0(paste0(rep("0", adders[i]), collapse = ""), 
                       x[i], collapse = "")
      }
    }
  }
  return(as.character(x))
}
v0demography <- v0demography %>% mutate(hhid = add_zero(hhid, n = 5))
safety <- safety %>% mutate(hhid = add_zero(hhid, n = 5))
safetynew <- safetynew %>% mutate(hhid = add_zero(hhid, n = 5))
efficacy <- efficacy %>% mutate(hhid = add_zero(hhid, n = 5))
healtheconbaseline <- healtheconbaseline %>% mutate(hhid = add_zero(hhid, n = 5))
healtheconnew <- healtheconnew %>% mutate(hhid = add_zero(hhid, n = 5))
healtheconmonthly <- healtheconmonthly %>% mutate(hhid = add_zero(hhid, n = 5))
pfu <- pfu %>% mutate(hhid = add_zero(hhid, n = 5))
pkday0 <- pkday0 %>% mutate(hhid = add_zero(hhid, n = 5))
pkdays123 <- pkdays123 %>% mutate(hhid = add_zero(hhid, n = 5))
pkfollowup <- pkfollowup %>% mutate(hhid = add_zero(hhid, n = 5))

# Capitalize all names
efficacy <- efficacy %>% 
  mutate(member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled))
# healtheconbaseline
healtheconbaseline <- healtheconbaseline %>%
  mutate(household_members = toupper(household_members))
# healtheconbaseline_repeat_cattle
# healtheconbaseline_repeat_disease
# healtheconbaseline_repeat_individual
healtheconbaseline_repeat_individual <- healtheconbaseline_repeat_individual %>%
  mutate(taken = toupper(taken),
         member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled))
# healtheconbaseline_repeat_miss_work_school
# healtheconbaseline_repeat_other_employment_details
# healtheconmonthly
healtheconmonthly <- healtheconmonthly %>%
  mutate(household_members = toupper(household_members))
# healtheconmonthly_repeat_cattle
# healtheconmonthly_repeat_disease
# healtheconmonthly_repeat_individual
healtheconmonthly_repeat_individual <- healtheconmonthly_repeat_individual %>%
  mutate(taken = toupper(taken),
         member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled))
# healtheconmonthly_repeat_miss_work_school
# healtheconmonthly_repeat_other_employment_details
# healtheconnew
# healtheconnew_repeat_individual
healtheconnew_repeat_individual <- healtheconnew_repeat_individual %>%
  mutate(firstname = toupper(firstname),
         lastname = toupper(lastname))
# healtheconnew_repeat_miss_work_school
# healtheconnew_repeat_other_employment_details
# pfu
pfu <- pfu %>%
  mutate(member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled))

# pkday0
pkday0 <- pkday0 %>%
  mutate(member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled))
# pkdays123
pkdays123 <- pkdays123 %>%
  mutate(member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled))
# pkfollowup
pkfollowup <- pkfollowup %>%
  mutate(member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled))
# safety
safety <- safety %>%
  mutate(household_members = toupper(household_members))
# safety_repeat_ae_symptom
# safety_repeat_drug
# safety_repeat_individual
safety_repeat_individual <- safety_repeat_individual %>%
  mutate(member_select = toupper(member_select),
         person_string = toupper(person_string),
         firstname = toupper(firstname),
         lastname = toupper(lastname),
         fullname = toupper(fullname),
         dob_pulled = toupper(dob_pulled),
         taken = toupper(taken))
  
# safetynew
safetynew <- safetynew %>%
  mutate(household_members = toupper(household_members))
# safetynew_repeat_individual
safetynew_repeat_individual <- safetynew_repeat_individual %>%
  mutate(firstname = toupper(firstname),
         lastname = toupper(lastname))
# v0demography
# v0demography_repeat_individual
v0demography_repeat_individual <- v0demography_repeat_individual %>%
  mutate(person_signed_icf = toupper(person_signed_icf),
         firstname = toupper(firstname),
         lastname = toupper(lastname))

# Define a date after which to retrieve data
start_from <- as.Date('2023-09-12')
efficacy <- efficacy %>% filter(todays_date >= start_from)
pfu <- pfu %>% filter(todays_date >= start_from)
pfu_repeat_preg_symptom <- pfu_repeat_preg_symptom %>% filter(PARENT_KEY %in% pfu$KEY)
pkday0 <- pkday0 %>% filter(todays_date >= start_from)
pkdays123 <- pkdays123  %>% filter(todays_date >= start_from)
pkfollowup <- pkfollowup %>% filter(todays_date >= start_from)
safety <- safety %>% filter(todays_date >= start_from)
safety_repeat_ae_symptom <- safety_repeat_ae_symptom %>% filter(PARENT_KEY %in% safety$KEY)
safety_repeat_drug <- safety_repeat_drug %>% filter(PARENT_KEY %in% safety$KEY)
safety_repeat_individual <- safety_repeat_individual %>% filter(PARENT_KEY %in% safety$KEY)
safetynew <- safetynew %>% filter(todays_date >= start_from)
safetynew_repeat_individual <- safetynew_repeat_individual %>% filter(PARENT_KEY %in% safetynew$KEY)
if(!folder %in% c('health_economics_testing', 'test_of_test', 'kwale')){
  v0demography <- v0demography %>% filter(todays_date >= start_from)
  v0demography_repeat_individual <- v0demography_repeat_individual %>% filter(PARENT_KEY %in% v0demography$KEY)
}
healtheconnew <- healtheconnew %>% filter(todays_date >= start_from)
healtheconnew_repeat_individual <- healtheconnew_repeat_individual %>% filter(PARENT_KEY %in% healtheconnew$KEY)
healtheconnew_repeat_miss_work_school <- healtheconnew_repeat_miss_work_school %>% filter(PARENT_KEY %in% healtheconnew$KEY)
healtheconnew_repeat_other_employment_details <- healtheconnew_repeat_other_employment_details  %>% filter(PARENT_KEY %in% healtheconnew$KEY)
healtheconbaseline <- healtheconbaseline %>% filter(todays_date >= start_from)
healtheconbaseline_repeat_cattle <- healtheconbaseline_repeat_cattle  %>% filter(PARENT_KEY %in% healtheconbaseline$KEY)
healtheconbaseline_repeat_disease <- healtheconbaseline_repeat_disease %>% filter(PARENT_KEY %in% healtheconbaseline$KEY)
healtheconbaseline_repeat_individual <- healtheconbaseline_repeat_individual %>% filter(PARENT_KEY %in% healtheconbaseline$KEY)
healtheconbaseline_repeat_miss_work_school <- healtheconbaseline_repeat_miss_work_school %>% filter(PARENT_KEY %in% healtheconbaseline$KEY)
healtheconbaseline_repeat_other_employment_details <- healtheconbaseline_repeat_other_employment_details %>% filter(PARENT_KEY %in% healtheconbaseline$KEY)
healtheconmonthly <- healtheconmonthly %>% filter(todays_date >= start_from)
healtheconmonthly_repeat_cattle <- healtheconmonthly_repeat_cattle %>% filter(PARENT_KEY %in% healtheconmonthly$KEY)
healtheconmonthly_repeat_disease <- healtheconmonthly_repeat_disease %>% filter(PARENT_KEY %in% healtheconmonthly$KEY)
healtheconmonthly_repeat_individual <- healtheconmonthly_repeat_individual %>% filter(PARENT_KEY %in% healtheconmonthly$KEY)
healtheconmonthly_repeat_miss_work_school <- healtheconmonthly_repeat_miss_work_school %>% filter(PARENT_KEY %in% healtheconmonthly$KEY)
healtheconmonthly_repeat_other_employment_details <- healtheconmonthly_repeat_other_employment_details %>% filter(PARENT_KEY %in% healtheconmonthly$KEY)


# Prior to beginning cohort-specific metadata generation, get the location of each house in v0demography and the cluster based on that location
# (this is no longer necessary, since now there are geography variables added to v0demography by aryton)
households_sp <- v0demography %>% 
  arrange(desc(start_time)) %>%
  dplyr::filter(!is.na(Longitude)) %>%
  arrange(desc(todays_date)) %>%
  dplyr::filter(!is.na(Longitude)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(lng = Longitude,
                lat = Latitude,
                hhid) %>%
  dplyr::mutate(x = lng,
                y = lat) %>%
  filter(!is.na(x))
coordinates(households_sp) <- ~x+y
load('../../data_public/spatial/new_clusters.RData')
# buffer clusters by 20 meters so as to 
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)
llcrs <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
clusters_projected <- spTransform(new_clusters, crs)
proj4string(households_sp) <- llcrs
households_sp_projected <- spTransform(households_sp, crs)
clusters_projected_buffered <- rgeos::gBuffer(spgeom = clusters_projected, byid = TRUE, width = 50)
o <- sp::over(households_sp_projected, polygons(clusters_projected_buffered))
households_sp_projected@data$not_in_cluster <- is.na(o)
households_sp_projected@data$cluster_correct <- clusters_projected_buffered@data$cluster_nu[o]
v0demography <- left_join(v0demography, households_sp_projected@data %>% dplyr::select(hhid, not_in_cluster, cluster_correct))
v0demography <- v0demography %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) 
# Remove those outside of cluster boundaries (not doing now due to the fact that this is just testing)
if(geo_filter){
  v0demography <- v0demography %>%
    filter(!geo_not_in_cluster) %>%
    # overwrite the cluster variable
    mutate(cluster = geo_cluster_num) %>%
    filter(!is.na(geo_cluster_num))
    # filter(!is.na(not_in_cluster)) %>%
    # filter(!not_in_cluster) %>%
    # mutate(cluster = cluster_correct) %>%
    # dplyr::select(-cluster_correct)
}

# Prepare some external datasets 
# actual randomization status
assignments <- read_csv('../../analyses/randomization/outputs/assignments.csv')
intervention_assignment <- read_csv('../../analyses/randomization/outputs/intervention_assignment.csv')
# Make fake manual modifications per project specifications
# # NEEDS TO BE CHANGED FOR REAL DATA COLLECTION
if(!real_preselections){
  # fake randomization statuses created by paula in 
  # https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=1430667203
  # g <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=1430667203')
  # g <- g %>% dplyr::distinct(cluster, intervention) %>% 
  #   arrange(intervention)
  # intervention_assignment <- tibble(arm = 2:1, intervention = c('Control', 'Treatment'))
  fake_assignments <- 
    structure(list(cluster_number = c(1, 12, 20, 34, 35, 56, 72), 
                   location = c("North", 'North',
                                "North", "North", "North", "North", "South"),
                   assignment = c(1, 2, 2, 1, 2, 1, 1)), 
              row.names = c(NA, 
                            -7L), 
              class = c("tbl_df", "tbl", "data.frame"))
  assignments <- assignments %>%
    filter(!cluster_number %in% fake_assignments$cluster_number) %>%
    bind_rows(fake_assignments)
} else {
  message('Using real assignations')
}


# End of prerequisites. Now beginning cohort-specific metadata generation
# https://docs.google.com/spreadsheets/d/1mTqNFfnFLnP-WKJNupajVhTJPbbyV2a32kzyIxyGTMM/edit#gid=0
#################################################################################################



##############################################################################
##############################################################################
##############################################################################
# <Health economics> ##############################################################################

# First, create individual data based solely on v0demography
starting_roster <- v0demography_repeat_individual %>% 
  left_join(v0demography %>% dplyr::select(hhid, start_time, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid)
# Add new individuals to the starting roster
new_people <- healtheconnew_repeat_individual %>%
  left_join(healtheconnew %>% dplyr::select(hhid, start_time, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(hhid = as.character(hhid))
starting_roster <- bind_rows(starting_roster, new_people)
starting_roster <- starting_roster %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::rename(fullname_id = roster_name) %>%
  mutate(hecon_name = paste0(firstname, '-', lastname, '-', extid))
# Get the starting health economics status of each individual 
# This should be most recent hecon_individual_status, which should be overridden and turned to eos if in the heconmonthly form hecon_household_status = eos
starting_hecon_statuses <- 
  bind_rows(
    healtheconnew_repeat_individual %>% dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>% 
      left_join(healtheconnew %>% dplyr::select(KEY, start_time) %>% mutate(start_time = as.POSIXct(start_time)), by = c('PARENT_KEY' = 'KEY')),
    healtheconbaseline_repeat_individual %>% dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>% 
      left_join(healtheconbaseline %>% dplyr::select(KEY, start_time) %>% mutate(start_time = as.POSIXct(start_time)), by = c('PARENT_KEY' = 'KEY')),
    healtheconmonthly_repeat_individual %>% dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>% 
      left_join(healtheconmonthly %>% dplyr::select(KEY, start_time) %>% mutate(start_time = as.POSIXct(start_time)), by = c('PARENT_KEY' = 'KEY'))
  ) %>%
  filter(!is.na(hecon_individual_status)) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_hecon_status = hecon_individual_status)
# In the case of someone not being in the above dataset but being preselected for health economics
# the status should be "out"
starting_roster <- left_join(starting_roster, starting_hecon_statuses) %>%
  mutate(starting_hecon_status = ifelse(is.na(starting_hecon_status), 'out', starting_hecon_status))
# if in the heconmonthly form hecon_household_status = eos, the individual should be eos
household_eos <- 
  bind_rows(
    healtheconmonthly %>% mutate(start_time = as.POSIXct(start_time)) %>% dplyr::select(hhid, start_time, hecon_household_status),
    healtheconbaseline %>% mutate(start_time = as.POSIXct(start_time)) %>% dplyr::select(hhid, start_time, hecon_household_status)
  ) %>%
  filter(hecon_household_status == 'eos')
if(nrow(household_eos) > 0){
  starting_roster$starting_hecon_status[starting_roster$hhid %in% household_eos$hhid] <- 'eos'
}
# If ever eos, always eos
ever_eos <- 
  bind_rows(
    healtheconnew_repeat_individual %>% dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>% 
      left_join(healtheconnew %>% dplyr::select(KEY, start_time) %>% mutate(start_time = as.POSIXct(start_time)), by = c('PARENT_KEY' = 'KEY')),
    healtheconbaseline_repeat_individual %>% dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>% 
      left_join(healtheconbaseline %>% dplyr::select(KEY, start_time) %>% mutate(start_time = as.POSIXct(start_time)), by = c('PARENT_KEY' = 'KEY')),
    healtheconmonthly_repeat_individual %>% dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>% 
      left_join(healtheconmonthly %>% dplyr::select(KEY, start_time) %>% mutate(start_time = as.POSIXct(start_time)), by = c('PARENT_KEY' = 'KEY'))
  ) %>%
  filter(!is.na(hecon_individual_status)) %>%
  filter(hecon_individual_status == 'eos') %>%
  filter(!is.na(extid)) %>%
  dplyr::pull(extid)
starting_roster$starting_hecon_status[starting_roster$extid %in% ever_eos] <- 'eos'

# Read in Almudena-created health economics randomization data
health_economics_clusters <- read_csv('../../analyses/randomization/outputs/health_economics_clusters.csv')
health_economics_households <- read_csv('../../analyses/randomization/outputs/health_economics_households.csv') %>%
  mutate(hhid = add_zero(hhid, 5))
ntd_efficacy_preselection <- read_csv('../../analyses/randomization/outputs/health_economics_ntd_efficacy_preselection.csv')
ntd_safety_preselection <- read_csv('../../analyses/randomization/outputs/health_economics_ntd_safety_preselection.csv')
# Paula's instructions (https://docs.google.com/document/d/1Tjpyh8O9oesnDiQgjEih1VpOIZFctpM7UA5aDK--N8o/edit)
starting_roster <- starting_roster %>%
  mutate(ntd_safety_preselected = ifelse(extid %in% ntd_safety_preselection$extid, 1, 0)) %>%
  mutate(ntd_efficacy_preselected = ifelse(extid %in% ntd_efficacy_preselection$extid, 1, 0))
# Get visit 1 safety status
v1_safety_status <- 
  bind_rows(
    safety_repeat_individual %>%
      left_join(safety %>% dplyr::select(KEY, visit, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.POSIXct(start_time), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(start_time, extid, safety_status, visit) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>%
      left_join(safetynew %>% dplyr::select(KEY, visit, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(start_time, extid, safety_status, visit) %>% mutate(form = 'safetynew')
  ) %>%
  # keep only visit 1
  filter(visit == 'V1') %>%
  # keep only most recent (relevant in case of duplicates)
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, v1_safety_status = safety_status)
# Join the safety statuses from v1; in the case of someone who doesn't have a v1 status, this should
# be NA per https://bohemiakenya.slack.com/archives/C058WT0ADBN/p1691172066177829?thread_ts=1691171197.538939&cid=C058WT0ADBN
starting_roster <- left_join(starting_roster, v1_safety_status)
# Deal with migrations / deaths
healthecon_departures <- 
  bind_rows(
    healtheconbaseline_repeat_individual %>% 
      left_join(healtheconbaseline %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
    mutate(extid = as.character(extid)) %>%
      mutate(start_time = as.POSIXct(start_time)) %>%
      filter(!is.na(person_absent_reason)) %>%
      filter(person_absent_reason != 'Absent') %>%
      dplyr::select(start_time, extid, person_absent_reason),
    healtheconmonthly_repeat_individual %>% 
      left_join(healtheconmonthly %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
    mutate(extid = as.character(extid)) %>%
      mutate(start_time = as.POSIXct(start_time)) %>%
      filter(!is.na(person_absent_reason)) %>%
      filter(person_absent_reason != 'Absent') %>%
      dplyr::select(start_time, extid, person_absent_reason)
  )
healthecon_deaths <- healthecon_departures %>%
  filter(person_absent_reason == 'Died')
healthecon_migrations <- healthecon_departures %>%
  filter(person_absent_reason == 'Migrated')
# Set all migrants and dead people to eos
if(nrow(healthecon_deaths) > 0){
  starting_roster$dead <- ifelse(starting_roster$extid %in% healthecon_deaths$extid, 1, 0)
  starting_roster$dead[is.na(starting_roster$dead)] <- 0
  starting_roster$starting_hecon_status <- ifelse(starting_roster$extid %in% healthecon_deaths$extid, 'eos', starting_roster$starting_hecon_status)
} else {
  starting_roster$dead <- 0
}
if(nrow(healthecon_migrations) > 0){
  starting_roster$migrated <- ifelse(starting_roster$extid %in% healthecon_migrations$extid, 1, 0)
  starting_roster$migrated[is.na(starting_roster$migrated)] <- 0
  starting_roster$starting_hecon_status <- ifelse(starting_roster$extid %in% healthecon_migrations$extid, 'eos', starting_roster$starting_hecon_status)
} else {
  starting_roster$migrated <- 0
}

# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1, dead != 1)

# Now use individual data to make household data
households <- starting_roster %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  group_by(hhid) %>%
  summarise(roster = paste0(roster_name[dead == 0 & migrated == 0], collapse = ', '),
            num_members = length(which(dead == 0 & migrated == 0))) %>%
  # get cluster
  left_join(v0demography %>% 
              dplyr::select(hhid, cluster)) %>%
  # get intervention
  left_join(assignments %>% dplyr::select(cluster = cluster_number,
                                          arm = assignment)) %>%
  # get interventions
  left_join(intervention_assignment) 
# Get household heads and geographic information
heads <- v0demography_repeat_individual %>% 
  filter(hh_head_yn == 'yes') %>%
  left_join(v0demography %>% dplyr::select(hhid, start_time, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, extid) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  mutate(household_head = paste0(firstname, ' ', lastname)) %>%
  dplyr::select(hhid, household_head)
households <- households %>%
  left_join(heads)
# Get visits done and baseline in/out status
visits_done <- healtheconbaseline %>%
  mutate(visit = 'V1', hhid = as.character(hhid)) %>%
  dplyr::select(hhid, visit) %>%
  bind_rows(healtheconmonthly %>% 
              mutate(hhid = as.character(hhid)) %>%
              dplyr::select(hhid, visit)) %>% 
  group_by(hhid) %>%
  summarise(visits_done = paste0(sort(unique(visit)), collapse = ', '))
# Get baseline in/out status
right <- healtheconbaseline %>%
  mutate(start_time = as.POSIXct(start_time)) %>%
  dplyr::select(start_time, hhid, hecon_hh_status = hecon_household_status) %>%
  bind_rows(healtheconmonthly %>%
              mutate(start_time = as.POSIXct(start_time)) %>%
              dplyr::select(start_time, hhid, hecon_hh_status = hecon_household_status)) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) 
visits_done <- left_join(visits_done, right)
households <- left_join(households, visits_done)
# If NA hecon_hh_status, this means that the household did not have any  visit, and 
# should be considered "out"
households <- households %>%
  mutate(hecon_hh_status = ifelse(is.na(hecon_hh_status), 'out', hecon_hh_status))

# Get hecon_hh_preselected, 
households <- households %>%
  mutate(hecon_hh_preselected = ifelse(hhid %in% health_economics_households$hhid, 1, 0))
  # Get hecon_members
households$hecon_members <- households$roster
# Create the herd preselected variable (just random values for now)
households <- households %>%
  mutate(herd_preselected = ifelse(hhid %in% health_economics_households$hhid[health_economics_households$herd_preselected == 'yes'], 1, 0))
# Keep only health economics households
households <- households %>% filter(hhid %in% health_economics_households$hhid)
starting_roster <- starting_roster %>% filter(hhid %in% households$hhid)

# Reformat clusters and household IDs
households <- households %>%
  mutate(cluster = add_zero(cluster, 2))

# Write NTD data before removing those who are out/eos for health econ ########################
individuals <- starting_roster %>%
  filter(extid %in% ntd_efficacy_preselection$extid |
           extid %in% ntd_safety_preselection$extid)
if(!dir.exists('ntd_metadata')){
  dir.create('ntd_metadata')
}
write_csv(individuals, 'ntd_metadata/individual_data.csv')

# Write health economic baseline data #########################################
if(!dir.exists('healtheconbaseline_metadata')){
  dir.create('healtheconbaseline_metadata')
}
write_csv(households, 'healtheconbaseline_metadata/household_data.csv')
write_csv(starting_roster, 'healtheconbaseline_metadata/individual_data.csv')

# Create "visit control sheets" for health economics based on these specifications:
# https://docs.google.com/spreadsheets/d/1Ok1JAq4RhAv0dMnVRjl38Ig-6XdEyvBJbCtMzFql9k4/edit#gid=0
save(households, individuals, v0demography, v0demography_repeat_individual, file = 'rmds/health_economics_tables.RData')

# Render the visit 0 household health economics visit control sheet
if(FALSE){
  rmarkdown::render('rmds/health_economics_visit_control_sheet.Rmd')
}

# Write health economic monthly followup data  #########################################
# Remove from health econ those who are out/eos
households <- households %>% filter(!is.na(hecon_hh_status)) %>% filter(hecon_hh_status == 'in')
starting_roster <- starting_roster %>% filter(hhid %in% households$hhid)

if(!dir.exists('healtheconmonthly_metadata')){
  dir.create('healtheconmonthly_metadata')
}
write_csv(households, 'healtheconmonthly_metadata/household_data.csv')
write_csv(starting_roster, 'healtheconmonthly_metadata/individual_data.csv')



# </Health economics> ##############################################################################
##############################################################################
##############################################################################
##############################################################################

##############################################################################
##############################################################################
##############################################################################
# <Safety> ##############################################################################

# Get arrivals
arrivals <- safetynew_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  left_join(safetynew %>% dplyr::select(hhid, KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Arrival')
# Get departures
safety_departures <- safety_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  filter(person_left_household == 1| person_migrated == 1 | person_out_migrated == 1) %>%
  left_join(safety %>% dplyr::select(hhid, KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>%
  mutate(type = 'Departure')
safety_deaths <- safety_repeat_individual %>%
  left_join(safety %>% dplyr::select(KEY, start_time, hhid), by = c('PARENT_KEY' = 'KEY')) %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  mutate(firstname = as.character(firstname), lastname = as.character(lastname),
         sex = as.character(sex), extid = as.character(extid)) %>%
  mutate(start_time = as.POSIXct(start_time)) %>%
  mutate(dob = lubridate::as_datetime(dob)) %>%
  filter(!is.na(person_absent_reason)) %>%
  filter(person_absent_reason %in% c('Died')) %>%
  # filter(person_absent_reason %in% c('Died', 'Migrated')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Died')
# Get efficacy departures (but ignore migrations, per project instructions)
# https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1690186129913529?thread_ts=1689946560.024259&cid=C042KSRLYUA
efficacy_departures <- efficacy %>% 
  filter(!is.na(lastname), !is.na(dob)) %>%
  mutate(firstname = as.character(firstname), lastname = as.character(lastname),
         sex = as.character(sex), extid = as.character(extid)) %>%
  mutate(start_time = as.POSIXct(start_time)) %>%
  mutate(dob = lubridate::as_datetime(dob)) %>%
  filter(!is.na(person_absent_reason)) %>%
  filter(person_absent_reason != 'Absent')
efficacy_deaths <- efficacy_departures %>%
  filter(person_absent_reason %in% c('Died')) %>%
  # filter(person_absent_reason %in% c('Died', 'Migrated')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Died')
# Combine safety and efficacy departures
departures <- bind_rows(safety_departures, safety_deaths, efficacy_deaths)
# events <- bind_rows(arrivals, departures) %>% arrange(todays_date)
# Get the starting roster
starting_roster <- v0demography_repeat_individual %>% 
  left_join(v0demography %>% dplyr::select(hhid, start_time, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(remove = FALSE) %>%
  mutate(index = 1:nrow(.))
# Go through each departure and flag people as dead or migrated
starting_roster$dead <- starting_roster$migrated <- 0
if(nrow(departures) > 0){
  starting_roster$migrated[starting_roster$extid %in% departures$extid[departures$type == 'Departure']] <- 1
  starting_roster$dead[starting_roster$extid %in% departures$extid[departures$type == 'Died']] <- 1
}
# stash those who are dead to keep in starting safety status for efficacy
dead_in_safety <- starting_roster %>%
  filter(dead == 1) %>%
  dplyr::select(extid) %>%
  mutate(starting_safety_status = 'eos')
# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1, dead != 1)

# Go through each arrival and add information
roster <- bind_rows(
  starting_roster,
  arrivals %>% mutate(hhid = as.character(hhid))
) %>%
  arrange(desc(start_time)) %>%
  # keep only the most recent case
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::select(hhid, extid, firstname, lastname, sex, dob, roster_name, dead, migrated) %>%
  # set dead and migrated to 0 if new arrivals
  mutate(dead = ifelse(is.na(dead), 0, dead),
         migrated = ifelse(is.na(migrated), 0, migrated))
# Get household heads and geographic information
heads <- v0demography_repeat_individual %>% 
  filter(hh_head_yn == 'yes') %>%
  left_join(v0demography %>% dplyr::select(hhid, start_time, KEY, village, ward), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid, village, ward) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  mutate(household_head = paste0(firstname, ' ', lastname)) %>%
  dplyr::select(hhid, household_head, village, ward)
# Get number of visits done
visits_done <- 
  safety %>%
  group_by(hhid = as.character(hhid)) %>%
  summarise(visits_done = paste0(sort(unique(visit)), collapse = ', '))
# Build up the metadata, starting with the household IDs
households <- roster %>% 
  group_by(hhid) %>%
  # summarise(roster = paste0(roster_name, collapse = ', '),
  #           num_members = n()) %>%
  summarise(roster = paste0(roster_name[dead == 0 & migrated == 0], collapse = ', '),
            num_members = length(which(dead == 0 & migrated == 0))) %>%
  # get cluster
  left_join(v0demography %>% 
              dplyr::distinct(hhid, .keep_all = TRUE) %>%
              dplyr::select(hhid, cluster))
# get assignments
households <- households %>%
  left_join(assignments %>% dplyr::select(cluster = cluster_number,
                                          arm = assignment)) %>%
  # get interventions
  left_join(intervention_assignment) %>%
  # get household head and geographic info
  left_join(heads) %>%
  # get visits done
  left_join(visits_done)
# Households done, now get individuals
individuals <- roster %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  dplyr::rename(fullname_id = roster_name) %>%
  # get intervention, village, ward, cluster
  left_join(households %>% dplyr::select(hhid, intervention, village, ward, cluster))
# Get starting safety status
starter <- 
  bind_rows(
    v0demography_repeat_individual %>%
      left_join(v0demography %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(start_time, extid, safety_status) %>% mutate(form = 'v0demography'),
    safety_repeat_individual %>%
      left_join(safety %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), safety_status = as.character(safety_status), pregnancy_status = as.character(pregnancy_status)) %>%
      dplyr::select(start_time, extid, safety_status, pregnancy_status) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>%
      left_join(safetynew %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(start_time, extid, safety_status) %>% mutate(form = 'safetynew'),
    efficacy %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(start_time, extid, safety_status) %>% mutate(form = 'efficacy')
  ) %>%
  filter(!is.na(safety_status)) %>%
  arrange(desc(start_time)) %>%
  mutate(starting_pregnancy_status = ifelse(is.na(pregnancy_status), 'out', pregnancy_status)) %>%
  dplyr::select(-pregnancy_status)
right <- starter %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  filter(!is.na(extid), !is.na(safety_status)) %>%
  dplyr::select(extid, starting_safety_status = safety_status, starting_pregnancy_status) 
# Any individual who is not in the above gets an "out" assignation
individuals <- left_join(individuals, right) %>%
  mutate(starting_safety_status = ifelse(is.na(starting_safety_status), 'out', starting_safety_status))
# Double check: any individual who was ever eos is always eos
ever_eos <- starter %>% 
  filter(!is.na(safety_status)) %>%
  filter(safety_status == 'eos') %>% 
  filter(!is.na(extid)) %>%
  # dplyr::distinct(extid) %>%
  pull(extid)
individuals$starting_safety_status[individuals$extid %in% ever_eos] <- 'eos'
# Double check: any individual who was ever pregnant is always safety eos and in pfu
ever_pregnant <- 
  bind_rows(
    safety_repeat_individual %>% filter(!is.na(pregnancy_status)) %>%
      filter(pregnancy_status == 'in') %>%
      dplyr::select(extid),
    pfu %>% 
      dplyr::select(extid)) %>%
  pull(extid)
individuals$starting_safety_status[individuals$extid %in% ever_pregnant] <- 'eos'
individuals$starting_pregnancy_status[individuals$extid %in% ever_pregnant] <- 'in'

# Get starting weight
starting_weights <- 
  bind_rows(
    safety_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(safety %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(start_time, extid, starting_weight = current_weight) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(safetynew %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(start_time, extid, starting_weight = current_weight) %>% mutate(form = 'safetynew'),
    efficacy %>% filter(!is.na(current_weight)) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(start_time, extid, starting_weight = current_weight) %>% mutate(form = 'efficacy'),
    pfu %>% filter(!is.na(weight)) %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), weight = as.character(weight)) %>%
      dplyr::select(start_time, extid, starting_weight = weight) %>% mutate(form = 'pfu')
  ) %>%
  filter(!is.na(extid), !is.na(starting_weight)) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_weight)
individuals <- left_join(individuals, starting_weights)
# Get starting height
starting_heights <- 
  bind_rows(
    safety_repeat_individual %>% 
      mutate(height = ifelse(is.na(height), height_short, height)) %>%
      filter(!is.na(height)) %>%
      left_join(safety %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(start_time, extid, starting_height = height) %>% mutate(form = 'safety') %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), starting_height = as.character(starting_height)),
    safetynew_repeat_individual %>% filter(!is.na(height)) %>%
      left_join(safetynew %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(start_time, extid, starting_height = height) %>% mutate(form = 'safetynew') %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), starting_height = as.character(starting_height)),
    efficacy %>% filter(!is.na(height)) %>%
      dplyr::select(start_time, extid, starting_height = height) %>% mutate(form = 'efficacy') %>%
      mutate(start_time = as.POSIXct(start_time), extid = as.character(extid), starting_height = as.character(starting_height))
  ) %>%
  filter(!is.na(extid), !is.na(starting_height)) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_height)
individuals <- left_join(individuals, starting_heights)

# stash starting safety status for efficacy
starting_safety_statuses <- individuals %>%
  dplyr::select(extid, starting_safety_status)
# Add the efficacy dead
starting_safety_statuses <- 
  bind_rows(
    dead_in_safety,
    starting_safety_statuses
  ) %>%
  dplyr::distinct(extid, .keep_all = TRUE)

# Write csvs
if(!dir.exists('safety_metadata')){
  dir.create('safety_metadata')
}
write_csv(households, 'safety_metadata/household_data.csv')
write_csv(individuals, 'safety_metadata/individual_data.csv')

# Create "visit control sheets" for safety based on these specifications:
# https://docs.google.com/spreadsheets/d/1nco1rPFVk9ZgevR02FdjDF1D8m3jyu9n104vpPXYQ5Q/edit#gid=0

# For the visit control sheets, we need to show the "reason" someone is out
obvious_screening <- 
  bind_rows(
    safety_repeat_individual %>% left_join(safety, by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(extid, start_time, obvious_screening) %>%
      mutate(src = 'Safety'),
    safetynew_repeat_individual %>% left_join(safetynew, by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(extid, start_time, obvious_screening) %>%
      mutate(src = 'Safety')
  ) %>%
  filter(!is.na(obvious_screening)) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, obvious_screening)
ever_present <- safety_repeat_individual %>%
  filter(!is.na(person_present)) %>%
  filter(person_present == 'yes') %>%
  dplyr::select(extid) %>%
  bind_rows(safetynew_repeat_individual %>% dplyr::select(extid)) %>%
  dplyr::distinct(extid)
household_ever_visited <- safety %>%
  dplyr::distinct(hhid)

# If one :
#   is NOT in the "obvious screening" category (ie, they have never been flagged for obvious screening), AND
# HAS had a non-absent visit AND
# is OUT
# this means they get an "out" status without any in-parentheses text.

# Create a dataframe for reasons why "out"
reason_out <- individuals %>%
  dplyr::filter(starting_safety_status == 'out') %>%
  dplyr::select(extid, hhid) %>%
  mutate(in_parentheses = NA) %>%
  left_join(obvious_screening) %>%
  mutate(in_parentheses = ifelse(!is.na(obvious_screening),
                                 obvious_screening,
                                 in_parentheses)) %>%
  mutate(in_parentheses = ifelse(!extid %in% ever_present & hhid %in% household_ever_visited$hhid, 
                                 'absent', 
                                 in_parentheses)) %>%
  mutate(in_parentheses = ifelse(is.na(in_parentheses), '',
                                 paste0(' (', in_parentheses, ')'))) %>%
  mutate(status = paste0('out', in_parentheses)) %>%
  dplyr::select(extid, status)

# Visit control sheets should be divided not by cluster
# but by "sub-cluster" based on the number of fieldworkers in that cluster
# this instruction has been provided at https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1695366280214239
# Load the spreadsheet from the project
if('cl_per_cluster.RData' %in% dir()){
  load('cl_per_cluster.RData')
} else {
  cl_per_cluster <- readxl::read_excel('inputs/CL per cluster for DBrew.xlsx')
  save(cl_per_cluster, file = 'cl_per_cluster.RData')
}
# Assign a "vcs" to each individual (ie, which sub-group they are part of)
vcs_list <- list()
for(i in 1:nrow(cl_per_cluster)){
  this_input <- cl_per_cluster[i,]  
  these_households <- households %>%
    filter(cluster == this_input$`Cluster assigned`)
  n_hhs <- nrow(these_households)
  n_cls <- this_input$`Total Cls in cluster`
  hhs_per_cl <- ceiling(n_hhs / n_cls)
  # Define the division points
  groupings <- rep(1:n_cls, each = hhs_per_cl)
  groupings <- LETTERS[groupings]
  groupings <- groupings[1:n_hhs]
  out <- these_households %>% dplyr::select(hhid, cluster) %>%
    mutate(grp = groupings) %>%
    mutate(vcs = paste0(add_zero(cluster, 2), grp))
  vcs_list[[i]] <- out
}
vcs_data <- bind_rows(vcs_list) %>%
  dplyr::select(hhid, VCS = vcs)
# Add to individual dataframe
individuals <- individuals %>% left_join(vcs_data)


save(reason_out, individuals, households, v0demography, v0demography_repeat_individual, file = 'rmds/safety_tables.RData')

# Render the visit 0 household health economics visit control sheet
options(kableExtra.latex.load_packages = FALSE)

if(FALSE){
  if(!dir.exists('rmds/safety_visit_control_sheets')){
    dir.create('rmds/safety_visit_control_sheets')
  }
  # # Reload the data file
  load('rmds/safety_tables.RData')
  vcs_list <- sort(unique(individuals$VCS))
  for(a in 1:length(vcs_list)){
    this_vcs <- vcs_list[a]
    message(a, ' of ', length(vcs_list), ' WD: ', getwd())
    rmarkdown::render('rmds/safety_visit_control_sheet.Rmd', params = list('vcs' = this_vcs),
                      output_file = paste0( getwd(), '/safety_visit_control_sheets/', add_zero(this_vcs, 3), '.pdf'))
  }
  # Count how many pages in each document
  pdfs <- dir('rmds/safety_visit_control_sheets')
  pdfs_data_list <- list()
  for(i in 1:length(pdfs)){
    file_name <- pdfs[i]
    pdf_data <- pdf_info(paste0('rmds/safety_visit_control_sheets/', file_name))
    out <- tibble(file_name,
                  n_pages = pdf_data$pages
    )
    pdfs_data_list[[i]] <- out
  }
  pdfs_data <- bind_rows(pdfs_data_list)
  # See if odd number of pages
  for(i in 1:nrow(pdfs_data)){
    this_file_name <- pdfs_data$file_name[i]
    this_n_pages <- pdfs_data$n_pages[i]
    is_odd <- this_n_pages %% 2 != 0
    if(is_odd){
      file.copy('blank_page.pdf',
                paste0('rmds/safety_visit_control_sheets/',
                       gsub('.pdf', '_blank.pdf', this_file_name)),
                overwrite = TRUE)
    }
  }
  # Now stitch them all together
  owd <- getwd()
  setwd('rmds/safety_visit_control_sheets/')
  system_text <- paste0('pdftk *.pdf cat output safety_visit_control_sheets.pdf')
  system(system_text)
  setwd(owd)
}
# </Safety> ##############################################################################
##############################################################################
##############################################################################
##############################################################################




# <Efficacy> ##############################################################################

# Get the starting roster
starting_roster <- v0demography_repeat_individual %>% 
  left_join(v0demography %>% dplyr::select(hhid, start_time, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(remove = FALSE) %>%
  mutate(index = 1:nrow(.))
# Flag the deaths (the "efficacy_deaths" and "efficacy_deaths" object is created in the safety section)
# Go through each departure and flag people as dead or migrated
starting_roster$dead <- starting_roster$migrated <- 0

if(nrow(efficacy_deaths) > 0){
  starting_roster$dead[starting_roster$extid %in% efficacy_deaths$extid] <- 1
}
if(nrow(efficacy_departures) > 0){
  starting_roster$migrated[starting_roster$extid %in% efficacy_departures$extid[efficacy_departures$person_absent_reason == 'Migrated']] <- 1
}

# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1, dead != 1)

roster <- starting_roster %>%  
  arrange(desc(start_time)) %>%
  # keep only the most recent case
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::select(hhid, extid, firstname, lastname, sex, dob, roster_name, dead, migrated) %>%
  # set dead and migrated to 0 if new arrivals
  mutate(dead = ifelse(is.na(dead), 0, dead),
         migrated = ifelse(is.na(migrated), 0, migrated))
# Add more info to individuals
individuals <- roster %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  dplyr::rename(fullname_id = roster_name) %>%
  # get intervention, village, ward, cluster
  left_join(households %>% dplyr::select(hhid, intervention, village, ward, cluster))
# Get starting weight
# (generated in safety section)
individuals <- left_join(individuals, starting_weights)
# Get starting height (generated in safety section)
individuals <- left_join(individuals, starting_heights)
# # Get efficacy status  ################################################
# g <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=1430667203')
# g$extid[g$efficacy_preselected == 1]
if(real_preselections){
  efficacy_selection <- read_csv('../../analyses/randomization/outputs/efficacy_selection.csv')
  efficacy_preselected_ids <- sort(unique(efficacy_selection$extid))
  # # # one-off: list of households in efficacy with cls for mercy
  # mercy <- efficacy_selection %>%
  #   left_join(v0demography_repeat_individual %>% dplyr::select(extid, PARENT_KEY)) %>%
  #   left_join(v0demography %>% dplyr::distinct(hhid, .keep_all = TRUE) %>%
  #               dplyr::select(hhid, KEY, wid), by = c('PARENT_KEY' = 'KEY')) %>%
  #   mutate(hhid = substr(extid, 1, 5)) %>%
  #   group_by(hhid) %>%
  #   summarise(efficacy_ids = paste0(sort(unique(extid)), collapse = ';'),
  #             cl = dplyr::first(wid)) %>%
  #   ungroup %>%
  #   arrange(cl, hhid)
  # write_csv(mercy, '~/Desktop/efficacy_selections_with_cl.csv')
} else {
  efficacy_preselected_ids <- c("01000-01", "01000-04", "12013-03", "34102-02", "34102-03", 
                                "20001-01", "20001-02", "72034-01", "72034-02")
}

individuals$efficacy_preselected <- ifelse(individuals$extid %in% efficacy_preselected_ids, 1, 0)
efficacy_ids <- sort(unique(individuals$extid[individuals$efficacy_preselected == 1]))
# Get some further efficacy status variables
# starting_efficacy_status
right <- 
  efficacy %>% arrange(desc(start_time)) %>%
  filter(!is.na(efficacy_status)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(starting_efficacy_status = efficacy_status) %>%
  dplyr::select(extid, starting_efficacy_status) %>%
  mutate(extid = as.character(extid))
# if someone has not yet been visited in efficacy AND they are preselected, they should get
# a starting status of "out"
individuals <- left_join(individuals, right) %>%
  mutate(starting_efficacy_status = ifelse(is.na(starting_efficacy_status) & extid %in% efficacy_ids,
                                           'out',
                                           starting_efficacy_status))
# efficacy_absent_most_recent_visit
right <- 
  efficacy %>% arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  filter(!is.na(person_absent), !is.na(extid)) %>%
  dplyr::mutate(efficacy_absent_most_recent_visit = person_absent) %>%
  dplyr::select(extid, efficacy_absent_most_recent_visit)
if(nrow(right) > 0){
  individuals <- left_join(individuals, right) %>%
    mutate(efficacy_absent_most_recent_visit = ifelse(is.na(efficacy_absent_most_recent_visit), 0, efficacy_absent_most_recent_visit))
} else {
  individuals$efficacy_absent_most_recent_visit <- 0
}
# efficacy_most_recent_visit_present
right <- 
  efficacy %>% arrange(desc(start_time)) %>%
  filter(!is.na(person_present_continue)) %>%
  filter(person_present_continue == 1) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(efficacy_absent_most_recent_visit = as.numeric(gsub('V', '', visit))) %>%
  dplyr::select(extid, efficacy_most_recent_visit_present,
                efficacy_most_recent_present_date = todays_date) %>%
  mutate(efficacy_most_recent_present_date = paste0('.', as.character(efficacy_most_recent_present_date))) %>%
  mutate(extid = as.character(extid))
individuals <- left_join(individuals, right) 
# efficacy_visits_done
# (this includes absent visits) https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1690378013017559?thread_ts=1690307177.615709&cid=C042KSRLYUA
right <- efficacy %>%
  filter(!is.na(extid)) %>%
  group_by(extid) %>%
  summarise(efficacy_visits_done = paste0(sort(unique(visit)), collapse = ', '))
if(nrow(right) > 0){
  individuals <- left_join(individuals, right) 
} else {
  individuals$efficacy_visits_done <- ""
}
# If ever eos, always eos
ever_eos <- efficacy %>% filter(efficacy_status == 'eos') %>% filter(!is.na(extid)) %>% dplyr::pull(extid)

# Keep only individuals who are currently "in" or "out" of efficacy
individuals <- individuals %>% 
  filter(!extid %in% ever_eos) %>%
  filter(!is.na(starting_efficacy_status)) %>%
  filter(starting_efficacy_status %in% c('in', 'out'))

# Add a "starting_safety_status" to efficacy
individuals <- individuals %>% left_join(starting_safety_statuses)

# Write csvs
if(!dir.exists('efficacy_metadata')){
  dir.create('efficacy_metadata')
}
write_csv(individuals, 'efficacy_metadata/individual_data.csv')

# Create "visit control sheets" for efficacy based on these specifications:
# https://docs.google.com/spreadsheets/d/1nco1rPFVk9ZgevR02FdjDF1D8m3jyu9n104vpPXYQ5Q/edit#gid=683638136
save(individuals, v0demography, v0demography_repeat_individual, file = 'rmds/efficacy_tables.RData')

# Render the visit 0 household health economics visit control sheet
# Render the visit 0 household health economics visit control sheet
options(kableExtra.latex.load_packages = FALSE)
# for(i in 1:5){
#   Sys.sleep(1)
#   message(i, '\n')
# }

if(TRUE){
  if(!dir.exists('rmds/efficacy_visit_control_sheets')){
    dir.create('rmds/efficacy_visit_control_sheets')
  }
  load('rmds/efficacy_tables.RData')
  vcs_list <- sort(unique(individuals$cluster))
  for(a in 1:length(vcs_list)){
    this_vcs <- vcs_list[a]
    message(a, ' of ', length(vcs_list), ' WD: ', getwd())
    rmarkdown::render('rmds/efficacy_visit_control_sheet.Rmd', params = list('vcs' = this_vcs),
                      output_file = paste0( getwd(), '/efficacy_visit_control_sheets/', add_zero(this_vcs, 3), '.pdf'))
  }
  
 
  # Now stitch them all together
  owd <- getwd()
  setwd('rmds/efficacy_visit_control_sheets/')
  system_text <- paste0('pdftk *.pdf cat output efficacy_visit_control_sheets.pdf')
  system(system_text)
  setwd(owd)
  
  
}
# </Efficacy> ##############################################################################
##############################################################################
##############################################################################
##############################################################################




# <pfu> ##############################################################################

# Get anyone who was ever pregnant
# (no need for safetynew since they would be excluded from safety and therefore pregnancy in the first place)
pfu_in <-   
  bind_rows(
    pfu %>% filter(!is.na(pregnancy_status)) %>%
      dplyr::select(start_time, extid, pregnancy_status, visit) %>% mutate(form = 'pfu') %>%
      mutate(start_time = as.POSIXct(start_time)) %>%
      arrange(desc(start_time)),
    safety_repeat_individual %>% filter(!is.na(pregnancy_status)) %>%
      left_join(safety %>% dplyr::select(KEY, visit, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(extid, pregnancy_status, visit, start_time) %>% mutate(form = 'safety') %>%
      mutate(start_time = as.POSIXct(start_time)) %>%
      arrange(desc(start_time)),
  ) %>%
  # arrange(desc(todays_date)) %>%
  # arrange(desc(start_time)) %>%
  filter(!is.na(extid), !is.na(pregnancy_status)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, pregnancy_status) %>%
  filter(pregnancy_status == 'in')

# Get the starting roster
starting_roster <- v0demography_repeat_individual %>% 
  dplyr::select(PARENT_KEY, firstname, lastname, dob, sex, extid) %>%
  left_join(v0demography %>% dplyr::select(hhid, start_time, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  bind_rows(safetynew_repeat_individual %>%
              dplyr::select(PARENT_KEY, firstname, lastname, dob, sex, extid) %>%
              left_join(safetynew %>% dplyr::select(KEY, hhid, start_time), by = c('PARENT_KEY' = 'KEY'))) %>%
  bind_rows(safety_repeat_individual %>%
              dplyr::select(PARENT_KEY, firstname, lastname, dob, sex, extid) %>%
              left_join(safety %>% dplyr::select(KEY, hhid, start_time), by = c('PARENT_KEY' = 'KEY'))) %>%
  # fix dates
  mutate(dob = as.character(as.Date(dob))) %>%
  # # keep only those who are in pfu
  # filter(extid %in% pfu_in$extid) %>%
  filter(extid %in% pfu_in$extid) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(remove = FALSE) 
# Go through each departure and flag people as dead or migrated
starting_roster$dead <- starting_roster$migrated <- 0
pfu_departures <- pfu %>%
  filter(!is.na(person_absent_reason)) %>%
  filter(person_absent_reason != 'Absent')
if(nrow(pfu_departures) > 0){
  starting_roster$dead[starting_roster$extid %in% pfu_departures$extid[pfu_departures$person_absent_reason == 'Died']] <- 1
  starting_roster$migrated[starting_roster$extid %in% pfu_departures$extid[pfu_departures$person_absent_reason == 'Migrated']] <- 1
}

# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1, dead != 1)

roster <- starting_roster %>%  
  arrange(desc(start_time)) %>%
  # keep only the most recent case
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::select(hhid, extid, firstname, lastname, sex, dob, roster_name, dead, migrated)
# Add more info to individuals
individuals <- roster %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  dplyr::rename(fullname_id = roster_name) #%>%
  # # get intervention, village, ward, cluster
  # left_join(households %>% dplyr::select(hhid, intervention, village, ward, cluster))
# Get starting weight
# (generated in safety section)
individuals <- left_join(individuals, starting_weights)
# Get starting height (generated in safety section)
individuals <- left_join(individuals, starting_heights)
# starting_pregnancy_status
# (in for everybody, already filtered in the pfu)
individuals <- individuals %>% mutate(starting_pregnancy_status = 'in')
# pregnancy_consecutive_absences	 ########################## 
# individuals$pregnancy_consecutive_absences <- NA #sample(c(NA, 0:7), nrow(individuals), replace = TRUE)
# See documentation at https://docs.google.com/document/d/1BVMsJE1KX0gG5Blu21HrGbuZ15x93cdRYiThyNp6jDQ/edit
# July 31 2023: agreed with Xing to deprecate this in favor of "date of last non-absent pregnancy visit"
# pregnancy_visits_done
right <-  pfu %>%
  filter(!is.na(extid)) %>%
  group_by(extid) %>%
  summarise(pregnancy_visits_done = paste0(sort(unique(visit)), collapse = ', '))
individuals <- left_join(individuals, right) 
#pregnancy_most_recent_present_date	
right <- 
  pfu %>% arrange(desc(start_time)) %>%
  filter(!is.na(person_present_continue)) %>%
  filter(person_present_continue == 1) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  # dplyr::mutate(pregnancy_most_recent_visit_present = as.numeric(gsub('V', '', visit))) %>%
  dplyr::select(extid, #pregnancy_most_recent_visit_present,
                pregnancy_most_recent_present_date = todays_date) %>%
  mutate(pregnancy_most_recent_present_date = paste0('.', as.character(pregnancy_most_recent_present_date)))
individuals <- left_join(individuals, right) 

# Write csvs
if(!dir.exists('pfu_metadata')){
  dir.create('pfu_metadata')
}
write_csv(individuals, 'pfu_metadata/individual_data.csv')
# </pfu> ##############################################################################
##############################################################################
##############################################################################
##############################################################################


# <pk> ##############################################################################
# Column specs: https://docs.google.com/spreadsheets/d/1mTqNFfnFLnP-WKJNupajVhTJPbbyV2a32kzyIxyGTMM/edit#gid=0
# Card https://trello.com/c/1o8gzkTg/2003-pk-refactoring-remove-from-safety-create-stand-alone
# "Should be a static list of all those who are PK preselected

# Get PK clusters
pk_clusters <- read_csv('../../analyses/randomization/outputs/pk_clusters.csv')
# Get PK individuals
pk_individuals <- read_csv('../../analyses/randomization/outputs/pk_individuals.csv')
pk_preselected_ids <- pk_individuals$extid
# Create list of PK individuals
# Get the starting roster
individuals <- v0demography_repeat_individual %>% 
  left_join(v0demography %>% dplyr::select(hhid, start_time, cluster, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  mutate(cluster = add_zero(cluster, 2)) %>%
  dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid, cluster) %>%
  filter(extid %in% pk_preselected_ids) %>%
  arrange(desc(start_time)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  mutate(fullname_id = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%  
  dplyr::select(firstname, lastname,
                fullname_dob,
                fullname_id,
                sex, hhid,
                extid, cluster)
# Based on that list of individuals, create a households data set
households <- individuals %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(cluster, hhid) %>%
  arrange(cluster, hhid)
# Remove cluster variable from individuals (not specified)
individuals$cluster <- NULL

# Write csvs
if(!dir.exists('pk_metadata')){
  dir.create('pk_metadata')
}
write_csv(individuals, 'pk_metadata/individual_data.csv')
write_csv(households, 'pk_metadata/household_data.csv')
# </pk> ##############################################################################



# Combine them all into one
file.remove('efficacy_metadata.zip')
file.remove('health_economics_metadata.zip')
file.remove('pfu_metadata.zip')
file.remove('safety_metadata.zip')
file.remove('ntd_metadata.zip')
file.remove('pk_metadata.zip')
zip(zipfile = 'efficacy_metadata.zip', files = 'efficacy_metadata/')
zip(zipfile = 'health_economics_metadata.zip', files = 'health_economics_metadata//')
zip(zipfile = 'pfu_metadata.zip', files = 'pfu_metadata/')
zip(zipfile = 'safety_metadata.zip', files = 'safety_metadata/')
zip(zipfile = 'ntd_metadata.zip', files = 'ntd_metadata/')
zip(zipfile = 'pk_metadata.zip', files = 'pk_metadata/')


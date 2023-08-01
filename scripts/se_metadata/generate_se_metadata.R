# visit for which metadata is being prepared
visit_number <- 1

# https://trello.com/c/QORpzA5d/1938-se-metadata-scripting
# https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=389444343

library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(sp)
library(lubridate)
library(readr)

# Define production
is_production <- FALSE
if(is_production){
  Sys.setenv(PIPELINE_STAGE = 'production') 
  # raw_or_clean <- 'clean'
} else {
  Sys.setenv(PIPELINE_STAGE = 'develop')
  # raw_or_clean <- 'raw'
}
raw_or_clean <- 'clean'
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
start_fresh <- TRUE

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
                'pk_day0', 'pkdays123',
                'pkfollowup')
  datasets_names <- datasets
  
  # Loop through each dataset and retrieve
  # bucket <- 'databrew.org'
  # folder <- 'kwale'
  bucket <- 'databrew.org'
  if(is_production){
    folder <- 'kwale'
  } else {
    folder <- 'kwale_testing'
  }
  
  for(i in 1:length(datasets)){
    this_dataset <- datasets[i]
    object_keys <- glue::glue('/{folder}/{raw_or_clean}-form/{this_dataset}',
                              folder = folder,
                              this_dataset = this_dataset)
    output_dir <- glue::glue('{folder}/{raw_or_clean}-form/{this_dataset}',
                             folder = folder,
                             this_dataset = this_dataset)
    dir.create(object_keys, recursive = TRUE, showWarnings = FALSE)
    print(object_keys)
    cloudbrewr::aws_s3_bulk_get(
      bucket = bucket,
      prefix = as.character(object_keys),
      output_dir = output_dir
    )
  }
  
  # Read in the datasets
  middle_path <- glue::glue('{folder}/{raw_or_clean}-form/')
  # Safety
  safety <- read_csv(paste0(middle_path, 'safety/safety.csv'))
  safety_repeat_drug <- read_csv(paste0(middle_path, 'safety/safety-repeat_drug.csv'))
  safety_repeat_individual <- read_csv(paste0(middle_path, 'safety/safety-repeat_individual.csv'))
  safety_repeat_ae_symptom <- read_csv(paste0(middle_path, 'safety/safety-repeat_ae_symptom.csv'))
  # Safety new
  safetynew <- read_csv(paste0(middle_path, 'safetynew/safetynew.csv'))
  safetynew_repeat_individual <- read_csv(paste0(middle_path, 'safetynew/safetynew-repeat_individual.csv'))
  #v0demography
  v0demography <- read_csv(paste0(middle_path, 'v0demography/v0demography.csv'))
  v0demography_repeat_individual <- read_csv(paste0(middle_path, 'v0demography/v0demography-repeat_individual.csv'))
  # efficacy
  efficacy <- read_csv(paste0(middle_path, 'efficacy/efficacy.csv'))
  # pregnancy follow-up
  pfu <- read_csv(paste0(middle_path, 'pfu/pfu.csv'))
  pfu_repeat_preg_symptom <- read_csv(paste0(middle_path, 'pfu/pfu-repeat_preg_symptom.csv'))
  # pk_day0
  pk_day0 <- read_csv(paste0(middle_path, 'pk_day0/pk_day0.csv'))
  # pkdays123
  pkdays123 <- read_csv(paste0(middle_path, 'pkdays123/pkdays123.csv'))
  # pkfollowup
  pkfollowup <- read_csv(paste0(middle_path, 'pkfollowup/pkfollowup.csv'))
  
  save(safety, safety_repeat_drug,
       safety_repeat_individual, safety_repeat_ae_symptom,
       safetynew, safetynew_repeat_individual,
       v0demography,
       v0demography_repeat_individual, efficacy,
       pfu, pfu_repeat_preg_symptom,
       pk_day0, pkdays123, pkfollowup,
       file = 'data.RData')
} else {
  load('data.RData')
}

# Define a date after which to retrieve data
start_from <- as.Date('2023-08-01')
efficacy <- efficacy %>% filter(todays_date >= start_from)
pfu <- pfu %>% filter(todays_date >= start_from)
pfu_repeat_preg_symptom <- pfu_repeat_preg_symptom %>% filter(PARENT_KEY %in% pfu$KEY)
pk_day0 <- pk_day0 %>% filter(todays_date >= start_from)
pkdays123 <- pkdays123  %>% filter(todays_date >= start_from)
pkfollowup <- pkfollowup %>% filter(todays_date >= start_from)
safety <- safety %>% filter(todays_date >= start_from)
safety_repeat_ae_symptom <- safety_repeat_ae_symptom %>% filter(PARENT_KEY %in% safety$KEY)
safety_repeat_drug <- safety_repeat_drug %>% filter(PARENT_KEY %in% safety$KEY)
safety_repeat_individual <- safety_repeat_individual %>% filter(PARENT_KEY %in% safety$KEY)
safetynew <- safetynew %>% filter(todays_date >= start_from)
safetynew_repeat_individual <- safetynew_repeat_individual %>% filter(PARENT_KEY %in% KEY)
v0demography <- v0demography %>% filter(todays_date >= start_from)
v0demography_repeat_individual <- v0demography_repeat_individual %>% filter(PARENT_KEY %in% v0demography$KEY)

# Get arrivals
arrivals <- safetynew_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  left_join(safetynew %>% dplyr::select(hhid, KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Arrival')

# Get departures
safety_departures <- safety_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  filter(person_left_household == 1| person_migrated == 1 | person_out_migrated == 1) %>%
  left_join(safety %>% dplyr::select(hhid, KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid)
    
# Get efficacy departures (but ignore migrations, per project instructions)
# https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1690186129913529?thread_ts=1689946560.024259&cid=C042KSRLYUA
efficacy_departures <- efficacy %>% 
  filter(!is.na(lastname), !is.na(dob)) %>%
  mutate(firstname = as.character(firstname), lastname = as.character(lastname),
         sex = as.character(sex), extid = as.character(extid)) %>%
  mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
  mutate(dob = lubridate::as_datetime(dob)) %>%
  filter(!is.na(person_absent_reason)) %>%
  # filter(person_absent_reason %in% c('Died')) %>%
  filter(person_absent_reason %in% c('Died', 'Migrated')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Departure')
departures <- bind_rows(safety_departures, efficacy_departures)

events <- bind_rows(arrivals, departures) %>% arrange(todays_date)

# Get the starting roster
starting_roster <- v0demography_repeat_individual %>% 
  left_join(v0demography %>% dplyr::select(hhid, todays_date, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(remove = FALSE) %>%
  mutate(index = 1:nrow(.))

# Go through each departure and remove people
if(nrow(departures) > 0){
  for(i in 1:nrow(departures)){
    this_departure <- departures[i,]
    this_removal <- starting_roster %>%
      filter(extid == this_departure$extid,
             hhid == this_departure$hhid)
    if(nrow(this_removal) > 0){
      message('OK: Departure of ', this_departure$extid, ' from household: ', this_departure$hhid)
      starting_roster$remove[starting_roster$index %in% this_removal$index] <- TRUE
    } else {
      message('NOT OK: Departure of ', this_departure$extid, ' from household: ', this_departure$hhid, '. This person was not at this house in the first place, so skipping.')
      
    }
  }
  starting_roster <- starting_roster %>% filter(!remove)
}

# Go through each arrival and add
roster <- bind_rows(
  starting_roster,
  arrivals
) %>%
  arrange(desc(todays_date)) %>%
  # keep only the most recent case
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::select(hhid, extid, firstname, lastname, sex, dob, roster_name) 

# Prepare some external datasets
assignments <- read_csv('../../analyses/randomization/outputs/assignments.csv')
intervention_assignment <- read_csv('../../analyses/randomization/outputs/intervention_assignment.csv')

# Get household heads and geographic information
heads <- v0demography_repeat_individual %>% 
  filter(hh_head_yn == 'yes') %>%
  left_join(v0demography %>% dplyr::select(hhid, todays_date, KEY, village, ward), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid, village, ward) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  mutate(household_head = paste0(firstname, ' ', lastname)) %>%
  dplyr::select(hhid, household_head, village, ward)

# Get number of visits done
visits_done <- 
  safety %>%
  group_by(hhid) %>%
  summarise(visits_done = paste0(sort(unique(visit)), collapse = ', '))

# Build up the metadata, starting with the household IDs
households <- roster %>% 
  group_by(hhid) %>%
  summarise(roster = paste0(roster_name, collapse = ', '),
            num_members = n()) %>%
  # get cluster
  left_join(v0demography %>% 
              # filter(!is.na(cluster)) %>%
              dplyr::distinct(hhid, .keep_all = TRUE) %>%
              dplyr::select(hhid, cluster))

# Get location and filter out households which are not in a cluster
if(FALSE){ # not currently running since no test households are in clusters
  households_sp <- households %>%
    left_join(v0demography %>% 
                dplyr::filter(!is.na(Longitude)) %>%
                dplyr::distinct(hhid, .keep_all = TRUE) %>%
                dplyr::select(lng = Longitude,
                              lat = Latitude,
                              hhid) %>%
                dplyr::mutate(x = lng,
                              y = lat)) %>%
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
  clusters_projected_buffered <- rgeos::gBuffer(spgeom = clusters_projected, byid = TRUE, width = 20)
  o <- sp::over(households_sp_projected, polygons(clusters_projected_buffered))
  households_sp_projected@data$not_in_cluster <- is.na(o)
  households <- households %>%
    left_join(households_sp_projected@data %>% dplyr::select(not_in_cluster, hhid)) %>%
    filter(!not_in_cluster) %>%
    dplyr::select(-not_in_cluster)
}


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
      left_join(v0demography %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'v0demography'),
    safety_repeat_individual %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>%
      left_join(safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'safetynew'),
    efficacy %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'efficacy')
  ) %>%
  arrange(desc(todays_date))
right <- starter %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  filter(!is.na(extid), !is.na(safety_status)) %>%
  dplyr::select(extid, starting_safety_status = safety_status) 
# Any individual who is not in the above gets an "out" assignation
individuals <- left_join(individuals, right) %>%
  mutate(starting_safety_status = ifelse(is.na(starting_safety_status), 'out', starting_safety_status))
# Double check: any individual who was ever eos is always eos
ever_eos <- starter %>% 
  filter(!is.na(safety_status)) %>%
  filter(safety_status == 'eos') %>% 
  filter(!is.na(extid)) %>%
  dplyr::distinct(extid) %>%
  pull(extid)
individuals$starting_safety_status[individuals$extid %in% ever_eos] <- 'eos'
# Double check: any individual who was ever pregnant is always safety eos
# THIS IS MISSING SOMETHING: PREGNANCY FROM SAFETYNEW
ever_pregnant <- 
  bind_rows(
    safety_repeat_individual %>% filter(!is.na(pregnancy_status)) %>%
      filter(pregnancy_status == 'in') %>%
      dplyr::select(extid),
    pfu %>% 
      dplyr::select(extid)) %>%
  pull(extid)
individuals$starting_safety_status[individuals$extid %in% ever_pregnant] <- 'eos'

# Get starting weight
right <- 
  bind_rows(
    safety_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'safetynew'),
    efficacy %>% filter(!is.na(current_weight)) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'efficacy'),
    pfu %>% filter(!is.na(weight)) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), weight = as.character(weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = weight) %>% mutate(form = 'pfu')
  ) %>%
  filter(!is.na(extid), !is.na(starting_weight)) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_weight)
individuals <- left_join(individuals, right)

# Get starting height
right <- 
  bind_rows(
    safety_repeat_individual %>% 
      mutate(height = ifelse(is.na(height), height_short, height)) %>%
      filter(!is.na(height)) %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'safety') %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), starting_height = as.character(starting_height)),
    safetynew_repeat_individual %>% filter(!is.na(height)) %>%
      left_join(safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'safetynew') %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), starting_height = as.character(starting_height)),
    efficacy %>% filter(!is.na(height)) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'efficacy') %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), starting_height = as.character(starting_height))
  ) %>%
  filter(!is.na(extid), !is.na(starting_height)) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_height)
individuals <- left_join(individuals, right)

# Get pk_status
# Get pk status ################################################# (placeholder)
individuals$pk_preselected <- sample(c(NA, 0, 1), nrow(individuals), replace = TRUE) ############# placeholder
pk_ids <- sort(unique(individuals$extid[individuals$pk_preselected == 1]))
right <- bind_rows(
  safety_repeat_individual %>% 
    filter(!is.na(pk_status)) %>%
    left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'safety'),
  pk_day0 %>%
    filter(!is.na(pk_status)) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pk_day0'),
  pkdays123 %>%
    filter(!is.na(pk_status)) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pkdays123'),
  pkfollowup %>%
    filter(!is.na(pk_status)) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pkfollowup'),
) %>%
  filter(!is.na(pk_status), !is.na(extid)) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_pk_status = pk_status)
individuals <- left_join(individuals, right) %>%
  # if one is pk pre-selected by not in any of the "right" data, she is "out"; otherwise NA
  mutate(starting_pk_status = ifelse(is.na(starting_pk_status) & extid %in% pk_ids, 
                            'out',
                            starting_pk_status))

# Get efficacy status (placeholder) ################################################
individuals$efficacy_preselected <- sample(c(NA, 0, 1), nrow(individuals), replace = TRUE) ############### placeholder
efficacy_ids <- sort(unique(individuals$extid[individuals$efficacy_preselected == 1]))

# Get some further efficacy status variables
# starting_efficacy_status
right <- 
  efficacy %>% arrange(desc(todays_date)) %>%
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
  efficacy %>% arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  filter(!is.na(person_absent), !is.na(extid)) %>%
  dplyr::mutate(efficacy_absent_most_recent_visit = person_absent) %>%
  dplyr::select(extid, starting_efficacy_status)
if(nrow(right) > 0){
  individuals <- left_join(individuals, right) %>%
    mutate(efficacy_absent_most_recent_visit = ifelse(is.na(efficacy_absent_most_recent_visit), 0, efficacy_absent_most_recent_visit))
} else {
  individuals$efficacy_absent_most_recent_visit <- 0
}


# efficacy_most_recent_visit_present
right <- 
  efficacy %>% arrange(desc(todays_date)) %>%
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
  individuals$efficacy_visits_done <- 0
}

# starting_pregnancy_status
# THIS IS MISSING SOMETHING, PREGNANCY STATUS FROM SAFETYNEW
right <-   
  bind_rows(
    safety_repeat_individual %>% filter(!is.na(pregnancy_status)) %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, pregnancy_status) %>% mutate(form = 'safety'),
    # safetynew_repeat_individual %>% filter(!is.na(pregnant_yn)) %>%
    #   left_join(safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
    #   dplyr::select(todays_date, extid, pregnancy_status) %>% mutate(form = 'safetynew'), #???
    
    pfu %>% filter(!is.na(pregnancy_status)) %>%
      dplyr::select(todays_date, extid, pregnancy_status) %>% mutate(form = 'pfu')
  ) %>%
  arrange(desc(todays_date)) %>%
  filter(!is.na(extid), !is.na(pregnancy_status)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, pregnancy_status)
individuals <- left_join(individuals, right)

# pregnancy_consecutive_absences	 ########################## 
individuals$pregnancy_consecutive_absences <- NA #sample(c(NA, 0:7), nrow(individuals), replace = TRUE)
# See documentation at https://docs.google.com/document/d/1BVMsJE1KX0gG5Blu21HrGbuZ15x93cdRYiThyNp6jDQ/edit
# July 31 2023: agreed with Xing to deprecate this in favor of "date of last non-absent pregnancy visit"



#pregnancy_most_recent_visit_present	
right <- 
  pfu %>% arrange(desc(todays_date)) %>%
  filter(!is.na(person_present_continue)) %>%
  filter(person_present_continue == 1) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(pregnancy_most_recent_visit_present = as.numeric(gsub('V', '', visit))) %>%
  dplyr::select(extid, pregnancy_most_recent_visit_present,
                pregnancy_most_recent_present_date = todays_date) %>%
  mutate(pregnancy_most_recent_present_date = paste0('.', as.character(pregnancy_most_recent_present_date)))
individuals <- left_join(individuals, right) 

# pregnancy_visits_done
right <-  pfu %>%
  filter(!is.na(extid)) %>%
  group_by(extid) %>%
  summarise(pregnancy_visits_done = paste0(sort(unique(visit)), collapse = ', '))
individuals <- left_join(individuals, right) 

if(!dir.exists('metadata')){
  dir.create('metadata')
}

write_csv(households, 'metadata/households.csv')
write_csv(individuals, 'metadata/individuals.csv')

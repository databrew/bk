library(readr)
library(dplyr)
library(babynames)

# Read in intervention assignments
intervention_assignment <- read_csv('../../analyses/randomization/outputs/intervention_assignment.csv')
assignments <- read_csv('../../analyses/randomization/outputs/assignments.csv')

# Add cluster / ward / village
# Use recon data
if('recon_hierarchy.RData' %in% dir()){
  load('recon_hierarchy.RData')
} else {
  load('../se_vcs/recon.RData')
  households <- rgdal::readOGR('../../data_public/spatial/households/', 'households')
  households@data <- left_join(households@data %>% dplyr::select(-village,
                                                                 -ward,
                                                                 -community0),
                               recon %>% dplyr::select(
                                 hh_id = hh_id_clean,
                                 hh_id_raw,
                                 village,
                                 ward,
                                 community_unit = community_health_unit
                               )) %>%
    mutate(cluster = cluster_n0)
  recon_hierarchy <- households@data %>% dplyr::distinct(
    cluster,
    hh_id,
    ward, 
    village)
  save(recon_hierarchy, file = 'recon_hierarchy.RData')
}


# Define function for adding zero to numbers
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

# # Get a list of possible household IDs
# right_side <- 1:999
# left_side <- 1:96
# right_side <- add_zero(right_side, 3)
# left_side <- add_zero(left_side, 2)
# possible_household_ids <- expand.grid(
#   l = left_side,
#   r = right_side
# ) %>%
#   mutate(hhid = paste0(l, r)) %>%
#   pull(hhid)
possible_household_ids <- recon_hierarchy$hh_id

# Generate households
n_households <- 200
indices <- sample(1:nrow(recon_hierarchy), n_households)
households <- 
  tibble(
    hhid = possible_household_ids[indices],
    num_members = sample(1:12, size = n_households, replace = TRUE),
    cluster = recon_hierarchy$cluster[indices],
    healthecon_preselected = sample(0:1, size = n_households, replace = TRUE),
    ward = recon_hierarchy$ward[indices],
    village = recon_hierarchy$village[indices]
  )
households$visits_done <- NA
for(i in 1:nrow(households)){
  vd1 <- sample(c('V1', ''), 1)
  vd2 <- sample(c('V2', ''), 1)
  vd3 <- sample(c('V3', ''), 1)
  vds <- c(vd1, vd2, vd3)
  vds <- vds[vds != '']
  vd <- paste0(vds, collapse = ',')
  households$visits_done[i] <- vd
}
# households$intervention <- (households$cluster %% 2) + 1
# Get intervention and assignment
households <- households %>%
  left_join(assignments %>%
              dplyr::select(cluster = cluster_number,
                            assignment)) %>%
  left_join(intervention_assignment %>%
              dplyr::rename(assignment = arm)) %>%
  dplyr::select(-assignment)



# Generate people data
counter <- 0
people_list <- list()
for(i in 1:nrow(households)){
  message('Household ', i, ' of ', nrow(households))
  this_household <- households[i,]
  hhid <- this_household$hhid
  num_members <- this_household$num_members
  lastname <- Hmisc::capitalize(paste0(sample(letters, 10), collapse = ''))
  intervention <- this_household$intervention
  intervention <- ifelse(intervention == 1, 'Treatment', 'Control')
  for(j in 1:num_members){
    extid <- paste0(hhid, '-', add_zero(j, 2))
    sex <- sample(c('Male', 'Female'), 1)
    sex_letter <- substr(sex, 1, 1)
    firstname <- sample(babynames$name[babynames$sex == sex_letter], 1)
    dob <- sample(seq(as.Date('1900-01-01'), Sys.Date(), by = 'day'), 1)
    age <- as.numeric(Sys.Date() - dob) / 365.25
    fullname <- paste0(firstname, ' ', lastname)
    fullname_dob <- paste0(fullname, ' | ', dob)
    fullname_id <- paste0(fullname, ' (', extid, ')')
    starting_safety_status <- sample(c('icf', 'in', 'out', 'refusal', 'eos', 'completion'), 1)
    if(starting_safety_status == 'in'){
      starting_weight <- sample(15:100, 1)
      starting_height <- sample(20:200, 1)
    } else {
      starting_weight <- sample(3:100, 1)
      starting_height <- sample(50:200, 1)
      
    }
    pk_preselected <- sample(0:1, 1)
    efficacy_preselected <- sample(0:1, 1)
    migrated<- sample(0:1, 1)
    pregnancy_absences <- sample(0:5, 1, prob = 6:1)
    efficacy_absences <- sample(0:5, 1, prob = 6:1)
    efficacy_absent_most_recent_visit <- sample(0:1, 1, prob = c(6,1))
    efficacy_most_recent_visit_present <- sample(1:7, 1, prob = 7:1)
    starting_efficacy_status <- sample(c('in', 'out', 'eos'), 1)
    efficacy_active <- ifelse(starting_efficacy_status %in% c('in', 'out'), 1, 0) # this is much more complicated https://trello.com/c/SoWdHH4Q/1855-changes-additions-to-metadata-files
    if(sex == 'Male' | age < 13 | age > 49){
      starting_pregnancy_status <- NA
      pregnancy_absent_most_recent_visit <- NA
      pregnancy_most_recent_visit_present <- NA
    } else {
      starting_pregnancy_status <- sample(c('in', 'out', 'eos'), 1)
      pregnancy_absent_most_recent_visit <- sample(0:1, 1, prob = c(5,1))
      pregnancy_most_recent_visit_present <- sample(2:13, 1, prob = 13:2)
    }
    if(starting_safety_status == 'in' & sex == 'Female'){
      starting_pregnancy_status <- 'out'
    }
    if(!is.na(starting_pregnancy_status)){
      if((starting_pregnancy_status == 'in' | starting_pregnancy_status == 'eos')){
        starting_safety_status <- 'eos'
      }
    }
    if(starting_safety_status %in% c('icf', 'out')){
      starting_weight <- starting_height <- NA
    }
    if(starting_safety_status %in% c('icf', 'out')){
      migrated <- 0
    }
    if(!is.na(starting_pregnancy_status)){
      if(starting_pregnancy_status == 'out'){
        pregnancy_absences <- 0
      }
    }
    if(is.na(starting_pregnancy_status)){
      pregnancy_absences <- NA
    }
    if(starting_efficacy_status == 'out'){
      efficacy_absences <- 0
    }
    if(is.na(starting_efficacy_status)){
      efficacy_absences <- NA
      efficacy_absent_most_recent_visit <-NA
      efficacy_most_recent_visit_present <- NA
    }
    out <- tibble(firstname,
                  lastname,
                  fullname,
                  fullname_dob,
                  fullname_id,
                  dob,
                  sex,
                  hhid,
                  extid,
                  intervention,
                  starting_safety_status,
                  starting_pregnancy_status,
                  starting_weight,
                  starting_height,
                  pk_preselected,
                  efficacy_preselected,
                  migrated,
                  pregnancy_absences,
                  efficacy_absences,
                  starting_efficacy_status,
                  efficacy_active,
                  efficacy_absent_most_recent_visit,
                  pregnancy_absent_most_recent_visit,
                  pregnancy_most_recent_visit_present
                  )
    counter <- counter + 1
    people_list[[counter]] <- out
  }
}
individuals <- bind_rows(people_list)

# Add efficacy visits done
individuals$efficacy_visits_done <- NA
for(i in 1:nrow(individuals)){
  vd1 <- sample(c('V1', ''), 1)
  vd2 <- sample(c('V2', ''), 1)
  vd3 <- sample(c('V3', ''), 1)
  vds <- c(vd1, vd2, vd3)
  vds <- vds[vds != '']
  vd <- paste0(vds, collapse = ',')
  individuals$efficacy_visits_done[i] <- vd
}
individuals$efficacy_visits_done[individuals$efficacy_preselected == 0] <- ""

# Add pregnancy visits done
individuals$pregnancy_visits_done <- NA
for(i in 1:nrow(individuals)){
  vd1 <- sample(c('V1', ''), 1)
  vd2 <- sample(c('V2', ''), 1)
  vd3 <- sample(c('V3', ''), 1)
  vds <- c(vd1, vd2, vd3)
  vds <- vds[vds != '']
  vd <- paste0(vds, collapse = ',')
  individuals$pregnancy_visits_done[i] <- vd
}
individuals$pregnancy_visits_done[individuals$sex == 'Male'] <- ""


# add missing variables to households
# roster, arm household_head
right <- individuals %>%
  group_by(hhid) %>%
  summarise(household_head = dplyr::first(fullname),
            roster = paste0(sort(unique(fullname_id)), collapse = ', '))
households <- left_join(households, right)

# Add missing variables (June 6 2023)
individuals <- left_join(
  individuals,
  households %>%
    dplyr::select(hhid,
                  village, 
                  ward,
                  cluster)
)
individuals <- individuals %>%
  mutate(efficacy_most_recent_visit_present = ifelse(efficacy_preselected == 0, NA, sample(1:7, nrow(individuals), replace = T)))

individuals <- individuals %>%
  mutate(pregnancy_consecutive_absences = 
           ifelse(starting_pregnancy_status == 'out', 0, sample(c(NA, 0, 1, 2, 3, 4), nrow(.), replace = T)))

# Get pfu_members: a comma-separated list of all members of the household whose `starting_pregnancy_status` contains the value "in"
pfu <- individuals %>%
  ungroup %>%
  group_by(hhid) %>%
  summarise(pfu_members = paste0(sort(unique(fullname_id[starting_pregnancy_status == 'in'])), collapse = ','))
households <- households %>%
  left_join(pfu)

# Remove "healthecon_preselected"
households$healthecon_preselected <- NULL

# # Inject NAs
# households$household_head[sample(1:nrow(households), (round(0.2 * nrow(households))))] <- NA
# na_columns <- c('starting_safety_status', 'starting_pregnancy_status', 'starting_weight', 'pk_preselected', 'efficacy_preselected', 'migrated', 'pregnancy_absences', 'efficacy_absences')
# for(j in 1:length(na_columns)){
#   this_column <- na_columns[j]
#   individuals[sample(1:nrow(individuals), (round(0.2 * nrow(households)))),this_column] <- NA
# }



if(!dir.exists('dummy_metadata')){
  dir.create('dummy_metadata')
}
write_csv(households, 'dummy_metadata/households.csv')
write_csv(individuals, 'dummy_metadata/individuals.csv')


all_names <- c(names(households), names(individuals))
for(i in 1:length(x)){
  print(i)
  z <- x[i] %in% all_names
  message(i, '. ', z)
}

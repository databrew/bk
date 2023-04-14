library(readr)
library(dplyr)
library(babynames)


# Generate households
n_households <- 500
households <- 
  tibble(
    hhid = sample(1:1000000, n_households),
    num_members = sample(1:12, size = n_households, replace = TRUE),
    cluster = sample(1:98, size = n_households, replace = TRUE),
    healthecon_preselected = sample(0:1, size = n_households, replace = TRUE)
  )

households$arm <- (households$cluster %% 2) + 1

# Generate people data
counter <- 0
people_list <- list()
for(i in 1:nrow(households)){
  message('Household ', i, ' of ', nrow(households))
  this_household <- households[i,]
  hhid <- this_household$hhid
  num_members <- this_household$num_members
  lastname <- Hmisc::capitalize(paste0(sample(letters, 10), collapse = ''))
  arm <- this_household$arm
  intervention <- ifelse(arm == 1, 'Treatment', 'Control')
  for(j in 1:num_members){
    extid <- paste0(hhid, '-', j)
    sex <- sample(c('Male', 'Female'), 1)
    sex_letter <- substr(sex, 1, 1)
    firstname <- sample(babynames$name[babynames$sex == sex_letter], 1)
    dob <- sample(seq(as.Date('1900-01-01'), Sys.Date(), by = 'day'), 1)
    fullname <- paste0(firstname, ' ', lastname)
    fullname_dob <- paste0(fullname, ' | ', dob)
    fullname_id <- paste0(fullname, ' (', extid, ')')
    starting_safety_status <- sample(c('icf', 'in', 'out', 'refusal', 'eos', 'completion'), 1)
    starting_pregnancy_status <- sample(c('in', 'out', 'eos'), 1)
    starting_weight <- sample(3: 100, 1)
    pk_preselected <- sample(0:1, 1)
    efficacy_preselected <- sample(0:1, 1)
    migrated<- sample(0:1, 1)
    pfu_absences <- sample(0:5, 1, prob = 6:1)
    efficacy_absences <- sample(0:5, 1, prob = 6:1)
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
                  pk_preselected,
                  efficacy_preselected,
                  migrated,
                  pfu_absences,
                  efficacy_absences)
    counter <- counter + 1
    people_list[[counter]] <- out
  }
}
individuals <- bind_rows(people_list)

# add missing variables to households
# roster, arm household_head
right <- individuals %>%
  group_by(hhid) %>%
  summarise(household_head = dplyr::first(fullname),
            roster = paste0(sort(unique(fullname_id)), collapse = ', '))
households <- left_join(households, right)

# Inject NAs
households$household_head[sample(1:nrow(households), (round(0.2 * nrow(households))))] <- NA
na_columns <- c('starting_safety_status', 'starting_pregnancy_status', 'starting_weight', 'pk_preselected', 'efficacy_preselected', 'migrated', 'pfu_absences', 'efficacy_absences')
for(j in 1:length(na_columns)){
  this_column <- na_columns[j]
  individuals[sample(1:nrow(individuals), (round(0.2 * nrow(households)))),this_column] <- NA
}

if(!dir.exists('dummy_metadata')){
  dir.create('dummy_metadata')
}
write_csv(households, 'dummy_metadata/households.csv')
write_csv(individuals, 'dummy_metadata/individuals.csv')



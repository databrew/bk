# https://docs.google.com/spreadsheets/d/1O5eq21j04HaTspSrMUIVmVn7niu9L5nvsIQ9fGFII4w/edit#gid=227626065
library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(lubridate)
library(readr)

# Define production
folder <- 'kwale'
# folder <- 'test_of_test'

# Nuke the folder prior to data retrieval
unlink(folder, recursive = TRUE)

raw_or_clean <- 'clean'
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE") # set using .Renviron file
start_fresh <- TRUE

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
  datasets <- c('safetynew', 'safety')
  datasets_names <- datasets

  # Loop through each dataset and retrieve
  # bucket <- 'databrew.org'
  # folder <- 'kwale'
  bucket <- 'databrew.org'
  if(!dir.exists(folder)){
    dir.create(folder)
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
  safety <- read_csv(paste0(middle_path, 'safety/safety.csv'))
  safety_repeat_drug <- read_csv(paste0(middle_path, 'safety/safety-repeat_drug.csv'))
  safety_repeat_individual <- read_csv(paste0(middle_path, 'safety/safety-repeat_individual.csv'))
  safety_repeat_ae_symptom <- read_csv(paste0(middle_path, 'safety/safety-repeat_ae_symptom.csv'))
  
  # Safety new
  safetynew <- read_csv(paste0(middle_path, 'safetynew/safetynew.csv'))
  safetynew_repeat_individual <- read_csv(paste0(middle_path, 'safetynew/safetynew-repeat_individual.csv'))
  
  save(safety, 
       safety_repeat_drug,
       safety_repeat_individual,
       safety_repeat_ae_symptom,
       safetynew,
       safetynew_repeat_individual,
       file = 'data.RData')
  
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
safety <- safety %>% mutate(hhid = add_zero(hhid, n = 5))
safetynew <- safetynew %>% mutate(hhid = add_zero(hhid, n = 5))

# Capitalize all names
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

# Get the adverse events
ae <- safety_repeat_ae_symptom %>%
  left_join(safety_repeat_individual %>% dplyr::select(KEY, extid, safety_key = PARENT_KEY),
            by = c('PARENT_KEY' = 'KEY')) %>%
  left_join(safety %>% dplyr::select(KEY, hhid, todays_date, start_time),
            by = c('safety_key' = 'KEY')) %>%
  arrange(extid, start_time)

# Refer to the spreadsheet that categorizes adverse events
# https://bohemiakenya.slack.com/archives/C064F6TR1H6/p1698839198132699
ae <- ae %>% mutate(symptom = toupper(symptom_name))
symptom_names <- sort(unique(ae$symptom))
ae <- ae %>%
  mutate(symptom_category = case_when(
    symptom == 'ABDOMINAL PAIN' ~ 'Gastrointestinal',
    symptom == 'ASMA' ~ 'Respiratory',
    symptom == 'BLEEDING' ~ 'Circulatory',
    symptom == 'BREAST PAINS' ~ 'Musculoskeletal',
    symptom == 'BROKEN BONE/OTHER INJURY' ~ 'Musculoskeletal',
    symptom == 'CONSTIPATION' ~ 'Gastrointestinal',
    symptom == 'COUGH' ~ 'Respiratory',
    symptom == 'DIARRHEA' ~ 'Gastrointestinal',
    symptom == 'DIZZINESS' ~ 'Sensory',
    symptom == 'FATIGUE' ~ 'Systemic',
    symptom == 'FELT TIRED AND SLEEPY' ~ 'Systemic',
    symptom == 'FEVER' ~ 'Systemic',
    symptom == 'FLUE' ~ 'Systemic',
    symptom == 'GENERAL MALAISE' ~ 'Systemic',
    symptom == 'HEADACHE' ~ 'Neurologic',
    symptom == 'HUNGRY' ~ 'Gastrointestinal',
    symptom == 'ITCHING' ~ 'Dermatologic',
    symptom == 'JOINT PAIN' ~ 'Musculoskeletal',
    symptom == 'LACK OF APPETITE' ~ 'Gastrointestinal',
    symptom == 'MALARIA' ~ 'Systemic',
    symptom == 'MUSCLE PAIN' ~ 'Musculoskeletal',
    symptom == 'RASH' ~ 'Dermatologic',
    symptom == 'RESPIRATORY DISEASE/LUNG CONDITION' ~ 'Respiratory',
    symptom == 'SOMNOLENCE' ~ 'Neurologic',
    symptom == 'STOMACH' ~ 'Gastrointestinal',
    symptom == 'SWEATING' ~ 'Systemic',
    symptom == 'TREMOR' ~ 'Neurologic',
    symptom == 'URINE COLOUR RED' ~ 'Other',
    symptom == 'VERTIGO' ~ 'Sensory',
    symptom == 'VERY OKAY' ~ 'Other',
    symptom == 'VISUAL ALTERATIONS' ~ 'Sensory',
    symptom == 'VOMITING OR NAUSEA' ~ 'Gastrointestinal',
    symptom == 'WOUNDS IN THE MOUTH' ~ 'Gastrointestinal',
    .default = 'Other'
  ))

# Get drugs
drugs <- 
  safety_repeat_drug %>%
  dplyr::select(PARENT_KEY,
                drug, drug_other_specify,
                num_days_after_visit_drug_start,
                still_taking_drug_yn,
                num_days_drug) %>%
  left_join(safety_repeat_individual %>%
              dplyr::select(KEY, extid, safety_key = PARENT_KEY),
            by = c('PARENT_KEY' = 'KEY'))




# Conform the data to these specifications: https://docs.google.com/spreadsheets/d/1O5eq21j04HaTspSrMUIVmVn7niu9L5nvsIQ9fGFII4w/edit#gid=227626065

# Start by combining the individuals from the two safety forms
valid_names <- unique(c('ae_symptoms', 'short_safety_inclusion_pass', 'pregnancy_status',
                        'symptom_other_specify', 'symptom_accident_specify',
                        'symptom_start',
                        'symptom_end',
                        'symptom_still_have',
                        'symptom_interfere_activities',
                        'symptom_seek_care',
                        'symptom_seek_care_where', 'symptom_seek_care_other_specify',
                        'seek_care_hospital_night',
                        'num_nights_hospital',
                        'treatment_for_malaria',
                        'treatment_for_malaria_specify',
                        'treatment_after_study_drug',
                        'prescription_available',
                        'num_drugs',
                        'drug', 'drug_other_specify',
                        'num_days_after_visit_drug_start',
                        'still_taking_drug_yn',
                        'num_days_drug',
                        'PARENT_KEY', 'KEY', 'start_time', 'cluster',
                        'extid', 'arm', 'dob', 'age', 'gender', 'first_visit_date',
                        'last_visit_number', 'last_visit_date', 'icf_completed',
                        'passed_safety_inclusion_criteria', 'safety_status', 'icf_completed',
                        'todays_date', 'visit',
                        'sex', 'ind_icf_completed', 'safety_inclusion_pass', 'participant_take_drug',
                        'participant_take_drug_2',
                        'not_take_drug_reason', 'not_take_drug_reason_2',
                        # IP_start date
                        'num_albendzole_pills', 'num_extra_albendazole', 'num_albendzole_pills_2', 'num_extra_albendazole_2',
                        'num_ivermectin_tablets_taken', 'num_extra_ivermectin', 'num_ivermectin_tablets_taken_2', 'num_extra_ivermectin_2',
                        # IP_end_date
                        'num_albendzole_pills', 'num_extra_albendazole', 'num_albendzole_pills_2', 'num_extra_albendazole_2',
                        'num_ivermectin_tablets_taken', 'num_extra_ivermectin', 'num_ivermectin_tablets_taken_2', 'num_extra_ivermectin_2',
                        'pregnancy_status',
                        'ae_symptoms', 'symptom_other_specify', 'symptom_accident_specify',
                        'symptom_start',
                        'symptom_end',
                        'symptom_still_have',
                        'symptom_interfere_activities',
                        'symptom_seek_care',
                        'symptom_seek_care_where', 'symptom_seek_care_other_specify',
                        'seek_care_hospital_night',
                        'num_nights_hospital',
                        'treatment_for_malaria',
                        'treatment_for_malaria_specify',
                        'treatment_after_study_drug',
                        'prescription_available',
                        'num_drugs',
                        'drug', 'drug_other_specify',
                        'num_days_after_visit_drug_start',
                        'still_taking_drug_yn',
                        'num_days_drug', 'symptom_start'
                        # names(safetynew_repeat_individual), names(safetynew)
                        ))
individuals_safety <- safety_repeat_individual %>% filter(person_present_continue == 1)
individuals_safety <- individuals_safety[,names(individuals_safety) %in% valid_names]
individuals_safetynew <- safetynew_repeat_individual[,names(safetynew_repeat_individual) %in% valid_names]
individuals_safety <- 
  left_join(individuals_safety, 
            safety %>%
              dplyr::select(hhid, cluster, start_time, todays_date, KEY, visit), 
            by = c('PARENT_KEY' = 'KEY')) 
individuals_safetynew <- 
  left_join(individuals_safetynew, 
            safetynew %>%
              dplyr::select(hhid, cluster, start_time, todays_date, KEY, visit), 
            by = c('PARENT_KEY' = 'KEY')) 
individuals_safety <- individuals_safety[,names(individuals_safety) %in% valid_names]
individuals_safetynew <- individuals_safetynew[,names(individuals_safetynew) %in% valid_names]
individual_visits <- bind_rows(
  individuals_safety %>% mutate(form = 'safety'),
  individuals_safetynew %>% mutate(form = 'safetynew')
) %>%
  # order from latest to earliest
  arrange(desc(start_time))

# Get intervention status based on cluster
assignments <- read_csv('../../analyses/randomization/outputs/assignments.csv') %>%
  mutate(cluster_number = add_zero(cluster_number, 2))
individual_visits <- 
  left_join(individual_visits %>%
              mutate(cluster = add_zero(cluster, 2)),
            assignments %>% dplyr::select(cluster = cluster_number, arm = assignment))


# Get some individual-level (not visit level) information
individuals <- individual_visits %>%
  group_by(extid) %>%
  summarise(cluster = dplyr::first(cluster),
            arm = dplyr::first(arm),
            dob = dplyr::first(dob),
            age = dplyr::first(age),
            gender = dplyr::first(sex),
            first_visit_date = dplyr::last(todays_date),
            last_visit_number = dplyr::first(visit),
            last_visit_date = dplyr::first(todays_date),
            icf_completed = dplyr::first(ind_icf_completed[!is.na(ind_icf_completed)]),
            passed_safety_inclusion_criteria = any(safety_inclusion_pass == 1 | short_safety_inclusion_pass),
            safety_status = dplyr::first(safety_status)) %>%
  mutate(icf_completed = ifelse(is.na(icf_completed), 'No',
                                ifelse(icf_completed == 1, 'Yes',
                                       ifelse(icf_completed == 0, 'No', NA)))) %>%
  ungroup() 



# Now get some visit level info
visit_level <- 
  individual_visits %>%
  dplyr::select(visit, extid, todays_date,
                participant_take_drug, participant_take_drug_2,
                not_take_drug_reason, not_take_drug_reason_2,
                # IP_start date
                num_albendzole_pills, num_extra_albendazole, num_albendzole_pills_2, num_extra_albendazole_2,
                num_ivermectin_tablets_taken, num_extra_ivermectin, num_ivermectin_tablets_taken_2, num_extra_ivermectin_2,
                # IP_end_date
                num_albendzole_pills, num_extra_albendazole, num_albendzole_pills_2, num_extra_albendazole_2,
                num_ivermectin_tablets_taken, num_extra_ivermectin, num_ivermectin_tablets_taken_2, num_extra_ivermectin_2,
                pregnancy_status,
                ae_symptoms, symptom_other_specify, symptom_accident_specify,
                # symptom_start,
                # symptom_end,
                # symptom_still_have,
                # symptom_interfere_activities,
                # symptom_seek_care,
                # symptom_seek_care_where, symptom_seek_care_other_specify,
                # seek_care_hospital_night,
                # num_nights_hospital,
                # treatment_for_malaria,
                # treatment_for_malaria_specify,
                treatment_after_study_drug,
                prescription_available,
                num_drugs)

save(visit_level, safety,
     safety_repeat_individual,
     individuals, drugs, ae,
     safetynew,
     safetynew_repeat_individual,
     file = 'rmd_data.RData')

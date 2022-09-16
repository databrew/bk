library(googledrive)
library(gsheet)
library(dplyr)
library(ruODK)
library(yaml)
library(readr)
# Useful docs on API: https://odkcentral.docs.apiary.io/# 

# Create data or use the already submitted data?
first_time <- FALSE

# Overwrite the form definition or just update the metadata
overwrite_form_definition <- FALSE

# Configure ruODK and file paths
credentials_file <- '../credentials/credentials.yaml'
creds <- yaml::yaml.load_file(credentials_file)
ruODK::ru_setup(
  svc = 'https://databrew.org/v1/projects/17/forms/testing.svc',
  un = creds$un,
  pw = creds$pw
)
fid <- ruODK::get_default_fid()
csv_fname <- '/tmp/people_data.csv'
csv_households_fname <- '/tmp/households_data.csv'

# Check to see if the form exists or not
# fl <- ruODK::form_list()
# If it doesn't, must be manually created the first time

# We need to create the initial roster database
if('starting_roster.RData' %in% dir()){
  load('starting_roster.RData')
} else {
  # first time
  # Write a csv of people
  n_people <- 100
  people <- tibble(id = 1:n_people,
                   household_id = sample(1:15, n_people, replace = TRUE),
                   first_name = sample(babynames::babynames$name, n_people),
                   last_name = sample(babynames::babynames$name, n_people)) %>%
    mutate(name = paste0(first_name, '.', last_name)) %>%
    arrange(household_id,
            id)
  # Write a csv of households
  households <- people %>%
    group_by(household_id) %>%
    summarise(n_members = n())
  save(people, households, file = 'starting_roster.RData')
}

# Read in forms submitted to data
if(first_time){
  # just use the starting roster as the metadata
  write_csv(households, csv_households_fname)  
  write_csv(people, csv_fname)
} else {
  # Use starting roster plus previous submissions
  ruODK::submission_export(
    local_dir = td,
    overwrite = TRUE,
    media = FALSE,
    repeats = TRUE,
    fid = fid,
    verbose = TRUE
  )
  ed <- paste0(td, 'unzipped/')
  zip_path <- paste0(td, fid, '.zip')
  unzip(zipfile = zip_path, exdir = ed)
  
  # Read in the downloaded files
  file_names <- dir(ed)
  data_list <- list()
  fid_list <- c()
  for(f in 1:length(file_names)){
    this_file_name <- file_names[f]
    this_sub_form <- gsub('.csv', '', this_file_name)
    # See if this is the main submission form or not
    is_main <- !grepl('-', this_sub_form)
    this_sub_form <- unlist(lapply(strsplit(this_sub_form, split = '-'), function(x){x[length(x)]}))
    if(is_main){
      this_sub_form <- 'Submissions'
    }
    fid_list <- c(fid_list, this_sub_form)
    file_path <- paste0(ed, this_file_name)
    this_data <- readr::read_csv(file_path, guess_max = Inf)
    # Clean the column names
    clean_column_names <- TRUE
    if(clean_column_names){
      names(this_data) <- unlist(lapply(strsplit(names(this_data), '-'), function(a){a[length(a)]}))
    }
    this_data$id <- this_data$KEY
    this_data <- janitor::clean_names(this_data)
    this_data <- this_data[,!duplicated(names(this_data))]
    data_list[[f]] <- this_data
  }
  names(data_list) <- fid_list
  submissions <- data_list$Submissions
  repeat_members_gone <- data_list$repeat_members_gone
  repeat_new_people <- data_list$repeat_new_people
  # Get submission date-times
  repeat_members_gone <- repeat_members_gone %>%
    left_join(submissions %>% dplyr::select(key,
                                            household_id,
                                            submission_date),
              by = c('parent_key' = 'key'))
  repeat_new_people <- repeat_new_people %>%
    left_join(submissions %>% dplyr::select(key,
                                            household_id,
                                            submission_date),
              by = c('parent_key' = 'key'))
  # Get the outs
  outs <- 
    repeat_members_gone %>%
    dplyr::select(person = member_gone, submission_date, household_id) %>%
    mutate(remove = TRUE) %>%
    dplyr::distinct(household_id, person, .keep_all = TRUE)
  # Get the ins
  ins <- repeat_new_people %>%
    mutate(person = paste0(first_name, '.', last_name)) %>%
    dplyr::select(person, submission_date, household_id) %>%
    # keep most recent observation only
    arrange(desc(submission_date)) %>%
    dplyr::distinct(household_id, person, .keep_all = TRUE)
  # Combine the ins and the outs, keeping
  # only the most recent observation for each household_id, person
  io <- outs %>%
    mutate(type = 'out') %>%
    bind_rows(
      ins %>%
        mutate(type = 'in')
    ) %>%
    arrange(desc(submission_date)) %>%
    dplyr::distinct(household_id, person, .keep_all = TRUE)
  # Apply the outs first
  people <- people %>%
    left_join(outs) %>%
    mutate(remove = ifelse(is.na(remove), FALSE, remove)) %>%
    filter(!remove) %>% dplyr::select(-remove)
  # reformat ins to format of people and then apply
  ins <- ins %>%
    dplyr::mutate(id = sample(999:9999, nrow(ins))) %>%
    mutate(first_name = unlist(lapply(strsplit(person, split = '.', fixed = TRUE), function(x){x[1]})),
           last_name = unlist(lapply(strsplit(person, split = '.', fixed = TRUE), function(x){x[2]}))) %>%
    mutate(name = person) %>%
    dplyr::select(id, household_id,
                  first_name, last_name,
                  name)
  people <- people %>% 
    bind_rows(ins) %>%
    arrange(household_id, id)
  # Update the households file
  households <- people %>%
    group_by(household_id) %>%
    summarise(n_members = n())
  # Overwrite the metadata files
  write_csv(households, csv_households_fname)  
  write_csv(people, csv_fname)
}



if(overwrite_form_definition){
  # Define some parameters
  td <- '/tmp/'
  fid <- 'testing'
  sheet_url <- 'https://docs.google.com/spreadsheets/d/1bHZMWc3SgrdfTRPlY6oKR__f9ZWhbuhVP0hbzdnFgPw/edit#gid=0'
  
  # Retrieve the most recent form definition
  sheet_id <- as_id(sheet_url)
  xlsx_name <- paste0(fid, '.xlsx')
  xml_name <- paste0(fid, '.xml')
  temp_path <- file.path(td, xlsx_name)
  # Download
  drive_download(file = sheet_id, 
                 path = temp_path,
                 type = "xlsx", overwrite = TRUE)
  # Convert to xml
  owd <- getwd()
  setwd(td)
  system(paste0('xls2xform ', xlsx_name, ' ', xml_name))
  Sys.sleep(1)
  setwd(owd)
  the_body <- httr::upload_file(paste0(td, xml_name))
} else {
  the_body <- NULL
}

# Create a draft form
# (the below fails if not already created on the server)
res <- httr::RETRY("POST",
                   paste0(ruODK::get_default_url(),
                          "/v1/projects/",
                          ruODK::get_default_pid(),
                          "/forms/",
                          fid,
                          "/draft"),
                   body = the_body,
                   httr::authenticate(
                     ruODK::get_default_un(),
                     ruODK::get_default_pw())) %>%
  httr::content(.)

# If first time, give user permission

# Upload metadata
# people_data.csv
res <- httr::RETRY("POST",
                   paste0(ruODK::get_default_url(), 
                          "/v1/projects/", 
                          ruODK::get_default_pid(), "/forms/", 
                          fid, "/draft/attachments/people_data.csv"),
                   body = httr::upload_file(csv_fname),
                   httr::authenticate(
                     ruODK::get_default_un(), 
                     ruODK::get_default_pw()))

# households_data.csv
res <- httr::RETRY("POST",
                   paste0(ruODK::get_default_url(), 
                          "/v1/projects/", 
                          ruODK::get_default_pid(), "/forms/", 
                          fid, "/draft/attachments/households_data.csv"),
                   body = httr::upload_file(csv_households_fname),
                   httr::authenticate(
                     ruODK::get_default_un(), 
                     ruODK::get_default_pw()))

#Publish a draft form
# current_version <- 1
# new_version <- as.character(current_version + 0.000001)
new_version <- round(as.numeric(Sys.time()))
res <- httr::RETRY("POST",
                   paste0(ruODK::get_default_url(), "/v1/projects/", 
                          ruODK::get_default_pid(), 
                          "/forms/", 
                          fid, 
                          "/draft/publish?version=",new_version),
                   httr::authenticate(ruODK::get_default_un(), 
                                      ruODK::get_default_pw())) %>% 
  httr::content(.)

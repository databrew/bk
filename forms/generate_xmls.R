message('Starting process')
library(googledrive)
library(gsheet)
library(dplyr)
library(yaml)
library(readr)

# Read in the index of Bohemia Kenya forms
index <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/10fsAAnARFzLqn5OVHgVhxIfPkmplrgir7n7BOGLyhfI/edit#gid=456395473')

# Remove those which are deprecated
index <- index %>%
  filter(status != 'deprecated')

# For each sheet, donwload, convert to xml, and dump into the "forms" directory"
td <- paste0('/tmp/sheets')
if(dir.exists(td)){
  unlink(td, recursive = TRUE)
}
dir.create(td)
owd <- getwd()
out_list <- list()
# for(i in 1:nrow(index)){
for(i in 1:nrow(index)){
  this_row <- index[i,]
  message(i, ' of ', nrow(index), ': ', this_row$`form ID`)
  sheet_url <- this_row$google_url
  # Get form id
  fid <- this_row$`form ID`
  result <- 
    try({
      sheet_id <- as_id(sheet_url)
      xlsx_name <- paste0(fid, '.xlsx')
      xml_name <- paste0(fid, '.xml')
      temp_path <- file.path(td, xlsx_name)
      # Download
      drive_download(file = sheet_id, 
                     path = temp_path,
                     type = "xlsx", overwrite = TRUE)
      # Indicate where to deposit
      final_folder <- paste0('../forms/', fid, '/')
      if(!dir.exists(final_folder)){
        dir.create(final_folder)
      }
      final_location <- paste0(final_folder, xlsx_name)
      file.copy(from = temp_path,
                to = final_location,
                overwrite = TRUE)
      # Convert to xml
      setwd(final_folder)
      system(paste0('xls2xform ', xlsx_name, ' ', xml_name))
      Sys.sleep(1)
      setwd(owd)
    })
  if(class(result) == 'try-error'){
    success <- FALSE
  } else {
    success <- TRUE
  }
  out <- tibble(fid = fid,
                success = success)
  out_list[[i]] <- out
}
out <- bind_rows(out_list)
print(head(out, nrow(out)))


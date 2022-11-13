library(dplyr)
library(readr)

# Download reconaregistration, reconbhousehold from AWS
# https://s3.console.aws.amazon.com/s3/object/databrew-testing-databrew.org?region=us-east-1&prefix=kwale/raw-form/reconaregistration/reconaregistration.csv
recona <- read_csv('reconaregistration.csv')
reconb <- read_csv('reconbhousehold.csv')

# Later, the above needs to be coded in a way that it retrieves the data from the AWS bucket automatically
# not through point and click download

# Define function to identify anomalies
identify_anomalies <- function(
    recona_data = recona,
    reconb_data = reconb){
  
  # Placeholder list for combining anomalous instance
  out_list <- list()
  
  # List of recona anomalies defined here:
  # https://docs.google.com/spreadsheets/d/1kcTXqt2SREr9J4XQLFsLdXLfzcQPkQM8RHXI_-SUe_4/edit#gid=0
  
  # # Identical CHA ID submitted more than once
  # (not implementing, since expected)
  
  # Identical CHV ID submitted more than once
  x <- 
    recona %>% group_by(wid = `worker_details_2-wid`) %>%
    summarise(n = n(),
              instances = paste0(sort(unique(`meta-instanceID`)), collapse = ';')) %>%
    mutate(description = paste0('Worker ID ', wid, ' was used ', n, ' times in the Recon A / registration form, but worker IDs should be unique and therefore only entered once.')) %>%
    filter(n > 0) %>%
    mutate(type = 'recona_duplicate_id') %>%
    mutate(anomaly_id = paste0(type, '_', instances)) %>%
    dplyr::select(type, anomaly_id, description)
  out_list[[(length(out_list) + 1)]] <- x
  
  # Identical ID used by a CHA and CHV
  x <- 
    recona %>%
    dplyr::distinct(`worker_details_2-wid`, .keep_all = TRUE) %>%
    dplyr::select(`id` = `worker_details_2-wid`,
                  chv_instance = `meta-instanceID`) %>%
    mutate(chv = TRUE) %>%
    left_join(recona %>%
                mutate(wid_cha = ifelse(is.na(`worker_identification-cha_wid_qr`),
                                        `worker_identification-cha_wid_manual`,
                                        `worker_identification-cha_wid_qr`)) %>%
                dplyr::select(`id` = wid_cha,
                              cha_instance = `meta-instanceID`) %>%
                mutate(cha = TRUE)) %>%
      filter(!is.na(cha)) %>%
    mutate(description = paste0('ID: ', id, ' was used for both a CHV (instance = ',
                                chv_instance, ') and a CHA (instance = ',
                                cha_instance, ').')) %>%
    mutate(type = 'recona_id_for_both_cha_and_chv') %>%
    mutate(anomaly_id = paste0(type, '_', cha_instance, '_', chv_instance)) %>%
    dplyr::select(type, anomaly_id, description)
  out_list[[(length(out_list) + 1)]] <- x
  
  # Mismatch between number of CHVs that indicate a CHA as their supervisor and the number of reported CHVs supervised by a CHA
  
  # List of reconb anomalies defined here:
  # https://docs.google.com/spreadsheets/d/1kcTXqt2SREr9J4XQLFsLdXLfzcQPkQM8RHXI_-SUe_4/edit#gid=1385682357
  # Same Household ID used more than once
  
  
  return(anomalies)
}
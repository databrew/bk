# NOTE: Run the metadata file and get individuals metadata before running the QC

#' checks healthecon individual status diff based on visits
#' @return tibble of errors in status changes

check_status_diff <- function(data, col, key){
  ### Individual Checks
  qc_data <- data %>%
    group_by(!!sym(key)) %>%
    mutate(prev := lag(!!sym(col),
                       n=1,
                       order_by=SubmissionDate)) %>%
    dplyr::select(prev, any_of(col)) %>%
    dplyr::ungroup() %>%
    dplyr::filter((prev == 'in' & !!sym(col) == 'out'),
                  (prev == 'out' & !!sym(col) == 'eos'),
                  (prev == 'eos' & !!sym(col) == 'in'))

  return(qc_data)
}


# HealthEcon Individual Metadata Checks
t <- dplyr::bind_rows(
  healtheconnew %>%
    dplyr::inner_join(healtheconnew_repeat_individual, by = c('KEY'='PARENT_KEY')) %>%
    dplyr::mutate(visit = 'V1', hecon_individual_status = 'in') %>%
    dplyr::distinct(SubmissionDate, extid, hhid,visit, hecon_individual_status),
  healtheconbaseline %>%
    dplyr::inner_join(healtheconbaseline_repeat_individual, by = c('KEY'='PARENT_KEY')) %>%
    dplyr::mutate(visit = 'V1') %>%
    dplyr::distinct(SubmissionDate, extid, hhid, visit, hecon_household_status, hecon_individual_status),
  healtheconmonthly %>%
    dplyr::inner_join(healtheconmonthly_repeat_individual, by = c('KEY'='PARENT_KEY')) %>%
    dplyr::distinct(SubmissionDate, extid, hhid, visit, hecon_household_status, hecon_individual_status)
) %>%
  dplyr::inner_join(starting_roster %>% dplyr::select(extid), by = c('extid'))
check_status_diff(t, col = 'hhid', 'hecon_household_status')
check_status_diff(t, col = 'extid', 'hecon_individual_status')


# Efficacy Metadata Checker
t <- efficacy %>%
  dplyr::inner_join(individuals %>% dplyr::select(extid), by = c('extid'))
check_status_diff(t, col = 'extid', 'starting_efficacy_status')


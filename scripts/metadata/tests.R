# This script is used as a utility script to do spot checks for metadata in healthecon


#' checks healthecon individual status diff based on visits
#' @return tibble of errors in status changes
check_hecon_ind_status_diff <- function(individuals, status_col){
  ### Checks Variables
  possible_visits_list <- c('V1', 'V2', 'V3', 'V4', 'V5')
  unique_visit <- unique(individuals$visit)
  add_cols <- setdiff(possible_visits_list, unique_visit)

  ### Individual Checks
  qc_data <- individuals %>%
    dplyr::distinct(extid, visit, !!sym(status_col)) %>%
    tidyr::pivot_wider(names_from = visit, id_cols = c(extid), values_from = !!sym(status_col)) %>%
    bind_cols(setNames(rep(list(NA), length(add_cols)), add_cols))

  checks <- qc_data %>%
    dplyr::mutate(error_flag = case_when(
      (!V1 %in% c('in', 'out')) ~ 'V0 -> V1 hecon hh status error',
      (!is.na(V1) & !is.na(V2)) & (V1 == 'in'  &  V2 == 'out') ~ 'V1 -> V2 hecon ind status error in -> out is not possible',
      (!is.na(V1) & !is.na(V2)) & (V1 == 'out' &  (V2 %in% c('in', 'eos'))) ~ 'V1 -> V2 hecon ind status error out -> in or eos is not possible',
      (!is.na(V2) & !is.na(V3)) & (V2 == 'in'  &  V3 == 'out') ~ 'V2 -> V3 hecon ind status error in -> out is not possible',
      (!is.na(V2) & !is.na(V3)) & (V2 == 'out' &  (V3 %in% c('in', 'eos'))) ~ 'V2 -> V3 hecon ind status error out -> in or eos is not possible',
      (!is.na(V3) & !is.na(V4)) & (V3 == 'in'  & V4 == 'out') ~ 'V3 -> V4 hecon ind status error in -> out is not possible',
      (!is.na(V3) & !is.na(V4)) & (V3 == 'out' & (V4 %in% c('in', 'eos'))) ~ 'V3 -> V4 hecon ind status error out -> in or eos is not possible',
      TRUE ~ 'pass'
    )) %>%
    dplyr::filter(error_flag != 'pass')

  return(checks)

}

#' checks healthecon household status diff based on visits
#' @return tibble of errors in status changes
check_hecon_hh_status_diff <- function(households, status_col) {
  ### Checks Variables
  possible_visits_list <- c('V1', 'V2', 'V3', 'V4', 'V5')
  unique_visit <- unique(households$visit)
  add_cols <- setdiff(possible_visits_list, unique_visit)

  ### Individual Checks
  qc_data <- households %>%
    dplyr::distinct(hhid, visit, !!sym(status_col)) %>%
    tidyr::pivot_wider(names_from = visit, id_cols = c(hhid), values_from = !!sym(status_col)) %>%
    bind_cols(setNames(rep(list(NA), length(add_cols)), add_cols))

  checks <- qc_data %>%
    dplyr::mutate(error_flag = case_when(
      (!V1 %in% c('in', 'out')) ~ 'V0 -> V1 hecon hh status error',
      (!is.na(V1) & !is.na(V2)) & (V1 == 'in'  &  V2 == 'out') ~ 'V1 -> V2 hecon ind status error in -> out is not possible',
      (!is.na(V1) & !is.na(V2)) & (V1 == 'out' &  (V2 %in% c('in', 'eos'))) ~ 'V1 -> V2 hecon ind status error out -> in or eos is not possible',
      (!is.na(V2) & !is.na(V3)) & (V2 == 'in'  &  V3 == 'out') ~ 'V2 -> V3 hecon ind status error in -> out is not possible',
      (!is.na(V2) & !is.na(V3)) & (V2 == 'out' &  (V3 %in% c('in', 'eos'))) ~ 'V2 -> V3 hecon ind status error out -> in or eos is not possible',
      (!is.na(V3) & !is.na(V4)) & (V3 == 'in'  & V4 == 'out') ~ 'V3 -> V4 hecon ind status error in -> out is not possible',
      (!is.na(V3) & !is.na(V4)) & (V3 == 'out' & (V4 %in% c('in', 'eos'))) ~ 'V3 -> V4 hecon ind status error out -> in or eos is not possible',
      TRUE ~ 'pass'
    )) %>%
    dplyr::filter(error_flag != 'pass')

  return(checks)
}

library(readr)
library(dplyr)
library(ggplot2)
library(tidyr)
library(lubridate)
library(bohemiase) # github library
# Credentials
bohemiase::credentials_check('../../credentials/bohemia_credentials_cism.yaml')
creds <- yaml::yaml.load_file(Sys.getenv('bohemia_credentials'))

# Define a function for converting "time since invermectin" (in which the higher the value, the LESS protection)
# into a "protection" variable (ie, the higher the value, the greater the protection)
quantify_protection <- function(days_since_ivermectin){
  
  # linear decay rate
  right <- 
    tibble(days_since_ivermectin = 0:30,
           protection = rev((0:30) / 30))
  
  # # Update, 2022-08-18, from Carlos
  # g <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/1T-dqxXSYyf-4CNvttMv1R6MzWZM8k_Ct/edit?usp=sharing&ouid=117219419132871344734&rtpof=true&sd=true')
  right <- structure(list(days_since_ivermectin = c(0, 1, 2, 3, 4, 5, 6, 
                                                    7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 
                                                    23, 24, 25, 26, 27, 28, 29, 30, 31), protection = c(1, 0.96, 
                                                                                                        0.92, 0.88, 0.85, 0.81, 0.77, 0.73, 0.6, 0.47, 0.33, 0.32, 0.31, 
                                                                                                        0.3, 0.3, 0.28, 0.26, 0.24, 0.22, 0.2, 0.18, 0.15, 0.13, 0.11, 
                                                                                                        0.09, 0.07, 0.05, 0.03, 0.03, 0, 0, 0)), row.names = c(NA, -32L
                                                                                                        ), spec = structure(list(cols = list(days_since_ivermectin = structure(list(), class = c("collector_double", 
                                                                                                                                                                                                 "collector")), protection = structure(list(), class = c("collector_double", 
                                                                                                                                                                                                                                                         "collector"))), default = structure(list(), class = c("collector_guess", 
                                                                                                                                                                                                                                                                                                               "collector")), delim = ","), class = "col_spec"),class = c("spec_tbl_df", 
                                                                                                                                                                                                                                                                                                                                                                          "tbl_df", "tbl", "data.frame"))
  
  
  left <- tibble(days_since_ivermectin)
  joined <- left_join(left, right)
  joined$protection[is.na(joined$protection)] <- 0
  out <- joined$protection
  return(out)
}


# Helper functions
extract_id <- function(x){
  gsub('ID:', '', unlist(lapply(strsplit(x, split = '|', fixed = TRUE), function(a){a[2]})))
}
# Define function for calculating time since event
# (based on function in doBy package)
time_since_event <- function (yvar, tvar = seq_along(yvar)) {
  if (!(is.numeric(yvar) | is.logical(yvar))) {
    stop("yvar must be either numeric or logical")
  }
  yvar[is.na(yvar)] <- 0
  event.idx <- which(yvar == 1)
  if (length(event.idx) == 0) {
    return(NULL)
  }
  n.event <- length(event.idx)
  event.time <- tvar[event.idx]
  rrr <- do.call(rbind, lapply(event.idx, function(ii) tvar - 
                                 tvar[ii]))
  abs.tse <- apply(abs(rrr), 2, min)
  ewin <- rep.int(NA, length(yvar))
  if (n.event > 1) {
    ff <- event.time[1:(n.event - 1)] + diff(event.time)/2
    ewin[tvar <= ff[1]] <- 1
    for (ii in 2:(length(ff) - 0)) {
      ewin[tvar > ff[ii - 1] & tvar <= ff[ii]] <- ii
    }
    ewin[tvar > ff[length(ff)]] <- n.event
  }
  else {
    ewin[] <- n.event
  }
  ggg <- list()
  for (ii in 1:(length(event.idx))) {
    ggg[[ii]] <- rrr[ii, ewin == ii]
  }
  ggg <- unlist(ggg)
  sign.tse <- sign(ggg) * abs.tse
  run <- cumsum(yvar)
  un <- unique(run)
  tlist <- list()
  for (ii in 1:length(un)) {
    vv <- un[ii]
    yy <- yvar[run == vv]
    tt <- tvar[run == vv]
    tt <- tt - tt[1]
    tlist[[ii]] <- tt
  }
  tae <- unlist(tlist)
  tae[run == 0] <- NA
  yvar2 <- rev(yvar)
  tvar2 <- rev(tvar)
  run2 <- cumsum(yvar2)
  un2 <- unique(run2)
  tlist2 <- list()
  for (ii in 1:length(un2)) {
    vv <- un2[ii]
    yy <- yvar2[run2 == vv]
    tt <- tvar2[run2 == vv]
    tt <- tt - tt[1]
    tlist2[[ii]] <- tt
  }
  tbe <- unlist(tlist2)
  tbe[run2 == 0] <- NA
  tbe <- rev(tbe)
  run[run == 0] <- NA
  ans <- cbind(data.frame(yvar = yvar, tvar = tvar), abs.tse, 
               sign.tse, ewin = ewin, run, tae = tae, tbe = tbe)
  ans
}


# Get data for Mozambique
if('sefull.RData' %in% dir()){
  load('sefull.RData')
} else {
  aws_data_sefull <- retrieve_data_from_aws(fid = 'sefull', clean = TRUE)
  sefull <- aws_data_sefull$data
  save(sefull, file = 'sefull.RData')  
}

# Get data for Kenya (generated in the metadata script)
load('~/Documents/bk/scripts/metadata/pre_safety.RData')

# Read in Mozambique/Kenya malaria cases (downstream from efficacy, created by Matthew May 8-9 2024)
ken_efficacy <- read_csv('kenya_incident_cases.csv')
moz_efficacy <- read_csv('moz_incident_cases.csv')  # https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1715251127958939

# Clean up moz efficacy and get more information about it
moz_efficacy <- 
  moz_efficacy %>% 
  dplyr::rename(intervention = arm) %>%
  dplyr::select(
    extid, 
    todays_date,
    intervention,
    cluster,
    incident_case
  ) %>%
  dplyr::arrange(todays_date)

# Combine the two efficacy datasets
efficacy <-
  bind_rows(
    ken_efficacy %>% dplyr::select(extid,
                                   intervention = assignment,
                                   cluster,
                                   todays_date,
                                   incident_case) %>%
      mutate(country = 'KEN'),
    moz_efficacy %>% dplyr::select(extid, 
                                   intervention,
                                   cluster,
                                   todays_date,
                                   incident_case) %>%
      mutate(country = 'MOZ')
  )

# Now done with efficacy ##################################

# Time to reshape safety into "protection scores"
# Start with Mozambique
# We want to get time since treated (with ivermectin) for everyone
# Structure the data accordingly, keeping only relevant variables
tt <- 
  sefull$repeat_individual_questionnaire %>%
  left_join(sefull$Submissions %>% dplyr::select(key, household_id, current_visit, todays_date, intervention, cluster),
            by = c('parent_key' = 'key')) %>%
  dplyr::select(
    household_id,
    extid, 
    drug_swallow,
    absent_twice,
    current_visit,
    todays_date,
    intervention,
    cluster
  )

# Create a cluster-date table with the current visit for each cluster-date
cluster_date_moz <- tt %>%
  group_by(date = todays_date,
           cluster, 
           visit = current_visit) %>%
  tally %>%
  ungroup %>%
  arrange(desc(n)) %>%
  dplyr::distinct(date, cluster, .keep_all = TRUE) %>%
  arrange(cluster, date) %>% dplyr::select(-n)

# Create a separate table based on the "roster"; this will be used for knowing when people were observed
roster <- sefull$repeat_correct_roster %>%
  left_join(sefull$Submissions %>% dplyr::select(key, todays_date, household_id, current_visit),
            by = c('parent_key' = 'key')) %>%
  mutate(extid = extract_id(this_individual)) %>%
  dplyr::select(household_id,
                extid, todays_date, current_visit)
right <- sefull$repeat_individual_questionnaire %>%
  dplyr::select(extid)

# Use the roster to get the observation periods of people (ie, the first and last time they were
# observed by the project; absences count as observations, since it means they still live there, even if temporarily gone)
observation_period <- 
  roster %>%
  group_by(extid) %>%
  summarise(start_time = min(todays_date),
            stop_time = max(todays_date) + 30)

# Get instances of ivermectin distribution
ivermectin <- tt %>% filter(!is.na(drug_swallow)) %>%
  filter(drug_swallow == 'Yes') %>%
  dplyr::distinct(extid, date = todays_date) %>%
  mutate(ivermectin = TRUE)

# Use the start/stop times to create a one row per person-day time series dataframe
# the below loop takes approximately 1 hour to run
if('ts.RData' %in% dir()){
  load('ts.RData')
} else {
  ts_list <- list()
  observation_period <- observation_period %>% filter(!is.na(extid))
  nop <- nrow(observation_period)
  for(i in 1:nop){
    message(i, ' of ', nop)
    this_extid <- observation_period$extid[i]
    this_data <- observation_period %>% filter(extid == this_extid)
    these_dates <- seq(this_data$start_time, this_data$stop_time, by = 1)
    out <- tibble(extid = this_extid,
                  date = these_dates)
    # Join to the ivermectin data
    out <- left_join(out, ivermectin, by = c('extid', 'date')) %>%
      mutate(ivermectin = ifelse(is.na(ivermectin), FALSE, ivermectin))
    # Get days since ivermectin administration
    days_since_ivermectin <- time_since_event(yvar = out$ivermectin, tvar = out$date)
    out$days_since_ivermectin <- days_since_ivermectin$tae
    ts_list[[i]] <- out
  }
  # Combine the results of the list into one dataframe
  ts <- bind_rows(ts_list)
  save(ts, file = 'ts.RData')
}
# Convert the NA values to 30 (ie, time before receiving any invermectin)
ts$days_since_ivermectin[is.na(ts$days_since_ivermectin)] <- 30
# Apply the protection quantification to each person
ts$protection <- quantify_protection(ts$days_since_ivermectin)
# Get the household / cluster level info for each individual
# this ignores the specific dates when someone may have moved
# households/clusters, but is approximate / accurate enough
# for the purposes of aggregate level analysis
right <- roster %>%
  dplyr::distinct(extid, 
                  date = todays_date, 
                  current_visit,
                  household_id,
                  .keep_all = TRUE) %>%
  dplyr::select(extid, date, household_id, current_visit) %>%
  arrange(desc(date)) %>%
  dplyr::distinct(extid, date, .keep_all = TRUE)
ts <- left_join(ts, right)
ts <- ts %>%
  arrange(extid, date) %>%
  group_by(extid) %>%
  fill(household_id, current_visit, .direction = 'down')

# Now join the intervention level data to ts
right <- sefull$Submissions %>%
  filter(!is.na(arm),
         !is.na(intervention),
         !is.na(cluster)) %>%
  dplyr::distinct(household_id, .keep_all = TRUE) %>%
  dplyr::select(household_id, intervention, arm, cluster)
ts <- left_join(ts, right)

# Overwrite intervention variable for blinding
ts$intervention <- ts$arm

# Overwrite to 30 for those > 30 days ago
ts$days_since_ivermectin <- ifelse(ts$days_since_ivermectin > 30,
                                   30, 
                                   ts$days_since_ivermectin)
ts <- ts %>% ungroup

####### Kenya
# get kenya in ts format
kenya <- safety_repeat_individual %>%
  left_join(safety %>% dplyr::select(todays_date, KEY, current_visit = visit, cluster),
            by = c('PARENT_KEY' = 'KEY')) %>%
  mutate(ivermectin =  participant_take_drug == 'yes' | participant_take_drug_2 == 'yes') %>%
  bind_rows(safetynew_repeat_individual %>%
              left_join(safetynew %>% dplyr::select(todays_date, KEY, current_visit = visit, cluster),
                        by = c('PARENT_KEY' = 'KEY')) %>%
              mutate(ivermectin =  participant_take_drug == 'yes' | participant_take_drug_2 == 'yes'))
# Create a cluster-date table with the current visit for each cluster-date
cluster_date_ken <- kenya %>%
  group_by(date = todays_date,
           cluster, 
           visit = current_visit) %>%
  tally %>%
  ungroup %>%
  arrange(desc(n)) %>%
  dplyr::distinct(date, cluster, .keep_all = TRUE) %>%
  arrange(cluster, date) %>% dplyr::select(-n)

# Get instances of ivermectin distribution for kenya
ivermectin <- kenya %>% 
  filter(ivermectin) %>%
  dplyr::distinct(extid, date = todays_date) %>%
  mutate(ivermectin = TRUE)
observation_period <- kenya %>%
  group_by(extid) %>%
  summarise(start_time = min(todays_date),
            stop_time = max(todays_date) + 30)  # + 30????????????????????????
if('tsk.RData' %in% dir()){
  load('tsk.RData')
} else {
  
  ts_list <- list()
  observation_period <- observation_period %>% filter(!is.na(extid))
  nop <- nrow(observation_period)
  for(i in 1:nop){
    message(i, ' of ', nop)
    this_extid <- observation_period$extid[i]
    this_data <- observation_period %>% filter(extid == this_extid)
    these_dates <- seq(this_data$start_time, this_data$stop_time, by = 1)
    out <- tibble(extid = this_extid,
                  date = these_dates)
    # Join to the ivermectin data
    out <- left_join(out, ivermectin, by = c('extid', 'date')) %>%
      mutate(ivermectin = ifelse(is.na(ivermectin), FALSE, ivermectin))
    # Get days since ivermectin administration
    days_since_ivermectin <- time_since_event(yvar = out$ivermectin, tvar = out$date)
    out$days_since_ivermectin <- days_since_ivermectin$tae
    ts_list[[i]] <- out
  }
  # Combine the results of the list into one dataframe
  tsk <- bind_rows(ts_list)
  save(tsk, file = 'tsk.RData')
}
# Get cluster / intervention assignments
right <- safety_repeat_individual %>%
  left_join(safety %>% dplyr::select(KEY, cluster, current_visit = visit), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  bind_rows(safetynew_repeat_individual %>%
              left_join(safetynew %>% dplyr::select(todays_date, KEY, current_visit = visit),
                        by = c('PARENT_KEY' = 'KEY'))) %>%
  dplyr::select(extid, cluster, intervention, current_visit) %>%
  dplyr::distinct(extid, .keep_all = TRUE) 
tsk <- left_join(tsk, right)
# Overwrite to 30 for those > 30 days ago
tsk$days_since_ivermectin <- ifelse(tsk$days_since_ivermectin > 30,
                                    30, 
                                    tsk$days_since_ivermectin)
# Apply the protection quantification to each person
tsk$protection <- quantify_protection(tsk$days_since_ivermectin)
# combine tsk and ts
tsk$household_id <- substr(tsk$extid, 1, 5)
ts$arm <- NULL
ts$intervention <- as.character(ts$intervention)
protection <-
  bind_rows(
    ts %>% mutate(country = 'MOZ'),
    tsk %>% mutate(country = 'KEN')
  )
save.image('temp.RData')
# Now we have "efficacy" and "protection"
###################################################

# # Filter out irrelevant dates
protection <- protection %>%
  filter(
    (country == 'KEN' & date >= '2023-10-03' & date <= '2024-02-28') |
      (country == 'MOZ' & date >= '2022-03-17' & date <= '2022-11-30')
  )

# Get the denominator (total number of people in each cluster)
# This is an approximation due to migration, etc.
denominator <- tsk %>%
  group_by(cluster) %>%
  summarise(total_people = length(unique(extid))) %>%
  mutate(country = 'KEN') %>%
  bind_rows(
    ts %>%
      group_by(cluster) %>%
      summarise(total_people = length(unique(extid))) %>%
      mutate(country = 'MOZ') 
  )



# Get CLUSTER LEVEL insectocidal score
insectocidal_score <- protection %>%
  group_by(country, date, cluster) %>%
  summarise(numerator = sum(protection) ) %>%
  left_join(denominator) %>%
  mutate(score = numerator / total_people)

# sanity plot
g <- ggplot(data = insectocidal_score,
       aes(x = date,
           y = score)) +
  geom_line(alpha = 0.2, aes(group = cluster)) +
  facet_wrap(~country, scales = 'free_x', ncol = 1) +
  theme(legend.position = 'bottom')
g
# Add when tests occurred
pdx <- efficacy %>%
  group_by(country, date = todays_date) %>%
  summarise(n_tests = n()) %>%
  group_by(country) %>%
  mutate(p_tests = n_tests / sum(n_tests))
g +
  geom_col(data = pdx,
           aes(x = date,
               y = p_tests * 10),
           alpha = 0.5,
           fill = 'red')
# # Side note, get safety net information for kenya
# if(FALSE){
#   nets <- safety_repeat_individual %>%
#     left_join(safety %>% dplyr::select(KEY, 
#                                        visit,
#                                        cluster,
#                                        hhid, 
#                                        todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
#     dplyr::select(hhid,
#                   extid,
#                   cluster,
#                   todays_date,
#                   person_absent,
#                   visit,
#                   sleep_net_last_night,
#                   nights_sleep_net) %>%
#     mutate(src = 'safety') %>%
#     bind_rows(
#       safetynew_repeat_individual %>%
#         left_join(safetynew %>% dplyr::select(KEY, 
#                                            visit,
#                                            cluster,
#                                            hhid, 
#                                            todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
#         dplyr::select(hhid,
#                       extid,
#                       cluster,
#                       todays_date,
#                       visit,
#                       sleep_net_last_night,
#                       nights_sleep_net) %>%
#         mutate(src = 'safetynew')
#     )
#   write_csv(nets, 'nets.csv')
# }


# Get cluster level CUMULATIVE insectocidal score
# but first get which visit number corresponds to which cluster for which date
cluster_dates <- bind_rows(
  cluster_date_moz %>% mutate(country = 'MOZ'),
  cluster_date_ken %>% mutate(country = 'KEN')
)
ggplot(data = cluster_dates %>% filter(date >= '2022-01-01',
                                       visit %in% paste0('V', 1:4)),
       aes(x = date,
           y = cluster,
           color = visit)) +
  geom_point(alpha = 0.6, size = 0.5) +
  facet_wrap(~country, scales = 'free') +
  theme_bw() +
  theme(legend.position = 'bottom')
cumulative_insectocidal_score <- insectocidal_score %>%
  ungroup %>%
  left_join(cluster_dates) %>%
  filter(visit %in% paste0('V', 1:3)) %>%
  # get only the maximum value for each visit
  arrange(desc(score)) %>%
  dplyr::distinct(country, visit, cluster, .keep_all = TRUE) %>%
  arrange(country, cluster, date)
ggplot(data = cumulative_insectocidal_score,
       aes(x = visit,
           y = score)) +
  # geom_violin(alpha = 0.6, fill = 'yellow') +
  # geom_line(aes(group = cluster)) +
  geom_boxplot(outliers = FALSE) +
  geom_jitter(width = 0.05, height = 0, alpha = 0.5, fill = NA) +
  facet_wrap(~country, scales = 'free_x') +
  theme_bw()

# Now get the cumulative (sum) protection score for each cluster
right_hand <- cumulative_insectocidal_score %>%
  group_by(country, cluster) %>%
  summarise(protection = sum(score))

ggplot(data = right_hand,
       aes(x = protection)) +
  geom_density(aes(fill = country), 
               alpha = 0.7) +
  theme_bw() +
  theme(legend.position = 'bottom') +
  scale_fill_manual(name = 'Country', values = c('red', 'blue'))

# Now get the cumulative (sum) incidence for each cluster
left_hand <- efficacy %>%
  # simplify intervention
  mutate(intervention = ifelse(grepl('Alb|Cont', intervention), 'Control', 'Ivermectin')) %>%
  group_by(country, intervention, cluster) %>%
  summarise(cases = sum(incident_case, na.rm = TRUE),
            tries = n()) %>%
  ungroup %>% 
  mutate(incidence = cases / tries * 100)

# Join the protection and incidence scores
model_data <- left_join(left_hand, right_hand)

# Plot
ggplot(data = model_data,
       aes(x = protection,
           y = incidence,
           color = country)) +
  geom_point(aes(size = tries), alpha = 0.5) +
  facet_wrap(~intervention) +
  geom_smooth(method = 'lm', se = FALSE) +
  labs(x = 'Cumulative protection (sum of each "peak" protection score)',
       y = 'Cumulative incidence',
       caption = 'One dot = one cluster')
fit <- lm(incidence ~ protection*intervention, data = model_data)
summary(fit)
ors <- data.frame(ors)
names(ors)[2:3] <- c('lwr', 'upr')
p_value <- summary(fit)$coefficients[2,4]
ors
p_value


# PERSON LEVEL DATA
# Get the peak insecticidal score for the person's cluster
# during the 11-25 rearview window
efficacy$insectocidal_score <- NA
nre <- nrow(efficacy)
for(i in 1:nrow(efficacy)){
  message(i, ' of ', nre)
  this_efficacy <- efficacy[i,]
  this_country <- this_efficacy$country
  this_date <- this_efficacy$todays_date
  this_cluster <- this_efficacy$cluster
  starter <- this_date - 45
  ender <- this_date - 15
  this_insectocidal_score <- insectocidal_score %>%
    ungroup %>%
    filter(country == this_country,
           cluster == this_cluster) %>%
    filter(
           date >= starter,
           date <= ender) %>%
    summarise(max_score = max(score, na.rm = TRUE)) %>%
    pull(max_score)
  efficacy$insectocidal_score[i] <- this_insectocidal_score
}

# Remove visit 1 efficacy tests from moz (already removed from kenya)
# NOT DONE
# "infinite" means no protection (ie, no safety data)
efficacy$insectocidal_score[!is.finite(efficacy$insectocidal_score)] <- 0
save.image('temp2.RData')

# Only look at the first few visits, but not first
efficacy <- efficacy %>%
  mutate(date = todays_date) %>%
  filter(
    (country == 'KEN' & date >= '2023-11-03' & date <= '2024-02-01') |
      (country == 'MOZ' & date >= '2022-04-15' & date <= '2022-08-01')
  )

# Get arms
efficacy <- efficacy %>%
  mutate(arm = ifelse(grepl('Human|Ivermec', intervention), 
                    'Treatment', 'Control')) 

# Set control arm to 0 insectocidal score
efficacy <- efficacy %>%
  mutate(insectocidal_score = ifelse(arm == 'Control', 0, insectocidal_score))

# Model code
fit <- glm(incident_case ~ insectocidal_score, data = efficacy, family = binomial('logit'))
ors <- exp(cbind(OR = coef(fit), confint(fit)))
ors <- data.frame(ors)
names(ors)[2:3] <- c('lwr', 'upr')
p_value <- summary(fit)$coefficients[2,4]
ors
p_value


# Do at cluster level
cluster_level <- efficacy %>%
  arrange(todays_date) %>%
  mutate(month = lubridate::floor_date(date, 'month')) %>%
  group_by(country, cluster, month, arm) %>%
  summarise(cases = sum(incident_case, na.rm = TRUE),
            tries = length(incident_case),
            insectocidal_score = mean(insectocidal_score)) %>%
  ungroup %>%
  group_by(country) %>%
  mutate(ttm_last_month = dplyr::lag(cases, 1)) %>%
  mutate(ttm_last_month = ifelse(is.na(ttm_last_month), 0, ttm_last_month)) %>%
  mutate(prevalence = cases / tries * 100)
ggplot(data = cluster_level %>% filter(tries >= 10),
       aes(x = (insectocidal_score),
           y = (prevalence),
           color = country)) +
  geom_jitter(aes(size = tries), width = 0.01, height = 2, alpha = 0.6) +
  theme_bw() +
  # geom_smooth(aes(group = NA, color = NA), method.args = list(family = 'binomial')) +
  geom_smooth(aes(group = NA, color = NA)) +
  theme(legend.position = 'bottom') 
  

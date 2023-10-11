library(dplyr)
library(tidyr)

v0 <- read.csv("../Downloads/v0.csv")
v0dem <- read.csv("../Downloads/v0demography.csv")

# Join ind and household datasets
dat <- v0 %>%
    left_join(v0dem %>% dplyr::select(KEY, todays_date, wid, hhid, cluster),
            by = c('PARENT_KEY' = 'KEY')) %>%
  select(firstname, sex, extid, cluster, hhid, wid)

# Keep names with more than 10 repetitions in data set
dat10 <- dat %>%
  group_by(firstname) %>%
  filter(n() >= 5) %>%
  ungroup()

dat10$firstname[dat10$firstname == ""] <- NA
dat10 <- dat10[complete.cases(dat10), ]

# Make all uppercase
dat10$firstname <- toupper(dat10$firstname)

# Create a table with first name and number for each gender in names gender table (ngt)
ngt <- dat10 %>%
  group_by(firstname = firstname, sex) %>%
  summarise(count = n()) %>%
  pivot_wider(names_from = sex, values_from = count, values_fill = 0)

# Calculate the percentage of females and males for each row
ngt <- ngt %>%
  mutate(total = Male + Female) %>%
  mutate(female_percentage = round((Female / total) * 100, 2)) %>%
  mutate(male_percentage = round((Male / total) * 100, 2))

# Create column with likely gender for each first name (ie names where 90-99.9% of participants are from one gender)
ngt <- ngt %>%
  mutate(likely_gender = ifelse(female_percentage >= 80 & female_percentage <= 99.9, 'Female',
                                ifelse(male_percentage >= 80 & male_percentage <= 99.9, 'Male', 'NA')))
# Drop those that are <90% and 100% male or female
mf <- ngt %>%  filter(likely_gender %in% c('Male', 'Female'))

# Join with main dataset
mismatch <- left_join(mf, dat10, by = "firstname")

# Create column 'gender_mismatch' with TRUE if v0 gender doesn't match likely_gender
mismatch <- mismatch %>%
  mutate(gender_mismatch = sex != likely_gender) %>%
  select(firstname, extid, sex, likely_gender, gender_mismatch, hhid, cluster, wid) %>%
  filter(gender_mismatch == TRUE)

# Fix HHID/Cluster
mismatch$hhid <- ifelse(nchar(mismatch$hhid) == 4, paste0("0", mismatch$hhid), mismatch$hhid)
mismatch$cluster <- ifelse(nchar(mismatch$cluster) == 1, paste0("0", mismatch$cluster), mismatch$cluster)

# See which WIDs are making the most mistakes
wid <- table(mismatch$wid)
wid <- as.data.frame(wid)
wid <- wid[order(wid$Freq), ] 
wid <- wid %>% filter(Freq > 2)

write.csv(wid, 'cl_genderissues.csv')

#################

# Flag potential age issues for hh_head < 16 years, bday is todays_date 
dat2 <- v0 %>%
    left_join(v0dem %>% dplyr::select(KEY, todays_date, wid, hhid, cluster),
            by = c('PARENT_KEY' = 'KEY')) %>%
  select(firstname, extid, cluster, hhid, wid, age, dob, hh_head_yn, todays_date)

dat2$firstname[dat2$firstname == ""] <- NA
dat2 <- dat2[complete.cases(dat2), ]

young_head <- dat2 %>%
  mutate(young_head = hh_head_yn =='yes' & age <16) %>%
  filter(young_head == TRUE)

bdaytoday <- dat2 %>% filter(todays_date == dob)

# Group the data by household_id and dob, then count the number of individuals with the same DOB in each household
dob_counts <- dat2 %>%
  group_by(hhid, dob) %>%
  summarise(num_individuals = n()) %>%
  ungroup()

# Filter households where more than one individual shares the same DOB
anomalies <- dob_counts %>%
  filter(num_individuals > 1)

anomalies$hhid <- ifelse(nchar(anomalies$hhid) == 4, paste0("0", anomalies$hhid), anomalies$hhid)

v0dem$hhid <- as.character(v0dem$hhid)
v0dem$hhid <- ifelse(nchar(v0dem$hhid) == 4, paste0("0", v0dem$hhid), v0dem$hhid)

anomalies <- anomalies %>% left_join(v0dem %>% dplyr::select(wid, hhid),
            by = c('hhid' = 'hhid'))

# See which WIDs are making the most mistakes
wid2 <- table(anomalies$wid)
wid2 <- as.data.frame(wid2)
wid2 <- wid2[order(wid2$Freq), ]
wid2 <- wid2 %>% filter(Freq > 3)

write.csv(wid2, "cl_dobrepeats.csv")

###
write.csv(mismatch, "namegendermismatch.csv", row.names = FALSE)
write.csv(bdaytoday, "bdaytoday.csv", row.names = FALSE)
write.csv(young_head, "young_hh_head.csv", row.names = FALSE)
write.csv(anomalies, "multiple_same_dob.csv", row.names = FALSE)


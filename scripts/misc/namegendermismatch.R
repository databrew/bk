library(dplyr)
library(tidyr)

v0 <- read.csv("../Downloads/v0.csv")
vf <- v0 %>% select(firstname, sex)
vf$firstname <- toupper(vf$firstname)

# Get frequency of names
name_frequency <- table(vf$firstname)

# Sort the frequency table in descending order
sorted_frequency <- sort(name_frequency, decreasing = TRUE)

# Get the top 50 most common names
top_100_names <- names(sorted_frequency)[1:100]
print(top_100_names)

# Keep top 50 names
keep <- c(
"FATUMA", "MWANASHA", "ALI", "BAKARI", "MWANASITI", "JUMA", "MWANAKOMBO", "HAMISI", "MOHAMED", "MWANAMISI", "AMINA", "HASSAN", "OMARI", "SALIM", "SAUMU", "TIMA", "MARIAM", "ASHA", "HALIMA", "RAMA", "SULEIMAN", "ABDALLA", "SHEE", "KASSIM", "MOHAMMED", "REHEMA", "OMAR", "MWANAULU", "SAID", "ABDALLAH", "MWANAISHA", "SAIDI", "AISHA", "MWANAMKASI", "RIZIKI", "HAMADI", "MWANAJUMA", "RASHID", "MWANATUMU", "RUKIA", "MWANAIDI", "MWAJUMBE", "HADIJA", "MASUDI", "MWAKA", "IBRAHIM", "UMAZI", "KHADIJA", "MBWANA", "MWANAMKUU", "CHIZI", "HUSSEIN", "SALIMU", "NASSORO", "ISMAIL", "SOFIA", "MARY", "MWANAMVUA", "MEJUMAA", "BAHATI", "BIASHA", "LUVUNO", "ATHUMAN", "MLONGO", "JOSEPH", "MWANALIMA", "SALMA", "ABDUL", "YUSUF", "MBEYU", "ZAINAB", "ATHUMANI", "ISSA", "BINTI", "SHABAN", "HASSANI", "MARIAMU", "SALAMA", "MISHI", "ZULFA", "JOHN", "MWANDAZI", "ZUHURA", "ADAM", "DANIEL", "GRACE", "SAUM", "SWALEHE", "KOMBO", "MWANAPILI", "MWATIME", "NEEMA", "ABUBAKAR", "HAMAD", "NURU", "ESTHER", "MUSA", "ZAINABU", "MARINDA", "MWALIMU"
)

# Filter the data frame
keep_names <- subset(vf,vf$firstname %in% keep)

# Create a table with first name and number for each gender in names gender table (ngt)
ngt <- keep_names %>%
  group_by(firstname, sex) %>%
  summarize(count = n()) %>%
  pivot_wider(names_from = sex, values_from = count, values_fill = 0)

# Calculate the total count for each row
ngt <- ngt %>%
  mutate(total = Female + Male)

# Calculate the percentage of females and males for each row
ngt <- ngt %>%
  mutate(female_percentage = (Female / total) * 100)
ngt <- ngt %>%
  mutate(male_percentage = (Male / total) * 100)

# Create column with likely gender for each first name (ie names where 90-99.9% of participants are from one gender)
ngt <- ngt %>%
  mutate(likely_gender = ifelse(female_percentage >= 90 & female_percentage <= 99.9, 'female',
                                ifelse(male_percentage >= 90 & male_percentage <= 99.9, 'male', 'NA')))
# Drop those that are <90% and 100% male or female
mf <- ngt %>%  filter(likely_gender %in% c('male', 'female'))

# Prepare main dataset 
v0mini <- v0 %>% select(firstname, lastname, extid, sex)
v0mini$sex <- tolower(v0mini$sex)

# Join with main dataset
merged <- left_join(mf, v0mini, by = "firstname")

# Create column 'gender_mismatch' with TRUE if v0 gender doesn't match likely_gender
merged <- merged %>%
  mutate(gender_mismatch = sex != likely_gender) %>%
  select(firstname, lastname, extid, sex, likely_gender, gender_mismatch) %>%
  filter(gender_mismatch == TRUE)




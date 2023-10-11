#### CLs making many mistakes

library(dplyr)
safety <- read.csv("../../../../Downloads/safety.csv")
safety_ind <- read.csv("../../../../Downloads/safety_ind.csv")

## From Safety Form V1
##  Detect short completion time

sv <- safety %>% select(todays_date, start_time, end_time, hhid, wid, visit, num_members, Accuracy, sum_members_left_household, sum_absences, cluster) 

sv$hhid <- ifelse(nchar(sv$hhid) == 4, paste0("0", sv$hhid), sv$hhid)

sv$all_absent <- ifelse(sv$num_members == sv$sum_absences, 1, 0)
sv$all_left <- ifelse(sv$num_members == sv$sum_members_left_household, 1, 0)


# Convert date-time strings to POSIXct objects
sv$start_time <- as.POSIXct(sv$start_time, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
sv$end_time <- as.POSIXct(sv$end_time, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")

# Calculate the duration
sv$duration <- round(difftime(sv$end_time, sv$start_time, units = "mins"), 2)

## all left household table
allgone <- sv %>% filter(all_left == 1) %>% select(hhid, num_members, sum_members_left_household, wid)
write.csv(allgone, "all_left.csv", row.names = FALSE)

## all absent
allabsent <- sv %>% filter(all_absent == 1) %>% select(hhid, num_members, sum_absences, wid)
write.csv(allabsent, "all_absent.csv", row.names = FALSE)

## short visit = duration <= 10 mins
short <- sv %>% filter(duration <= 10 & all_left == 0 & all_absent == 0) %>% select(hhid, num_members, duration, wid)

short2 <- sv %>% filter(duration <= 15 & all_left == 0 & all_absent == 0) %>% select(hhid, num_members, duration, wid)

# Show CLs with >1 short visits
short_wid <- table(short$wid)
short_wid <- as.data.frame(short_wid)
short_wid <- short_wid[order(short_wid$Freq), ]
short_wid <- short_wid %>% filter(Freq > 2)

write.csv(short_wid, "cl_short_visits.csv", row.names = FALSE)

## Inaccurate geocoding
geo <- sv %>% filter(Accuracy >= 20) %>% select(hhid, Accuracy, wid, cluster)
geo_wid <- table(geo$wid)
geo_wid <- as.data.frame(geo_wid)
geo_wid <- geo_wid[order(geo_wid$Freq), ]
geo_wid <- geo_wid %>% filter(Freq > 2)

write.csv(geo, "geolocation_issues.csv")
write.csv(geo_wid, "cl_inaccurate_geolocation.csv", row.names = FALSE)

## Height and weight issues in safety individual form

hw <- safety_ind %>%
    left_join(safety %>% dplyr::select(KEY, wid, hhid),
            by = c('PARENT_KEY' = 'KEY')) %>%
  select(extid, hhid, wid, height, weight, bmi, age, suspicious_height, suspicious_weight) %>%
  filter(!is.na(height))
hw$suspicious_bmi <- ifelse(hw$bmi < 14 |hw$bmi > 35, 1, 0)                       

hw_issues <- hw %>% filter(suspicious_height == 1 | suspicious_weight == 1 | (age > 6 & suspicious_bmi == 1))

# By WID
hw_wid <- table(hw_issues$wid)
hw_wid  <- as.data.frame(hw_wid)
hw_wid <- hw_wid[order(hw_wid$Freq), ]
hw_wid <- hw_wid %>% filter(Freq > 2)

write.csv(hw_wid, "cl_hw_issues.csv", row.names = FALSE)


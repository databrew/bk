## Explore V0 data 
library(dplyr)
library(tidyr)

hhfirst <- read.csv("../Downloads/entohhfirstvisitke.csv")
lefirst <- read.csv("../Downloads/entolefirstvisitke.csv")

setwd("bk/scripts/ento_metadata")

hhlist <- hhfirst %>%
  mutate(hhid = ifelse(nchar(hhid) == 4, paste0("0", hhid), hhid)) %>% # add leading 0 to 4-digit hhids 
  arrange(hhid) %>% 
  mutate(name = as.character(hhid),   # Convert 'hhid' to character and store in 'name'
         label = name,                 # Duplicate 'name' into 'label'
         list_name = "hhid") %>%      # Create 'list_name' column with value "hhid"
 select(list_name, name, label) 
 hhlist$name <- sprintf("%s", hhlist$name)
 hhlist$label<- sprintf("%s", hhlist$label)


lelist <- lefirst %>% select(leid) %>%
  arrange(leid) %>% 
   mutate(name = leid,   # Convert 'hhid' to character and store in 'name'
         label = name,                 # Duplicate 'name' into 'label'
         list_name = "leid") %>%      # Create 'list_name' column with value "hhid"
 select(list_name, name, label) 

write.table(hhlist, "first_visit_hhids.csv", sep = ",", row.names = FALSE)
write.table(lelist, "first_visit_leids.csv", sep = ",", row.names = FALSE)


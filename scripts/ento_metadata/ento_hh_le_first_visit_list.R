## Explore V0 data 
library(dplyr)
library(tidyr)

setwd("bk/scripts/ento_metadata")

hhfirst <- read.csv("../Downloads/entohhfirstvisitke.csv")
lefirst <- read.csv("../Downloads/entolefirstvisitke.csv")

hhfirst <- hhfirst %>% select(hhid) %>% 
  mutate(hhid = ifelse(nchar(hhid) == 4, paste0("0", hhid), hhid)) %>%
  arrange(hhfirst)

hhfirst$hhid <- sprintf("%s", hhfirst$hhid)

lefirst <- lefirst %>% select(leid) %>%
  arrange(lefirst)

write.table(hhfirst, "first_visit_hhids.xlsx", row.names = FALSE, col.names = FALSE)
write.table(lefirst, "first_visit_leids.xlsx", row.names = FALSE, col.names = FALSE)


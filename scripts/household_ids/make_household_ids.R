library(dplyr)

# Define function for filling out number of zeroes
add_zero <- function (x, n) {
  x <- as.character(x)
  adders <- n - nchar(x)
  adders <- ifelse(adders < 0, 0, adders)
  for (i in 1:length(x)) {
    if (!is.na(x[i])) {
      x[i] <- paste0(paste0(rep("0", adders[i]), collapse = ""), 
                     x[i], collapse = "")
    }
  }
  return(x)
}

# Define letters excluding those with ambiguous shapes
lets <- LETTERS
lets <- lets[!lets %in% c('G', 'S', 'O', 'I')]

df <-
  expand.grid(
    a = lets,
    b = lets,
    c = 1:1000
  ) %>% 
  mutate(id = paste0(a, b, '-', add_zero(c, n = 3))) %>%
  arrange(a,b,c) %>%
  mutate(is_valid = !grepl('1000', id))
readr::write_csv(df %>% filter(is_valid), 'ids_with_letters.csv')
all_ids <- df

dir.create('household_lists')
chunk_starters <- seq(1, nrow(all_ids), by = 200)
for(i in 1:length(chunk_starters)){
  message(i, ' of ', length(chunk_starters))
  this_start <- chunk_starters[i]
  this_end <- this_start + 199
  this_data <- all_ids[this_start:this_end,] %>%
    filter(is_valid)
  
  file_name <- paste0(this_data$id[1], '_to_', this_data$id[nrow(this_data)], '.pdf')
  full_path <- paste0('household_lists/', file_name)
  rmarkdown::render(input = 'household_ids_list.Rmd',
                    output_file = full_path,
                    params = list(data = this_data))
  
}
# 
# this_df <- df[1:200,]
# 
# 
# df <- tibble(number_id = 1:99999) %>%
#   mutate(id = add_zero(number_id, 5)) %>%
#   filter(number_id >= 10000)# %>%
#   # filter(!grepl('0', id)) %>%
#   # filter(!grepl('11', id)) %>%
#   # filter(!grepl('22', id)) %>%
#   # filter(!grepl('33', id)) %>%
#   # filter(!grepl('44', id)) %>%
#   # filter(!grepl('55', id)) %>%
#   # filter(!grepl('66', id)) %>%
#   # filter(!grepl('77', id)) %>%
#   # filter(!grepl('88', id)) %>%
#   # filter(!grepl('99', id)) 
# readr::write_csv(df, 'ids_with_only_numbers.csv')
# 
# # library(ggplot2)
# # x <- rnorm(n = 100, mean = 0, sd = 2)
# # y <- rnorm(n = 100, mean = 0, sd = 2)
# # df <- tibble(x, y)
# # out_list <- list()
# # for(i in 1:nrow(df)){
# #   new_rows <- df %>%
# #     mutate(x = x + rnorm(n = 100, mean = 0, sd = 1),
# #            y = y + rnorm(n = 100, mean = 0, sd = 1)) %>%
# #     mutate(village = 1:nrow(df))
# #   out_list[[i]] <- new_rows
# # }
# # done <- bind_rows(out_list) %>%
# #   mutate(village = factor(village))
# # ggplot(data = done,
# #        aes(x = x, 
# #            y = y,
# #            color = village)) +
# #   geom_jitter(alpha = 0.5, size = 0.5) +
# #   xlim(-5, 5) +
# #   ylim(-5, 5) +
# #   theme(legend.position = 'none')

# https://docs.google.com/spreadsheets/d/1O5eq21j04HaTspSrMUIVmVn7niu9L5nvsIQ9fGFII4w/edit#gid=227626065
library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(lubridate)
library(readr)
library(ggplot2)

# Define production
folder <- 'kwale'
# folder <- 'test_of_test'

# Nuke the folder prior to data retrieval
unlink(folder, recursive = TRUE)

raw_or_clean <- 'clean'
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE") # set using .Renviron file
start_fresh <- TRUE

rr <- function(x){
  message('removing ', nrow(x), ' rows')
  return(head(x, 0))
}

if(start_fresh){
  # Log in
  tryCatch({
    logger::log_info('Attempt AWS login')
    # login to AWS - this will be bypassed if executed in CI/CD environment
    cloudbrewr::aws_login(
      role_name = 'cloudbrewr-aws-role',
      profile_name =  'cloudbrewr-aws-role',
      pipeline_stage = env_pipeline_stage)

  }, error = function(e){
    logger::log_error('AWS Login Failed')
    stop(e$message)
  })


  # Define datasets for which I'm retrieving data
  datasets <- c('efficacy', 'v0demography')
  datasets_names <- datasets

  # Loop through each dataset and retrieve
  # bucket <- 'databrew.org'
  # folder <- 'kwale'
  bucket <- 'databrew.org'
  if(!dir.exists(folder)){
    dir.create(folder)
  }
  for(i in 1:length(datasets)){
    this_dataset <- datasets[i]
    object_keys <- glue::glue('/{folder}/{raw_or_clean}-form/{this_dataset}',
                              folder = folder,
                              this_dataset = this_dataset)
    output_dir <- glue::glue('{folder}/{raw_or_clean}-form/{this_dataset}',
                             folder = folder,
                             this_dataset = this_dataset)
    unlink(output_dir, recursive = TRUE)
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    print(object_keys)
    aws_s3_bulk_get(
      bucket = bucket,
      prefix = as.character(object_keys),
      output_dir = output_dir
    )
  }

  # Read in the datasets
  middle_path <- glue::glue('{folder}/{raw_or_clean}-form/')
  efficacy <- read_csv(paste0(middle_path, 'efficacy/efficacy.csv'))
  v0demography <- read_csv(paste0(middle_path, 'v0demography/v0demography.csv'))
  v0demography_repeat_individual <- read_csv(paste0(middle_path, 'v0demography/v0demography-repeat_individual.csv'))
  
  save(efficacy,
       v0demography,
       v0demography_repeat_individual,
       file = 'data.RData')
  
} else {
  load('data.RData')
}

# Load clusters
load('../../data_public/spatial/new_clusters.RData')
load('../../data_public/spatial/pongwe_kikoneni_ramisi.RData')

# Make household ID 5 characters
add_zero <- function (x, n) {
  if(length(x) > 0){
    x <- as.character(x)
    adders <- n - nchar(x)
    adders <- ifelse(adders < 0, 0, adders)
    for (i in 1:length(x)) {
      if (!is.na(x[i])) {
        x[i] <- paste0(paste0(rep("0", adders[i]), collapse = ""),
                       x[i], collapse = "")
      }
    }
  }
  return(as.character(x))
}
efficacy <- efficacy %>% mutate(hhid = add_zero(hhid, n = 5))
v0demography <- v0demography %>% mutate(hhid = add_zero(hhid, n = 5))

# Deal with multiple tests
efficacy$positive <- efficacy$pan_result == 'Positive' | efficacy$pf_result == 'Positive'
efficacy$valid <- efficacy$control_validity_b == 'valid' | efficacy$control_validity == 'valid'

# Keep only valid
efficacy <- efficacy %>% filter(valid)

# Get overall prevalence
efficacy %>% group_by(positive) %>%
  tally %>% ungroup %>%
  mutate(p = n / sum(n) * 100)


# Get prevalence by cluster
by_cluster <- efficacy %>%
  group_by(cluster) %>%
  summarise(positives = length(which(positive)),
            tests = n()) %>%
  ungroup %>%
  mutate(p = positives / tests * 100)
by_cluster$cluster <- add_zero(by_cluster$cluster, n = 2)

# Make map
map <- new_clusters
map@data$cluster <- add_zero(map@data$cluster_nu, n = 2)



map@data$id <- map@data$cluster
md_fortified <- fortify(map, region = 'id')
md_fortified <- left_join(md_fortified,
                          by_cluster,
                          by = c('id' = 'cluster'))

library(sp)
label_df <- coordinates(map)
label_df <- data.frame(label_df)
names(label_df) <- c('x', 'y')
label_df$cluster <- map@data$cluster
label_df <- left_join(label_df, by_cluster)
label_df$label <- paste0(
  # 'Cluster ', label_df$cluster, '\n',
  label_df$positives, '/', label_df$tests, '\n', round(label_df$p), '%'
)
ggplot() +
  geom_polygon(data = pongwe_kikoneni_ramisi_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               fill = 'beige',
               color = 'white') +
  geom_polygon(data = md_fortified,
               aes(x = long,
                   y = lat,
                   group = group,
                   fill = p)) +
  theme_bw() +
  scale_fill_gradient2(name = 'Prevalence', mid = 'orange', high = 'darkred') +
  theme(legend.position = 'bottom') +
  labs(x = '', y = '') +
  coord_map() +
  geom_text(data = label_df,
            aes(x = x,
                y = y,
                label = label),
            size = 0.65,
            color = 'white')

library(leaflet)
md <- map_data
bins <- c(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
pal <- colorBin("YlOrRd", domain = md@data$p, bins = bins)

l <- leaflet() %>%
  addTiles() %>%
  # addProviderTiles(providers$Stamen.TonerBackground) %>%
  addPolygons(data= pongwe_kikoneni_ramisi,
              fillColor = 'beige',
              color = 'black',
              weight = 1) %>%
  addPolygons(data = md,
    fillColor = ~pal(p),
    weight = 0.2,
    opacity = 1,
    color = "black",
    # dashArray = "3",
    fillOpacity = 0.7,
    label = paste0('Cluster ', md@data$cluster),
    popup = paste0(md@data$positives, '/', md@data$tests, '=', round(md@data$p), '%')) %>%
  addLegend(pal = pal, values = md@data$p, opacity = 0.7, title = 'Prevalence',
            position = "bottomright")

# htmltools::save_html(l, file = '~/Desktop/efficacyv1.html')
htmlwidgets::saveWidget(widget = l, file = '~/Desktop/efficacyv1.html', selfcontained = TRUE)

write_csv(by_cluster, 'by_cluster.csv')

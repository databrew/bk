# Libraries
library(dplyr)
library(readr)
if('assignments.csv' %in% dir()){
  assignments <- read_csv('assignments.csv')
} else {
  # Set a randomization seed
  set.seed(1537)
  # Define a list of As and Bs
  assignment_options <- c(rep(1, 48), rep(2, 48))
  # Randomize that list
  assignment_options <- sample(assignment_options, size = length(assignment_options), replace = FALSE)
  # Add to cluster numbers
  assignments <- tibble(cluster_number = 1:96, assignment = assignment_options)
  # Write a csv
  write_csv(assignments, 'assignments.csv')
}

#!/bin/sh
cd ~/Documents/bk/misc/proof_of_concept_remote_database/;
rm -f log_01_update.log
Rscript 01_update.R >> log_01_update.log 2>&1


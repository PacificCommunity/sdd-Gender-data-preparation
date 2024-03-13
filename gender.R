#Load libraries needed to process the primary productions data
library(dplyr)
library(tidyverse)
library(RODBC)
library(RSQLite) #Use SQLite database to store and read data
library(haven) #Reading in Stata files library

#Setting up path environment
repository <- file.path(dirname(rstudioapi::getSourceEditorContext()$path))
setwd(repository)

# Connect to the VCMB SQL database system from server vbosinfo
mydb <- odbcConnect("gender", uid = "sa", pwd = "R@admin00x")

#source the function the generate the dataframes and populate the MSSQL database
#source("dataPreparation.R")

#Confidentiality threshold declaration
conf_threshold <- 3

#Source the functions that return the tables and the metadata
source("functions/education.R")
source("functions/householdHead.R")
source("functions/married18.R")
source("functions/genderWageGap.R")


#Read in the returned objects from the functions
gender_edu_d <- education_data(edu_attain)
gender_edu_m <- education_meta(edumeta)

gender_mar18_d <- married18_data(mar18_d)
gender_mar18_m <- married18_meta(mar18_m)

gender_hhHead_d <- hhHead_data(hhHead_d)
gender_hhHead_m <- hhHead_meta(hhHead_m)

gender_wageGap_d <- wGap_d(wageGap_data)
gender_wageGap_m <- wGap_m(wageGap_meta)


#Write the tables to CSV files
write.csv(gender_edu_d, "output/education/gender_education_data.csv", row.names = FALSE)
write.csv(gender_edu_m, "output/education/gender_education_metadata.csv", row.names = FALSE)

write.csv(gender_mar18_d, "output/married18/gender_married18_data.csv", row.names = FALSE)
write.csv(gender_mar18_m, "output/married18/gender_married18_metadata.csv", row.names = FALSE)

write.csv(gender_hhHead_d, "output/HHhead/gender_hhHead_data.csv", row.names = FALSE)
write.csv(gender_hhHead_m, "output/HHhead/gender_hhHead_metadata.csv", row.names = FALSE)

write.csv(gender_wageGap_d, "output/wageGap/gender_wageGap_data.csv", row.names = FALSE)
write.csv(gender_wageGap_m, "output/wageGap/gender_wageGap_metadata.csv", row.names = FALSE)

#Disconnect from the MS SQL server database
odbcClose(mydb)
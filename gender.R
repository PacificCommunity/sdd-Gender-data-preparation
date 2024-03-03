
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


gender <- read_dta("data/SPC_REG-10_2010-2021_Gender.dta")
country <- read.csv("other/country.csv")
isco <- read.csv("other/isco.csv")
isic <- read.csv("other/isic.csv")


datasource <- data.frame(
  aSourceID = c(1, 2, 3),
  dSourceCode = c("HIES", "NPH", "LFS"),
  dSourceDesc = c("Household Income and Expenditure Survey", "Population and Housing Census", "Labour Force Survey")
)

urbrur <- data.frame(
  urbrurID = c(1, 2),
  urbrurCode = c("U", "R"),
  urbrurDesc = c("Urban", "Rural")
)

sex <- data.frame(
  sexID = c(1, 2),
  sexCode = c("M","F"),
  sexDesc = c("Male", "Female")
)

ageGroup1 <- data.frame(
  ageGroupID = c(0, 1, 2, 3, 4),
  ageGroupCode = c("Y00T14", "Y15T24", "Y25T54", "Y55T64", "Y65T999"),
  ageGroupDesc = c("0-14", "15-24", "25-54", "55-64", "65+")
)

edattain <- data.frame(
  attainID = c(1, 2, 3, 4, 5, 6, 99),
  attainCode = c("02", "1", "2", "3", "4", "6", "9"),
  attainDesc = c("Less than primary", "Minimum primary", "Minimum lower secondary", "Minimum upper secondary", "Minimum post-secondary", "Minimum tertiary", "Not elsewhere classified")
)

hhHeadSex <- data.frame(
  hheadSexID = c(1, 2),
  hheadSexCode = c("MHEAD","FHEAD"),
  hheadSexDesc = c("Male HH head", "Female HH head")
)

disability <- data.frame(
  disabilityID = c(0, 1),
  disabilityCode = c("PWD", "PW"),
  disabilityDesc = c("No difficulty", "Some Difficulty")
)



occupation <- data.frame(
  occupationID = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
  occupationCode = c("1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "X"),
  occupationDesc = c("Managers", "Professionals", "Technicians and associate professionals",
                     "Clerical support workers", "Service and sales workers", "Skilled agricultural, forestry and fishery workers",
                     "Craft and related trades workers", "Plant and machine operators, and assemblers",
                     "Elementary occupations", "Armed forces occupations", "Not elsewhere classified"
  )
)

sqlSave(mydb, gender, tablename = "gender", append = TRUE)
sqlSave(mydb, datasource, tablename = "datasource", append = FALSE)
sqlSave(mydb, ageGroup1, tablename = "ageGroup1", append = FALSE)
sqlSave(mydb, edattain, tablename = "edattain", append = FALSE)
sqlSave(mydb, occupation, tablename = "occupation", append = FALSE)
sqlSave(mydb, sex, tablename = "sex", append = FALSE)
sqlSave(mydb, urbrur, tablename = "urbrur", append = FALSE)
sqlSave(mydb, hhHeadSex, tablename = "hhHeadSex", append = FALSE)
sqlSave(mydb, country, tablename = "country", append = FALSE)
sqlSave(mydb, isco, tablename = "isco", append = FALSE)
sqlSave(mydb, isic, tablename = "isic", append = FALSE)
sqlSave(mydb, disability, tablename = "disability", append = FALSE)

#Generate cube table for population by sex from gender dataset
gender_pop <- sqlQuery(mydb, "SELECT country.countryID,
                                     gender.year, 
                                     urbrur.urbrurCode, 
                                     sex.sexCode, 
                                     ageGroup1.ageGroupCode, 
                                     round(sum(gender.fweight), 0) AS OBS_VALUE
                              FROM gender
                              INNER JOIN country ON gender.country = country.country
                              INNER JOIN urbrur ON gender.rururb = urbrur.urbrurID
                              INNER JOIN sex ON gender.sex = sex.sexID
                              INNER JOIN ageGroup1 ON gender.age_grp1 = ageGroup1.ageGroupID
                              GROUP BY CUBE (countryID, year, urbrurCode, sexCode, ageGroupCode)
                 ")

gender_pop <- gender_pop[!is.na(gender_pop$year), ]
gender_pop <- gender_pop[!is.na(gender_pop$countryID), ]
gender_pop[is.na(gender_pop)] <- "_T"
names(gender_pop) <- c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "OBS_VALUE")

gender_pop <- gender_pop[, c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "OBS_VALUE")]

#Add the rest of the columns

gender_pop <- mutate(gender_pop,
                       FREQ ="A",
                       INDICATOR = "POP",
                       UNIT_MEASURE = "UNIT",
                       UNIT_MULT = "",
                       DATASOURCE = "",
                       OBS_STATUS = "",
                       OBS_COMMENT = ""
                       
)
#Reorder the columns in the standard logical order
new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANISATION", "SEX", "AGE", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT")
gender_pop <- gender_pop[, new_order]

write.csv(gender_pop, "output/gender_pop.csv", row.names = FALSE)

#### *********************** Gender by Education Attainment *******************************####

#Generate cube table for population by sex and education attainment from gender dataset
gender_education <- sqlQuery(mydb, "SELECT country.countryID,
                                     gender.year, 
                                     urbrur.urbrurCode, 
                                     sex.sexCode, 
                                     ageGroup1.ageGroupCode,
                                     disability.disabilityCode,
                                     edattain.attainCode,
                                     round(sum(gender.fweight), 0) AS OBS_VALUE
                              FROM gender
                              INNER JOIN country ON gender.country = country.country
                              INNER JOIN urbrur ON gender.rururb = urbrur.urbrurID
                              INNER JOIN sex ON gender.sex = sex.sexID
                              INNER JOIN ageGroup1 ON gender.age_grp1 = ageGroup1.ageGroupID
                              INNER JOIN disability ON gender.disabled = disability.disabilityID
                              INNER JOIN edattain ON gender.edattain = edattain.attainID
                              WHERE gender.age_grp1 > 1
                              GROUP BY CUBE (countryID, year, urbrurCode, sexCode, ageGroupCode, disabilityCode, attainCode)
                 ")

gender_education <- gender_education[!is.na(gender_education$year), ]
gender_education <- gender_education[!is.na(gender_education$countryID), ]
gender_education[is.na(gender_education)] <- "_T"

names(gender_education) <- c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "DISABILITY", "EDUCATION", "OBS_VALUE")
gender_education <- gender_education[, c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "DISABILITY", "EDUCATION", "OBS_VALUE")]

gender_education <- mutate(gender_education,
                     FREQ ="A",
                     INDICATOR = "POPCNT",
                     UNIT_MEASURE = "N",
                     UNIT_MULT = "",
                     DATASOURCE = "",
                     OBS_STATUS = "",
                     OBS_COMMENT = "",
                     CONF_STATUS = ""
                     
)

#Reorder the columns in the standard logical order
new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANISATION", "SEX", "AGE", "DISABILITY",  "EDUCATION", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT", "CONF_STATUS")
gender_education <- gender_education[, new_order]

write.csv(gender_education, "output/education/gender_education.csv", row.names = FALSE)

#Generate Gender by education Metadata file

gender_education_metadata <- sqlQuery(mydb, "select   country.countryID AS GEO_PICT,
                                                      gender.year AS TIME_PERIOD,
                                                      datasource.dsourceCode AS 'DATA_SOURCE.DATA_SOURCE_TITLE',
                                                      '~' AS URBANIZATION,
                                                      '~' AS SEX,
                                                      '~' AS AGE,
                                                      '~' AS DISABILITY,
                                                      '~' AS EDUCATION,
          											                      count(gender.rownames) AS mycount
                                            FROM gender
                                            INNER JOIN country ON gender.country = country.country
                                            INNER JOIN datasource ON gender.datasource = datasource.aSourceID
									                          WHERE gender.age_grp1 >0 AND gender.married_less18 = 1
									                          GROUP BY countryID, year, dSourceCode
                            ")
names(gender_education_metadata) <- c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE", "URBANIZATION", "SEX", "AGE", "DISABILITY", "EDUCATION")
gender_education_metadata <- gender_education_metadata[, c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE", "URBANIZATION", "SEX", "AGE", "DISABILITY", "EDUCATION")]

gender_education_metadata <- mutate(gender_education_metadata,
                                    STRUCTURE ="dataflow",
                                    STRUCTURE_ID = "SPC:DF_MARITAL_STATUS(1.0)",
                                    ACTION = "A",
                                    FREQ ="~",
                                    INDICATOR = "~",
                                    DATA_SOURCE.DATA_SOURCE_ORGANIZATION = "",
                                    DATA_SOURCE.DATA_SOURCE_LICENSE = "",
                                    DATA_SOURCE.DATA_SOURCE_DATE = "",
                                    DATA_SOURCE.DATA_SOURCE_LINK = "",
                                    DATA_SOURCE.DATA_SOURCE_COMMENT = "",
                                    DATA_PROCESSING = "",
                                    DATA_REVISION = "",
                                    DATA_COMMENT ="",
                                    CONF_STATUS = ""
)

new_order <- c("STRUCTURE",
               "STRUCTURE_ID",
               "ACTION",
               "FREQ", 
               "TIME_PERIOD", 
               "GEO_PICT", 
               "INDICATOR", 
               "SEX", 
               "AGE",
               "URBANIZATION",
               "DISABILITY",
               "EDUCATION",
               "DATA_SOURCE.DATA_SOURCE_ORGANIZATION",
               "DATA_SOURCE.DATA_SOURCE_TITLE",
               "DATA_SOURCE.DATA_SOURCE_LICENSE",
               "DATA_SOURCE.DATA_SOURCE_DATE",
               "DATA_SOURCE.DATA_SOURCE_LINK",
               "DATA_SOURCE.DATA_SOURCE_COMMENT",
               "DATA_PROCESSING",
               "DATA_REVISION",
               "DATA_COMMENT",
               "CONF_STATUS"
)
gender_education_metadata <- gender_education_metadata[, new_order]
write.csv(gender_education_metadata, "output/education/gender_education_metadata.csv", row.names = FALSE)


#### ********************** Gender by Household head *********************************** ####

# Extracting Cube data

gender_HHhead <- sqlQuery(mydb, "SELECT country.countryID,
                                     gender.year, 
                                     urbrur.urbrurCode, 
                                     sex.sexCode AS SEX, 
                                     disability.disabilityCode,
                                     isco.iscoCode,
                                     isic.isicCode,
                                     round(sum(gender.fweight), 0) AS OBS_VALUE
                              FROM gender
                              INNER JOIN country ON gender.country = country.country
                              INNER JOIN urbrur ON gender.rururb = urbrur.urbrurID
                              INNER JOIN sex ON gender.sex = sex.sexID
                              INNER JOIN disability ON gender.disabled = disability.disabilityID
                              INNER JOIN isco ON gender.job1_occ1 = isco.iscoID
                              INNER JOIN isic ON gender.job1_eco1 = isic.isicID
                              WHERE gender.age_grp1 > 1
                              GROUP BY CUBE (countryID, year, urbrurCode, sexCode, disabilityCode, iscoCode, isicCode)
                 ")


#Clear totals from constat fields
gender_HHhead <- gender_HHhead[!is.na(gender_HHhead$year), ]
gender_HHhead <- gender_HHhead[!is.na(gender_HHhead$countryID), ]
gender_HHhead[is.na(gender_HHhead)] <- "_T"

#Rename the columns to align with the template
names(gender_HHhead) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR", "OBS_VALUE")
gender_HHhead <- gender_HHhead[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR", "OBS_VALUE")]

#Insert the rest of the columns into the dataframe
gender_HHhead <- mutate(gender_HHhead,
                           FREQ ="A",
                           INDICATOR = "POPCNT",
                           UNIT_MEASURE = "N",
                           UNIT_MULT = "",
                           DATA_SOURCE = "",
                           OBS_STATUS = "",
                           OBS_COMMENT = "",
                           CONF_STATUS = ""
                           
)

new_order <- c("FREQ", 
               "TIME_PERIOD", 
               "GEO_PICT", 
               "INDICATOR", 
               "SEX", 
               "URBANIZATION", 
               "DISABILITY", 
               "OCCUPATION", 
               "ECONOMIC_SECTOR", 
               "OBS_VALUE", 
               "UNIT_MEASURE", 
               "UNIT_MULT", 
               "OBS_STATUS", 
               "DATA_SOURCE",  
               "OBS_COMMENT", 
               "CONF_STATUS")

gender_HHhead <- gender_HHhead[, new_order]

write.csv(gender_HHhead, "output/HHhead/gender_HHhead.csv", row.names = FALSE)

#Generating Gender by Head of household metadata file

gender_HHhead_metadata <- sqlQuery(mydb, "select  country.countryID,
                                                  gender.year,
		                                              datasource.dSourceCode,
                                                  count(gender.indid) as total
                                          FROM gender
                                          INNER JOIN country ON gender.country = country.country
                                          INNER JOIN datasource ON gender.datasource = datasource.aSourceID
                                          WHERE gender.age_grp1 >0 AND gender.HHhead_sex > 0
                                          GROUP BY countryID, year, dSourceCode
                                  ")
names(gender_HHhead_metadata) <- c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE")
gender_HHhead_metadata <- gender_HHhead_metadata[, c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE")]

gender_HHhead_metadata <- mutate(gender_HHhead_metadata,
                                    STRUCTURE ="dataflow",
                                    STRUCTURE_ID = "SPC:DF_MARITAL_STATUS(1.0)",
                                    ACTION = "A",
                                    FREQ ="~",
                                    INDICATOR = "~",
                                    URBANIZATION = "~",
                                    SEX = "~",
                                    DISABILITY ="~",
                                    OCCUPATION ="~",
                                    ECONOMIC_SECTOR = '~',
                                    DATA_SOURCE.DATA_SOURCE_ORGANIZATION = "",
                                    DATA_SOURCE.DATA_SOURCE_LICENSE = "",
                                    DATA_SOURCE.DATA_SOURCE_DATE = "",
                                    DATA_SOURCE.DATA_SOURCE_LINK = "",
                                    DATA_SOURCE.DATA_SOURCE_COMMENT = "",
                                    DATA_PROCESSING = "",
                                    DATA_REVISION = "",
                                    DATA_COMMENT ="",
                                    CONF_STATUS = ""
)

new_order <- c("STRUCTURE",
               "STRUCTURE_ID",
               "ACTION",
               "FREQ", 
               "TIME_PERIOD", 
               "GEO_PICT", 
               "INDICATOR", 
               "SEX", 
               "URBANIZATION",
               "DISABILITY",
               "OCCUPATION",
               "ECONOMIC_SECTOR",
               "DATA_SOURCE.DATA_SOURCE_ORGANIZATION",
               "DATA_SOURCE.DATA_SOURCE_TITLE",
               "DATA_SOURCE.DATA_SOURCE_LICENSE",
               "DATA_SOURCE.DATA_SOURCE_DATE",
               "DATA_SOURCE.DATA_SOURCE_LINK",
               "DATA_SOURCE.DATA_SOURCE_COMMENT",
               "DATA_PROCESSING",
               "DATA_REVISION",
               "DATA_COMMENT",
               "CONF_STATUS"
)
gender_HHhead_metadata <- gender_HHhead_metadata[, new_order]
write.csv(gender_HHhead_metadata, "output/HHhead/gender_HHhead_metadata.csv", row.names = FALSE)



















#Generate cube table for population by sex and isco from gender dataset

isco_isic <- sqlQuery(mydb, "SELECT * FROM gender WHERE age_grp1 > 0 AND job1_occ1 != 'NA'")
sqlSave(mydb, isco_isic, tablename = "isco_isic", append = FALSE)

gender_occupation <- sqlQuery(mydb, "select   country.countryID,
                                          		isco_isic.year,
                                          		urbrur.urbrurCode,
                                          		sex.sexCode,
                                          		ageGroup1.ageGroupCode,
                                          		isco.iscoCode, 
                                          		round(sum(isco_isic.fweight),0) as total 
                                      FROM isco_isic
                                      INNER JOIN country ON isco_isic.country = country.country
                                      INNER JOIN urbrur ON isco_isic.rururb = urbrur.urbrurID
                                      INNER JOIN sex ON isco_isic.sex = sex.sexID
                                      INNER JOIN ageGroup1 ON isco_isic.age_grp1 = ageGroup1.ageGroupID
                                      INNER JOIN isco ON isco_isic.job1_occ1 = isco.iscoID
                                      where isco_isic.age_grp1 > 0 
                                      group by CUBE(countryID, year, urbrurCode, sexCode, ageGroupCode, iscoCode)
                 ")

gender_occupation <- gender_occupation[!is.na(gender_occupation$year), ]
gender_occupation <- gender_occupation[!is.na(gender_occupation$countryID), ]
gender_occupation[is.na(gender_occupation)] <- "_T"

names(gender_occupation) <- c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "OCCUPATION", "OBS_VALUE")
gender_occupation <- gender_occupation[, c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "OCCUPATION", "OBS_VALUE")]

gender_occupation <- mutate(gender_occupation,
                           FREQ ="A",
                           INDICATOR = "ISCO",
                           UNIT_MEASURE = "UNIT",
                           UNIT_MULT = "",
                           DATASOURCE = "",
                           OBS_STATUS = "",
                           OBS_COMMENT = ""
                           
)

#Reorder the columns in the standard logical order
new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANISATION", "SEX", "AGE", "OCCUPATION", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT")
gender_occupation <- gender_occupation[, new_order]

write.csv(gender_occupation, "output/gender_occupation.csv", row.names = FALSE)

#Generating the Gender by Industry table from the gender dataset

gender_industry <- sqlQuery(mydb, "select   country.countryID,
                                            isco_isic.year,
                                            urbrur.urbrurCode,
                                            sex.sexCode,
                                            ageGroup1.ageGroupCode,
                                            isic.isicCode, 
                                            round(sum(isco_isic.fweight),0) as total 
                                    FROM isco_isic
                                    INNER JOIN country ON isco_isic.country = country.country
                                    INNER JOIN urbrur ON isco_isic.rururb = urbrur.urbrurID
                                    INNER JOIN sex ON isco_isic.sex = sex.sexID
                                    INNER JOIN ageGroup1 ON isco_isic.age_grp1 = ageGroup1.ageGroupID
                                    INNER JOIN isic ON isco_isic.job1_eco1 = isic.isicID
                                    where isco_isic.age_grp1 > 0 
                                    group by CUBE(countryID, year, urbrurCode, sexCode, ageGroupCode, isicCode)
                            ")

gender_industry <- gender_industry[!is.na(gender_industry$year), ]
gender_industry <- gender_industry[!is.na(gender_industry$countryID), ]
gender_industry[is.na(gender_industry)] <- "_T"

names(gender_industry) <- c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "ECONOMIC_SECTOR", "OBS_VALUE")
gender_industry <- gender_industry[, c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "ECONOMIC_SECTOR", "OBS_VALUE")]

gender_industry <- mutate(gender_industry,
                            FREQ ="A",
                            INDICATOR = "ISIC",
                            UNIT_MEASURE = "UNIT",
                            UNIT_MULT = "",
                            DATASOURCE = "",
                            OBS_STATUS = "",
                            OBS_COMMENT = ""
                            
)

#Reorder the columns in the standard logical order
new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANISATION", "SEX", "AGE", "ECONOMIC_SECTOR", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT")
gender_industry <- gender_industry[, new_order]

write.csv(gender_industry, "output/gender_economic_sector.csv", row.names = FALSE)


#### **************Gender gender by Getting married early (18 or less years)****************** #####

gender_married18 <- sqlQuery(mydb, "select  country.countryID,
                                            gender.year,
                                            urbrur.urbrurCode,
                                            'F' AS sexCode,
                                            'Y15T24' AS AGE,
                                            disability.disabilityCode,
                                            'MA' AS MaritalStatus,
                                            round(sum(gender.fweight),0) as total
                                    FROM gender
                                    INNER JOIN country ON gender.country = country.country
                                    INNER JOIN urbrur ON gender.rururb = urbrur.urbrurID
                                    INNER JOIN disability ON gender.disabled = disability.disabilityID
                                    WHERE gender.age_grp1 >0 AND married_less18 = 1
									                  GROUP BY CUBE(countryID, year, urbrurCode, disabilityCode)
                            ")

gender_married18 <- gender_married18[!is.na(gender_married18$year), ]
gender_married18 <- gender_married18[!is.na(gender_married18$countryID), ]
gender_married18[is.na(gender_married18)] <- "_T"

names(gender_married18) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "MARITAL_STATUS", "OBS_VALUE")
gender_married18 <- gender_married18[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "MARITAL_STATUS", "OBS_VALUE")]

gender_married18 <- mutate(gender_married18,
                          FREQ ="A",
                          INDICATOR = "POPCNT",
                          UNIT_MEASURE = "N",
                          UNIT_MULT = "",
                          DATA_SOURCE = "",
                          OBS_STATUS = "",
                          OBS_COMMENT = "",
                          CONF_STATUS = ""
                          
)

new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "SEX", "AGE", "URBANIZATION", "DISABILITY", "MARITAL_STATUS", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "DATA_SOURCE",  "OBS_COMMENT", "CONF_STATUS")
gender_married18 <- gender_married18[, new_order]




write.csv(gender_married18, "output/married18/gender_married18.csv", row.names = FALSE)


#Generate Metadata file

gender_married18_metadata <- sqlQuery(mydb, "select   country.countryID,
                                                      gender.year,
                                                      datasource.dsourceCode AS 'DATA_SOURCE.DATA_SOURCE_TITLE',
                                                      '~' AS URBANIZATION,
                                                      '~' AS sexCode,
                                                      '~' AS AGE,
                                                      '~' AS disabilityCode,
                                                      '~' AS MaritalStatus,
          											                      count(gender.rownames) AS mycount
                                            FROM gender
                                            INNER JOIN country ON gender.country = country.country
                                            INNER JOIN datasource ON gender.datasource = datasource.aSourceID
									                          WHERE gender.age_grp1 >0 AND gender.married_less18 = 1
									                          GROUP BY countryID, year, dSourceCode
                            ")

names(gender_married18_metadata) <- c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE", "URBANIZATION", "SEX", "AGE", "DISABILITY")
gender_married18_metadata <- gender_married18_metadata[, c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE", "URBANIZATION", "SEX", "AGE", "DISABILITY")]

gender_married18_metadata <- mutate(gender_married18_metadata,
                          STRUCTURE ="dataflow",
                          STRUCTURE_ID = "SPC:DF_MARITAL_STATUS(1.0)",
                          ACTION = "A",
                          FREQ ="~",
                          INDICATOR = "~",
                          DATA_SOURCE.DATA_SOURCE_ORGANIZATION = "",
                          DATA_SOURCE.DATA_SOURCE_LICENSE = "",
                          DATA_SOURCE.DATA_SOURCE_DATE = "",
                          DATA_SOURCE.DATA_SOURCE_LINK = "",
                          DATA_SOURCE.DATA_SOURCE_COMMENT = "",
                          DATA_PROCESSING = "",
                          DATA_REVISION = "",
                          DATA_COMMENT ="",
                          CONF_STATUS = ""
)

new_order <- c("STRUCTURE",
               "STRUCTURE_ID",
               "ACTION",
               "FREQ", 
               "TIME_PERIOD", 
               "GEO_PICT", 
               "INDICATOR", 
               "SEX", 
               "AGE",
               "URBANIZATION",
               "DISABILITY",
               "DATA_SOURCE.DATA_SOURCE_ORGANIZATION",
               "DATA_SOURCE.DATA_SOURCE_TITLE",
               "DATA_SOURCE.DATA_SOURCE_LICENSE",
               "DATA_SOURCE.DATA_SOURCE_DATE",
               "DATA_SOURCE.DATA_SOURCE_LINK",
               "DATA_SOURCE.DATA_SOURCE_COMMENT",
               "DATA_PROCESSING",
               "DATA_REVISION",
               "DATA_COMMENT",
               "CONF_STATUS"
               )
gender_married18_metadata <- gender_married18_metadata[, new_order]


write.csv(gender_married18_metadata, "output/married18/gender_married18_metadata.csv", row.names = FALSE)

####********************* Generate dataset for work part-time by gender ***************####

gender_work_part_time <- sqlQuery(mydb, "select  country.countryID,
                                            gender.year,
                                            urbrur.urbrurCode,
                                            sex.sexCode,
                                            ageGroup1.ageGroupCode,
                                            disability.disabilityCode,
                                            isco.iscoCode,
                                            isic.isicCode,
                                            round(sum(gender.fweight),0) as total
                                    FROM gender
                                    INNER JOIN country ON gender.country = country.country
                                    INNER JOIN urbrur ON gender.rururb = urbrur.urbrurID
                                    INNER JOIN sex ON gender.sex = sex.sexID
                                    INNER JOIN ageGroup1 ON gender.age_grp1 = ageGroup1.ageGroupID
                                    INNER JOIN disability ON gender.disabled = disability.disabilityID
                                    INNER JOIN isco ON gender.job1_occ1 = isco.iscoID
                                    INNER JOIN isic ON gender.job1_eco1 = isic.isicID
                                    WHERE gender.age_grp1 >0 AND gender.part_time = 1
									                  GROUP BY CUBE(countryID, year, urbrurCode, sexCode, ageGroupCode, disabilityCode, iscoCode, isicCode)
                            ")

gender_work_part_time <- gender_work_part_time[!is.na(gender_work_part_time$year), ]
gender_work_part_time <- gender_work_part_time[!is.na(gender_work_part_time$countryID), ]
gender_work_part_time[is.na(gender_work_part_time)] <- "_T"

names(gender_work_part_time) <- c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR",  "OBS_VALUE")
gender_work_part_time <- gender_work_part_time[, c("GEO_PICT", "TIME_PERIOD", "URBANISATION", "SEX", "AGE", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR", "OBS_VALUE")]

gender_work_part_time <- mutate(gender_work_part_time,
                           FREQ ="A",
                           INDICATOR = "WPT",
                           UNIT_MEASURE = "UNIT",
                           UNIT_MULT = "",
                           DATASOURCE = "",
                           OBS_STATUS = "",
                           OBS_COMMENT = "",
                           CONF_STATUS = ""
)

new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANISATION", "SEX", "AGE", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR",  "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT", "CONF_STATUS")
gender_work_part_time <- gender_work_part_time[, new_order]

write.csv(gender_work_part_time, "output/gender_work_part_time.csv", row.names = FALSE)

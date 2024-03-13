
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

names(gender_occupation) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "OCCUPATION", "OBS_VALUE")
gender_occupation <- gender_occupation[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "OCCUPATION", "OBS_VALUE")]

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
new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANIZATION", "SEX", "AGE", "OCCUPATION", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT")
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

names(gender_industry) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "ECONOMIC_SECTOR", "OBS_VALUE")
gender_industry <- gender_industry[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "ECONOMIC_SECTOR", "OBS_VALUE")]

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
new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANIZATION", "SEX", "AGE", "ECONOMIC_SECTOR", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT")
gender_industry <- gender_industry[, new_order]

write.csv(gender_industry, "output/gender_economic_sector.csv", row.names = FALSE)

#Generate Metadata file




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

names(gender_work_part_time) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR",  "OBS_VALUE")
gender_work_part_time <- gender_work_part_time[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR", "OBS_VALUE")]

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

new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANIZATION", "SEX", "AGE", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR",  "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "DATASOURCE", "OBS_STATUS", "OBS_COMMENT", "CONF_STATUS")
gender_work_part_time <- gender_work_part_time[, new_order]

write.csv(gender_work_part_time, "output/gender_work_part_time.csv", row.names = FALSE)



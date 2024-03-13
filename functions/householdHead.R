#### ********************** Gender by Household head *********************************** ####

# Extracting Cube data

hhHead_data <- function(hhHead_d){
  gender_HHhead <- sqlQuery(mydb, "SELECT country.countryID AS GEO_PICT,
                                     year AS TIME_PERIOD, 
                                     sex.sexCode AS SEX,
                                     ageGroup1.ageGroupCode AS AGE,
                                     hh_type AS HHCOMP,
                                     urbrur.urbrurCode AS URBANIZATION,
                                     disability.disabilityCode AS DISABILITY,
                                     isco.iscoCode AS OCCUPATION,
                                     isic.isicCode AS ECONOMIC_SECTOR,
                                     round(sum(fweight), 0) AS OBS_VALUE,
                                     COUNT(indid) AS totCount
                              FROM gender
                              INNER JOIN country ON gender.country = country.country
                              INNER JOIN sex ON gender.sex = sex.sexID
                              INNER JOIN ageGroup1 ON gender.age_grp1 = ageGroup1.ageGroupID
                              INNER JOIN urbrur ON gender.rururb = urbrur.urbrurID
                              INNER JOIN disability ON gender.disabled = disability.disabilityID
                              INNER JOIN isco ON gender.job1_occ1 = isco.iscoID
                              INNER JOIN isic ON gender.job1_eco1 = isic.isicID
                              WHERE gender.datasource = 1 AND gender.HHhead_sex > 0 AND age_grp1 >0
                              GROUP BY CUBE (country.countryID, year, urbrur.urbrurCode, sex.sexCode, ageGroup1.ageGroupCode, disability.disabilityCode, hh_type, isco.iscoCode, isic.isicCode)
                 ")
  
  gender_HHhead <- gender_HHhead[!is.na(gender_HHhead$TIME_PERIOD), ]
  gender_HHhead <- gender_HHhead[!is.na(gender_HHhead$GEO_PICT), ]
  gender_HHhead[is.na(gender_HHhead)] <- "_T"
  
  #Apply confidentiality to the dataframe
  gender_HHhead$CONF_STATUS[gender_HHhead$totCount < conf_threshold] <- "C"
  gender_HHhead$OBS_COMMENT[gender_HHhead$totCount < conf_threshold] <- "n<3"
  gender_HHhead$OBS_VALUE[gender_HHhead$totCount < conf_threshold] <- ""
  
  #Remove all NA
  gender_HHhead[is.na(gender_HHhead)] <- ""
  gender_HHhead <- subset(gender_HHhead, select = -totCount)
  
  #Insert the rest of the columns into the dataframe
  gender_HHhead <- mutate(gender_HHhead,
                          FREQ ="A",
                          INDICATOR = "HHCNT",
                          UNIT_MEASURE = "N",
                          UNIT_MULT = "",
                          DATA_SOURCE = "",
                          OBS_STATUS = ""
  )
  
  new_order <- c("FREQ", 
                 "TIME_PERIOD", 
                 "GEO_PICT", 
                 "INDICATOR", 
                 "SEX",
                 "AGE",
                 "URBANIZATION",
                 "DISABILITY",
                 "OCCUPATION", 
                 "ECONOMIC_SECTOR", 
                 "HHCOMP",
                 "OBS_VALUE", 
                 "UNIT_MEASURE", 
                 "UNIT_MULT", 
                 "OBS_STATUS", 
                 "DATA_SOURCE",  
                 "OBS_COMMENT", 
                 "CONF_STATUS")
  
  hhHead_d <- gender_HHhead[, new_order]
  return(hhHead_d)
  
}

#Generating Gender by Head of household metadata file
hhHead_meta <-function(hhHead_m) {
gender_HHhead_metadata <- sqlQuery(mydb, "select  country.countryID,
                                                  gender.year,
		                                              datasource.dSourceCode,
                                                  count(gender.indid) as total
                                          FROM gender
                                          INNER JOIN country ON gender.country = country.country
                                          INNER JOIN datasource ON gender.datasource = datasource.aSourceID
                                          WHERE gender.age_grp1 >0 AND gender.HHhead_sex > 0 AND gender.datasource = 1
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
hhHead_m <- gender_HHhead_metadata[, new_order]
return(hhHead_m)

}

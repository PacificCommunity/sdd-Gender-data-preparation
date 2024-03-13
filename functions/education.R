#### *********************** Gender by Education Attainment *******************************####
#Function to generate cube table for population by sex and education attainment from gender dataset

education_data <- function(edu_attain){
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
                              WHERE gender.age_grp1 > 1 AND gender.datasource = 2
                              GROUP BY CUBE (countryID, year, urbrurCode, sexCode, ageGroupCode, disabilityCode, attainCode)
                 ")
  
  gender_education <- gender_education[!is.na(gender_education$year), ]
  gender_education <- gender_education[!is.na(gender_education$countryID), ]
  gender_education[is.na(gender_education)] <- "_T"
  
  #Apply confidentiality to the dataframe
  gender_education$CONF_STATUS[gender_education$OBS_VALUE < conf_threshold] <- "C"
  gender_education$OBS_COMMENT[gender_education$OBS_VALUE < conf_threshold] <- "n<3"
  gender_education$OBS_VALUE[gender_education$OBS_VALUE < conf_threshold] <- ""
  
  #Remove all NA
  gender_education[is.na(gender_education)] <- ""
  
  names(gender_education) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "EDUCATION", "OBS_VALUE", "CONF_STATUS", "OBS_COMMENT")
  gender_education <- gender_education[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "EDUCATION", "OBS_VALUE", "CONF_STATUS", "OBS_COMMENT")]
  
  gender_education <- mutate(gender_education,
                             FREQ ="A",
                             INDICATOR = "POPCNT",
                             UNIT_MEASURE = "N",
                             UNIT_MULT = "",
                             DATA_SOURCE = "",
                             OBS_STATUS = ""
  )
  
  #Reorder the columns in the standard logical order
  new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "SEX", "AGE", "URBANIZATION", "DISABILITY",  "EDUCATION", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "DATA_SOURCE", "OBS_COMMENT", "CONF_STATUS")
  edu_attain <- gender_education[, new_order]
  return(edu_attain)
  
}
  
  
  #Function to generate Gender by education Metadata file
  
  education_meta <- function(edumeta){
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
									                          WHERE gender.age_grp1 >1 AND gender.datasource = 2
									                          GROUP BY countryID, year, dSourceCode
                            ")
    names(gender_education_metadata) <- c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE", "URBANIZATION", "SEX", "AGE", "DISABILITY", "EDUCATION")
    gender_education_metadata <- gender_education_metadata[, c("GEO_PICT", "TIME_PERIOD", "DATA_SOURCE.DATA_SOURCE_TITLE", "URBANIZATION", "SEX", "AGE", "DISABILITY", "EDUCATION")]
    
    gender_education_metadata <- mutate(gender_education_metadata,
                                        STRUCTURE ="dataflow",
                                        STRUCTURE_ID = "SPC:DF_EDUCATION(1.0)",
                                        ACTION = "A",
                                        FREQ ="A",
                                        INDICATOR = "~",
                                        DATA_SOURCE.DATA_SOURCE_ORGANIZATION = "",
                                        DATA_SOURCE.DATA_SOURCE_LICENSE = "",
                                        DATA_SOURCE.DATA_SOURCE_DATE = "",
                                        DATA_SOURCE.DATA_SOURCE_LINK = "",
                                        DATA_SOURCE.DATA_SOURCE_COMMENT = "",
                                        DATA_PROCESSING = "",
                                        DATA_REVISION = "",
                                        DATA_COMMENT =""
                                        
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
                   "DATA_COMMENT"
    )
    edumeta <- gender_education_metadata[, new_order]  
    return(edumeta)
  }  

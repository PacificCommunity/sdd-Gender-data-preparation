#### **************Gender gender by Getting married early (18 or less years)****************** #####
#Generate the data cube file
married18_data <- function(mar18_d){
  gender_married18 <- sqlQuery(mydb, "select  country.countryID,
                                            gender.year,
                                            urbrur.urbrurCode,
                                            'F' AS sexCode,
                                            'Y15T24' AS AGE,
                                            disability.disabilityCode,
                                            'MA' AS MaritalStatus,
                                            round(sum(gender.fweight),0) as OBS_VALUE
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
  
  #Apply confidentiality to the dataframe
  gender_married18$CONF_STATUS[gender_married18$OBS_VALUE < conf_threshold] <- "C"
  gender_married18$OBS_COMMENT[gender_married18$OBS_VALUE < conf_threshold] <- "n<3"
  gender_married18$OBS_VALUE[gender_married18$OBS_VALUE < conf_threshold] <- ""
  
  #Remove all NA
  gender_married18[is.na(gender_married18)] <- ""
  
  names(gender_married18) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "MARITAL_STATUS", "OBS_VALUE", "CONF_STATUS", "OBS_COMMENT")
  gender_married18 <- gender_married18[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "SEX", "AGE", "DISABILITY", "MARITAL_STATUS", "OBS_VALUE", "CONF_STATUS", "OBS_COMMENT")]
  
  gender_married18 <- mutate(gender_married18,
                             FREQ ="A",
                             INDICATOR = "POPCNT",
                             UNIT_MEASURE = "N",
                             UNIT_MULT = "",
                             DATA_SOURCE = "",
                             OBS_STATUS = ""
                             
                             
  )
  
  new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "SEX", "AGE", "URBANIZATION", "DISABILITY", "MARITAL_STATUS", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "DATA_SOURCE",  "OBS_COMMENT", "CONF_STATUS")
  mar18_d <- gender_married18[, new_order]
  return(mar18_d)
  
}


#Creating Married18 Metadata file

married18_meta <- function(mar18_m){

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
                                      FREQ ="A",
                                      INDICATOR = "~",
                                      MARITAL_STATUS = "~",
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
                 "MARITAL_STATUS",
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
  mar18_m <- gender_married18_metadata[, new_order]
  return(mar18_m)
  
}





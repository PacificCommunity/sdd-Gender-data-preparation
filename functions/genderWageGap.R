#### ************************ Generate gender wage gap data file ************************ ####
wGap_d<- function(wageGap_data) {
  #Generate wage gap data
  wGap <- sqlQuery(mydb, "SELECT country.countryID,
                               gender.year,
                               urbrur.urbrurCode,
                               disability.disabilityCode,
                               isco.iscoCode,
                               isic.isicCode,
                               COUNT(indid) AS totCount,
                               AVG(wagegap) AS OBS_VALUE
                        FROM gender
                        INNER JOIN country ON gender.country = country.country
                        INNER JOIN urbrur ON gender.rururb = urbrur.urbrurID
                        INNER JOIN disability ON gender.disabled = disability.disabilityID
                        INNER JOIN isco ON gender.job1_occ1 = isco.iscoID
                        INNER JOIN isic ON gender.job1_eco1 = isic.isicID
                        WHERE datasource = 1
                        GROUP BY CUBE(countryID, year, urbrurCode, disabilityCode, iscoCode, isicCode)
                 
                 ")
  
  #Drop records where dimensions contain NA
  wGap <- wGap[!is.na(wGap$year), ]
  wGap <- wGap[!is.na(wGap$countryID), ]
  wGap <- wGap[!is.na(wGap$OBS_VALUE), ]
  wGap[is.na(wGap)] <- "_T"
  
  #Apply confidentiality to the dataframe
  wGap$CONF_STATUS[wGap$totCount < conf_threshold] <- "C"
  wGap$OBS_COMMENT[wGap$totCount < conf_threshold] <- "n<3"
  wGap$OBS_VALUE[wGap$totCount < conf_threshold] <- ""
  
  wGap <- subset(wGap, select = -totCount)
  
  
  #Remove all NA
  wGap[is.na(wGap)] <- ""
  
  names(wGap) <- c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR", "OBS_VALUE", "CONF_STATUS", "OBS_COMMENT")
  wGap <- wGap[, c("GEO_PICT", "TIME_PERIOD", "URBANIZATION", "DISABILITY", "OCCUPATION", "ECONOMIC_SECTOR", "OBS_VALUE", "CONF_STATUS", "OBS_COMMENT")]
  
  #Add additional columns to the dataframe
  wGap <- mutate(wGap,
                 FREQ ="A",
                 INDICATOR = "GWG",
                 UNIT_MEASURE = "RATIO_POP",
                 UNIT_MULT = "",
                 DATA_SOURCE = "",
                 OBS_STATUS = ""
  )
  
  #Reorder the columns in the standard logical order
  new_order <- c("FREQ", "TIME_PERIOD", "GEO_PICT", "INDICATOR", "URBANIZATION", "DISABILITY",  "OCCUPATION", "ECONOMIC_SECTOR", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "DATA_SOURCE", "OBS_COMMENT", "CONF_STATUS")
  wageGap_data <- wGap[, new_order]
  
  return(wageGap_data)
  
}


#### ***************************** Generating Metadata *************************** ####

wGap_m <- function(wageGap_meta){
  #Generate wage gap metadata
  wageGap_meta <- sqlQuery(mydb, "SELECT 'dataflow' AS STRUCTURE,
                                'SPC:DF_GWG(1.0)' AS STRUCTURE_ID,
                                'A' AS ACTION,
                                'A' AS FREQ,
                               gender.year AS TIME_PERIOD,
                               country.countryID AS GEO_PICT,
                               '~' AS INDICATOR,
                               '~' AS URBANIZATION,
                               '~' AS DISABILITY,
                               '~' AS OCCUPATION,
                               '~' AS ECONOMIC_SECTOR,
                               '~' AS [DATA_SOURCE.DATA_SOURCE_ORGANIZATION],
                               datasource.dsourceCode AS [DATA_SOURCE.DATA_SOURCE_TITLE],
                               '~' AS [DATA_SOURCE.DATA_SOURCE_LICENSE],
                               '' AS [DATA_SOURCE.DATA_SOURCE_DATE],
                               '' AS [DATA_SOURCE.DATA_SOURCE_LINK],
                               '' AS [DATA_SOURCE.DATA_SOURCE_COMMENT],
                               '' AS [DATA_PROCESSING],
                               '' AS DATA_REVISION,
                               '' AS DATA_COMMENT,
                               '' AS CONF_STATUS,
                               COUNT(indid) AS totCount
                        FROM gender
                        INNER JOIN country ON gender.country = country.country
                        INNER JOIN datasource ON gender.datasource = datasource.asourceID
                        WHERE datasource = 1
                        GROUP BY gender.year, country.countryId, datasource.dSourceCode
                 
                 ")
  
  return(wageGap_meta)
  
}

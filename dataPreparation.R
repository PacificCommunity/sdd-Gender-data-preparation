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
  attainCode = c("0", "1", "2", "3", "4", "5-8", "9"),
  attainDesc = c("Less than primary", "Minimum primary", "Minimum lower secondary", "Minimum upper secondary", "Minimum post-secondary", "Minimum tertiary", "Not elsewhere classified")
)

hhHeadSex <- data.frame(
  hheadSexID = c(1, 2),
  hheadSexCode = c("MHEAD","FHEAD"),
  hheadSexDesc = c("Male HH head", "Female HH head")
)

disability <- data.frame(
  disabilityID = c(0, 1),
  disabilityCode = c("PWD", "PD"),
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

#Replace NA with 999 to include the not stated and not application in our cube data
gender$disabled[is.na(gender$disabled)] <- 999
gender$job1_occ1[is.na(gender$job1_occ1)] <- 999
gender$job1_eco1[is.na(gender$job1_eco1)] <- 999

sqlSave(mydb, gender, tablename = "gender", append = FALSE)
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

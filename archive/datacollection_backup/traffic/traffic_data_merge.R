
rm(list = ls()) # Clear  memory.
cat("\014")  # Clear console.

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#
# Read Astra Traffic Data from Excel Files and Create Time Series
# Philipp Kronenberg
# Last Update: 04.02.2020
#
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

# Notes -------------------------------------------------------------------

# Link to the data:
# https://www.astra.admin.ch/astra/de/home/dokumentation/daten-informationsprodukte/verkehrsdaten/daten-publikationen/automatische-strassenverkehrszaehlung/neues-coronavirus_verkehrsentwicklung-auf-dem-nationalstrassennetz.html
# In the file Datenauswertungen_maerz_vgl_2019.xlsx the sheet for RENENS is missing.
# In the file Datenauswertungen_maerz_vgl_2019.xlsx the days 30.03.2020 and 31.03.2020 were missing.
# These missing dates were included in the excel files for having a full time series in the code. The missing values are NA.
# Last update of the data from the excel files: 03.02.2022
#
# Note: The excel files downloaded need to be adjusted by hand such that no merged cell are anymore there in the used columns and the text in those before merged cells need to be deleted!

 
# starting date of the different locations:
# UMF BERN OST: 2005
# SAN BERNADINO: 2005
# BASEL: 2009
# CHIASSO: 2006
# SIMPLON: 2005
# GOTTHARDTUNNEL: 2005
# COPPET 01.02.2005
# WUERENLOS: 2011
# RENENS: 2008
# AESCHERTUNNEL: 2010
 
setwd("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data")
#setwd("C:/Users/kphilipp/GitHub/wai_ind/archive/datacollection_backup/traffic/astra_raw_data")
# packages ----------------------------------------------------------------

library(readxl)
library(openxlsx)
library(zoo)
library(forecast)

# Read in Data from Excel files -------------------------------------------

zaehlstellen <- c("UMF. BERN OST","SAN BERNARDINO","BASEL"," CHIASSO","SIMPLON","GOTTHARDTUNNEL","COPPET","WUERENLOS","RENENS","AESCHERTUNNEL")

total <- list()

for (x in zaehlstellen){
  

traffic_2005_2019 <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/2020-06-17 Jahresganglinien 2005 bis 2019 ausgewaehlter Zaehlstellen.xlsx",
                                     sheet = x,                                                  
                                rows = 9:5487,
                                cols = 1:4,
                                detectDates = TRUE)

traffic_2020_Jan <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/2020-04-17 Datenauswertungen Januar 2020 mit Vgl. zu 2019.xlsx",
                                    sheet = x,
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)
                                   

traffic_2020_Feb <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/2020-04-17 Datenauswertungen Februar 2020 mit Vgl. zu 2019.xlsx",
                               sheet = x,                                                  
                              rows = 10:39,
                              cols = 6:9,
                              detectDates = TRUE)
# Include the month March 2020 by hand for location Renens since this sheet is missing in the excel file
if (x == "RENENS"){
  traffic_2020_Mar <- matrix(nrow = 31, ncol = 4)
    colnames(traffic_2020_Mar) <- colnames(traffic_2005_2019)
}else{
traffic_2020_Mar <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/Datenauswertungen_maerz_vgl_2019.xlsx",
                               sheet = x,                                                  
                              rows = 4:35,
                              cols = 6:9,
                              detectDates = TRUE)
}
traffic_2020_Apr <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/Datenauswertungen_April_2020_vgl_2019.xlsx",
                               sheet = x,                                                  
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Mai <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/Datenauswertungen_Mai_2020_vgl_2019.xlsx",
                               sheet = x,                                                  
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Jun <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/juni_19_20.xlsx",
                               sheet = x,                                                  
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Jul <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/juli_19_20.xlsx",
                              sheet = x,                                                  
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Aug <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/august_19_20.xlsx",
                              sheet = x,                                                  
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Sep <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/september_19_20.xlsx",
                              sheet = x,                                                  
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Okt <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/oktober_19_20.xlsx",
                              sheet = x,                                                  
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Nov <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/november_19_20.xlsx",
                              sheet = x,                                                  
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2020_Dez <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/dezember_19_20.xlsx",
                              sheet = x,                                                  
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2021_Jan <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/januar_19_21.xlsx",
                              sheet = x,                                                  
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)
# Jan 2021 has for San Bernardino no classes only total, starting with 11 January

traffic_2021_Feb <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/februar_19_21.xlsx",
                              sheet = x,                                                  
                              rows = 10:38,
                              cols = 6:9,
                              detectDates = TRUE)
# Simplon was closed from 1 Feb until 3 Feb

traffic_2021_Mar <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/maerz_19_21.xlsx",
                              sheet = x,                                                  
                              rows = 10:38,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2021_Apr <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/april_19_21.xlsx",
                              sheet = x,                                                  
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2021_Mai <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/mai_19_21.xlsx",
                              sheet = x,                                                  
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2021_Jun <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/juni_19_21.xlsx",
                              sheet = x,                                                  
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)

traffic_2021_Jul <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/juli_19_21.xlsx",
                              sheet = x,
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)
# Coppet is missing for classes from 19-31 Juli, Umf.Bern Ost is missing


traffic_2021_Aug <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/august_19_21.xlsx",
                              sheet = x,
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)
# Umf Bern Ost missing, Coppet missing classes, Nyon missing

traffic_2021_Sep <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/september_19_21.xlsx",
                              sheet = x,
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)
# Umf Bern Ost missing, coppet missing, Nyon missing

traffic_2021_Okt <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/oktober_19_21.xlsx",
                              sheet = x,
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)
# Umf Bern Ost missing, coppet missing

traffic_2021_Nov <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/November_19_21.xlsx",
                              sheet = x,
                              rows = 10:40,
                              cols = 6:9,
                              detectDates = TRUE)
# Umf Bern Ost missing, Coppet missing

traffic_2021_Dez <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/dezember_19_21.xlsx",
                              sheet = x,
                              rows = 10:41,
                              cols = 6:9,
                              detectDates = TRUE)
# Umf Bern Ost missing, Coppet missing

traffic_2022_Jan <- read.xlsx("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/astra_raw_data/januar_19_22.xlsx",
                              sheet = x,
                              rows = 10:33,
                              cols = 6:9,
                              detectDates = TRUE)
# Umf Bern Ost missing, Coppet missing

colnames(traffic_2020_Jan)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Feb)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Mar)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Apr)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Mai)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Jun)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Jul)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Aug)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Sep)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Okt)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Nov)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2020_Dez)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Jan)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Feb)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Mar)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Apr)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Mai)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Jun)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Jul)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Aug)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Sep)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Okt)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Nov)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2021_Dez)[1] <- colnames(traffic_2005_2019)[1]
colnames(traffic_2022_Jan)[1] <- colnames(traffic_2005_2019)[1]

total[[x]] <- rbind(traffic_2005_2019, traffic_2020_Jan, traffic_2020_Feb, traffic_2020_Mar, traffic_2020_Apr, 
                    traffic_2020_Mai, traffic_2020_Jun, traffic_2020_Jul, traffic_2020_Aug, traffic_2020_Sep, 
                    traffic_2020_Okt, traffic_2020_Nov, traffic_2020_Dez, traffic_2021_Jan, traffic_2021_Feb, 
                    traffic_2021_Mar, traffic_2021_Apr, traffic_2021_Mai, traffic_2021_Jun, traffic_2021_Jul, 
                    traffic_2021_Aug, traffic_2021_Sep, traffic_2021_Okt, traffic_2021_Nov, traffic_2021_Dez, traffic_2022_Jan) 
}

# Include dates for March 2020 since there was no sheet of this month in the excel file for Renens
total$RENENS[5539:5569,1] <- seq(as.Date("2020-03-01"), as.Date("2020-03-31"), by="days")


# create zoo object and interpolate to get rid of NAs
out_traffic <- list()
category <- c("PW+.Busse/Car","LW","Gesamtergebnis")
for (y in 1:(length(category))){
  out_traffic[[y]] <- lapply(names(total), function(x){
    na.approx(
      zoo(x = total[[x]][,y+1], order.by = as.Date(total[[x]][,1]))
    ,na.rm = FALSE
    #,rule = 2
    )
  })  
  names(out_traffic[[y]]) <- names(total)
}
names(out_traffic) <- category

# create output
save(out_traffic, total, file = "L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/traffic/traffic.RData")


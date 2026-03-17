rm(list = ls()) # Clear  memory.
cat("\014")  # Clear console.
Sys.setlocale("LC_TIME", "English") 

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#
# Read and Reorganize Historical Swissgrid Data
# Furkan Oguz, Heiner Mikosch
# Started Date: 15.06.2020
# Last Update: 04.02.2020 by Philipp Kronenberg
#
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 


# NOTES -------------------------------------------------------------------

# New Files can be found on the website of Swissgrid: https://www.swissgrid.ch/de/home/operation/grid-data/generation.html#downloads
# The files are save as xls format. However, the download files are xlsx. Thus, first save the xlsx file as a xls file!
# In the excel file of 2021 we needed to add a space between the date and the time such that the date column is read correctly as date format of excel in R. Check this for later excel inputs!
# There is one datacolumn missing in the actual dataset (2018-today): canton of Fribourg. 
# Assmuning that there is France confounded with Fribourg.


# PRELIMINARIES -----------------------------------------------------------

# Installing and loading necessary packages for approach
# install.packages("readxl", "data.tree","datetimeutils")
library(readxl)
# library(plyr)
# library(data.tree)
# library(data.table)
# library(dplyr)
# library(lubridate)
library(reshape2)
# library(tidyverse)
# library(tidyr)
library(datetimeutils)
library(zoo)

path <- "L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/swissgrid/swissgrid_raw_data/"

# READ DATA 2009-14 AND REORGANIZE ------------------------------------------------

# Note: There is only national data available and no cantonal data.
# Note: 2012 and 2014: There are 4 datapoints more given for each variable. 

swissgridlist <- {}

yrlist <- c(2009:2014)
for (yr in 1:length(yrlist)){

  swissgrid <- read_excel(paste0(path,"EnergieUebersichtCH-",yrlist[yr],".xls",sep=""), sheet="Zeitreihen0h15", col_types = "guess", trim_ws = TRUE)
  swissgrid <- swissgrid[-c(1), ]
  if (yrlist[yr] == 2014){
    swissgrid <- subset(swissgrid, select=-c(5, 6, 7, 8, 9, 10, 19, 20, 21, 22, 23, 24, 25))}
  else {
    swissgrid <- subset(swissgrid, select=-c(5, 6, 7, 8, 9, 10, 19, 20, 21))
  }
  colnames(swissgrid)
  names(swissgrid)[1] <- "time"
  names(swissgrid)[2] <- "out*ch"
  names(swissgrid)[3] <- "in*ch"
  names(swissgrid)[4] <- "out_incl_losses*ch"
  names(swissgrid)[5] <- "out*at"
  names(swissgrid)[6] <- "in*at"
  names(swissgrid)[7] <- "out*de"
  names(swissgrid)[8] <- "in*de"
  names(swissgrid)[9] <- "out*fr"
  names(swissgrid)[10] <- "in*fr"
  names(swissgrid)[11] <- "out*it"
  names(swissgrid)[12] <- "in*it"

  # To separate the value from the other variables region and direction
  swissgrid <- melt(swissgrid, id=(c("time")))
  names(swissgrid)[2] <- "variable"
  swissgrid$variable <- as.character(swissgrid$variable)
  swissgrid$value <- as.numeric(swissgrid$value)
  trimws(swissgrid$variable)

  # To split variable into region and direction and combine it again with swissgrid
  variable <- swissgrid$variable
  variable<- strsplit(swissgrid$variable, "[*]")
  variable <- data.frame(matrix(unlist(variable), nrow=length(variable), byrow=T))
  names(variable)[1] <- "direction"
  names(variable)[2] <- "region"
  swissgrid <- cbind(swissgrid, variable)
  swissgrid <- subset(swissgrid, select=-c(2))

  # Reordering dataframe.
  swissgrid <- swissgrid[c("time", "direction", "region", "value")]

  # Modify factor variables into strings and time variable into numeric
  swissgrid$direction <- as.character(swissgrid$direction)
  swissgrid$region <- as.character(swissgrid$region)
  swissgrid$time <- as.numeric(swissgrid$time)

  # Converting the numeric excel datetime into R format
  swissgrid$time <- convert_date(swissgrid$time, type = "Excel", fraction = TRUE)

  swissgridlist[[yr]] <- swissgrid

}


# READ DATA 2015-19 AND REORGANIZE ----------------------------------------

# Note: There is national data and cantonal data available.

yrlist <- c(2015:2021)
for (yr in 1:length(yrlist)){

  swissgrid <- read_excel(paste0(path,"EnergieUebersichtCH-",yrlist[yr],".xls",sep=""), sheet="Zeitreihen0h15", col_types = "guess", trim_ws = TRUE)
  swissgrid <- swissgrid[-c(1), ]
  colnames(swissgrid)
  swissgrid <- subset(swissgrid, select=-c(5, 6, 7, 8, 9, 10, 19, 20, 21, 22, 23, 24, 25, 64, 65))
  names(swissgrid)[1] <- "time"
  names(swissgrid)[2] <- "out*ch"
  names(swissgrid)[3] <- "in*ch"
  names(swissgrid)[4] <- "out_incl_losses*ch"
  names(swissgrid)[5] <- "out*at"
  names(swissgrid)[6] <- "in*at"
  names(swissgrid)[7] <- "out*de"
  names(swissgrid)[8] <- "in*de"
  names(swissgrid)[9] <- "out*fr"
  names(swissgrid)[10] <- "in*fr"
  names(swissgrid)[11] <- "out*it"
  names(swissgrid)[12] <- "in*it"
  names(swissgrid)[13] <- "in*ag"
  names(swissgrid)[14] <- "out*ag"
  names(swissgrid)[15] <- "in*fri"
  names(swissgrid)[16] <- "out*fri"
  names(swissgrid)[17] <- "in*gl"
  names(swissgrid)[18] <- "out*gl"
  names(swissgrid)[19] <- "in*gr"
  names(swissgrid)[20] <- "out*gr"
  names(swissgrid)[21] <- "in*lu"
  names(swissgrid)[22] <- "out*lu"
  names(swissgrid)[23] <- "in*ne"
  names(swissgrid)[24] <- "out*ne"
  names(swissgrid)[25] <- "in*so"
  names(swissgrid)[26] <- "out*so"
  names(swissgrid)[27] <- "in*sg"
  names(swissgrid)[28] <- "out*sg"
  names(swissgrid)[29] <- "in*ti"
  names(swissgrid)[30] <- "out*ti"
  names(swissgrid)[31] <- "in*tg"
  names(swissgrid)[32] <- "out*tg"
  names(swissgrid)[33] <- "in*vs"
  names(swissgrid)[34] <- "out*vs"
  names(swissgrid)[35] <- "in*aiar"
  names(swissgrid)[36] <- "out*aiar"
  names(swissgrid)[37] <- "in*blbs"
  names(swissgrid)[38] <- "out*blbs"
  names(swissgrid)[39] <- "in*beju"
  names(swissgrid)[40] <- "out*beju"
  names(swissgrid)[41] <- "in*szzg"
  names(swissgrid)[42] <- "out*szzg"
  names(swissgrid)[43] <- "in*ownw"
  names(swissgrid)[44] <- "out*ownw"
  names(swissgrid)[45] <- "in*gevd"
  names(swissgrid)[46] <- "out*gevd"
  names(swissgrid)[47] <- "in*shzh"
  names(swissgrid)[48] <- "out*shzh"
  names(swissgrid)[49] <- "in*bridging"
  names(swissgrid)[50] <- "out*bridging"
  
  # To separate the value from the other variables region and direction
  swissgrid <- melt(swissgrid, id=(c("time")))
  names(swissgrid)[2] <- "variable"
  swissgrid$variable <- as.character(swissgrid$variable)
  swissgrid$value <- as.numeric(swissgrid$value)
  trimws(swissgrid$variable)
  
  # To split variable into region and direction and combine it again with swissgrid
  variable <- swissgrid$variable
  variable<- strsplit(swissgrid$variable, "[*]")
  variable <- data.frame(matrix(unlist(variable), nrow=length(variable), byrow=T))
  names(variable)[1] <- "direction"
  names(variable)[2] <- "region"
  swissgrid <- cbind(swissgrid, variable)
  swissgrid <- subset(swissgrid, select=-c(2))
  
  # Reordering dataframe.
  swissgrid <- swissgrid[c("time", "direction", "region", "value")]

  # Modify factor variables into strings and time variable into numeric
  swissgrid$direction <- as.character(swissgrid$direction)
  swissgrid$region <- as.character(swissgrid$region)
  swissgrid$time <- as.numeric(swissgrid$time)

  # Converting the numeric excel datetime into R format
  swissgrid$time <- convert_date(swissgrid$time, type = "Excel", fraction = TRUE)
  
  swissgridlist[[6+yr]] <- swissgrid

}

names(swissgridlist) <- c(2009:2021)


# BUILD BIG DATA FRAME ----------------------------------------------------

# Appending all dataframes together (2009-2013 are identically formatted, 2014 is unique and 2015-2019 are also identically formatted)
swissgrid <- rbind(swissgridlist[["2009"]],
                   swissgridlist[["2010"]],
                   swissgridlist[["2011"]],
                   swissgridlist[["2012"]],
                   swissgridlist[["2013"]],
                   swissgridlist[["2014"]],
                   swissgridlist[["2015"]],
                   swissgridlist[["2016"]],
                   swissgridlist[["2017"]],
                   swissgridlist[["2018"]],
                   swissgridlist[["2019"]],
                   swissgridlist[["2020"]],
                   swissgridlist[["2021"]])

# AGGREGATE TO DAILY SERIES (ONLY FOR CH) AND SAVE ---------------------------------

ch <- swissgrid[swissgrid$region=="ch",]

out.ch <- ch[ch$direction=="out",]
out.ch <- out.ch[,-c(2,3)]
out.ch$by1d = cut(out.ch$time, breaks="1 day")
out.ch = aggregate(value ~ by1d, FUN=sum, data=out.ch)
names(out.ch)[1] <- "date"

out_incl_losses.ch <- ch[ch$direction=="out_incl_losses",]
out_incl_losses.ch <- out_incl_losses.ch[,-c(2,3)]
out_incl_losses.ch$by1d = cut(out_incl_losses.ch$time, breaks="1 day")
out_incl_losses.ch = aggregate(value ~ by1d, FUN=sum, data=out_incl_losses.ch)
names(out_incl_losses.ch)[1] <- "date"

in.ch <- ch[ch$direction=="in",]
in.ch <- in.ch[,-c(2,3)]
in.ch$by1d = cut(in.ch$time, breaks="1 day")
in.ch = aggregate(value ~ by1d, FUN=sum, data=in.ch)
names(in.ch)[1] <- "date"

ch <- merge(out.ch,out_incl_losses.ch,by="date")
ch <- merge(ch,in.ch,by="date")
names(ch) <- c("date","out.ch","out_incl_losses.ch","in.ch")
ch <- ch[-c(dim(ch)[1]),] # Get rid of start of new year

write.table(ch, 'L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/swissgrid/swissgrid.ch.2009_2021.csv', sep = ',', row.names = FALSE)

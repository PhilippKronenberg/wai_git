
# Notes -------------------------------------------------------------------
# To update the series, run this file.
# To check the keywords, principal component and the indicator, uncomment the relevant sections.
# To add new keywords to the indicator, add them to "keywords" and add proc_keyword_init() with the specific keyword in section "Creation of indicator"
# For more infos how to create daily indicators out of keywords see:
# https://trendecon.github.io/trendecon/articles/intro.html
# https://trendecon.github.io/trendecon/articles/daily-series.html
# https://www.datacareer.ch/blog/analyzing-google-trends-with-r-retrieve-and-plot-with-gtrendsr/
# https://github.com/trendecon/trendecon

# Packages ----------------------------------------------------------------

#install.packages("remotes")
#remotes::install_github("trendecon/trendecon")

library(zoo)
library(tstools)
library(tempdisagg)
library(forecast)
library(timeseriesdb)
library(seasonal)
library(tempdisagg)
library(tis) 
library(trendecon)
library(prophet)
library(tsbox)
library(dplyr)
library(tibble)
library(TTR)

# Set working directory
setwd("~/GitHub/weeklyeconomicactivityindicator/datacollection/google_trends_indicators")

# define keywords ---------------------------------------------------------

#keywords <- c("Arbeitslosigkeit","arbeitslos","Arbeitsamt","Arbeitslosenversicherung","Arbeitslosengeld","Kurzarbeit","offene Stellen","Jobsuche")
 ## Test with aggregation of keywords for different languages
keywords <- c("Arbeitslosigkeit + chômage + disoccupazione",
              "arbeitslos + au chômage + disoccupato",
              "Arbeitsamt + agence pour l'emploi + ufficio del lavoro",
              "Kurzarbeit + chômage partiel + lavoro a orario ridotto"
)

# Creation of indicator ---------------------------------------------------

### initialize keywords (needs only be run once to create the series)
## proc_keyword_init("Arbeitslosigkeit + chômage + disoccupazione", "CH")
## proc_keyword_init("arbeitslos + au chômage + disoccupato", "CH")
## proc_keyword_init("Arbeitsamt + agence pour l'emploi + ufficio del lavoro", "CH")
## proc_keyword_init("Arbeitslosenversicherung + assurance chômage + assicurazione contro la disoccupazione", "CH")
## proc_keyword_init("Kurzarbeit + chômage partiel + lavoro a orario ridotto", "CH")
## proc_keyword_init("Arbeitslosengeld + allocation de chômage + indennità di disoccupazione", "CH")


# update and create indicator ----------------------------------------------------

# update indicators
lapply(keywords, proc_keyword)
data <- read_keywords(keywords, suffix = "sa", id = "seas_adj")

# check if they have the same span
smry <- ts_summary(data)
#smry
stopifnot(nrow(dplyr::distinct(smry, start, end)) == 1)

# create index by using principal component
x_prcomp <- filter(ts_prcomp(data), id == "PC1") %>%

  # mutate(value = -value) %>%
  select(-id) %>%
  ts_scale()

# Save output -------------------------------------------------------------

# output created daily seasonally adjusted index
write_keyword(x_prcomp, "Arbeitsmarkt", "ch", "sa")

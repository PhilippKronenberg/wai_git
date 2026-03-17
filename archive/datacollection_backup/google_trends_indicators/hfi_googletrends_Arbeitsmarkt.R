
rm(list = ls())
cat("\014")

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#
# Download Data from Google Trends and Create a Time Series with Principal Components
# Philipp Kronenberg
# Last Update: 04.02.2020
#
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

# Notes -------------------------------------------------------------------

# To update the series, run this file.
# To check the keywords, principal component and the indicator, uncomment the relevant sections.
# To add new keywords to the indicator, add them to "keywords" and add proc_keyword_init() with the specific keyword in section "Creation of indicator"
# For more infos how to create daily indicators out of keywords see:
# https://trendecon.github.io/trendecon/articles/intro.html
# https://trendecon.github.io/trendecon/articles/daily-series.html
# https://www.datacareer.ch/blog/analyzing-google-trends-with-r-retrieve-and-plot-with-gtrendsr/
# https://github.com/trendecon/trendecon

# Note: If some error occurs due to some NAs in the series. There was probably some error in downloading. 
# Then delete the existing csv files and download the data from scratch by running the proc_keyword_init() command.


# Packages ----------------------------------------------------------------

#install.packages("remotes")
#remotes::install_github("trendecon/trendecon")
#setwd("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/google_trends_indicatorss/functions")
setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators/functions")


# Load all the R functions in the folder. The folder contains all the functions from the trendecon and the tempdisagg package. This guarantees stability.
lapply(list.files(pattern = "[.]R$", recursive = TRUE), source)

# libraies used in this file
library(zoo)
#library(tstools)  # tsplot is only used in commented sections
library(tempdisagg)
library(forecast)
library(timeseriesdb)
library(seasonal)

# libraries from trendecon
#library(trendecon)
library(tis) 
library(prophet)
library(tsbox)
library(dplyr)
library(tibble)
library(TTR)

# libraries from tempdisagg
#library(tempdisagg)
library(graphics)
library(stats)
library(utils)

# imports from td function
library(gtrendsR)
library(readr)
library(tidyr)

# Set working directory
#setwd("L:/Groups/Economic Forecasting/Internationale Konjunktur/Sonstiges/WAI/datacollection/google_trends_indicators")
setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators")


# define keywords ---------------------------------------------------------

#keywords <- c("Arbeitslosigkeit","arbeitslos","Arbeitsamt","Arbeitslosenversicherung","Arbeitslosengeld","Kurzarbeit","offene Stellen","Jobsuche")
 ## Test with aggregation of keywords for different languages
keywords <- c("Arbeitslosigkeit + chômage + disoccupazione",
              "arbeitslos + au chômage + disoccupato",
              "Arbeitsamt + agence pour l'emploi + ufficio del lavoro",
              "Kurzarbeit + chômage partiel + lavoro a orario ridotto"
)

# test for relevance of keywords/categories ------------------------------------------

# # download data in monthly basis and define keylist
# y <- ts_gtrends(keywords, geo = "CH", time = "all")
# keylist <- list()
# ## There is improvement in correlation with unemployment for some keywords when other national languages are included.
# ## But including englisch seems to be better for global effects such as 2008/09 but worse in general. Thus englisch is excluded.
# keylist$`Arbeitslosigkeit + chômage + disoccupazione`<- filter(y, id == "Arbeitslosigkeit + chômage + disoccupazione")
# keylist$`arbeitslos + au chômage + disoccupato` <- filter(y, id == "arbeitslos + au chômage + disoccupato")
# keylist$`Arbeitsamt + agence pour l'emploi + ufficio del lavoro` <- filter(y, id == "Arbeitsamt + agence pour l'emploi + ufficio del lavoro")
# keylist$`Kurzarbeit + chômage partiel + lavoro a orario ridotto` <- filter(y, id == "Kurzarbeit + chômage partiel + lavoro a orario ridotto")
# 
#  ## could also include whole categories. But they do not move similar to unemployment. In general, Categories do not capture short time movements that good.
#  # Welfare & Unemployment: 706, Labor & Employment Law: 701, Bankruptcy: 423, Job Listings: 960
#  # y_2 <- ts_gtrends(category = "706",geo = "CH", time = "all")
#  # id <- rep(0,dim(y_2)[1])
#  # y_cat<- as_tibble(cbind(id, y_2))
#  # keylist$cat_welfareunemp <- y_cat
# 
# 
# # apply smoothing to monthly series and create time series
# x_ts <- lapply(names(keylist),function(x){
#   window(SMA(ts(keylist[[x]][,3],start = c(2004, 1), frequency = 12),
#                   n=12),
#              start = c(2004,12),end = c(2020,5))
# })
# names(x_ts) <- names(keylist)
# # create index for visual comparison
# x_ts_2 <- list()
# x_ts_2 <- lapply(names(keylist),function(x){
#           x_ts[[x]]/x_ts[[x]][1]*100
# })
# names(x_ts_2) <- names(keylist)
# 
# # read in unemployment rate of Switzerland from csv and create index (SWUN%TOTQ)
# unemployment_ch <- read.csv("~/GitHub/weeklyeconomicactivityindicator/datacollection/google_trends_indicators/unemployment_ch.csv", sep=";")
# unemp_ts <- window(ts(unemployment_ch[,2],start = c(1970,1), frequency = 12),
#                    start=c(2004,12), end = c(2020,5))
# unemp_ix <- unemp_ts/unemp_ts[1]*100
# 
# # create output
# output <- matrix(unlist(x_ts_2), ncol =length(names(keylist)) , byrow = FALSE)
# colnames(output) <- names(keylist)
# output <- cbind(output, unemp_ix)
# 
# cor(output)
# tsplot(output[,-5]) # plot without Kurzarbeit

# Creation of indicator ---------------------------------------------------

### Option to use cached CSVs instead of downloading from Google Trends
# Priority for determining `USE_CACHE`:
# 1. Environment variable `WAI_USE_CACHE` (recommended when sourcing / Rscript)
# 2. Existing variable `USE_CACHE` in the environment (if set before sourcing)
# 3. Default to TRUE
env_val <- Sys.getenv("WAI_USE_CACHE", unset = NA)
if (!is.na(env_val) && nzchar(env_val)) {
  USE_CACHE <- tolower(env_val) %in% c("1", "true", "t", "yes", "y")
} else if (exists("USE_CACHE", envir = .GlobalEnv)) {
  # keep user-supplied USE_CACHE if present
  USE_CACHE <- get("USE_CACHE", envir = .GlobalEnv)
} else {
  USE_CACHE <- TRUE
}
message(sprintf("USE_CACHE = %s", USE_CACHE))

### initialize keywords (needs only be run once to create the series)
if (!USE_CACHE) {
  proc_keyword_init("Arbeitslosigkeit + chômage + disoccupazione", "CH")
  proc_keyword_init("arbeitslos + au chômage + disoccupato", "CH")
  proc_keyword_init("Arbeitsamt + agence pour l'emploi + ufficio del lavoro", "CH")
  proc_keyword_init("Kurzarbeit + chômage partiel + lavoro a orario ridotto", "CH")

  proc_keyword_init("Arbeitslosenversicherung + assurance chômage + assicurazione contro la disoccupazione", "CH")
  proc_keyword_init("Arbeitslosengeld + allocation de ch?mage + indennit? di disoccupazione", "CH")
} else {
  message("USE_CACHE=TRUE: skipping Google Trends downloads and using archived CSVs if available.")
}


# update and create indicator ----------------------------------------------------

# update indicators
if (!USE_CACHE) {
  lapply(keywords, proc_keyword)
} else {
  message("USE_CACHE=TRUE: skipping proc_keyword updates (using cached files)")
}
data <- read_keywords(keywords, suffix = "sa", id = "seas_adj")

# check if they have the same span
smry <- ts_summary(data)
# If the individual keyword series have different spans, trim to the common
# intersection so subsequent processing doesn't fail.
if (nrow(dplyr::distinct(smry, start, end)) != 1) {
  message("Multiple spans detected for keywords; trimming to common intersection.")
  # try to compute latest start and earliest end and span the data accordingly
  starts <- as.Date(smry$start)
  ends <- as.Date(smry$end)
  new_start <- max(starts, na.rm = TRUE)
  new_end <- min(ends, na.rm = TRUE)
  data <- tsbox::ts_span(data, start = new_start, end = new_end)
  smry <- ts_summary(data)
  if (nrow(dplyr::distinct(smry, start, end)) != 1) {
    stop("Could not align keyword series spans after trimming.")
  }
}

# create index by using principal component
x_prcomp <- filter(ts_prcomp(data), id == "PC1") %>%

  # mutate(value = -value) %>%
  select(-id) %>%
  ts_scale()

# Check principal component -----------------------------------------------

#     # check principal components
#     pca <- prcomp(ts_ts(data), scale = TRUE)
#     library(ggfortify)
#     autoplot(pca, loadings = TRUE, loadings.colour = 'blue', loadings.label = TRUE)
#     w_pc1 <- enframe(pca$rotation[, 'PC1'], "keyword", "weight")
# 
#     # using weights from pca to calculate orig
#     orig <- read_keywords(keywords, id = "orig") %>%
#       ts_scale() %>%
#       left_join(w_pc1, by = "keyword") %>%
#       group_by(time) %>%
#       summarize(value = -sum(value * weight)) %>%
#       ungroup() %>%
#       ts_scale()
# 
#     # # Compare time series with and without seasonal adjustment with each other in a plot
#     #  ts_dygraphs(ts_c(
#     #    sa = select(read_keywords("Arbeitslosigkeit", id = "seas_adj"), -keyword),
#     #    orig = select(read_keywords("Arbeitslosigkeit", id = "orig"), -keyword)
#     #  ))
# 
#  # store components together, plot all or single components
#  ans <- ts_c(x_prcomp, orig, seas_comp = orig  %ts-% x_prcomp)
#  ts_dygraphs(ans)
#  ts_dygraphs(ts_pick(ans, "orig")) # chose component to visualize
#  ts_dygraphs(ts_pick(ans, "seas_comp")) # chose component to visualize
# 
# #plot
# ts_dygraphs(x_prcomp)
# ts_plot(x_prcomp)


# Save output -------------------------------------------------------------

# output created daily seasonally adjusted index
write_keyword(x_prcomp, "Arbeitsmarkt", "ch", "sa")

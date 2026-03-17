
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

remove.packages("tempdisagg")
require(devtools)
install_version("tempdisagg", version = "0.25", repos = "http://cran.us.r-project.org")

library(zoo)
library(tstools)
# library(tempdisagg)
library(forecast)
library(timeseriesdb)
library(seasonal)
library(tempdisagg)
library(trendecon)
library(tis) 
library(prophet)
library(tsbox)
library(dplyr)
library(tibble)
library(TTR)


# Functions ---------------------------------------------------------------

proc_keyword_alt <- function (keyword = "Insolvenz", geo = "CH", n_windows = 2) 
{
  stop_if_no_data(keyword, geo)
  previous_google_date <- check_when_last_processed(keyword, 
                                                    geo)
  if (previous_google_date == .latest_google_date) {
    message("keyword ", keyword, " already processed today. skipping.")
    return(TRUE)
  }
  else {
    proc_keyword_latest(keyword = keyword, geo = geo, n_windows = n_windows)
    proc_combine_freq_alt(keyword = keyword, geo = geo)
    proc_seas_adj(keyword = keyword, geo = geo)
    .latest_google_date <<- latest_google_date(keyword, geo)
    return(invisible(TRUE))
  }
}

proc_combine_freq_alt <- function(keyword = "Insolvenz", geo = "ch") {
  message("combining frequencies of keyword: ", keyword)
  
  h <- select(read_keyword(keyword, geo, "h"), -n)
  d <- select(read_keyword(keyword, geo, "d"), -n)
  w <- select(read_keyword(keyword, geo, "w"), -n)
  m <- select(read_keyword(keyword, geo, "m"), -n)
  
  
  # extend daily data with hourly data
  # to do so - adjust the mean level of hourly data to match overlapping daily values
  message("extend daily data by hourly data for the missing recent days")
  dh <- inner_join(d, h, by="time", suffix=c(".d", ".h"))
  h <- h %>%
    mutate(value = value * mean(dh$value.d) / mean(dh$value.h)) %>%
    mutate(value = if_else(value > 100, 100, value)) %>%
    filter(time > max(d$time))
  d <- rbind(d, h)
  
  
  message("align daily data to weekly")
  m_wd <- tempdisagg::td(w ~ d, method = "fast", conversion = "mean")
  
  # the slope coefficient should be around 1 and significant, otherwise the movement is not copied well
  # summary(m_wd)
  wd <- predict(m_wd)
  
  # 2. bend daily series (which fullfills weekly constraint) so that monthly
  # values are identical to monthly series
  message("align weekly data to monthly")
  m_mwd <- tempdisagg::td(m ~ wd, method = "fast", conversion = "mean")
  mwd <- predict(m_mwd)
  
  write_keyword(mwd, keyword, geo, "mwd")
  
  # mwd_old <- read_keyword(keyword, "mwd")
  # write_csv(ts_c(mwd_old, mwd), "data/indicator_doc/mwd_old.csv")
  # ts_dygraphs(read_csv("data/indicator_doc/mwd_old.csv"))
}



# Set working directory

# setwd("~/rdata/nestefan/wai/datacollection/google_trends_indicators")
setwd("R:/nestefan/wai/datacollection/google_trends_indicators")

# define keywords ---------------------------------------------------------

#keywords <- c("Arbeitslosigkeit","arbeitslos","Arbeitsamt","Arbeitslosenversicherung","Arbeitslosengeld","Kurzarbeit","offene Stellen","Jobsuche")
 ## Test with aggregation of keywords for different languages
keywords <- c("Arbeitslosigkeit + chômage + disoccupazione",
              "arbeitslos + au chômage + disoccupato",
              "Arbeitsamt + agence pour l'emploi + ufficio del lavoro",
              "Kurzarbeit + chômage partiel + lavoro a orario ridotto"
)

# # test for relevance of keywords/categories ------------------------------------------
# 
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
#              start = c(2004,12),end = c(2022,1))
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
#                    start=c(2004,12), end = c(2021,5))
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

### initialize keywords (needs only be run once to create the series)
# proc_keyword_init("Arbeitslosigkeit + ch?mage + disoccupazione", "CH")
# proc_keyword_init("arbeitslos + au ch?mage + disoccupato", "CH")
# proc_keyword_init("Arbeitsamt + agence pour l'emploi + ufficio del lavoro", "CH")
# proc_keyword_init("Arbeitslosenversicherung + assurance ch?mage + assicurazione contro la disoccupazione", "CH")
# proc_keyword_init("Kurzarbeit + ch?mage partiel + lavoro a orario ridotto", "CH")
# proc_keyword_init("Arbeitslosengeld + allocation de ch?mage + indennit? di disoccupazione", "CH")


# update and create indicator ----------------------------------------------------

# update indicators
lapply(keywords, proc_keyword_alt)
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

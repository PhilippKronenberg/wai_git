
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
keywords <- c(
  "Staatshilfe + aide publique + aiuti di Stato",
  "Rettungspaket + plan de sauvetage +  pacchetto di salvataggio"
)

# test for relevance of keywords/categories ------------------------------------------

# download data in monthly basis and define keylist
y <- ts_gtrends(keywords, geo = "CH", time = "all")
keylist <- list()
#keylist$Staatshilfe <- filter(y, id == "Staatshilfe")
## There is improvement in correlation with unemployment for some keywords when other national languages are included.
## But including englisch seems to be better for global effects such as 2008/09 but worse in general. Thus englisch is excluded.
keylist$`Staatshilfe + aide publique + aiuti di Stato`<- filter(y, id == "Staatshilfe + aide publique + aiuti di Stato")
keylist$`Rettungspaket + plan de sauvetage +  pacchetto di salvataggio`<- filter(y, id == "Rettungspaket + plan de sauvetage +  pacchetto di salvataggio")

 ## could also include whole categories. But they do not move similar to unemployment. In general, Categories do not capture short time movements that good.
 # Welfare & Unemployment: 706, Labor & Employment Law: 701, Bankruptcy: 423, Job Listings: 960
 # y_2 <- ts_gtrends(category = "706",geo = "CH", time = "all")
 # id <- rep(0,dim(y_2)[1])
 # y_cat<- as_tibble(cbind(id, y_2))
 # keylist$cat_welfareunemp <- y_cat


# apply smoothing to monthly series and create time series
x_ts <- lapply(names(keylist),function(x){
  window(ts(keylist[[x]][,3],start = c(2004, 1), frequency = 12),
             start = c(2004,12),end = c(2020,5))
})
names(x_ts) <- names(keylist)
# create index for visual comparison
# x_ts_2 <- list()
# x_ts_2 <- lapply(names(keylist),function(x){
#           x_ts[[x]]/x_ts[[x]][1]*100
# })
# names(x_ts_2) <- names(keylist)

# create output
output <- matrix(unlist(x_ts), ncol =length(names(keylist)) , byrow = FALSE)
colnames(output) <- names(keylist)
output <- cbind(output)

cor(output)
tsplot(output[,1]) # plot without Kurzarbeit

# Creation of indicator ---------------------------------------------------

### initialize keywords (needs only be run once to create the series)
 proc_keyword_init("Staatshilfe + aide publique + aiuti di Stato", "CH")
 proc_keyword_init("Rettungspaket + plan de sauvetage +  pacchetto di salvataggio", "CH")
## proc_keyword_init("Arbeitsamt + agence pour l'emploi + ufficio del lavoro", "CH")
## proc_keyword_init("Arbeitslosenversicherung + assurance chômage + assicurazione contro la disoccupazione", "CH")
## proc_keyword_init("Kurzarbeit + chômage partiel + lavoro a orario ridotto", "CH")
## proc_keyword_init("Arbeitslosengeld + allocation de chômage + indennità di disoccupazione", "CH")


# update and indicator ----------------------------------------------------

# update indicators
lapply(keywords, proc_keyword)
data <- read_keywords(keywords, suffix = "sa", id = "seas_adj")

# check if they have the same span
smry <- ts_summary(data)
smry
stopifnot(nrow(dplyr::distinct(smry, start, end)) == 1)

# create index by using principal component
x_prcomp <- filter(ts_prcomp(data), id == "PC1") %>%

  # mutate(value = -value) %>%
  select(-id) %>%
  ts_scale()

# Check principal component -----------------------------------------------

    # check principal components
    pca <- prcomp(ts_ts(data), scale = TRUE)
    library(ggfortify)
    autoplot(pca, loadings = TRUE, loadings.colour = 'blue', loadings.label = TRUE)
    w_pc1 <- enframe(pca$rotation[, 'PC1'], "keyword", "weight")

    # using weights from pca to calculate orig
    orig <- read_keywords(keywords, id = "orig") %>%
      ts_scale() %>%
      left_join(w_pc1, by = "keyword") %>%
      group_by(time) %>%
      summarize(value = -sum(value * weight)) %>%
      ungroup() %>%
      ts_scale()

    # # Compare time series with and without seasonal adjustment with each other in a plot
    #  ts_dygraphs(ts_c(
    #    sa = select(read_keywords("Arbeitslosigkeit", id = "seas_adj"), -keyword),
    #    orig = select(read_keywords("Arbeitslosigkeit", id = "orig"), -keyword)
    #  ))

 # store components together, plot all or single components
 ans <- ts_c(x_prcomp, orig, seas_comp = orig  %ts-% x_prcomp)
 ts_dygraphs(ans)
 ts_dygraphs(ts_pick(ans, "orig")) # chose component to visualize
 ts_dygraphs(ts_pick(ans, "seas_comp")) # chose component to visualize

#plot
ts_dygraphs(x_prcomp)
ts_plot(x_prcomp)


# Save output -------------------------------------------------------------

# output created daily seasonally adjusted index
write_keyword(x_prcomp, "Staatshilfe", "ch", "sa")

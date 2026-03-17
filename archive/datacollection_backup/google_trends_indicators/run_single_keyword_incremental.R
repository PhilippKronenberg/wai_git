#!/usr/bin/env Rscript
# Incremental update script for a single keyword
# - Downloads only data after the latest date in the archived 'sa' file
# - Uses WAI_WAIT and WAI_RETRY env vars for backoff
# - Respects HTTPS_PROXY / HTTP_PROXY environment variables if set

args <- commandArgs(trailingOnly = TRUE)
keyword <- if (length(args) >= 1) args[1] else "Arbeitsamt + agence pour l'emploi + ufficio del lavoro"
geo <- if (length(args) >= 2) args[2] else "CH"

# config from environment
wait_val <- as.integer(Sys.getenv("WAI_WAIT", unset = "120"))
retry_val <- as.integer(Sys.getenv("WAI_RETRY", unset = "30"))
use_cache <- tolower(Sys.getenv("WAI_USE_CACHE", unset = "TRUE")) %in% c("1","true","t","y","yes")

message(sprintf("Running incremental update for keyword: %s (geo=%s)", keyword, geo))
message(sprintf("WAI_WAIT=%s WAI_RETRY=%s USE_CACHE=%s", wait_val, retry_val, use_cache))

# source functions
setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators/functions")
lapply(list.files(pattern = "[.]R$", recursive = TRUE), source)
setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators")

# load packages used by helper functions (ensure functions like read_keyword work)
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(fs)
  library(tsbox)
  library(gtrendsR)
  library(tibble)
  library(tidyr)
  library(prophet)
  library(seasonal)
  library(tempdisagg)
  library(zoo)
})

# helper to get latest date in sa file
latest_sa_date <- function(keyword, geo) {
  if (fs::file_exists(path_keyword(keyword, geo, "sa"))) {
    d <- read_keyword(keyword, geo, "sa")
    if (nrow(d) == 0) return(as.Date("1900-01-01"))
    return(as.Date(max(d$time)))
  }
  return(as.Date("1900-01-01"))
}

last_date <- latest_sa_date(keyword, geo)
message(sprintf("Latest local sa date for '%s': %s", keyword, last_date))

if (last_date >= Sys.Date()) {
  message("Data already up-to-date. Nothing to download.")
  quit(status = 0)
}

# compute from date (start downloading from next day)
from_date <- as.character(last_date + 1)
message(sprintf("Will attempt to download data from %s to today.", from_date))

# wrap download calls with tryCatch and backoff wrapper present in gtrends_with_backoff
# Daily
message("Downloading daily windows covering missing range...")
try_daily <- try({
  d <- ts_gtrends_windows(
    keyword = keyword,
    geo = geo,
    from = from_date,
    stepsize = "15 days",
    windowsize = "6 months",
    n_windows = floor(as.numeric(Sys.Date() - as.Date(from_date)) / 15) + 1,
    wait = wait_val,
    retry = retry_val,
    prevent_window_shrinkage = TRUE
  )
}, silent = TRUE)

if (inherits(try_daily, "try-error")) {
  message("Daily download failed: ")
  message(try_daily)
  if (use_cache) {
    message("Falling back to cache (no write). Exiting with partial results.")
    quit(status = 0)
  } else {
    stop(try_daily)
  }
}

# Weekly
message("Downloading weekly windows covering missing range...")
try_weekly <- try({
  w <- ts_gtrends_windows(
    keyword = keyword,
    geo = geo,
    from = seq(Sys.Date(), length.out = 2, by = "-1 year")[2],
    stepsize = "1 week",
    windowsize = "1 year",
    n_windows = 12,
    wait = wait_val,
    retry = retry_val,
    prevent_window_shrinkage = FALSE
  )
}, silent = TRUE)

if (inherits(try_weekly, "try-error")) {
  message("Weekly download failed: ")
  message(try_weekly)
  if (use_cache) {
    message("Falling back to cache (no write). Exiting with partial results.")
    quit(status = 0)
  } else {
    stop(try_weekly)
  }
}

# Monthly
message("Downloading monthly windows covering missing range...")
try_monthly <- try({
  m <- ts_gtrends_windows(
    keyword = keyword,
    geo = geo,
    from = "2006-01-01",
    stepsize = "1 month",
    windowsize = "20 years",
    n_windows = 12,
    wait = wait_val,
    retry = retry_val,
    prevent_window_shrinkage = FALSE
  )
}, silent = TRUE)

if (inherits(try_monthly, "try-error")) {
  message("Monthly download failed: ")
  message(try_monthly)
  if (use_cache) {
    message("Falling back to cache (no write). Exiting with partial results.")
    quit(status = 0)
  } else {
    stop(try_monthly)
  }
}

# Aggregate and merge with existing data
message("Aggregating and merging with existing keyword data...")
# aggregate_windows returns aggregated averages
d_aggr <- aggregate_windows(d)
w_aggr <- aggregate_windows(w)
m_aggr <- aggregate_windows(m)

# combine with existing if present
if (fs::file_exists(path_keyword(keyword, geo, "d"))) {
  old_d <- read_keyword(keyword, geo, "d")
  new_d <- aggregate_averages(old_d, d_aggr)
} else {
  new_d <- d_aggr
}
write_keyword(new_d, keyword, geo, "d")

if (fs::file_exists(path_keyword(keyword, geo, "w"))) {
  old_w <- read_keyword(keyword, geo, "w")
  new_w <- aggregate_averages(old_w, w_aggr)
} else {
  new_w <- w_aggr
}
write_keyword(new_w, keyword, geo, "w")

if (fs::file_exists(path_keyword(keyword, geo, "m"))) {
  old_m <- read_keyword(keyword, geo, "m")
  new_m <- aggregate_averages(old_m, m_aggr)
} else {
  new_m <- m_aggr
}
write_keyword(new_m, keyword, geo, "m")

# combine freqs and seasonal adjust
message("Combining frequencies and seasonal adjustment...")
proc_combine_freq(keyword = keyword, geo = geo)
proc_seas_adj(keyword = keyword, geo = geo)

# Verify - show last lines of sa
sa_path <- path_keyword(keyword, geo, "sa")
if (fs::file_exists(sa_path)) {
  message("Successful run; last lines of seasonally adjusted file:")
  df_sa <- read_keyword(keyword, geo, "sa")
  print(tail(df_sa, 10))
} else {
  message("No seasonally-adjusted file produced.")
}

# Finally update the overall Arbeitsmarkt indicator if needed
message("Updating global indicator (Arbeitsmarkt)")
# Read keywords list from this script's folder - use keywords defined in main script
all_keywords <- c("Arbeitslosigkeit + chômage + disoccupazione",
                  "arbeitslos + au chômage + disoccupato",
                  "Arbeitsamt + agence pour l'emploi + ufficio del lavoro",
                  "Kurzarbeit + chômage partiel + lavoro a orario ridotto")

# Build indicator from seasonally adjusted keywords
msg <- try({
  data <- read_keywords(all_keywords, suffix = "sa", id = "seas_adj")
  smry <- ts_summary(data)
  if (nrow(dplyr::distinct(smry, start, end)) != 1) {
    starts <- as.Date(smry$start)
    ends <- as.Date(smry$end)
    new_start <- max(starts, na.rm = TRUE)
    new_end <- min(ends, na.rm = TRUE)
    data <- tsbox::ts_span(data, start = new_start, end = new_end)
  }
  x_prcomp <- filter(ts_prcomp(data), id == "PC1") %>% select(-id) %>% ts_scale()
  write_keyword(x_prcomp, "Arbeitsmarkt", "ch", "sa")
  message("Updated Arbeitsmarkt indicator successfully.")
}, silent = TRUE)

if (inherits(msg, "try-error")) {
  message("Could not update Arbeitsmarkt indicator:")
  message(msg)
}

message("Incremental update script finished.")

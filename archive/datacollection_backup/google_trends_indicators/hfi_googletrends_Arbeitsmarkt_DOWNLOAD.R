#!/usr/bin/env Rscript
#
# Incremental Google Trends download script
# Attempts to download fresh data for the Arbeitsmarkt indicator.
# If rate-limited by Google (429 errors), uses cached data and stops gracefully.
# Run this daily/weekly;  Google will eventually accept requests.
#

rm(list = ls())
cat("\014")

# Configuration ---------------------------------------------------------------
# Set these via environment variables to override:
# WAI_USE_CACHE: if TRUE or unset, falls back to cached CSVs when downloads fail
# WAI_WAIT: seconds to wait between retries (default 120)
# WAI_RETRY: number of retry attempts (default 60)
# WAI_PROXY: HTTP proxy URL (e.g., "http://proxy.example.com:8080")
#     Use this when your IP is blocked by Google Trends.
#     Example: WAI_PROXY="http://proxy.example.com:8080" Rscript hfi_googletrends_Arbeitsmarkt_DOWNLOAD.R

setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators/functions")
lapply(list.files(pattern = "[.]R$", recursive = TRUE), source)

library(zoo)
library(tempdisagg)
library(forecast)
library(timeseriesdb)
library(seasonal)
library(tis)
library(prophet)
library(tsbox)
library(dplyr)
library(tibble)
library(TTR)
library(graphics)
library(stats)
library(utils)
library(gtrendsR)
library(readr)
library(tidyr)

setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators")

# Keywords ------------------------------------------------------------------
keywords <- c(
  "Arbeitslosigkeit + chômage + disoccupazione",
  "arbeitslos + au chômage + disoccupato",
  "Arbeitsamt + agence pour l'emploi + ufficio del lavoro",
  "Kurzarbeit + chômage partiel + lavoro a orario ridotto"
)

# Parse environment variables -----------------------------------------------
env_val <- Sys.getenv("WAI_USE_CACHE", unset = "TRUE")
use_cache <- tolower(env_val) %in% c("1", "true", "t", "yes", "y")

wait_val <- as.integer(Sys.getenv("WAI_WAIT", unset = "120"))
retry_val <- as.integer(Sys.getenv("WAI_RETRY", unset = "60"))

message(sprintf("[CONFIG] USE_CACHE=%s, WAIT=%s, RETRY=%s", use_cache, wait_val, retry_val))

# Download function with graceful fallback ----------------------------------
safe_proc_keyword_latest <- function(keyword, geo = "CH", fallback_to_cache = TRUE) {
  tryCatch({
    message(sprintf("  Attempting to download: %s", keyword))
    proc_keyword_latest(keyword = keyword, geo = geo)
    message(sprintf("  SUCCESS: %s", keyword))
    return(TRUE)
  }, error = function(e) {
    err_msg <- as.character(e)
    if (grepl("429|rate limit|too soon", err_msg, ignore.case = TRUE) ||
        grepl("No data returned by the query|No data returned", err_msg, ignore.case = TRUE)) {
      message(sprintf("  RATE LIMITED/NO DATA: %s", keyword))
      message("     -> Use cached data instead")
      if (fallback_to_cache) {
        return(FALSE)  # signal that we used cache
      } else {
        stop(e)  # re-throw if cache not allowed
      }
    } else {
      message(sprintf("  ERROR: %s", err_msg))
      stop(e)
    }
  })
}

# Attempt updates for each keyword ------------------------------------------
message(paste0(rep("=", 70), collapse = ""))
message("STEP 1: Detecting date ranges and attempting incremental updates")
message(paste0(rep("=", 70), collapse = ""))
message("")

# First, detect which dates to download for each keyword
message("Auto-detecting latest date in each keyword's archive:")
for (kw in keywords) {
  existing <- tryCatch(
    {suppressWarnings(readr::read_csv(
      file.path("raw/ch", paste0(kw, "_sa.csv")),
      col_types = readr::cols()
    ))},
    error = function(e) NULL
  )
  if (!is.null(existing) && nrow(existing) > 0) {
    max_date <- max(existing$time, na.rm = TRUE)
    message(sprintf("  %s: latest = %s (will download from %s)", 
                    substr(kw, 1, 40), max_date, as.Date(max_date) + 1))
  } else {
    message(sprintf("  %s: no existing data found", substr(kw, 1, 40)))
  }
}
message("")

# Show proxy status
proxy_val <- Sys.getenv("WAI_PROXY", unset = NA)
if (!is.na(proxy_val) && nzchar(proxy_val)) {
  message(sprintf("Proxy configured: %s", proxy_val))
} else {
  message("No proxy configured (using direct connection)")
}
message("")
message("Starting downloads...")
message("")

all_downloaded <- TRUE
for (kw in keywords) {
  success <- safe_proc_keyword_latest(kw, fallback_to_cache = use_cache)
  if (isFALSE(success)) {
    all_downloaded <- FALSE
  }
}

message("")
if (all_downloaded) {
  message("✓ All keywords updated successfully!")
} else {
  message("⚠ Some keywords hit rate limits. Using cached data.")
  message("")
  message("  OPTIONS TO RESOLVE IP BLOCKING:")
  message("")
  message("  Option 1: Wait a few hours and re-run (Google may unblock the IP)")
  message("    WAI_WAIT=120 WAI_RETRY=60 Rscript hfi_googletrends_Arbeitsmarkt_DOWNLOAD.R")
  message("")
  message("  Option 2: Use a proxy to change your IP address")
  message("    WAI_PROXY=\"http://proxy.example.com:8080\" \\")
  message("    WAI_WAIT=120 WAI_RETRY=60 Rscript hfi_googletrends_Arbeitsmarkt_DOWNLOAD.R")
  message("")
  message("  Option 3: Run the download script from an unblocked IP address")
  message("    (e.g., from your local machine where Google has not blocked the IP)")
  message("")
}

# Read keywords and create indicator ----------------------------------------
message("")
message(paste0(rep("=", 70), collapse = ""))
message("STEP 2: Creating Arbeitsmarkt indicator from available data")
message(paste0(rep("=", 70), collapse = ""))
message("")

# Read keyword data (cached or newly downloaded)
data <- read_keywords(keywords, suffix = "sa", id = "seas_adj")

smry <- ts_summary(data)
if (nrow(dplyr::distinct(smry, start, end)) != 1) {
  message("  Trimming keyword series to common time span...")
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

message(sprintf("  Span: %s to %s", smry$start[1], smry$end[1]))

# Create principal component index
x_prcomp <- filter(ts_prcomp(data), id == "PC1") %>%
  select(-id) %>%
  ts_scale()

# Save output
message("  Writing indicator: Arbeitsmarkt_sa.csv")
write_keyword(x_prcomp, "Arbeitsmarkt", "ch", "sa")

message("")
message(paste0(rep("=", 70), collapse = ""))
message("✓ COMPLETE")
message(paste0(rep("=", 70), collapse = ""))
message(sprintf("Output written to: data/ch/Arbeitsmarkt_sa.csv"))
message("")

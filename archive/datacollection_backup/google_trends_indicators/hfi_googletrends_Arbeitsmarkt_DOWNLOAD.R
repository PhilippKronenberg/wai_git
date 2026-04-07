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
# WAI_WAIT: base seconds to wait between retries (default 90)
# WAI_RETRY: number of retry attempts (default 8)
# WAI_KEYWORD_PAUSE: seconds to sleep between keywords (default 45)
# WAI_WINDOWS: overlapping windows per frequency (default 2)
# WAI_STOP_AFTER_RATE_LIMIT: if TRUE, stop after the first rate limit (default TRUE)
# WAI_PROXY: HTTP proxy URL (e.g., "http://proxy.example.com:8080")
#     Use this when your IP is blocked by Google Trends.
#     Example: WAI_PROXY="http://proxy.example.com:8080" Rscript hfi_googletrends_Arbeitsmarkt_DOWNLOAD.R
setwd("C:/Users/kphilipp/GitHub/wai_git/archive/datacollection_backup/google_trends_indicators/functions")

#setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators/functions")
invisible(lapply(list.files(pattern = "[.]R$", recursive = TRUE), source))
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

#setwd("/workspaces/wai_git/archive/datacollection_backup/google_trends_indicators")
setwd("C:/Users/kphilipp/GitHub/wai_git/archive/datacollection_backup/google_trends_indicators")

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

wait_val <- as.integer(Sys.getenv("WAI_WAIT", unset = "90"))
retry_val <- as.integer(Sys.getenv("WAI_RETRY", unset = "8"))
pause_val <- as.integer(Sys.getenv("WAI_KEYWORD_PAUSE", unset = "45"))
n_windows <- as.integer(Sys.getenv("WAI_WINDOWS", unset = "2"))
stop_after_rl <- tolower(Sys.getenv("WAI_STOP_AFTER_RATE_LIMIT", unset = "TRUE")) %in%
  c("1", "true", "t", "yes", "y")

message(sprintf(
  "[CONFIG] USE_CACHE=%s, WAIT=%s, RETRY=%s, KEYWORD_PAUSE=%s, WINDOWS=%s, STOP_AFTER_RATE_LIMIT=%s",
  use_cache, wait_val, retry_val, pause_val, n_windows, stop_after_rl
))

is_rate_limit_error <- function(msg) {
  grepl("429|too many requests|rate limit|too soon|quota", msg, ignore.case = TRUE)
}

read_latest_time <- function(keyword, geo = "CH", suffix = "sa") {
  existing <- tryCatch(
    suppressWarnings(read_keyword(keyword, geo, suffix)),
    error = function(e) NULL
  )
  if (is.null(existing) || nrow(existing) == 0 || !"time" %in% names(existing)) {
    return(as.Date(NA))
  }
  max(as.Date(existing$time), na.rm = TRUE)
}

update_keyword <- function(keyword, geo = "CH") {
  before <- read_latest_time(keyword, geo, "sa")
  status <- tryCatch(
    proc_keyword_latest(keyword = keyword, geo = geo, n_windows = n_windows),
    error = function(e) {
      err_msg <- conditionMessage(e)
      if (is_rate_limit_error(err_msg) && use_cache) {
        message(sprintf("  RATE LIMITED while updating %s", keyword))
        return(structure(FALSE, rate_limited = TRUE))
      }
      stop(e)
    }
  )

  if (isFALSE(status)) {
    return(list(
      keyword = keyword,
      before = before,
      after = before,
      updated = FALSE,
      rate_limited = isTRUE(attr(status, "rate_limited"))
    ))
  }

  proc_combine_freq(keyword = keyword, geo = geo)
  proc_seas_adj(keyword = keyword, geo = geo)
  after <- read_latest_time(keyword, geo, "sa")

  list(
    keyword = keyword,
    before = before,
    after = after,
    updated = !is.na(after) && !identical(before, after),
    rate_limited = any(!unlist(status))
  )
}

# Attempt updates for each keyword ------------------------------------------
message(paste0(rep("=", 70), collapse = ""))
message("STEP 1: Detecting date ranges and attempting incremental updates")
message(paste0(rep("=", 70), collapse = ""))
message("")

# First, detect which dates to download for each keyword
message("Auto-detecting latest date in each keyword's archive:")
for (kw in keywords) {
  max_date <- read_latest_time(kw, "CH", "sa")
  if (!is.na(max_date)) {
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
results <- vector("list", length(keywords))
for (kw in keywords) {
  result <- update_keyword(kw, geo = "CH")
  results[[which(keywords == kw)[1]]] <- result
  if (isTRUE(result$rate_limited)) {
    all_downloaded <- FALSE
    if (isTRUE(stop_after_rl)) {
      message("Stopping after the first rate limit to avoid extending the block.")
      break
    }
  }
  if (!identical(kw, tail(keywords, 1)) && pause_val > 0) {
    message(sprintf("Cooling down for %s seconds before the next keyword...", pause_val))
    Sys.sleep(pause_val)
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

message("")
message("Keyword update summary:")
for (result in results[!vapply(results, is.null, logical(1))]) {
  message(sprintf(
    "  %s | before=%s | after=%s | updated=%s | rate_limited=%s",
    result$keyword,
    ifelse(is.na(result$before), "NA", as.character(result$before)),
    ifelse(is.na(result$after), "NA", as.character(result$after)),
    result$updated,
    result$rate_limited
  ))
}

# Read keywords and create indicator ----------------------------------------
message("")
message(paste0(rep("=", 70), collapse = ""))
message("STEP 2: Creating Arbeitsmarkt indicator from available data")
message(paste0(rep("=", 70), collapse = ""))
message("")

# Read keyword data (cached or newly downloaded)
data <- read_keywords(keywords, suffix = "sa", id = "detrended_seas_adj")

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
message(sprintf("Output written to: raw/ch/Arbeitsmarkt_sa.csv"))
message("")

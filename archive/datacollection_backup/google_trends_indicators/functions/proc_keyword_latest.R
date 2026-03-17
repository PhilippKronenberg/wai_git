#' Download latest data
#'
#' Downloads latest daily, weekly, and monthly data.
#'
#' @inheritParams ts_gtrends_windows
#' @param keyword A single keyword for which to process the data.
#' @param geo A character vector denoting the geographic region.
#'     Default is "CH".
#' @seealso [ts_gtrends_windows]
#'
#' @section Daily data:
#'     Downloads data for the last 90 days in two windows, with the same end
#'    date (today) but start date shifted by one day. File saved as
#'    `{keyword}_d_{today}.csv` in folder `data-raw/indicator_raw`.
#'
#' @section Weekly data:
#'     Downloads weekly data for two windows, first window starts 1 year ago,
#'     second window offset by one week, both windows end today. File saved
#'     as `{keyword}_w_{today}.csv` in folder `data-raw/indicator_raw`.
#'
#' @section Monthly data:
#'     Downloads monthly data for two windows, first window starts at
#'     2006-01-01, second window offset by  1 month, both windows end today.
#'     File saved as `{keyword}_m_{today}.csv` in folder
#'     `data-raw/indicator_raw`.
#'
proc_keyword_latest <- function(keyword = "Insolvenz",
                                geo = "CH",
                                n_windows = 12,
                                from = NULL) {
  today <- Sys.Date()
  
  # Read environment variables for rate-limiting
  wait_val <- as.integer(Sys.getenv("WAI_WAIT", unset = "20"))
  retry_val <- as.integer(Sys.getenv("WAI_RETRY", unset = "20"))
  
  # If from is not specified, start from the latest existing data
  if (is.null(from)) {
    existing_sa <- tryCatch(
      {suppressWarnings(read_keyword(keyword, geo, "sa"))},
      error = function(e) NULL
    )
    if (!is.null(existing_sa) && nrow(existing_sa) > 0) {
      from <- max(existing_sa$time, na.rm = TRUE) + 1
      message("Auto-detecting start date from existing data: ", from)
    } else {
      from <- seq(today, length.out = 2, by = "-90 days")[2]
      message("No existing data found; using default 90-day window")
    }
  }

  enhance_keyword <- function(data, keyword, geo, suffix){
    old <- read_keyword(keyword, geo, suffix) %>%
      mutate(n = as.integer(n))
    new <- aggregate_windows(data)
    write_keyword(aggregate_averages(old, new), keyword, geo, suffix)
  }

  message("Downloading keyword: ", keyword)

  message("Downloading hourly data")
  h <- ts_gtrends(
    keyword = keyword,
    geo = geo,
    time = "now 7-d",
    wait = wait_val,
    retry = retry_val,
  )
  h <- h %>%
    group_by(time) %>%
    summarize(value = mean(value, na.rm=TRUE)) %>%
    mutate(n = 1)
  # NOTE: this data is intentionally not re-written on every update, not aggregated with the rest
  write_keyword(h, keyword, geo, "h")

  message("Downloading daily data")
  # Use auto-detected from or fallback to 90 days
  daily_from <- max(from, seq(today, length.out = 2, by = "-90 days")[2])
  message(sprintf("  Daily: downloading from %s to %s", daily_from, today))
  d <- ts_gtrends_windows(
    keyword = keyword,
    geo = geo,
    from = daily_from,
    stepsize = "1 day", windowsize = "3 months",
    n_windows = n_windows, wait = wait_val, retry = retry_val,
    prevent_window_shrinkage = FALSE
  )
  enhance_keyword(d, keyword, geo, "d")

  message("Downloading weekly data")
  # Use auto-detected from or fallback to 1 year
  weekly_from <- max(from, seq(today, length.out = 2, by = "-1 year")[2])
  message(sprintf("  Weekly: downloading from %s to %s", weekly_from, today))
  w <- ts_gtrends_windows(
    keyword = keyword,
    geo = geo,
    from = weekly_from,
    stepsize = "1 week", windowsize = "1 year",
    n_windows = n_windows, wait = wait_val, retry = retry_val,
    prevent_window_shrinkage = FALSE
  )
  enhance_keyword(w, keyword, geo, "w")

  message("Downloading monthly data")
  # For monthly, use auto-detected from but not earlier than 2006
  monthly_from <- as.character(max(as.Date(from), as.Date("2006-01-01")))
  message(sprintf("  Monthly: downloading from %s to %s", monthly_from, today))
  m <- ts_gtrends_windows(
    keyword = keyword,
    geo = geo,
    from = monthly_from,
    stepsize = "1 month", windowsize = "20 years",
    n_windows = n_windows, wait = wait_val, retry = retry_val,
    prevent_window_shrinkage = FALSE
  )
  enhance_keyword(m, keyword, geo, "m")
}

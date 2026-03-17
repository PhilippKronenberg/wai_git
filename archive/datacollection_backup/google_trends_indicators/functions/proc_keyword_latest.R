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
  
  safe_read_keyword <- function(suffix) {
    tryCatch({
      suppressWarnings(read_keyword(keyword, geo, suffix))
    }, error = function(e) NULL)
  }

  # If from is not specified, start from the latest existing seasonal-adjusted data
  if (is.null(from)) {
    existing_sa <- safe_read_keyword("sa")
    if (!is.null(existing_sa) && nrow(existing_sa) > 0) {
      from <- max(as.Date(existing_sa$time), na.rm = TRUE) + 1
      message("Auto-detecting start date from existing SA data: ", from)
    } else {
      from <- seq(today, length.out = 2, by = "-90 days")[2]
      message("No existing SA data found; using default 90-day window")
    }
  }

  enhance_keyword <- function(data, keyword, geo, suffix){
    old <- safe_read_keyword(suffix)
    if (is.null(old) || nrow(old) == 0) {
      write_keyword(aggregate_windows(data), keyword, geo, suffix)
      return(invisible(NULL))
    }
    old <- old %>% mutate(n = as.integer(n))
    new <- aggregate_windows(data)
    write_keyword(aggregate_averages(old, new), keyword, geo, suffix)
  }

  message("Downloading keyword: ", keyword)

  safe_gtrends_windows <- function(expr, label) {
    tryCatch(
      expr,
      error = function(e) {
        err_msg <- conditionMessage(e)
        if (grepl("No data returned by the query|No data returned|429|rate limit|too soon", err_msg, ignore.case = TRUE)) {
          message("  WARNING: ", label, " failed with no data / rate limit; using cached data if available")
          return(NULL)
        }
        stop(e)
      }
    )
  }

  message("Downloading hourly data")
  h <- tryCatch(
    ts_gtrends(
      keyword = keyword,
      geo = geo,
      time = "now 7-d",
      wait = wait_val,
      retry = retry_val
    ),
    error = function(e) {
      err_msg <- conditionMessage(e)
      if (grepl("No data returned by the query|No data returned|429|rate limit|too soon", err_msg, ignore.case = TRUE)) {
        message("  WARNING: hourly query failed; keeping existing hourly cache")
        return(NULL)
      }
      stop(e)
    }
  )
  if (!is.null(h)) {
    h <- h %>%
      group_by(time) %>%
      summarize(value = mean(value, na.rm = TRUE)) %>%
      mutate(n = 1)
    write_keyword(h, keyword, geo, "h")
  }

  message("Downloading daily data")
  existing_d <- safe_read_keyword("d")
  if (!is.null(existing_d) && nrow(existing_d) > 0) {
    daily_from <- max(as.Date(max(existing_d$time, na.rm = TRUE)) + 1, as.Date(from))
  } else {
    daily_from <- as.Date(from)
  }
  if (daily_from > today) {
    message(sprintf("  Daily already up to date: latest existing date is %s", daily_from - 1))
  } else {
    message(sprintf("  Daily: downloading from %s to %s", daily_from, today))
    d <- safe_gtrends_windows(
      ts_gtrends_windows(
        keyword = keyword,
        geo = geo,
        from = as.character(daily_from),
        stepsize = "1 day",
        windowsize = "3 months",
        n_windows = n_windows,
        wait = wait_val,
        retry = retry_val,
        prevent_window_shrinkage = FALSE
      ),
      "daily"
    )
    if (!is.null(d)) {
      enhance_keyword(d, keyword, geo, "d")
    }
  }

  existing_w <- safe_read_keyword("w")
  if (!is.null(existing_w) && nrow(existing_w) > 0) {
    weekly_from <- max(as.Date(max(existing_w$time, na.rm = TRUE)) + 7, as.Date(from))
  } else {
    weekly_from <- as.Date(from)
  }
  if (weekly_from > today) {
    message(sprintf("  Weekly already up to date: latest existing date is %s", weekly_from - 7))
  } else {
    message(sprintf("  Weekly: downloading from %s to %s", weekly_from, today))
    w <- safe_gtrends_windows(
      ts_gtrends_windows(
        keyword = keyword,
        geo = geo,
        from = as.character(weekly_from),
        stepsize = "1 week",
        windowsize = "1 year",
        n_windows = n_windows,
        wait = wait_val,
        retry = retry_val,
        prevent_window_shrinkage = FALSE
      ),
      "weekly"
    )
    if (!is.null(w)) {
      enhance_keyword(w, keyword, geo, "w")
    }
  }

  message("Downloading monthly data")
  existing_m <- safe_read_keyword("m")
  monthly_from <- as.Date(max(as.Date(from), as.Date("2006-01-01")))
  if (!is.null(existing_m) && nrow(existing_m) > 0) {
    monthly_from <- max(monthly_from, as.Date(max(existing_m$time, na.rm = TRUE)) + 1)
  }
  if (monthly_from > today) {
    message(sprintf("  Monthly already up to date: latest existing date is %s", monthly_from - 1))
  } else {
    message(sprintf("  Monthly: downloading from %s to %s", monthly_from, today))
    m <- safe_gtrends_windows(
      ts_gtrends_windows(
        keyword = keyword,
        geo = geo,
        from = as.character(monthly_from),
        stepsize = "1 month",
        windowsize = "20 years",
        n_windows = n_windows,
        wait = wait_val,
        retry = retry_val,
        prevent_window_shrinkage = FALSE
      ),
      "monthly"
    )
    if (!is.null(m)) {
      enhance_keyword(m, keyword, geo, "m")
    }
  }
}

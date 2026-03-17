#' There is one datacolumn missing: canton of Fribourg. Assmunig that there is France confounded with Fribourg.
#' @import data.table
#' @importFrom readxl read_excel
#' @importFrom httr GET content write_disk
#' @importFrom rvest html_node html_attr
source_ch.swissgrid <- function() {
  dataset <- "ch.swissgrid"

  get_data <- function(year) {
    message("Grabbing data for ", year)

    # Find the link ('cause swissgrid also is not capable of maintaining file formats)
    overview <- content(GET("https://www.swissgrid.ch/de/home/operation/grid-data.html"))
    link <- html_attr(
      html_node(
        overview,
        xpath = sprintf("//a[contains(@href, '%d')]", year)
      ),
      "href"
    )

    if(length(link) == 0 || is.na(link)) {
      stop("Could not determine the download link!")
    }

    url <- sprintf("https://www.swissgrid.ch%s", link)

    tmp <- sprintf("tmp/%s", basename(link))

    # Somehow download_dataset messes up the file
    GET(url, write_disk(tmp, overwrite = TRUE))

    # No tibbles in my house thanks!
    dta <- data.table(read_excel(tmp, col_types = c("date", rep("numeric", 64)), sheet = 3))[-1]
    names(dta)[1] <- "time"

    canton_cols <- grepl("(Kanton|^time$)", names(dta))
    dta_cantons <- melt(dta[, canton_cols, with = FALSE], id.vars = "time")[!is.na(value)]
    dta_cantons[, time := as.Date(time)]
    dta_cantons <- dta_cantons[, .(value = sum(value)), by = list(time, variable)]
    dta_cantons[, direction := ifelse(grepl("Verbrauch", variable), "out", "in")]
    # TODO: While "proper", regmatches may actually be slower than gsub?
    dta_cantons[, geo := regmatches(variable, regexec("[A-Z]{2}(?:, [A-Z]{2})?", variable))][, geo := tolower(gsub(", ", "", geo))]
    # Weird...
    dta_cantons[geo == "character(0)", geo := "bridging"]

    io_cols <- grepl("(^time$|->)", names(dta))
    dta_io <- melt(dta[, io_cols, with = FALSE], id.vars = "time")[!is.na(value)]
    dta_io[, time := as.Date(time)]
    dta_io <- dta_io[, .(value = sum(value)), by = list(time, variable)]
    dta_io[, direction := ifelse(grepl("CH->", variable), "out", "in")]
    dta_io[, geo := regmatches(variable, regexec("[A-Z]{2}->[A-Z]{2}", variable))]
    dta_io[, geo := tolower(gsub("CH", "", gsub("->", "", geo)))]

    tot_cols <- grepl("(^time$|Summe)", names(dta))
    dta_tot <- melt(dta[, tot_cols, with = FALSE], id.vars = "time")[!is.na(value)]
    dta_tot[, time := as.Date(time)]
    dta_tot <- dta_tot[, .(value = sum(value)), by = list(time, variable)]
    dta_tot[, geo := "ch"]
    dta_tot[grepl("endverbraucht", variable), direction := "out"]
    dta_tot[grepl("produziert", variable), direction := "in"]
    dta_tot[is.na(direction), direction := "out_incl_losses"]

    rbindlist(list(
      dta_cantons[, .(time, direction, geo, value)],
      dta_io[, .(time, direction, geo, value)],
      dta_tot[, .(time, direction, geo, value)]
    ))[data.table::year(time) == year]
  }

  year_start <- year(Sys.Date())
  if(!file.exists(get_output_path(dataset))) {
    year_start <- 2018
  }

  dta <- rbindlist(lapply(year_start:year(Sys.Date()), get_data))

  update_dataset(dta, get_output_path(dataset))



  # metadata ----------------------------------------------------------------

  md <- list(
    title = list(
      de = "Energieübersicht Schweiz"
    ),
    source.name = list(
      de = "Swissgrid"
    ),
    source_url = "https://www.swissgrid.ch/de/home/operation/grid-data.html",
    units = list(
      all = "kWh"
    ),
    aggregate = list(
      all = "sum"
    ),
    dim.order = c("direction", "geo"),
    hierarchy = list(
      geo = list(
        direction = list(
          "in" = NA,
          out = NA,
          out_incl_losses = NA
        ),
        geo = list(
          ch = list(
            ag = NA,
            aiar = NA,
            beju = NA,
            blbs = NA,
            bridging = NA,
            gevd = NA,
            gl = NA,
            gr = NA,
            lu = NA,
            ne = NA,
            ownw = NA,
            sg = NA,
            shzh = NA,
            so = NA,
            szzg = NA,
            tg = NA,
            ti = NA,
            vs = NA
          ),
          de = NA,
          it = NA,
          fr = NA,
          at = NA
        )
      )
    ),
    labels = list(
      dimnames = list(
        direction = list(
          de = "Einspeisung/Verbrauch"
        ),
        geo = list(
          de = "Produzent/Verbraucher"
        )
      ),
      direction = list(
        "in" = list(
          de = "Einspeisung"
        ),
        out = list(
          de = "Verbrauch"
        ),
        out_incl_losses = list(
          de = "Verbrauch inklusive Verluste, Pumpenergie bei Pumpspeicherkraftwerken und Eigenbedarf von Kraftwerken"
        )
      ),
      geo = list(
        ch = list(
          ag = list(
            de = "Aargau"
          ),
          aiar = (
            de = "Appenzell Inner-/Ausserrhoden"
          ),
          beju = (
            de = "Bern und Jura"
          ),
          blbs = (
            de = "Basel Stadt und Basel Landschaft"
          ),
          bridging = (
            de = "Kantonsübergreifend"
          ),
          gevd = (
            de = "Genf und Waadt"
          ),
          gl = (
            de = "Glarus"
          ),
          gr = (
            de = "Graubünden"
          ),
          lu = (
            de = "Luzern"
          ),
          ne = (
            de = "Neuenburg"
          ),
          ownw = (
            de = "Ob-/Nidwalden"
          ),
          sg = (
            de = "St. Gallen"
          ),
          shzh = (
            de = "Schaffhausen und Zürich"
          ),
          so = (
            de = "Solothurn"
          ),
          szzg = (
            de = "Schwyz und Zug"
          ),
          tg = (
            de = "Thurgau"
          ),
          ti = (
            de = "Tessing"
          ),
          vs = (
            de = "Wallis"
          )
        ),
        de = (
          de = "Deutschland"
        ),
        it = (
          de = "Italien"
        ),
        fr = (
          de = "Frankreich"
        ),
        at = (
          de = "Österreich"
        )
      )
    ),
    details = list(
      de = "Die Zahlen für Deutschland, Italien, Frankreich und Österreich sind als Importe/Exporte zu verstehen."
    ),
    utc.updated = Sys.time()
  )

  writeLines(toJSON(md,
                    pretty = TRUE,
                    auto_unbox = TRUE,
                    null = "null"),
             get_output_path(dataset, meta = TRUE))
}

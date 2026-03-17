# Create user library if it doesn't exist
user_lib <- Sys.getenv("R_LIBS_USER")
if (user_lib == "") {
  user_lib <- path.expand("~/R/library")
}
dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

# List of packages to install
packages <- c("zoo", "tstools", "tempdisagg", "forecast", "timeseriesdb", 
              "seasonal", "tis", "prophet", "tsbox", "dplyr", "tibble", 
              "TTR", "gtrendsR", "readr", "tidyr")

# Install packages
message("Installing packages to: ", user_lib)
for (pkg in packages) {
  if (!require(pkg, character.only = TRUE)) {
    message("Installing ", pkg, "...")
    install.packages(pkg, repos = "http://cran.r-project.org", 
                   lib = user_lib, dependencies = TRUE)
  } else {
    message(pkg, " already installed")
  }
}

message("Installation complete!")

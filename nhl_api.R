







###################################
#' global variable that records the last time we hit the NHL API
#'
NHL_API_LAST_CALL <- Sys.time()







###################################
#' global variable that hold the NHL Cache it is a data frame with each page response as a row and some meta data in each column
#'
#'
nhl_cache_get_blank <- function(){
    tibble(key = character(),
           url = character(),
           fetched =  as.POSIXct(NA),
           val = list())
}


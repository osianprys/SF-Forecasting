
# Creating Forecast Table -------------------------------------------------


# Importing Packages ------------------------------------------------------

if(!require(DBI)) {
  install.packages("DBI")
  library(DBI)
}

if(!require(RPostgreSQL)) {
  install.packages("RPostgreSQL")
  library(RPostgreSQL)
}


#############################

##install lubridate 
if(!require(lubridate)) {
  install.packages("lubridate")
  library(lubridate)
}


##install forecast
if(!require(forecast)) {
  install.packages("forecast")
  library(forecast)
}

if(!require(purrr)) {
  install.packages("purrr")
}
library(purrr)

if(!require(zoo)) {
  install.packages("zoo")
}
library(zoo)

if(!require(tidyverse)){
  install.packages("tidyverse")
}
library(tidyverse)

# Functions ---------------------------------------------------------------


# function to build queries for SPC given a start/end date and customer base
buildSPCQuery <- function(start, end, base = c("TRADEPLUS",
                                               "TRADE",
                                               "B2B",
                                               "NTS")){
  
  bcodes <- switch(base,
                   TRADEPLUS = "and brand_code in ('PLUMBFIX', 'ELECTRICFX')",
                   TRADE = "and po.kf_trade_split_validated = 'Trade' and last_submit_brand not in ('PLUMBFIX', 'ELECTRICFX')",
                   B2B = "and po.kf_trade_split_validated = 'B2B'",
                   NTS = "and po.kf_trade_split_validated = 'NTS'")
  
  qry <- paste("select
               sum(fulfilled_sales)::float/count(distinct case when order_type = 'SALESORDER' then po.rp_person_id end)::float as spc
               from sf_analytics.prod_order po
               left join sf_analytics.prod_person pp
               on po.rp_person_id = pp.rp_person_id
               where
               po.rp_person_id > 0
               and
               cal_submit_date between '", start, "' and '", end,
                      "'", bcodes)
  qry
}




# This function will call the build query function and run the query - storing the results
runSPCQuery <- function(start, end, base = c("TRADEPLUS",
                                              "TRADE",
                                              "B2B",
                                              "NTS"), conn){
  qry <- buildSPCQuery(start, end, base)
  res <- dbGetQuery(conn, qry)
  res$start <- start
  res$end <- end
  res$base <- base
  res
}



# function to build queries for customer base size given a start/end date and customer base
buildNBaseQuery <- function(start, end, base = c("TRADEPLUS",
                                                 "TRADE",
                                                 "B2B",
                                                 "NTS")){
  bcodes <- switch(base,
                   TRADEPLUS = "and brand_code in ('PLUMBFIX', 'ELECTRICFX')",
                   TRADE = "and po.kf_trade_split_validated = 'Trade' and last_submit_brand not in ('PLUMBFIX', 'ELECTRICFX')",
                   B2B = "and po.kf_trade_split_validated = 'B2B'",
                   NTS = "and po.kf_trade_split_validated = 'NTS' or po.kf_trade_split_validated is null" )
  
  qry <- paste("select
               count(distinct case when order_type = 'SALESORDER' then po.rp_person_id end) as customers
               from sf_analytics.prod_order po
               left join sf_analytics.prod_person pp
               on po.rp_person_id = pp.rp_person_id
               where
               po.rp_person_id > 0
               and cal_submit_date between '", start, "' and '",  end,
               "'", bcodes )
  qry
}

# This function will call the build query function and run the query - storing the results
runNBaseQuery <- function(start, end, base = c("TRADEPLUS",
                                               "TRADE",
                                               "B2B",
                                               "NTS"), conn){
  qry <- buildNBaseQuery(start, end, base)
  res <- dbGetQuery(conn, qry)
  res$start <- start
  res$end <- end
  res$base <- base
  res
}


# Function to create list of start and end date sequences. Each element
# of start and end represent the start and end of one calculation window. 
# The window size is set using `window` argument and the rolling step size is 
# set with the `by` argument
create_query_dates <- function(start,
                               end,
                               by = c("year", "month"),
                               window = c("year", "month")) {
  
  window = switch(window,
                  year = years(1),
                  month = months(1))
  
  start_date = seq.Date(from = start,
                        to = end - window,
                        by = by) 
  
  end_date = seq.Date(from = start + window,
                      to = end,
                      by = by) - days(1)
  
  dates <- list(start = start_date,
                end = end_date)
  
}


# Connecting to database --------------------------------------------------


# Load credentials for your own Redshift login.
file <- paste0(getwd()[-2],"/credentials/my_credentials.csv")
my_credentials <- read.csv(file, header = TRUE, sep = ",")

# Load the credentials into variables.
port1 <- as.character(my_credentials[1,1])
host1 <- as.character(my_credentials[1,2])
user1 <- as.character(my_credentials[1,3])
password1 <- as.character(my_credentials[1,4])
dbname1 <- as.character(my_credentials[1,5])


# Import the data
## Connect to Redshift database ##
conn <- DBI::dbConnect(
  drv = RPostgreSQL::PostgreSQL(),
  dbname = dbname1, 
  host = host1, 
  port = port1, 
  user = user1, 
  password = password1
)



# SPC calculation ---------------------------------------------------------

cbases <-  list(TRADEPLUS = "TRADEPLUS",
                TRADE = "TRADE",
                B2B ="B2B",
                NTS = "NTS")

# Create the date intervals overwhich the SPC calculation is performed

dates <- create_query_dates(ymd("2014-12-01"),
                            ymd("2018-03-01"),
                            by = "month",
                            window = "year") 


# Map construct to query and return SPC for each customer base (`cbase`) over each 
# calculation window in `dates`.
# For each element is `cbase` the pmap performs the query over all of `start`
# and `end` and using `reduce(rbind)` binds the results into a single df.

SPC_df <- cbases %>% 
          map_df(~pmap_df(list(dates$start,
                               dates$end),
                               function(x, y) {runSPCQuery(as.Date(x),
                                                           as.Date(y),
                                                           base = .x,
                                                           conn)})) 


# SPC forecasts -----------------------------------------------------------

tday <- today()

next_fin_yr <- ifelse(tday > ymd(paste(year(tday), "02-01")),
                      ymd(paste(year(tday + years(1)), "02-01")),
                      ymd(paste(year(tday), "02-01"))) %>% as.Date()


n_fin_yr = length(seq.Date(from = tday,
                           to = next_fin_yr,
                           by = "month"))

n_15 = 15


SPC_ts_list <- SPC_df %>%
               split(.$base) %>%
               map(., ~ts(.x$spc, start = c(year(min(.x$end)),
                                     month(max(.x$end))),
                       frequency = 12))


SPC_fit_list <- SPC_ts_list %>%
                map(~auto.arima(.x))

SPC_fcast_list_15 <- SPC_fit_list %>%
                     map(~forecast(.x, h = n_15))



SPC_tday_values <- map_df(map(SPC_fcast_list_15, "x"), ~tail(.x, 1)) %>%
                   gather(key = "Base", value = "SPC-Today")


SPC_fcast_values <- map_df(map(SPC_fcast_list_15, "mean"), ~tail(.x, 1)) %>%
                    gather(key = "Base", value = "SPC-15")

SPC_fcast_upper <- map(map(SPC_fcast_list_15, "upper"), ~tail(.x, 1)) %>%
                   reduce(rbind) %>% 
                   as.data.frame()


SPC_fcast_table <- left_join(SPC_tday_values, SPC_fcast_values) %>%
                   cbind(., SPC_fcast_upper) %>%
                   mutate(SPC_Var95 = `95%` - `SPC-15`,
                          'SPC_Growth-Percent' =
                            100*((`SPC-15`-`SPC-Today`)/`SPC-Today`)) %>%
                   select(-`80%`, -`95%`) %>%
                   map_if(is.numeric, ~round(.x, 2)) %>%
                   as.data.frame()



# nBase calculation ---------------------------------------------------------

cbases <-  c("TRADEPLUS", "TRADE", "B2B", "NTS")

# Create the date intervals overwhich the SPC calculation is performed

dates <- create_query_dates(ymd("2014-12-01"),
                            ymd("2018-03-01"),
                            by = "month",
                            window = "year") 

# Map construct to query and return customer base size for each  
# customer base (`cbase`) over each calculation window in `dates`.
# For each element is `cbase` the pmap performs the query over all of `start`
# and `end` and using `reduce(rbind)` binds the results into a single df.

nBase_df <- cbases %>% 
            map_df(~pmap_df(list(dates$start,
                                 dates$end),
                                 function(x, y) {runNBaseQuery(as.Date(x),
                                                               as.Date(y),
                                                               base = .x,
                                                               conn)})) 




# nBase forecasts -----------------------------------------------------------

tday <- today()

next_fin_yr <- ifelse(tday > ymd(paste(year(tday), "02-01")),
                      ymd(paste(year(tday + years(1)), "02-01")),
                      ymd(paste(year(tday), "02-01"))) %>% as.Date()


n_fin_yr = length(seq.Date(from = tday,
                           to = next_fin_yr,
                           by = "month"))

n_15 = 15


nBase_ts_list <- nBase_df %>%
                 split(.$base) %>%
                 map(., ~ts(.x$customers,
                            start = c(year(min(.x$end)), month(max(.x$end))),
                            frequency = 12))


nBase_fit_list <- nBase_ts_list %>%
                  map(~auto.arima(.x))

nBase_fcast_list_15 <- nBase_fit_list %>%
                       map(~forecast(.x, h = 15))



nBase_tday_values <- map_df(map(nBase_fcast_list_15, "x"), ~tail(.x, 1)) %>%
                     gather(key = "Base", value = "nBase-Today")


nBase_fcast_values <- map_df(map(nBase_fcast_list_15, "mean"), ~tail(.x, 1)) %>%
                      gather(key = "Base", value = "nBase-15")

nBase_fcast_upper <- map(map(nBase_fcast_list_15, "upper"), ~tail(.x, 1)) %>%
                     reduce(rbind) %>% 
                     as.data.frame()


nBase_fcast_table <- left_join(nBase_tday_values, nBase_fcast_values) %>%
                     cbind(., nBase_fcast_upper) %>%
                     mutate(nBase_Var95 = `95%` - `nBase-15`,
                            'nBase_Growth-Percent' =
                             100*((`nBase-15`-`nBase-Today`)/`nBase-Today`)) %>%
                     select(-`80%`, -`95%`) %>%
                     map_if(is.numeric, ~round(.x, 2)) %>%
                     as.data.frame()



# Joining SPC and nBase ---------------------------------------------------

fcast_table <- left_join(SPC_fcast_table, nBase_fcast_table,
                         by = c("Base" = "Base")) %>%
               select(Base,
                      SPC.Today,
                      nBase.Today,
                      SPC.15,
                      nBase.15,
                      SPC_Var95,
                      nBase_Var95,
                      SPC_Growth.Percent,
                      nBase_Growth.Percent)


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

# Functions ---------------------------------------------------------------

buildSPCQuery <- function(start, end, base = c("TRADEPLUS",
                                               "TRADE",
                                               "B2B",
                                               "NTS")){
  
  bcodes <- switch(base,
                   TRADEPLUS = "and brand_code in ('PLUMBFIX', 'ELECTRICFX')",
                   TRADE = "and po.kf_trade_split_validated = 'Trade' and last_submit_brand not in ('PLUMBFIX', 'ELECTRICFX')",
                   B2B = "and po.kf_trade_split_validated = 'B2B'",
                   NTS = "and po.kf_trade_split_validated = 'NTS' or po.kf_trade_split_validated is null")
  
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





runSPCQuery <- function(start, end, base = c("TRADEPLUS",
                                              "TRADE",
                                              "B2B",
                                              "NTS"), conn){
  qry <- buildSPCQuery(start, end, base)
  res <- dbGetQuery(conn, qry)
  res$start <- start
  res$end <- end
  res
}

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

runNBaseQuery <- function(start, end, base = c("TRADEPLUS",
                                               "TRADE",
                                               "B2B",
                                               "NTS"), conn){
  qry <- buildNBaseQuery(start, end, base)
  res <- dbGetQuery(conn, qry)
  res$start <- start
  res$end <- end
  res
}

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


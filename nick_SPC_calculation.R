
##################### connect ######################
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

if(!requie(purrr)) {
  install.packages("purrr")
}
library(purrr)


# Functions ---------------------------------------------------------------

buildSPCQuery <- function(start, end){
  qry <- paste(" select 
                    sum(fulfilled_sales)/ (count(distinct rp_person_id)) 
                      AS SPC
                      from 
                      sf_analytics.prod_order
                      where cal_submit_date between '", start, "' and '", end,"' 
                      and brand_code in ('PLUMBFIX', 'ELECTRICFX')")
}

runSPCQuery <- function(start, end, conn){
  qry <- buildSPCQuery(start, end)
  res <- dbGetQuery(conn, qry)
  res$start <- start
  res$end <- end
  res
}



buildNBaseQuery <- function(start, end){
  qry <- paste("select
               count(distinct rp_person_id) as customers
               from sf_analytics.prod_order
               where
               cal_submit_date between '", start, "' and '",  end,"' 
               and brand_code in ('PLUMBFIX', 'ELECTRICFX')")
  qry
}

runNBaseQuery <- function(start, end, conn){
  qry <- buildNBaseQuery(start, end)
  res <- dbGetQuery(conn, qry)
  res$start <- start
  res$end <- end
  res
}



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


# Month on Month

start_date = seq.Date(from = ymd("2014-12-01"),
                      to = ymd("2018-01-01"),
                      by = "month")

end_date = seq.Date(from = ymd("2015-01-01"),
                    to = ymd("2018-02-01"),
                     by = "month")


# Year on Year

start_date = seq.Date(from = ymd("2014-12-01"),
                      to = ymd("2017-01-01"),
                      by = "year")

end_date = seq.Date(from = ymd("2015-12-01"),
                    to = ymd("2018-01-01"),
                    by = "year")



# Yearly Month on Month (comparing year on year for each month)


start_date = seq.Date(from = ymd("2014-12-01"),
                      to = ymd("2017-01-01"),
                      by = "month")

end_date = seq.Date(from = ymd("2015-12-01"),
                    to = ymd("2018-01-01"),
                    by = "month")



tic()
test <- map2(start_date, end_date, ~runSPCQuery(.x, .y, conn)) %>% reduce(rbind)
toc()




tic()
test2 <- map2(start_date, end_date, ~runNBaseQuery(.x, .y, conn)) %>% reduce(rbind)
tic()
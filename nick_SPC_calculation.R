
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

if(!requie(zoo)) {
  install.packages("zoo")
}
library(zoo)



# Functions ---------------------------------------------------------------

buildSPCQuery <- function(start, end, base = "TRADEPLUS"){
  
  bcodes <- switch(base,
                   TRADEPLUS = "and brand_code in ('PLUMBFIX', 'ELECTRICFX')")
  
  qry <- paste(" select 
                    sum(fulfilled_sales)/ (count(distinct rp_person_id)) 
                      AS SPC
                      from 
                      sf_analytics.prod_order
                      where cal_submit_date between '", start, "' and '", end,
                      "'", bcodes )
  qry
}

runSPCQuery <- function(start, end, base = "TRADEPLUS" conn){
  qry <- buildSPCQuery(start, end, base)
  res <- dbGetQuery(conn, qry)
  res$start <- start
  res$end <- end
  res
}



buildNBaseQuery <- function(start, end, base = "TRADEPLUS"){
  bcodes <- switch(base,
                   TRADEPLUS = "and brand_code in ('PLUMBFIX', 'ELECTRICFX')")
  
  qry <- paste("select
               count(distinct rp_person_id) as customers
               from sf_analytics.prod_order
               where
               cal_submit_date between '", start, "' and '",  end,
               "'", bcodes )
  qry
}

runNBaseQuery <- function(start, end, base = "TRADEPLUS", conn){
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


# # Year on Year
# 
# start_date = seq.Date(from = ymd("2014-12-01"),
#                       to = ymd("2017-01-01"),
#                       by = "year")
# 
# end_date = seq.Date(from = ymd("2015-12-01"),
#                     to = ymd("2018-01-01"),
#                     by = "year")
# 
# 

# Yearly Month on Month (comparing year on year for each month)


start_date = seq.Date(from = ymd("2014-12-01"),
                      to = ymd("2017-01-01"),
                      by = "month")

end_date = seq.Date(from = ymd("2015-12-01"),
                    to = ymd("2018-01-01"),
                    by = "month")




SPC_data <- map2(start_date,
                 end_date, ~runSPCQuery(.x, .y, conn)) %>% reduce(rbind)




nBase_data <- map2(start_date,
                   end_date, ~runNBaseQuery(.x, .y, conn)) %>% reduce(rbind)





# Forecasting -------------------------------------------------------------


create_test_train <- function(ts, dates, h = 15){
  
  min_date = min(dates)
  max_date = max(dates)
  list(holdout = tail(ts, h),
       train = ts(ts,
                  frequency = 12,
                  start = c(year(min_date),
                            month(min_date)),
                  end = c(year(max_date - months(h)),
                          month(max_date - months(h)))))
}






SPC_ts <- ts(SPC_data$spc,
             start = c(year(min(SPC_data$end)),
                       month(max(SPC_data$end))),
             frequency = 12)

nBase_ts <- ts(nBase_data$customers,
               start = c(year(min(nBase_data$end)),
                         month(max(nBase_data$end))),
               frequency = 12)


# SPC

dates = zoo::as.Date(time(SPC_ts))

samples <- create_test_train(SPC_ts, dates, h = 15)

fit <- auto.arima(samples$train)

pred = forecast(fit, h = 15)

ts.plot(samples$train, pred$mean, samples$holdout, lty = c(1, 3, 5),
        ylim = c(0, max(pred$mean, samples$train, samples$holdout)))


accuracy(f = pred$mean, 
         x = samples$holdout)


#nBase


dates = zoo::as.Date(time(nBase_ts))

samples <- create_test_train(nBase_ts, dates, h = 6)

fit <- auto.arima(samples$train)

pred = forecast(fit, h = 6)

ts.plot(samples$train, pred$mean, samples$holdout, lty = c(1, 3, 5))


accuracy(f = pred$pred, 
         x = samples$holdout)

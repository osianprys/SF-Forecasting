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


if(!require(xts)){
  install.packages("xts")
}
library(xts)



if(!require(tidyverse)){
  install.packages("tidyverse")
}
library(tidyverse)


if(!require(tseries)){
  install.packages("tseries")
}
library(tseries)


# Functions ---------------------------------------------------------------

create_test_train <- function(ts, h = 1){
  dates = as.Date(time(sf_tradeplus_ts))
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




# Data import -------------------------------------------------------------



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



# query1 <- paste(
#   "Select 
#    left(cal_submit_date, 4) || '-' || substring(cal_submit_date, 6, 2) || '-28' as date,
#    rp_person_id,
#    sum(fulfilled_sales)
#   from
#   sf_analytics.prod_order
#   where
#   cal_submit_date between dateadd(month, -24, '2018-04-01') and '2018-04-01'
#   and brand_code in ('PLUMBFIX', 'ELECTRICFX')
#   group by 1, rp_person_id
#   "
# )# Run the SQL Query.


query1 <- paste(
  "Select 
   left(cal_submit_date, 4) || '-' || substring(cal_submit_date, 6, 2) || '-28' as date,

   sum(fulfilled_sales)
  from
  sf_analytics.prod_order
  where
  cal_submit_date between dateadd(month, -48, '2018-03-28') and '2018-03-28'
  and brand_code in ('PLUMBFIX', 'ELECTRICFX')
  group by 1
  "
)# Run the SQL Query.




dat <- dbGetQuery(conn, query1)


dat$date <- ymd(dat$date)


# Creating time series ----------------------------------------------------

# Ordering data (migrate to SQL)

dat <- dat %>%
       arrange(date)

# drop first row due to incomplete sum

dat <- dat[-1, ]

min_date <- min(dat$date)
max_date <- max(dat$date)


sf_tradeplus_ts = ts(data = dat$sum,
                      start = c(year(min_date), month(min_date)),
                      end = c(year(max_date), month(max_date)),
                      frequency = 12)




# EDA ---------------------------------------------------------------------


# Raw sales plot - notice trend/seasonality/variance
plot.xts(as.xts(sf_tradeplus_ts/1E6),
         type = "o",
         ylab = "Sum of Sales [£m]",
         xlab = "Date",
         main = "Tradeplus Sales Over Time")


# Diff sales plot - notice increasing variance
plot.xts(as.xts(diff(sf_tradepluse_ts/1E6)),
         type = "o",
         ylab = "Sum of Sales [£m]",
         xlab = "Date",
         main = "Tradeplus Sales Over Time")


# log sales plot - notice trend/seasonality/variance
plot.xts(as.xts(log10(sf_tradepluse_ts/1E6)),
         type = "o",
         ylab = "Sum of Sales [£m]",
         xlab = "Date",
         main = "Tradeplus Sales Over Time")


# log sales plot - notice trend/seasonality/variance
plot.xts(as.xts(diff(log10(sf_tradepluse_ts/1E6))),
         type = "o",
         ylab = "Sum of Sales [£m]",
         xlab = "Date",
         main = "Tradeplus Sales Over Time")




boxplot(sf_tradeplus_ts~cycle(sf_tradeplus_ts))



###

zones=matrix(c(1,2), ncol=2, byrow=TRUE)

layout(zones, widths=c(4/5, 1/5))

diff_hist <- hist(diff(as.xts(sf_tradeplus_ts/1000)), 
                 plot = FALSE,
                 15)

top = max(c(diff_hist$counts))

par(mar=c(4,4,3,0))
plot.xts(diff(as.xts(sf_tradeplus_ts/1000)),
         type = "b",
         ylab = "Sum of Order Value [£m]",
         xlab = "Date",
         main = "Tradeplus Sum of Orders Differenced")

par(mar=c(4,0,3,1))
barplot(diff_hist$counts, axes=FALSE, xlim=c(0, top), space=0, horiz=TRUE)
par(oma=c(3,3,0,0))


###


###

zones=matrix(c(1,2), ncol=2, byrow=TRUE)

layout(zones, widths=c(4/5, 1/5))

diff_hist <- hist(diff(log10(as.xts(sf_tradeplus_ts/1E6))), 
                  plot = FALSE,
                  15)

top = max(c(diff_hist$counts))

par(mar=c(4,4,3,0))
plot.xts(diff(log10(as.xts(sf_tradeplus_ts/1E6))),
         type = "b",
         ylab = "Sum of Order Value [£m]",
         xlab = "Date",
         main = "Tradeplus Sum of Orders Differenced")

par(mar=c(4,0,3,1))
barplot(diff_hist$counts, axes=FALSE, xlim=c(0, top), space=0, horiz=TRUE)
par(oma=c(3,3,0,0))


###

monthplot(sf_tradeplus_ts)
seasonplot(sf_tradeplus_ts)

autoplot(stl(sf_tradeplus_ts/1E6,
         s.window = 'periodic')) + geom_point()



# Diff log sales plot - looks stationary
plot.xts(as.xts(diff(log10(sf_tradeplus_ts/1E6))),
         type = "o",
         ylab = "Sum of Sales [£m]",
         xlab = "Date",
         main = "Tradeplus Sales Over Time")

# Testing stationarity

adf_raw <-  adf.test(sf_tradeplus_ts, 
                     alternative = "stationary")

adf_diff <- adf.test(diff(sf_tradeplus_ts), 
                     alternative = "stationary")

adf_diff_log <- adf.test(diff(log10(sf_tradeplus_ts)), 
                         alternative = "stationary")




# ACF / PACF

acf(sf_tradeplus_ts)
acf(diff(sf_tradeplus_ts))

pacf(sf_tradeplus_ts)
pacf(diff(sf_tradeplus_ts))




# quick fit

samples <- create_test_train(sf_tradeplus_ts, 15)


fit <- auto.arima(samples$train)

pred = predict(fit, n.ahead = 15)

ts.plot(samples$train, pred$pred, samples$holdout, lty = c(1, 3, 5))


accuracy(f = pred$pred, 
         x = samples$holdout)


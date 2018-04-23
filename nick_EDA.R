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

# Ordering data 

dat <- dat %>%
       arrange(date)

# drop first row due to incomplete sum

dat <- dat[-1, ]

min_date <- min(dat$date)
max_date <- max(dat$date)


sf_tradepluse_ts = ts(data = dat$sum,
                      start = c(year(min_date), month(min_date)),
                      end = c(year(max_date), month(max_date)),
                      frequency = 12)




# EDA ---------------------------------------------------------------------


# Raw sales plot - notice trend/seasonality/variance
plot.xts(as.xts(sf_tradepluse_ts/1E6),
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


# Diff log sales plot - looks stationary
plot.xts(as.xts(diff(log10(sf_tradepluse_ts/1E6))),
         type = "o",
         ylab = "Sum of Sales [£m]",
         xlab = "Date",
         main = "Tradeplus Sales Over Time")

# Testing stationarity

adf_raw <-  adf.test(sf_tradepluse_ts, 
                     alternative = "stationary")

adf_diff <- adf.test(diff(sf_tradepluse_ts), 
                     alternative = "stationary")

adf_diff_log <- adf.test(diff(log10(sf_tradepluse_ts)), 
                         alternative = "stationary")




# ACF ? PACF

acf(sf_tradepluse_ts)
acf(diff(sf_tradepluse_ts))

pacf(sf_tradepluse_ts)
pacf(diff(sf_tradepluse_ts))





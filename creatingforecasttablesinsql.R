
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


##add in start date
datefcast <- ymd("2015-01-01")

#######################################################b2b

##create data frame
forecastb2b <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecastb2b[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecastb2b[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecastb2b) <- c("start_date", "end_date")

##set columns as dates
forecastb2b$start_date <- as_date(forecastb2b$start_date)
forecastb2b$end_date <- as_date(forecastb2b$end_date)

##pull in data using SQL 
for (i in 1:36) {
  queryb2b <- paste("select 
                    sum(fulfilled_sales)/ count(distinct 
                    case when order_type = 'SALESORDER' then e.rp_person_id end )
                    from 
                    sf_analytics.prod_order c
                    left join sf_analytics.prod_person e
                    on e.rp_person_id = c.rp_person_id
                    where  trunc(cal_submit_dt) between '", 
                    forecastb2b[i,1], "' and '", forecastb2b[i,2],"' 
                    and c.kf_trade_split_validated = 'B2B'
                    and  e.rp_person_id > 0 ")
  campaignb2b <- dbGetQuery(conn, queryb2b)
  forecastb2b[i,3] <- print(campaignb2b)
}

##Convert to time series for forecasting 
z.ts <- ts(forecastb2b[1:35, 2])
z.ts
z = ts(forecastb2b[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
z

##install forecasting package 
if(!require(forecast)) {
  install.packages("forecast")
  library(forecast)
}

##use forecasting tool for simple forecast for next 12 months 
fcastb2b <- forecast(ets(z), 15)

##build data frame 
for (i in 1:14) {forecastb2b[i+35,3] <- fcastb2b$mean[i+1]}

for (i in 1:14) {forecastb2b[i+35,4] <- 'forecast'}

for (i in 1:14) {forecastb2b[i+35,5] <- 'B2B'}

for (i in 1:35) {forecastb2b[i,3] <- datesb2b[i, 3]}

for (i in 1:35) {forecastb2b[i,4] <- 'actual'}

for (i in 1:35) {forecastb2b[i,5] <- 'B2B'}

##rename columns in data frame 

colnames(forecastb2b) <- c("start_date", "end_date", "spc", "forecast_y_n", "trade_type")

#######################################################trade

##create data frame
forecasttrade <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecasttrade[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecasttrade[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecasttrade) <- c("start_date", "end_date")

##set columns as dates
forecasttrade$start_date <- as_date(forecasttrade$start_date)
forecasttrade$end_date <- as_date(forecasttrade$end_date)

##pull in data using SQL 
for (i in 1:36) {
  querytrade <- paste(" select 
                    sum(total_sales_value_net_ex_vat)/ (count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then e.rp_person_id end ) 
                      + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end))
                      from 
                      sf_uk.sales_order c
                      left join sf_uk.sales_order_totals b
                      on c.rp_order_id = b.rp_order_id 
                      left join sf_analytics.prod_person e
                      on e.rp_person_id = c.rp_person_id
                      where  trunc(order_submit_dt) between '", forecasttrade[i,1], "' and '", forecasttrade[i,2],"' 
                      and kf_trade_split_validated = 'Trade'
                      and last_submit_brand = 'SCREWFIX'
                      and  (e.rp_person_id > 0 or rp_organisation_id > 0)
                      and  (e.rp_person_id > 0 or rp_organisation_id > 0)")
  campaigntrade <- dbGetQuery(conn, querytrade)
  forecasttrade[i,3] <- print(campaigntrade)
}

##Convert to time series for forecasting 
a.ts <- ts(forecasttrade[1:35, 2])
a.ts
a = ts(forecasttrade[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
a

##use forecasting tool for simple forecast for next 12 months 
fcasttrade <- forecast(ets(a), 15)

##build data frame 
for (i in 1:14) {forecasttrade[i+35,3] <- fcasttrade$mean[i+1]}

for (i in 1:14) {forecasttrade[i+35,4] <- 'forecast'}

for (i in 1:14) {forecasttrade[i+35,5] <- 'Trade'}

##for (i in 1:35) {forecasttrade[i,3] <- datesb2b[i, 3]}

for (i in 1:35) {forecasttrade[i,4] <- 'actual'}

for (i in 1:35) {forecasttrade[i,5] <- 'Trade'}

##rename columns in data frame 

colnames(forecasttrade) <- c("start_date", "end_date", "spc", "forecast_y_n", "trade_type")

forecasttrade$start_date <- as_date(forecasttrade$start_date)
forecasttrade$end_date <- as_date(forecasttrade$end_date)

#######################################################nts

##create data frame
forecastnts <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecastnts[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecastnts[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecastnts) <- c("start_date", "end_date")

##set columns as dates
forecastnts$start_date <- as_date(forecastnts$start_date)
forecastnts$end_date <- as_date(forecastnts$end_date)

##pull in data using SQL 
for (i in 1:36) {
  querynts <- paste(" select 
                    sum(total_sales_value_net_ex_vat)/ (count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then e.rp_person_id end ) 
                      + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end))
                      from 
                      sf_uk.sales_order c
                      left join sf_uk.sales_order_totals b
                      on c.rp_order_id = b.rp_order_id 
                      left join sf_analytics.prod_person e
                      on e.rp_person_id = c.rp_person_id
                      where  trunc(order_submit_dt) between '", forecastnts[i,1], "' and '", forecastnts[i,2],"' 
                      and kf_trade_split_validated = 'NTS'
                      and  (e.rp_person_id > 0 or rp_organisation_id > 0)")
  campaignnts <- dbGetQuery(conn, querynts)
  forecastnts[i,3] <- print(campaignnts)
}


##Convert to time series for forecasting 
b.ts <- ts(forecastnts[1:35, 2])
b.ts
b = ts(forecastnts[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
b

##use forecasting tool for simple forecast for next 12 months 
fcastnts <- forecast(ets(b), 15)

##build data frame 
for (i in 1:14) {forecastnts [i+35,3] <- fcastnts$mean[i+1]}

for (i in 1:14) {forecastnts [i+35,4] <- 'forecast'}

for (i in 1:14) {forecastnts [i+35,5] <- 'NTS'}

##for (i in 1:35) {forecastnts [i,3] <- datesnts[i, 3]}

for (i in 1:35) {forecastnts [i,4] <- 'actual'}

for (i in 1:35) {forecastnts [i,5] <- 'NTS'}

##rename columns in data frame 

colnames(forecastnts) <- c("start_date", "end_date", "spc", "forecast_y_n", "trade_type")

forecastnts$start_date <- as_date(forecastnts$start_date)
forecastnts$end_date <- as_date(forecastnts$end_date)

#######################################################tradeplus

##create data frame
forecasttradeplus <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecasttradeplus[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecasttradeplus[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecasttradeplus) <- c("start_date", "end_date")

##set columns as dates
forecasttradeplus$start_date <- as_date(forecasttradeplus$start_date)
forecasttradeplus$end_date <- as_date(forecasttradeplus$end_date)

##pull in data using SQL 
for (i in 1:36) {
  querytradeplus <- paste(" select 
                    sum(total_sales_value_net_ex_vat)/ (count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then e.rp_person_id end ) 
                          + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end))
                          from 
                          sf_uk.sales_order c
                          left join sf_uk.sales_order_totals b
                          on c.rp_order_id = b.rp_order_id 
                          left join sf_analytics.prod_person e
                          on e.rp_person_id = c.rp_person_id
                          where  trunc(order_submit_dt) between '", forecasttradeplus[i,1], "' and '", forecasttradeplus[i,2],"' 
                          and last_submit_brand = 'TRADEPLUS'
                          and  (e.rp_person_id > 0 or rp_organisation_id > 0)")
  campaigntradeplus <- dbGetQuery(conn, querytradeplus)
  forecasttradeplus[i,3] <- print(campaigntradeplus)
}


##Convert to time series for forecasting 
c.ts <- ts(forecasttradeplus[1:35, 2])
c.ts
c = ts(forecasttradeplus[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
c

##use forecasting tool for simple forecast for next 12 months 
fcasttradeplus <- forecast(ets(c), 15)

##build data frame 
for (i in 1:14) {forecasttradeplus [i+35,3] <- fcasttradeplus$mean[i+1]}

for (i in 1:14) {forecasttradeplus [i+35,4] <- 'forecast'}

for (i in 1:14) {forecasttradeplus [i+35,5] <- 'Tradeplus'}

##for (i in 1:35) {forecastnts [i,3] <- datesnts[i, 3]}

for (i in 1:35) {forecasttradeplus [i,4] <- 'actual'}

for (i in 1:35) {forecasttradeplus [i,5] <- 'Tradeplus'}

##rename columns in data frame 

colnames(forecasttradeplus) <- c("start_date", "end_date", "spc", "forecast_y_n", "trade_type")

forecasttradeplus$start_date <- as_date(forecasttradeplus$start_date)
forecasttradeplus$end_date <- as_date(forecasttradeplus$end_date)

#######################################################ka

##create data frame
forecastka <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecastka[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecastka[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecastka) <- c("start_date", "end_date")

##set columns as dates
forecastka$start_date <- as_date(forecastka$start_date)
forecastka$end_date <- as_date(forecastka$end_date)

##pull in data using SQL 
for (i in 1:36) {
  queryka <- paste(" select 
                    sum(total_sales_value_net_ex_vat)/ (count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_person_id end ) 
                          + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end))
                          from 
                          sf_uk.sales_order c
                          left join sf_uk.sales_order_totals b
                          on c.rp_order_id = b.rp_order_id 
                          where  trunc(order_submit_dt) between '", forecastka[i,1], "' and '", forecastka[i,2],"' 
                          and rp_organisation_id > 0")
  campaignka <- dbGetQuery(conn, queryka)
  forecastka[i,3] <- print(campaignka)
}


##Convert to time series for forecasting 
d.ts <- ts(forecastka[1:35, 2])
d.ts
d = ts(forecastka[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
d

##use forecasting tool for simple forecast for next 12 months 
fcastka <- forecast(ets(d), 15)

##build data frame 
for (i in 1:14) {forecastka [i+35,3] <- fcastka$mean[i+1]}

for (i in 1:14) {forecastka [i+35,4] <- 'forecast'}

for (i in 1:14) {forecastka [i+35,5] <- 'Tradeplus'}

##for (i in 1:35) {forecastnts [i,3] <- datesnts[i, 3]}

for (i in 1:35) {forecastka [i,4] <- 'actual'}

for (i in 1:35) {forecastka [i,5] <- 'Tradeplus'}

##rename columns in data frame 

colnames(forecastka) <- c("start_date", "end_date", "spc", "forecast_y_n", "trade_type")

forecastka$start_date <- as_date(forecastka$start_date)
forecastka$end_date <- as_date(forecastka$end_date)


forecastsspc <- rbind(forecasttrade, forecastnts)

forecastsspc <- rbind(forecastsspc, forecasttradeplus)

forecastsspc <- rbind(forecastsspc, forecastka)

forecastsspc <- rbind(forecastsspc, forecastb2b)

install.packages('devtools')
devtools::install_github("RcppCore/Rcpp")
devtools::install_github("rstats-db/DBI")
devtools::install_github("rstats-db/RPostgres")
install.packages("aws.s3", repos = c(getOption("repos"), "http://cloudyr.github.io/drat"))
devtools::install_github("sicarul/redshiftTools")

## Install library dependencies. ##
library(RPostgres)
library(DBI)
library(redshiftTools)
library("aws.s3")

# Load credentials for your own Redshift login.
file <- paste("D:\\Users\\fiona.halpin\\Documents\\credentials\\my_credentials.csv")
my_credentials <- read.csv(file, header = TRUE, sep = ",")

# Load the credentials into variables.
port1 <- as.character(my_credentials[1,1])
host1 <- as.character(my_credentials[1,2])
user1 <- as.character(my_credentials[1,3])
password1 <- as.character(my_credentials[1,4])
dbname1 <- as.character(my_credentials[1,5])


# Establish a connection with Redshift
connection <- dbConnect(RPostgres::Postgres(), host=host1, 
                        port=port1, user=user1, password=password1, 
                        dbname=dbname1, sslmode='require')

# Load credentials for uploading into Redshift
file <- paste("D:\\Users\\fiona.halpin\\Documents\\credentials\\credentials.csv")
credentials <- read.csv(file, header = FALSE, sep = ",")

# Load the credentials into variables.
region1 <- as.character(credentials[1,1])
bucket1 <- as.character(credentials[1,2])
access_key1 <- as.character(credentials[1,3])
secret_key1 <- as.character(credentials[1,4])

as.data.frame(forecastsspc)

## Call the function to to upload the table to Redshift. 
b=rs_create_table(as.data.frame(forecastsspc), dbcon=connection, 
                  table_name='sf_sandbox.fh_spc_forecast_20180223', bucket=bucket1, 
                  split_files=4, 
                  region=region1, access_key=access_key1, secret_key=secret_key1)






##base size 







#######################################################b2b

forecastb2bbase <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecastb2bbase[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecastb2bbase[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecastb2bbase) <- c("start_date", "end_date")

##set columns as dates
forecastb2bbase$start_date <- as_date(forecastb2bbase$start_date)
forecastb2bbase$end_date <- as_date(forecastb2bbase$end_date)

##pull in data using SQL 
for (i in 1:36) {
  queryb2bbase <- paste("select 
                        count(distinct case when order_type = 'SALESORDER' then e.rp_person_id end )
                        from 
                        sf_analytics.prod_order c
                        left join sf_analytics.prod_person e
                        on e.rp_person_id = c.rp_person_id
                        where  trunc(cal_submit_dt) between '", forecastb2bbase[i,1], "' and '", forecastb2bbase[i,2],"' 
                        and c.kf_trade_split_validated = 'B2B'
                        and  e.rp_person_id > 0 ")
  campaignb2bbase <- dbGetQuery(conn, queryb2bbase)
  forecastb2bbase[i,3] <- print(campaignb2bbase)
}
##Convert to time series for forecasting 
e.ts <- ts(forecastb2bbase[1:35, 2])
e.ts
e = ts(forecastb2bbase[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
e

##install forecasting package 
if(!require(forecast)) {
  install.packages("forecast")
  library(forecast)
}

##use forecasting tool for simple forecast for next 12 months 
fcastb2bbase <- forecast(ets(e), 15)

##build data frame 
for (i in 1:14) {forecastb2bbase[i+35,3] <- fcastb2bbase$mean[i+1]}

for (i in 1:14) {forecastb2bbase[i+35,4] <- 'forecast'}

for (i in 1:14) {forecastb2bbase[i+35,5] <- 'B2B'}

##for (i in 1:35) {forecastb2bbase[i,3] <- datesb2bbase[i, 3]}

for (i in 1:35) {forecastb2bbase[i,4] <- 'actual'}

for (i in 1:35) {forecastb2bbase[i,5] <- 'B2B'}

##rename columns in data frame 

colnames(forecastb2bbase) <- c("start_date", "end_date", "base", "forecast_y_n", "trade_type")

#######################################################trade

##create data frame
forecasttradebase <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecasttradebase[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecasttradebase[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecasttradebase) <- c("start_date", "end_date")

##set columns as dates
forecasttradebase$start_date <- as_date(forecasttradebase$start_date)
forecasttradebase$end_date <- as_date(forecasttradebase$end_date)

##pull in data using SQL 
for (i in 1:36) {
  querytradebase <- paste(" select 
                      count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then e.rp_person_id end )
                      + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end)
                      from 
                      sf_uk.sales_order c
                      left join sf_uk.sales_order_totals b
                      on c.rp_order_id = b.rp_order_id 
                      left join sf_analytics.prod_person e
                      on e.rp_person_id = c.rp_person_id
                      where  trunc(order_submit_dt) between '", forecasttradebase[i,1], "' and '", forecasttradebase[i,2],"' 
                      and kf_trade_split_validated = 'Trade'
                      and last_submit_brand = 'SCREWFIX'
                      and  (e.rp_person_id > 0 or rp_organisation_id > 0)
                      and  (e.rp_person_id > 0 or rp_organisation_id > 0)")
  campaigntradebase <- dbGetQuery(conn, querytradebase)
  forecasttradebase[i,3] <- print(campaigntradebase)
}

##Convert to time series for forecasting 
f.ts <- ts(forecasttradebase[1:35, 2])
f.ts
f = ts(forecasttradebase[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
f

##use forecasting tool for simple forecast for next 12 months 
fcasttradebase <- forecast(ets(f), 15)

##build data frame 
for (i in 1:14) {forecasttradebase[i+35,3] <- fcasttradebase$mean[i+1]}

for (i in 1:14) {forecasttradebase[i+35,4] <- 'forecast'}

for (i in 1:14) {forecasttradebase[i+35,5] <- 'Trade'}

##for (i in 1:35) {forecasttrade[i,3] <- datesb2b[i, 3]}

for (i in 1:35) {forecasttradebase[i,4] <- 'actual'}

for (i in 1:35) {forecasttradebase[i,5] <- 'Trade'}

##rename columns in data frame 

colnames(forecasttradebase) <- c("start_date", "end_date", "base", "forecast_y_n", "trade_type")

forecasttradebase$start_date <- as_date(forecasttradebase$start_date)
forecasttradebase$end_date <- as_date(forecasttradebase$end_date)

#######################################################nts

##create data frame
forecastntsbase <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecastntsbase[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecastntsbase[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecastntsbase) <- c("start_date", "end_date")

##set columns as dates
forecastntsbase$start_date <- as_date(forecastntsbase$start_date)
forecastntsbase$end_date <- as_date(forecastntsbase$end_date)

##pull in data using SQL 
for (i in 1:36) {
  queryntsbase <- paste(" select 
                    count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then e.rp_person_id end ) 
                    + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end)
                    from 
                    sf_uk.sales_order c
                    left join sf_uk.sales_order_totals b
                    on c.rp_order_id = b.rp_order_id 
                    left join sf_analytics.prod_person e
                    on e.rp_person_id = c.rp_person_id
                    where  trunc(order_submit_dt) between '", forecastntsbase[i,1], "' and '", forecastntsbase[i,2],"' 
                    and kf_trade_split_validated = 'NTS'
                    and  (e.rp_person_id > 0 or rp_organisation_id > 0)")
  campaignntsbase <- dbGetQuery(conn, queryntsbase)
  forecastntsbase[i,3] <- print(campaignntsbase)
}


##Convert to time series for forecasting 
g.ts <- ts(forecastntsbase[1:35, 2])
g.ts
g = ts(forecastntsbase[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
g

##use forecasting tool for simple forecast for next 12 months 
fcastntsbase <- forecast(ets(g), 15)

##build data frame 
for (i in 1:14) {forecastntsbase [i+35,3] <- fcastntsbase$mean[i+1]}

for (i in 1:14) {forecastntsbase [i+35,4] <- 'forecast'}

for (i in 1:14) {forecastntsbase [i+35,5] <- 'NTS'}

##for (i in 1:35) {forecastnts [i,3] <- datesnts[i, 3]}

for (i in 1:35) {forecastntsbase [i,4] <- 'actual'}

for (i in 1:35) {forecastntsbase [i,5] <- 'NTS'}

##rename columns in data frame 

colnames(forecastntsbase) <- c("start_date", "end_date", "base", "forecast_y_n", "trade_type")

forecastntsbase$start_date <- as_date(forecastntsbase$start_date)
forecastntsbase$end_date <- as_date(forecastntsbase$end_date)

#######################################################tradeplus

##create data frame
forecasttradeplusbase <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecasttradeplusbase[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecasttradeplusbase[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecasttradeplusbase) <- c("start_date", "end_date")

##set columns as dates
forecasttradeplusbase$start_date <- as_date(forecasttradeplusbase$start_date)
forecasttradeplusbase$end_date <- as_date(forecasttradeplusbase$end_date)

##pull in data using SQL 
for (i in 1:36) {
  querytradeplusbase <- paste(" select count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then e.rp_person_id end )
                          + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end)
                          from 
                          sf_uk.sales_order c
                          left join sf_uk.sales_order_totals b
                          on c.rp_order_id = b.rp_order_id 
                          left join sf_analytics.prod_person e
                          on e.rp_person_id = c.rp_person_id
                          where  trunc(order_submit_dt) between '", forecasttradeplusbase[i,1], "' and '", forecasttradeplusbase[i,2],"' 
                          and last_submit_brand = 'TRADEPLUS'
                          and  (e.rp_person_id > 0 or rp_organisation_id > 0)")
  campaigntradeplusbase <- dbGetQuery(conn, querytradeplusbase)
  forecasttradeplusbase[i,3] <- print(campaigntradeplusbase)
}


##Convert to time series for forecasting 
h.ts <- ts(forecasttradeplusbase[1:35, 2])
h.ts
h = ts(forecasttradeplusbase[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
h

##use forecasting tool for simple forecast for next 12 months 
fcasttradeplusbase <- forecast(ets(h), 15)

##build data frame 
for (i in 1:14) {forecasttradeplusbase [i+35,3] <- fcasttradeplusbase$mean[i+1]}

for (i in 1:14) {forecasttradeplusbase [i+35,4] <- 'forecast'}

for (i in 1:14) {forecasttradeplusbase [i+35,5] <- 'Tradeplus'}


for (i in 1:35) {forecasttradeplusbase [i,4] <- 'actual'}

for (i in 1:35) {forecasttradeplusbase [i,5] <- 'Tradeplus'}

##rename columns in data frame 

colnames(forecasttradeplusbase) <- c("start_date", "end_date", "base", "forecast_y_n", "trade_type")

forecasttradeplusbase$start_date <- as_date(forecasttradeplusbase$start_date)
forecasttradeplusbase$end_date <- as_date(forecasttradeplusbase$end_date)

#######################################################ka

##create data frame
forecastkabase <- data.frame(nrow = 49, ncol = 5)

##use a for loop to get your start dates
for (i in 0:49) {forecastkabase[i,1] <- print(datefcast - years(1) + months(i) - days(1))}

##use a for loop to get your end dates
for (i in 0:49) {forecastkabase[i,2] <- print(datefcast + months(i))}

##rename columns 
colnames(forecastkabase) <- c("start_date", "end_date")

##set columns as dates
forecastkabase$start_date <- as_date(forecastkabase$start_date)
forecastkabase$end_date <- as_date(forecastkabase$end_date)

##pull in data using SQL 
for (i in 1:36) {
  querykabase <- paste(" select 
                   count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_person_id end ) 
                   + count(distinct case when order_type_desc in ('SALESORDER', 'SUPPLY OF GOODS') then rp_organisation_id end)
                   from 
                   sf_uk.sales_order c
                   left join sf_uk.sales_order_totals b
                   on c.rp_order_id = b.rp_order_id 
                   where  trunc(order_submit_dt) between '", forecastkabase[i,1], "' and '", forecastkabase[i,2],"' 
                   and rp_organisation_id > 0")
  campaignkabase <- dbGetQuery(conn, querykabase)
  forecastkabase[i,3] <- print(campaignkabase)
}


##Convert to time series for forecasting 
i.ts <- ts(forecastkabase[1:35, 2])
i.ts
i = ts(forecastkabase[1:35, 3], start=c(2015, as.POSIXlt("2015-02-01")$ymonth+1),frequency=12)
i

##use forecasting tool for simple forecast for next 12 months 
fcastkabase <- forecast(ets(i), 15)

##build data frame 
for (i in 1:14) {forecastkabase [i+35,3] <- fcastkabase$mean[i+1]}

for (i in 1:14) {forecastkabase [i+35,4] <- 'forecast'}

for (i in 1:14) {forecastkabase [i+35,5] <- 'KA'}

for (i in 1:35) {forecastkabase [i,4] <- 'actual'}

for (i in 1:35) {forecastkabase [i,5] <- 'KA'}

##rename columns in data frame 

colnames(forecastkabase) <- c("start_date", "end_date", "base", "forecast_y_n", "trade_type")

forecastkabase$start_date <- as_date(forecastkabase$start_date)
forecastkabase$end_date <- as_date(forecastkabase$end_date)


forecastsbase <- rbind(forecasttradebase, forecastntsbase)

forecastsbase <- rbind(forecastsbase, forecasttradeplusbase)

forecastsbase <- rbind(forecastsbase, forecastkabase)

forecastsbase <- rbind(forecastsbase, forecastb2bbase)

install.packages('devtools')
devtools::install_github("RcppCore/Rcpp")
devtools::install_github("rstats-db/DBI")
devtools::install_github("rstats-db/RPostgres")
install.packages("aws.s3", repos = c(getOption("repos"), "http://cloudyr.github.io/drat"))
devtools::install_github("sicarul/redshiftTools")

## Install library dependencies. ##
library(RPostgres)
library(DBI)
library(redshiftTools)
library("aws.s3")

# Load credentials for your own Redshift login.
file <- paste("D:\\Users\\fiona.halpin\\Documents\\credentials\\my_credentials.csv")
my_credentials <- read.csv(file, header = TRUE, sep = ",")

# Load the credentials into variables.
port1 <- as.character(my_credentials[1,1])
host1 <- as.character(my_credentials[1,2])
user1 <- as.character(my_credentials[1,3])
password1 <- as.character(my_credentials[1,4])
dbname1 <- as.character(my_credentials[1,5])


# Establish a connection with Redshift
connection <- dbConnect(RPostgres::Postgres(), host=host1, port=port1, user=user1, password=password1, dbname=dbname1, sslmode='require')

# Load credentials for uploading into Redshift
file <- paste("D:\\Users\\fiona.halpin\\Documents\\credentials\\credentials.csv")
credentials <- read.csv(file, header = FALSE, sep = ",")

# Load the credentials into variables.
region1 <- as.character(credentials[1,1])
bucket1 <- as.character(credentials[1,2])
access_key1 <- as.character(credentials[1,3])
secret_key1 <- as.character(credentials[1,4])

as.data.frame(forecastsbase)

## Call the function to to upload the table to Redshift. Replace descriptive_df with the data frame you want to upload. ##
b=rs_create_table(as.data.frame(forecastsbase), dbcon=connection, table_name='sf_sandbox.fh_base_forecast_20180227', bucket=bucket1, split_files=4, 
                  region=region1, access_key=access_key1, secret_key=secret_key1)


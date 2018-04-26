
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

library(scales)



# Functions ---------------------------------------------------------------

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


# Function to create ggplot-friendly frame from forecast
fcastdf <- function(dn, fcast){ 
  require(zoo) # Needed for the 'as.yearmon()' function
  
  en <- min(time(fcast$mean)) # Extract the max date used in the forecast
  
  # Extract Training Data (ds) and hold out (dh)
  ds <- as.data.frame(window(dn, end = en))
  names(ds) <- "observed"
  ds$date <- as.Date(time(window(dn, end = en)))
  ds <- ds[-nrow(ds), ] # remove last obs (outside of training)
  dh <- as.data.frame(window(dn, start = en))
  names(dh) <- "holdout"
  dh$date <- as.Date(window(dn, start = en))
  
  
  # Extract the Fitted Values (need to figure out how to grab confidence intervals)
  dfit <- as.data.frame(fcast$fitted)
  dfit$date <- as.Date(time(fcast$fitted))
  names(dfit)[1] <- 'fitted'
  
  ds <- merge(ds, dfit, all.x = TRUE) # Merge fitted values with source and training data
  ds <- merge(ds, dh, all = TRUE)
  
  # Exract the Forecast values and confidence intervals
  dfcastn <- as.data.frame(fcast)
  dfcastn$date <- as.Date(as.yearmon(row.names(dfcastn)))
  names(dfcastn) <- c('forecast','lo80','hi80','lo95','hi95','date')

  range80 <- head(dfcastn$hi80, 1) - head(dfcastn$lo80, 1)
  range95 <- head(dfcastn$hi95, 1) - head(dfcastn$lo95, 1)

  # connection <- data.frame(forecast = tail(ds$observed, 1),
  #                          lo80 = tail(ds$observed, 1) - range80/2,
  #                          hi80 = tail(ds$observed, 1) + range80/2,
  #                          lo95 = tail(ds$observed, 1) - range95/2,
  #                          hi95 = tail(ds$observed, 1) + range95/2,
  #                          date = tail(ds$date, 1))
  # 
  # pd <- merge(ds, connection, all = TRUE)
  pd <- merge(ds, dfcastn, all = TRUE) # final data.frame for use in ggplot
  return(pd)
  
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
                    by = "month") - days(1)

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


# nBase


dates = zoo::as.Date(time(nBase_ts))

samples <- create_test_train(nBase_ts, dates, h = 15)

fit <- auto.arima(samples$train)

pred = forecast(fit, h = 15)

ts.plot(samples$train, pred$mean, samples$holdout, lty = c(1, 3, 5))


accuracy(f = pred$pred, 
         x = samples$holdout)


# loop to show forecast performance

pdf(file = "forecast_performance.pdf", width = 10, height = 6)
dates = zoo::as.Date(time(nBase_ts))
plotList <- list()
for(i in 15:5){
  samples <- create_test_train(nBase_ts, dates, h = i)
  
  fit <- auto.arima(samples$train)
  
  pred = forecast(fit, h = i)
  
  
  plot_frame <- fcastdf(nBase_ts, pred)
  
  
  
  plot <- ggplot(data = plot_frame,aes(x = date, y = observed))
  # plot <- plot + ylim(min(plot_frame$observed, na.rm = TRUE),
  #                     555000)
  plot <- plot + geom_line(size = 1.2)
  plot <- plot + geom_point(size = 2)
  plot <- plot + geom_ribbon(aes(ymin = lo95, ymax = hi95),
                             alpha = 0.25,
                             fill = "blue")
  plot <- plot + geom_ribbon(aes(ymin = lo80, ymax = hi80),
                             alpha = 0.25,
                             fill = "blue")
  plot <- plot + geom_line(aes(y = forecast), colour = "blue", size = 1.2)
  plot <- plot + geom_line(aes(y = holdout), colour = "red", size = 1.2)
  plot <- plot + scale_x_date(labels = date_format("%m-%Y"))
  
  plotList[[i]] <- plot
  
}
map(plotList, print)
dev.off()

# Using plotting dataframe convenience function



plot_frame <- fcastdf(nBase_ts, pred)


plot <- ggplot(data = plot_frame,aes(x = date, y = observed))
plot <- plot + geom_line(size = 1.2)
plot <- plot + geom_point(size = 2)
plot <- plot + geom_ribbon(aes(ymin = lo95, ymax = hi95),
                           alpha = 0.25,
                           fill = "blue")
plot <- plot + geom_ribbon(aes(ymin = lo80, ymax = hi80),
                           alpha = 0.25,
                           fill = "blue")
plot <- plot + geom_line(aes(y = forecast), colour = "blue", size = 1.2)
plot <- plot + geom_line(aes(y = holdout), colour = "red", size = 1.2)

plot <- plot + ylim(c(min(plot_frame$observed), max(plot_frame$forecast)))
plot <- plot + scale_x_date(labels = date_format("%m-%Y"))

plot


plot_frame_anim <- data.frame()
for(i in 15:5){
  samples <- create_test_train(nBase_ts, dates, h = i)
  
  fit <- auto.arima(samples$train)
  
  pred = forecast(fit, h = i)
  
  
  plot_frame <- fcastdf(nBase_ts, pred)
  plot_frame$run = i
  
  plot_frame_anim <- rbind(plot_frame_anim,
                           plot_frame)
  
}



plot <- ggplot(data = plot_frame,aes(x = date, y = observed))
#plot <- plot + geom_line(size = 1.2)
plot <- plot + geom_point(size = 2, aes(frame = run))
# plot <- plot + geom_ribbon(aes(ymin = lo95, ymax = hi95),
#                            alpha = 0.25,
#                            fill = "blue")
# plot <- plot + geom_ribbon(aes(ymin = lo80, ymax = hi80),
#                            alpha = 0.25,
#                            fill = "blue")
# plot <- plot + geom_line(aes(y = forecast), colour = "blue", size = 1.2)
# plot <- plot + geom_line(aes(y = holdout), colour = "red", size = 1.2)
# 
# plot <- plot + ylim(c(min(plot_frame$observed), max(plot_frame$forecast)))
# plot <- plot + scale_x_date(labels = date_format("%m-%Y"))

plot

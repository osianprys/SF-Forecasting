#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

library(shiny)
library(ggplot2)
library(forecast)

load(file = "SPCdata.RData")
load(file = "nBasedata.RData")

# Define server logic required to draw a histogram
function(input, output) {
   
  output$fcastPlot <- renderPlot({
    

    
  data <- switch(input$fcastMetric,
                 SPC = SPC_df,
                 nBase = nBase_df)
  
  metric <- switch(input$fcastMetric,
                   SPC = "spc",
                   nBase = "customers")
  
  
    ts <- data %>%
    filter(base == input$cbase) %>%
    select(metric) %>%
    map(~ts(.x,
               start = c(year(min(input$dateRange[1])), month(max(input$dateRange[2]))),
               frequency = 12))
  
  
  
    fit <- auto.arima(ts[[1]])
    fcast <- forecast(fit, h = input$horizon)
    autoplot(fcast)
  # nBase_fit_list <- nBase_ts_list %>%
  #   map(~auto.arima(.x))
  # 
  # nBase_fcast_list_15 <- nBase_fit_list %>%
  #   map(~forecast(.x, h = 15))
  # 
  
  
  

  #autoplot(data[[input$cbase]])
  })
  
}

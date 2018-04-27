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
library(tidyverse)

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

  })
  
  output$metrictable <- renderDataTable({

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
    
      tday_values <- tail(fcast$x, 1) %>% as.data.frame()
    
    
      fcast_values <- tail(fcast$mean, 1) %>% as.data.frame()
    
      fcast_upper <- tail(fcast$upper, 1) %>% as.data.frame()
      
      df <- cbind(tday_values$x[1], fcast_values$x[1], fcast_upper$`95%`[1]) %>%
            as.data.frame()
      names(df) <- c("Today", input$horizon, "Upper")
      df <- df %>%
            mutate(Var95 = Upper - Today) %>%
            select(-Upper)
    
     round(df, 2)   
  })
  
}

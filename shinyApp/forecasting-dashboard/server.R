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
                 SPC = SPC_fcast_list_15,
                 nBase = nBase_fcast_list_15) 

  autoplot(data[[input$cbase]])
  })
  
}

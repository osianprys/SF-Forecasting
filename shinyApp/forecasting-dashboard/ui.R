
library(shiny)
library(lubridate)

# Define UI for application that draws a histogram
fluidPage(
  
  # Application title
  titlePanel("Screwfix - forecasting dashboard"),
  
  # Sidebar with a slider input for number of bins 
  sidebarLayout(
    sidebarPanel(
    dateRangeInput("dateRange",
                   "Select Dates",
                   start = ymd("2015-01-01"),
                   end = ymd("2016-01-01"),
                   min = ymd("2014-12-01"),
                   max = ymd("2018-02-28")),
    selectInput("cbase",
                "Customer Base:",
                choices = c("TRADEPLUS", "TRADE", "B2B", "NTS"),
                selected = "TRADEPLUS"),
    selectInput("fcastMetric",
                "Forecast Metric:",
                choices = c("SPC", "nBase"),
                selected = "SPC"),
    numericInput("horizon",
                 "Horizon:",
                 value = 15,
                 min = 1,
                 max = 20)
    ),
    
    # Show a plot of the generated distribution
    mainPanel(
      plotOutput("fcastPlot")
    )
  )
)

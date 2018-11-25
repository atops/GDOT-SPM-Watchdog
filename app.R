#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
library(lubridate)
library(dplyr)
library(feather)


library(DT)
source("filter_SPM_alerts.R")
source("plot_functions.R")




# Define UI for application
ui <- fluidPage(theme = "style.css",
                tags$link(rel="stylesheet"),
                tags$head(includeHTML(("google-analytics.html"))),
                

    # Application title
    titlePanel(title=div(img(src="GDOTLogo.svg", align="left"), 
                         "Signal Performance Measures (SPM) Watchdog Alerts")),
    
    
    
    # Main panel with tabs: Map, Table, Plots
    fluidRow(
        column(width = 3,
               dateRangeInput("date_range", "Date Range:",
                            start = today() - days(28),
                            end = today(),
                            min = today() - days(365),
                            max = today(),
                            format = "mm/dd/yy", 
                            startview = "month", 
                            weekstart = 0,
                            separator = " - ")
        ),
        column(width = 3,
             selectInput("alert_type", "Alert:",
                         choices = c("Missing Records",
                                     "Pedestrian Activations",
                                     "Force Offs",
                                     "Max Outs",
                                     "Count",
                                     "Bad Detection"),
                         selected = "Missing Records")
        ),
        
        column(width = 2,
             selectInput("zone", "Zone/District:",
                         choices = c("All", 
                                     paste("Zone", seq_len(7)),
                                     paste("District", c(3,4,5,7))),
                         selected = "Zone 1")
        ),
        column(width = 1,
             selectInput("phase", "Phase:",
                         choices = c("All", seq_len(8)),
                         selected = "All")
        ),
        column(width = 3,
             textInput("id_filter", "Intersection Filter:")
        )
    ),
    fluidRow(
        tabsetPanel(type = "tabs", 
                    tabPanel("Plot", 
                             helpText("This chart shows which signals had the alert listed above on each day shown."),
                             uiOutput("ui_plot")),
                    tabPanel("Table", 
                             column(width=12, 
                                    DT::dataTableOutput("signals_table")))
        )
    )

)

# Define server logic
server <- function(input, output, session) {

    
    alerts <- get_alerts()
    

    
    observe({
        ph <- input$phase
        if (input$alert_type %in% c("Missing Records", "Count")) {
            ch <- "All"
            sel <- "All"
        } else {
            ch <- c("All", seq_len(8))
            sel <- ph
        }
        updateSelectInput(session, "phase", 
                          choices = ch,
                          selected = sel)
    })
    
    
    
    
    filtered_alerts <- reactive({
        
        alerts_by_date <- filter_alerts_by_date(alerts, input$date_range)
        
        filter_alerts(alerts_by_date,
                      input$alert_type,
                      input$zone,
                      input$phase,
                      input$id_filter)
        
    })
    
    
    
    
    plot_height <- function(n) {
        css_px <- as.character(50 + n * 14)
        return (paste0(css_px, "px"))
    }
    

    observe({

        dataset <- filtered_alerts()
        
        if (nrow(dataset$plot) > 0) {
            # plot
            output$signals_plot <- renderPlot({
                plot_alerts(dataset$plot, isolate(input$date_range))
            })
        } else {
            output$signals_plot <- renderPlot({
                plot_empty() #ggplot()
            })
            
        }
        # wrapper for plot
        output$ui_plot <- renderUI({
            plotOutput("signals_plot", 
                       height = plot_height(dataset$intersections), 
                       width = "95%")
        })

        
    })
    
    
    # data table
    
    observe({
        
        
            
        output$signals_table <- renderDataTable({
            datatable(filtered_alerts()$table, escape = FALSE,
                      
                      extensions = 'Scroller', options = list(
                          deferRender = TRUE,
                          scrollY = 500,
                          scroller = TRUE,
                          searching = FALSE)
            )
        })

    })
}

# Run the application 
shinyApp(ui = ui, server = server)


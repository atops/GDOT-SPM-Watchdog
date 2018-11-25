

library(lubridate)
library(dplyr)
library(tidyr)
library(forcats)
library(shiny)
library(yaml)
library(DT)
library(aws.s3)
library(httr)


if (Sys.info()["nodename"] == "GOTO3213490") { # The SAM
    set_config(
        use_proxy("gdot-enterprise", port = 8080,
                  username = Sys.getenv("GDOT_USERNAME"),
                  password = Sys.getenv("GDOT_PASSWORD")))
} else { # shinyapps.io
    Sys.setenv(TZ="America/New_York")
    
    config <- yaml.load_file("gdot_watchdog_config.yaml")
    
    Sys.setenv("AWS_ACCESS_KEY_ID" = config$AWS_ACCESS_KEY_ID,
               "AWS_SECRET_ACCESS_KEY" = config$AWS_SECRET_ACCESS_KEY,
               "AWS_DEFAULT_REGION" = config$AWS_DEFAULT_REGION)
}




filter_alerts_by_date <- function(alerts, dr) {
    
    start_date <- dr[1]
    end_date <- dr[2]
    
    alerts %>%
        filter(TimeStamp >= start_date & TimeStamp <= end_date)
}

filter_alerts <- function(alerts, alert_type_, zone_, phase_, id_filter_)  {
    
    df_is_empty <- FALSE
    
    if (nrow(alerts) == 0) {
        df_is_empty <- TRUE
    } else {
        df <- filter(alerts, Alert == alert_type_,
                     grepl(pattern = id_filter_, x = Name, ignore.case = TRUE))
        
        if (nrow(df) == 0) {
            df_is_empty <- TRUE
        } else {
            
            if (zone_ != "All") {
                df <- filter(df, Zone == zone_)
            }
            
            if (nrow(df) == 0) {
                df_is_empty <- TRUE
            } else {
                
                if (alert_type_ != "Missing Records" & phase_ != "All") {
                    df <- filter(df, Phase == as.numeric(phase_)) # filter
                }

                if (nrow(df) == 0) {
                    df_is_empty <- TRUE
                }
            }
        }
    }
    
    if (!df_is_empty) {
        
        if (alert_type_ == "Missing Records") {
            
            table_df <- df %>%
                group_by(Zone, SignalID, Name, Alert) %>% 
                summarize("Occurrences" = n()) %>% 
                ungroup() 
            
            plot_df <- df %>%
                unite(Name2, SignalID, Name, sep = ": ") %>%
                mutate(signal_phase = factor(Name2)) 
            
        } else if (alert_type_ == "Bad Detection") {
            
            table_df <- df %>%
                group_by(Zone, SignalID, Name, Detector = as.integer(as.character(DetectorID)), Alert) %>% 
                summarize("Occurrences" = n()) %>% 
                ungroup() 
            
            plot_df <- df %>%
                mutate(DetectorID = as.character(DetectorID)) %>%
                unite(signal_phase2, Name, DetectorID, sep = " | det ") %>%
                unite(signal_phase, SignalID, signal_phase2, sep = ": ") %>%
                mutate(signal_phase = factor(signal_phase))

        } else {
            
            table_df <- df %>%
                group_by(Zone, SignalID, Name, Phase, Alert) %>% 
                summarize("Occurrences" = n()) %>% 
                ungroup() 
            plot_df <- df %>%
                mutate(Phase = as.character(Phase)) %>% 
                unite(signal_phase2, Name, Phase, sep = " | ph ") %>% # potential problem
                unite(signal_phase, SignalID, signal_phase2, sep = ": ") %>%
                mutate(signal_phase = factor(signal_phase)) 
        }
        
        intersections <- length(unique(plot_df$signal_phase))
        
    } else { #df_is_empty
        
        plot_df <- data.frame()
        table_df <- data.frame()
        intersections <- 0
    }
    
    list("plot" = plot_df, 
         "table" = table_df, 
         "intersections" = intersections)
}





get_alerts <- function() {
    
    save_object(file = "SPMWatchDogErrorEvents.feather.zip",
                object = "SPMWatchDogErrorEvents.feather.zip",
                bucket = "gdot-devices")
    unzip("SPMWatchDogErrorEvents.feather.zip")
    
    alerts <- read_feather("SPMWatchDogErrorEvents.feather")
    
    alerts
}



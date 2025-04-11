#-------------------------------------------------------------------------------
library(httr)
library(jsonlite)
library(readxl)
library(writexl)
library(dplyr)
library(rvest)
library(stringr)
library(utf8)

# --- Load Excel File ---
df <- read_excel("Invesco Holdings.xlsx", sheet = 1)
colnames(df) <- c("Name", "ISIN", "Weight")

# --- API Key ---
api_key <- "" #see api file not pushed into github
  

# --- OpenFIGI Query Function ---
get_figi_data <- function(isin, api_key) {
  url <- "https://api.openfigi.com/v3/mapping"
  body <- list(list(idType = "ID_ISIN", idValue = isin))
  
  response <- tryCatch({
    POST(
      url,
      body = toJSON(body, auto_unbox = TRUE),
      add_headers(
        "Content-Type" = "application/json",
        "X-OPENFIGI-APIKEY" = api_key
      ),
      encode = "json"
    )
  }, error = function(e) return(NULL))
  
  if (is.null(response)) return(rep(NA, 4))
  parsed <- content(response, as = "parsed")
  data <- parsed[[1]]$data
  if (length(data) == 0) return(rep(NA, 4))
  
  return(c(
    data[[1]]$securityType,
    data[[1]]$marketSector,
    data[[1]]$ticker,
    data[[1]]$exchCode
  ))
}

# --- Fallback via Yahoo Finance Scraping ---
get_yahoo_info <- function(ticker) {
  if (is.na(ticker) || ticker == "") return(c(NA, NA))
  
  url <- paste0("https://finance.yahoo.com/quote/", ticker, "/profile")
  page <- tryCatch(read_html(url), error = function(e) return(NA))
  if (is.na(page)) return(c(NA, NA))
  
  sector <- page %>%
    html_nodes(xpath = "//span[text()='Sector']/following-sibling::span") %>%
    html_text(trim = TRUE)
  
  currency <- page %>%
    html_nodes(xpath = "//td[contains(text(),'Currency')]/following-sibling::td") %>%
    html_text(trim = TRUE)
  
  return(c(ifelse(length(sector) == 0, NA, sector),
           ifelse(length(currency) == 0, NA, currency)))
}

# --- Process All Rows ---
results <- head(df)
results$Asset_Class <- NA
results$Market_Sector <- NA
results$Ticker <- NA
results$Exchange <- NA
results$Sector <- NA
results$Currency <- NA

for (i in 1:nrow(results)) {
  isin <- results$ISIN[i]
  cat(sprintf("[%d/%d] Processing ISIN: %s\n", i, nrow(results), isin))
  
  # Step 1: Query OpenFIGI
  # Assuming get_figi_data uses an external connection, ensure it is closed or freed after the call
  figi_res <- tryCatch({
    get_figi_data(isin, api_key)
  }, error = function(e) {
    cat("Error querying OpenFIGI for ISIN:", isin, "\n")
    return(NULL)  # Return NULL on error to prevent crashing the loop
  })
  
  # Only continue if OpenFIGI data was fetched successfully
  if (!is.null(figi_res)) {
    results[i, c("Asset_Class", "Market_Sector", "Ticker", "Exchange")] <- as.list(figi_res)
    
    # Step 2: Fallback with Yahoo
    # Assuming get_yahoo_info uses an external connection, ensure it is closed or freed after the call
    yahoo_data <- tryCatch({
      get_yahoo_info(figi_res[3])
    }, error = function(e) {
      cat("Error querying Yahoo for Ticker:", figi_res[3], "\n")
      return(NULL)  # Return NULL on error to prevent crashing the loop
    })
    
    # Only continue if Yahoo data was fetched successfully
    if (!is.null(yahoo_data)) {
      results[i, c("Sector", "Currency")] <- as.list(yahoo_data)
    }
  }
  
  Sys.sleep(0.5)  # Be gentle to Yahoo
  gc()  # Optional: Call garbage collection to free memory after each iteration
}

# --- Export Results ---
write_xlsx(results, "Invesco_Holdings_Enriched.xlsx")
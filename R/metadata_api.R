# An interface for reading and writing metadata of Socrata assets
# 
# Author: Ryan Hall 2020-02-20
###############################################################################

# library('httr')       # for access to the HTTP header
# library('jsonlite')   # for parsing data types from Socrata
# library('plyr')       # for parsing JSON files
# library('tibble')     # for storing metadata as tibbles
# library('tidyr')      # for nesting/unnesting json
# library('dplyr')      # for creating helpful data elements
# library('magrittr')   # for creating pipelines


#' Get a Socrata dataset's metadata
#'
#' Returns all metadata fields available from the Metadata API.
#'
#' @param domain - a string; a Socrata site's domain name, without any http or other url params
#' @param dataset_id - a string; a four-by-four, or unique identifier for a Socrata dataset
#' @param email - Optional. The email to the Socrata account with read access to the dataset, 
#' or an API Key
#' @param password - Optional. The password associated with the email to the Socrata account, 
#' or an API Secret
#' @param return_df - Optional. Should the metadata be returned as a data frame instead of a 
#' list?
#' @return a list of metadata from a Socrata dataset
#' @author Ryan Hall \email{ryhallry@@gmail.com}
#' @examples
#' 
#' @importFrom httr content
#' @export
read.socrataMetadata <- function(domain, dataset_id, email = NULL, password = NULL, app_token = NULL,
                         return_df = FALSE) {
  if(!isFourByFour(dataset_id))
    stop(dataset_id, " is not a valid Socrata dataset unique identifier.")

  if(grepl("http", domain, ignore.case = TRUE))
      stop(domain, " - do not include https:// in the domain name argument.")
  
  validUrl <- paste0('https://', domain, '/api/views/metadata/v1/', dataset_id)
  
  response <- getResponse(validUrl, email, password)
  
  if(!return_df){
    ## Return response content as a list
    response_content <- httr::content(response, as = "parsed")}
  # else {
  #   response_content <- returnAsDataFrame(response) ## thinking through how this might help
  # }
  ## Return response content as a dataframe
  
  return(response_content) 
}
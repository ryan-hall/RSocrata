# Write data to Socrata datesets with the Data Management API
# 
# Author: Ryan Hall 2019-12-10
###############################################################################

# library('httr')       # for access to the HTTP header
# library('jsonlite')   # for parsing data types from Socrata
# library('mime')       # for guessing mime type
# library('plyr')       # for parsing JSON files

#' Open a new revision on a dataset
#'
#' This function opens a new revision, or draft, on an existing dataset on a Socrata domain.
#' This is the first step in updating or replacing a dataset on Socrata with new data. 
#'
#' @param dataset_id The dataset's unique ID, or four by four, located at the end of a dataset's URL 
#' @param action_type One of "update", "replace", or "delete", depending on how you want to update the dataset
#' @param domain_url The Socrata domain URL, not including https://
#' @param email Socrata username, or Socrata API Key
#' @param password Socrata password, or Socrata API Secret
#'
#' @return This function returns the response body from the httr call to the Socrata Data Management API
#'
#' @note More information on Data Management API available at https://socratapublishing.docs.apiary.io/
#'
#' @examples
#' \dontrun{
#' open_revision()
#' }
#' @importFrom httr POST add_headers authenticate user_agent parse_url content
#' @importFrom jsonlite fromJSON
#'
#' @export
## use fetch_user_agent()
## use isFourByFour()
open_revision <- function(dataset_id, action_type, domain_url, email, password) {
  domain <- httr::parse_url(domain_url)
  if(is.null(domain$scheme) | is.null(domain$hostname) | is.null(domain$path))
    stop(domain_url, " does not appear to be a valid URL.")
  
  dataset_id <- as.character(dataset_id)
  open_revision_endpoint <- paste0(domain$scheme, '://', domain$hostname, '/api/publishing/v1/revision/', dataset_id)
  
  if(!any(c(action_type == "update", action_type == "replace", action_type == "delete")))
    stop(action_type, " is not one of 'update', 'replace', or 'delete'.")
  open_revision_json <- paste0('{"action": {"type":"', action_type,'"}}')
  
  open_revision_response <- POST(open_revision_endpoint,
                                 body = open_revision_json,
                                 httr::add_headers("Content-Type" = "application/json"),
                                 httr::authenticate(email, password, type = "basic"),
                                 httr::user_agent(fetch_user_agent())
  )
  
  if(open_revision_response$status_code == '201') {
    message("Opened new revision on dataset ", dataset_id)
    open_revision_response_body <- jsonlite::fromJSON(httr::content(open_revision_response, as = "text", type = "application/json", encoding = "utf-8")) 
  } else {
    message("Failed to open revision with status code ", open_revision_response$status_code)
    stop_for_status(open_revision_response$status_code)
  }
}

#' Specify details about your data source within the revision
#'
#' Within a revision, a Source describes pertinent details about your data source, like a filename.
#' This function passes chosen options to the Source endpoint of your revision, and is the second step in
#' running a dataset update. 
#'
#' @param dataset_id The dataset's unique ID, or four by four, located at the end of a dataset's URL 
#' @param source_type One of "upload" or "url", specifying where the data for the update will come from. Default
#'     is 'upload'.
#' @param source_parse 'true' or 'false' for whether the source is of a data type Socrata can parse, including
#'     csv, tsv, xls, OpenXML, GeoJSON, KML, KMZ, Shapefile. Default is 'true'.
#' @param domain_url The Socrata domain URL, not including https://
#' @param email Socrata username, or Socrata API Key
#' @param password Socrata password, or Socrata API Secret
#'
#' @return This function returns the response body from the httr call to the Socrata Data Management API
#'
#' @note More information on Data Management API available at https://socratapublishing.docs.apiary.io/
#'
#' @examples
#' \dontrun{
#' open_revision()
#' }
#' @importFrom httr POST add_headers authenticate upload_file
#' @importFrom jsonlite fromJSON
#'
#' @export
create_source <- function(revision_response_object, filename, source_type = "upload", source_parse = "true", domain_url, email, password) {
  source_json_type <- paste0('{"type":"',source_type,'", "filename":"',filename,'"}')
  source_json_parse <- paste0('{"parse_source":"',source_parse,'"}')
  source_json <- paste0('{"source_type":',source_json_type,', "parse_options":',source_json_parse,'}')
  
  create_source_url <- paste0(domain_url, revision_response_object$links$create_source)
  
  create_source_response <- httr::POST(create_source_url,
                                       body = source_json,
                                       httr::add_headers("Content-Type" = "application/json"),
                                       httr::authenticate(email, password, type = "basic"),
                                       httr::user_agent(fetch_user_agent())
                                       )
  
  if (create_source_response$status_code == "201") {
    message("Created source for update to ", revision_response_object$resource$fourfour)
    create_source_response <- jsonlite::fromJSON(httr::content(create_source_response, as = "text", type = "application/json", encoding = "utf-8"))
  } else {
    message("Failed to create source with status code ",create_source_response$status_code)
    stop_for_status(create_source_response$status_code)
  }
}



upload_to_source <- function(create_source_response_object, filepath_to_data, domain_url, email, password) {
  upload_data_url <- paste0(domain_url, create_source_response_object$links$bytes)
  
  ## If the file is a csv:
  data_for_upload <- httr::upload_file(filepath_to_data)
  
  ## the body of the post is the new data
  upload_data_response <- httr::POST(upload_data_url,
                                     body = data_for_upload,
                                     httr::add_headers("Content-Type" = "text/csv"),
                                     httr::authenticate(email, password, type = "basic"),
                                     httr::user_agent(fetch_user_agent())
                                     )
  
  if (upload_data_response$status_code == "200") {
    message("Uploading data to draft.")
    upload_data_response <- jsonlite::fromJSON(httr::content(upload_data_response,as = "text",type = "application/json", encoding = "utf-8"))
  } else {
    message("Failed to upload data to source with status code ",upload_data_response$status_code)
    stop_for_status(upload_data_response$status_code)
  }
  
  poll_for_status <- 0
  repeat {
    
    poll_for_status <- poll_for_status + 1
    
    if (!is.null(upload_data_response$resource$failed_at)) {
      stop("Upload failed. Check upload response.")
    } else if (!is.null(upload_data_response$resource$finished_at)){
      message("Upload finished.")
      break
    } else if (poll_for_status == 100) {
      stop("Polling for upload status verification has timed out. Check upload response and/or increase poll limit.")
    } else {
      message("Polling for upload and data validation status. Stay tuned.")
      upload_data_response <- httr::GET(paste0(domain_url,upload_data_response$links$show),
                                        httr::authenticate(email, password, type = "basic"))
      
      httr::stop_for_status(upload_data_response)
      
      upload_data_response <- jsonlite::fromJSON(httr::content(upload_data_response,as = "text",type = "application/json", encoding = "utf-8"))
      
      Sys.sleep(1)
      
    }
  }
}



apply_revision <- function(revision_response_object, domain_url, email, password) {
  apply_revision_url <- paste0(domain_url, revision_response_object$links$apply)
  apply_revision_json <- paste0('{"resource": {"id":',revision_response_object$resource$revision_seq,'}}')
  
  apply_revision_response <- httr::PUT(apply_revision_url,
                                       body = apply_revision_json,
                                       httr::add_headers("Content-Type" = "application/json"),
                                       httr::authenticate(email, password, type = "basic"),
                                       httr::user_agent(fetch_user_agent())
                                       )
  
  if (apply_revision_response$status_code == "200") {
    message("Revision applied. Socrata is processing the update.")
  } else {
    message("Revision failed to apply. Check the apply_revision_response for details.")
    stop_for_status(apply_revision_response$status_code)
  }
}



update.socrata <- function(dataset_id, 
                           filename, 
                           filepath_to_data, 
                           source_type = "upload", 
                           source_parse = "true", 
                           domain_url, 
                           email, 
                           password) {
  
  open_revision_socrata <- open_revision(dataset_id, "update", domain_url, email, password)
  create_source_socrata <- create_source(open_revision_socrata, filename, source_type, source_parse, domain_url, email, password)
  upload_to_source_socrata <- upload_to_source(create_source_socrata, filepath_to_data, domain_url, email, password)
  apply_revision_socrata <- apply_revision(open_revision_socrata, domain_url, email, password)
  
}

replace.socrata <- function(dataset_id, 
                            filename, 
                            filepath_to_data, 
                            source_type = "upload", 
                            source_parse = "true", 
                            domain_url, 
                            email, 
                            password) {
  
  open_revision_socrata <- open_revision(dataset_id, "replace", domain_url, email, password)
  create_source_socrata <- create_source(open_revision_socrata, filename, source_type, source_parse, domain_url, email, password)
  upload_to_source_socrata <- upload_to_source(create_source_socrata, filepath_to_data, domain_url, email, password)
  apply_revision_socrata <- apply_revision(open_revision_socrata, domain_url, email, password)
  
}


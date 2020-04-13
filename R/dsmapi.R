# Write data to Socrata datasets with the Data Management API
# 
# Author: Ryan Hall 2019-12-10
###############################################################################

# library('httr')       # for access to the HTTP header
# library('jsonlite')   # for parsing data types from Socrata

#' Validate domain name provided
#'
#' @param domain a character string. The domain name of the Socrata site. 
#' @return a valid domain name for subsequent functions, with no scheme or path
#' @export
#'
#' @examples
validate_domain <- function(domain) {
  domain <- casefold(domain)
  domain_parse <- httr::parse_url(domain)
  
  if(!is.null(domain_parse$hostname)) {
    domain_valid <- domain_parse$hostname
  } else if(is.null(domain_parse$scheme) & is.null(domain_parse$host_name) &
            !is.null(domain_parse$path)) {
    domain_valid <- domain_parse$path
  } else if(is.null(domain_parse$scheme) & is.null(domain_parse$host_name) &
            is.null(domain_parse$path)) {
    stop(domain, " does not appear to be a valid domain name")
  }
  
  remove_path <- httr::parse_url(paste0("https://", domain_valid))
  domain_valid <- remove_path$hostname
  
  return(domain_valid)
}

#' Validate Source to send to Socrata
#'
#' @param source a data.frame object or filepath to a local csv
#' @return logical. Is the provided input a valid data.frame object or file?
#' @export
#'
#' @examples
validate_source <- function(source_for_socrata) {
  
  if(!is.data.frame(source_for_socrata) & !is.character(source_for_socrata)) {
    return(FALSE)
  }
  
  if (is.data.frame(source_for_socrata)) {
    return(TRUE)
  }
  
  if (is.character(source_for_socrata)) {
    if(grepl("\\.csv$", source_for_socrata)) {
      return(TRUE)
    } else {
      return(FALSE)
    }
  }
}

#' Revision: Open a new revision on a dataset
#'
#' This function opens a new revision, or draft, on an existing dataset on a 
#' Socrata domain.
#' This is the first step in updating or replacing a dataset on Socrata with 
#' new data. 
#'
#' @param dataset_id character vector. The dataset's unique ID, or four by four, 
#' located at the end of a dataset's URL 
#' @param action_type character vector. One of "update", "replace", or "delete", 
#' depending on how you want to update the dataset.
#' @param domain character vector. The Socrata domain URL, including scheme 
#' and hostname, i.e. https://data.seattle.gov
#' @param email character vector. Socrata username, or Socrata API Key
#' @param password character vector. Socrata password, or Socrata API Secret
#'
#' @return This function returns the response body from the httr call to the 
#' Socrata Data Management API revision endpoint
#'
#' @note More information on Data Management API available 
#' at https://socratapublishing.docs.apiary.io/
#'
#' @examples
#' \dontrun{
#' open_revision()
#' }
#' @importFrom httr POST add_headers authenticate user_agent parse_url content stop_for_status
#' @importFrom jsonlite fromJSON
#'
#' @export
open_revision <- function(dataset_id, action_type, domain, email, password) {
  ## Validate inputs. If anything doesn't look right, don't open a revision. 
  dataset_id <- casefold(as.character(dataset_id))
  
  if(!isFourByFour(dataset_id))
    stop(dataset_id, " does not appear to be of valid Socrata dataset identifier format.")
  
  action_type <- casefold(action_type)
  if(!any(c(action_type == "update", action_type == "replace", action_type == "delete")))
    stop(action_type, " is not one of 'update', 'replace', or 'delete'.")
  
  domain <- validate_domain(domain)
  
  open_revision_endpoint <- paste0('https://', domain, '/api/publishing/v1/revision/', dataset_id)
  
 
  open_revision_json <- paste0('{"action": {"type":"', action_type,'"}}')
  
  open_revision_response <- POST(open_revision_endpoint,
                                 body = open_revision_json,
                                 httr::add_headers("Content-Type" = "application/json"),
                                 httr::authenticate(email, password, type = "basic"),
                                 httr::user_agent(fetch_user_agent())
  )
  
  if(open_revision_response$status_code == '201') {
    message(dataset_id, " - Opened new revision on dataset ", dataset_id)
    open_revision_response_body <- jsonlite::fromJSON(httr::content(open_revision_response, as = "text", type = "application/json", encoding = "utf-8")) 
  } else {
    message(dataset_id, " - Failed to open revision with status code ", open_revision_response$status_code)
    stop_for_status(open_revision_response$status_code)
  }
}

#' Source: Specify details about your data source
#'
#' Within a revision, a Source describes pertinent details about your data 
#' source, like whether or not it can be parsed.
#' This function passes options to the Source endpoint of your revision, 
#' and is the second step in running a dataset update. 
#'
#' @param revision_response_object The response from the API from the 
#' open_revision function.  
#' @param source_type Default is "upload". Only uploads supported at this time. 
#' @param source_parse 'true' or 'false' for whether the source is of a data 
#' type Socrata can parse, including csv, tsv, xls, OpenXML, GeoJSON, KML, KMZ, 
#' Shapefile. Default is 'true'. 
#' @param filename The name of the file you are uploading. 
#' @param domain The Socrata domain URL, not including 'https://'
#' @param email Socrata username, or Socrata API Key
#' @param password Socrata password, or Socrata API Secret
#'
#' @return This function returns the response body from the httr call to the 
#' Socrata Data Management API's source endpoint
#'
#' @note More information on Data Management API available 
#' at https://socratapublishing.docs.apiary.io/
#'
#' @examples
#' \dontrun{
#' create_source()
#' }
#' @importFrom httr POST add_headers authenticate upload_file user_agent content stop_for_status
#' @importFrom jsonlite fromJSON
#'
#' @export
create_source <- function(revision_response_object, 
                          source_type = "upload", source_parse = "true", 
                          domain, email, password) {
  
  source_json_type <- paste0('{"type":"',source_type,'", "filename":"socrata_upload_temp.csv"}')
  source_json_parse <- paste0('{"parse_source":"',source_parse,'"}')
  source_json <- paste0('{"source_type":',source_json_type,', "parse_options":',
                        source_json_parse,'}')
  
  create_source_url <- paste0(domain, revision_response_object$links$create_source)
  
  create_source_response <- httr::POST(create_source_url,
                                       body = source_json,
                                       httr::add_headers("Content-Type" = "application/json"),
                                       httr::authenticate(email, password, type = "basic"),
                                       httr::user_agent(fetch_user_agent())
                                       )
  
  if (create_source_response$status_code == "201") {
    message("Created source for update to ", revision_response_object$resource$fourfour)
    create_source_response <- jsonlite::fromJSON(httr::content(create_source_response, 
                                                               as = "text", 
                                                               type = "application/json", 
                                                               encoding = "utf-8"))
  } else {
    message("Failed to create source with status code ",create_source_response$status_code)
    stop_for_status(create_source_response$status_code)
  }
}


#' Upload: Upload a data source to the Socrata revision
#'
#' This function supports streaming a comma separated values (csv) data file 
#' from a local filepath to the Socrata revision.
#' After the data is uploaded to the Socrata revision, Socrata processes the
#' new data, validating data types and running data transforms. Depending
#' on the size of your update, this can take a hot second or a hot minute. 
#'
#' @param create_source_response_object list. The response body from the 
#' create_source function.   
#' @param filepath_to_data character vector. Local filepath to a csv file.  
#' @param domain The Socrata domain URL, not including 'https://'
#' @param email Socrata username, or Socrata API Key
#' @param password Socrata password, or Socrata API Secret
#' @param status_checks an integer. Number of time to check for Socrata to
#' have finished processing the upload, roughly equivalent to seconds.
#'
#' @return This function returns the response body from the httr call to the 
#' Socrata Data Management API upload endpoint.
#'
#' @note More information on Data Management API available 
#' at https://socratapublishing.docs.apiary.io/
#'
#' @examples
#' \dontrun{
#' upload_to_source()
#' }
#' @importFrom httr POST GET add_headers authenticate user_agent content upload_file stop_for_status
#' @importFrom jsonlite fromJSON
#'
#' @export
upload_to_source <- function(create_source_response_object, filepath_to_data, 
                             domain, email, password, status_checks = 100) {
  
  upload_data_url <- paste0('https://', domain, create_source_response_object$links$bytes)
  
  ## Is the data a data frame or a filepath?
  # if(exists(deparse(substitute(filepath_to_data)))) {
  #   
  #   if(inherits(filepath_to_data, "data.frame")){
  #     data_for_upload <- serialize(filepath_to_data, connection = NULL, ascii = FALSE)
  #     ## is there something needed here for chunking/handling large data.frames?
  #     
  #     upload_headers <- httr::add_headers("Content-Type" = "application/octet-stream")
  #     
  #   } else if((file.access(filepath_to_data, mode = 4)) == 0) {
  #     if(regexpr(".*\\.csv$", filepath_to_data) == 0) {
  #       data_for_upload <- httr::upload_file(filepath_to_data)
  #       upload_headers <- httr::add_headers("Content-Type" = "text/csv")
  #       
  #     } else if(regexpr(".*\\.csv$", filepath_to_data) == -1) {
  #       stop(filepath_to_data, " does not appear to be a csv file.")
  #     } 
  #   } else if((file.access(filepath_to_data, mode = 4)) == -1) {
  #     stop("The file ", filepath_to_data, " does not appear to exist or be accessible")
  #   } else if(!inherits(filepath_to_data, "data.frame")) {
  #     stop("The R object ", filepath_to_data, " does not appear to be a data.frame")
  #   }
  # } else if(!exists(deparse(substitute(filepath_to_data)))) {
  #   stop(filepath_to_data, " does not appear to exist?")
  # }
  
  if(!is.data.frame(filepath_to_data) & !is.character(filepath_to_data)) {
    stop(filepath_to_data, " does not appear to be a data.frame or filepath")
  }
  
  if (is.data.frame(filepath_to_data)) {
    temp_file <- tempfile("socrata_upload_temp.csv")
    data.table::fwrite(filepath_to_data, temp_file)
    data_for_upload <- httr::upload_file(temp_file)
  }
  
  if (is.character(filepath_to_data)) {
    data_for_upload <- httr::upload_file(filepath_to_data)
  }
  
  ## the body of the post is the new data
  upload_data_response <- httr::POST(upload_data_url,
                                     body = data_for_upload,
                                     httr::add_headers("Content-Type" = "text/csv"),
                                     httr::authenticate(email, password, 
                                                        type = "basic"),
                                     httr::user_agent(fetch_user_agent())
                                     )
  
  if (upload_data_response$status_code == "200") {
    message("Uploading data to draft.")
    upload_data_response <- jsonlite::fromJSON(httr::content(upload_data_response,as = "text",
                                                             type = "application/json", 
                                                             encoding = "utf-8"))
  } else {
    message("Failed to upload data to source with status code ",
            upload_data_response$status_code)
    stop_for_status(upload_data_response$status_code)
  }
  
  poll_for_status <- 0
  repeat {
    
    poll_for_status <- poll_for_status + 1
    
    if (!is.null(upload_data_response$resource$failed_at)) {
      upload_data_response
      stop("Upload failed. Check upload response.")
    } else if (!is.null(upload_data_response$resource$finished_at)){
      message("Upload finished.")
      break
    } else if (poll_for_status == status_checks) {
      stop("Polling for upload status verification has timed out. Check upload 
           response and/or increase poll limit.")
    } else {
      message("Polling for upload and data validation status. Stay tuned.")
      upload_data_response <- httr::GET(paste0(domain,
                                               upload_data_response$links$show),
                                        httr::authenticate(email, password, 
                                                           type = "basic"))
      
      httr::stop_for_status(upload_data_response)
      
      upload_data_response <- jsonlite::fromJSON(httr::content(upload_data_response,
                                                               as = "text",
                                                               type = "application/json", 
                                                               encoding = "utf-8"))
      
      Sys.sleep(1)
      
    }
  }
}


#' Apply: Apply, or publish, revision
#'
#' This function applies the revision and publishes the data update, once the 
#' data has completed its data validation and transformation steps.
#'
#' @param revision_response_object list. The response body from the 
#' open_revision function.
#' @param domain The Socrata domain URL, not including 'https://'
#' @param email Socrata username, or Socrata API Key
#' @param password Socrata password, or Socrata API Secret
#'
#' @return This function returns the response body from the httr call to the 
#' Socrata Data Management API apply endpoint.
#'
#' @note More information on Data Management API available 
#' at https://socratapublishing.docs.apiary.io/
#'
#' @examples
#' \dontrun{
#' apply_revision()
#' }
#' @importFrom httr PUT add_headers authenticate user_agent content stop_for_status
#' @importFrom jsonlite fromJSON
#'
#' @export
apply_revision <- function(revision_response_object, domain, email, password) {
  
  apply_revision_url <- paste0(domain, revision_response_object$links$apply)
  apply_revision_json <- paste0('{"resource": {"id":',
                                revision_response_object$resource$revision_seq,
                                '}}')
  
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
    apply_revision_response <- jsonlite::fromJSON(httr::content(apply_revision_response,as = "text",type = "application/json", encoding = "utf-8"))
    return(apply_revision_response)
  }
}


#' Update a Socrata Dataset with the Data Management API
#'
#' In one step, complete the steps of opening a draft, uploading data, and 
#' publishing your draft. 
#' Set the 'action_type' argument to "update", "replace", or "delete". 
#'
#' @param dataset_id character vector. The dataset's unique ID, or four by four,
#'  located at the end of a dataset's URL 
#' @param filepath_to_data character vector. Local filepath to the data. 
#' @param action_type character vector. One of "update", "replace", or "delete",
#'  depending on how you want to update the dataset.
#' @param source_type Default is "upload". Only uploads supported at this time. 
#' @param source_parse 'true' or 'false' for whether the source is of a data 
#' type Socrata can parse, including
#'     csv, tsv, xls, OpenXML, GeoJSON, KML, KMZ, Shapefile. Default is 'true'. 
#' @param domain_url character vector. The Socrata domain, 
#' not including 'https://'
#' @param email character vector. Socrata username, or Socrata API Key
#' @param password character vector. Socrata password, or Socrata API Secret
#'
#' @return This function returns the response body from the httr call to the 
#' Socrata Data Management API apply endpoint.
#'
#' @note More information on Data Management API available 
#' at https://socratapublishing.docs.apiary.io/
#'
#' @examples
#' \dontrun{
#' update_socrata()
#' }
#' @importFrom httr PUT POST GET add_headers authenticate user_agent content stop_for_status upload_file
#' @importFrom jsonlite fromJSON
#'
#' @export
update_socrata <- function(dataset_id, 
                           filepath_to_data, 
                           action_type,
                           source_type = "upload", 
                           source_parse = "true", 
                           domain, 
                           email, 
                           password) {
  
  if(validate_source(filepath_to_data)) {
  
    open_revision_socrata <- open_revision(dataset_id, action_type, domain, email, password)
    create_source_socrata <- create_source(open_revision_socrata, source_type, source_parse, domain, email, password)
    upload_to_source_socrata <- upload_to_source(create_source_socrata, filepath_to_data, domain, email, password)
    apply_revision_socrata <- apply_revision(open_revision_socrata, domain, email, password)
    return(apply_revision_socrata)
  } else {
    stop("The data source does not appear to be a data.frame or filepath to a csv")
  }
}


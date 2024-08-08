rm(list=ls())

library(tidyverse)
library(httr2)
library(RCurl)
library(httpuv)
library(jsonlite)
library(data.table)
library(writexl)

setwd("C:/Users/lbeatty/Documents/Arkansas")

source("C:/Users/lbeatty/Documents/SecretSauce.R")

base.url = 'https://api.enverus.com/v3/direct-access'

#############################
### GET Well Data ##############
#############################
token.path <- "https://api.enverus.com/v3/direct-access/tokens"

#headers
toke <- request(token.path) %>%
  req_body_json(list(secretKey = API_Key)) %>%
  req_perform()%>%
  resp_body_json()

###
my_token = toke$token


### GET DATA

well.path <- "https://api.enverus.com/v3/direct-access/wells"

wells = request(well.path)%>%
  req_auth_bearer_token(my_token)%>%
  req_url_query(StateProvince = "AR",
                DeletedDate = 'NULL',
                pagesize=10000)%>%
  req_perform()

tempdat = wells%>%
  resp_body_json(simplifyVector=T)

doc.path <- "https://api.enverus.com/v3/direct-access/wells?docs"
docs = request(doc.path)%>%
  req_auth_bearer_token(my_token)%>%
  req_perform()%>%
  resp_body_json(simplifyVector=T)

wells_docs=docs

nxtpg = wells$headers$Link
nxtpg = str_match_all(nxtpg, "<(.*?)>")[[1]][1,2]
nxtpg = paste(base.url, nxtpg, sep="")

data=tempdat

request=1

while(nrow(tempdat)!=0){
  tempdat = request(nxtpg)%>%
    req_auth_bearer_token(my_token)%>%
    req_perform()
  
  nxtpg = tempdat$headers$Link
  nxtpg = str_match_all(nxtpg, "<(.*?)>")[[1]][1,2]
  nxtpg = paste(base.url, nxtpg, sep="")
  
  tempdat = tempdat%>%
    resp_body_json(simplifyVector=T)
  
  data=rbind(data, tempdat)
  
  print(paste("Request #", request))
  request = request+1
  if(is_empty(tempdat)){break}
}
metadata = data.frame(Data_Name="Enverus_AR_Wells", Request=wells$request$url, Request_Date=today())
write.csv(data, "Data/Enverus_AR_Wells.csv")


###########
## Production

prod.path = "https://api.enverus.com/v3/direct-access/production"

dates=seq.Date(from=as.Date('2000-01-01'), to=as.Date('2000-12-01'), by='month')

for(d in 1:length(dates)){
  date=as.character(dates[d])
  print(date)
  prod = request(prod.path)%>%
    req_auth_bearer_token(my_token)%>%
    req_url_query(StateProvince = "AR",
                  DeletedDate = 'NULL',
                  ProducingMonth = date,
                  pagesize=10000)%>%
    req_perform()
  
  tempdat = prod%>%
    resp_body_json(simplifyVector=T)
  
  
  nxtpg = prod$headers$Link
  nxtpg = str_match_all(nxtpg, "<(.*?)>")[[1]][1,2]
  nxtpg = paste(base.url, nxtpg, sep="")
  
  data=tempdat
  
  request=1
  
  while(!is_empty(tempdat)){
    tempdat = request(nxtpg)%>%
      req_auth_bearer_token(my_token)%>%
      req_perform()
    
    nxtpg = tempdat$headers$Link
    nxtpg = str_match_all(nxtpg, "<(.*?)>")[[1]][1,2]
    nxtpg = paste(base.url, nxtpg, sep="")
    
    tempdat = tempdat%>%
      resp_body_json(simplifyVector=T)
    
    data=rbind(data, tempdat)
    
    print(paste("Request #", request))
    request = request+1
  }
  write.csv(data, paste("Data/Production_", date, '.csv', sep=''))
}

doc.path <- "https://api.enverus.com/v3/direct-access/production?docs"
docs = request(doc.path)%>%
  req_auth_bearer_token(my_token)%>%
  req_perform()%>%
  resp_body_json(simplifyVector=T)
production_docs=docs
metadata = rbind(metadata, data.frame(Data_Name="Production", Request=prod$request$url, Request_Date=today()))

write_xlsx(list(metadata=metadata, wells_dictionary = wells_docs, production_dictionary=production_docs), path="C:/Users/lbeatty/Documents/ArkansasDecline/Data/metadata.xlsx")

#install required packages
if(!require(pacman))install.packages("pacman")
pacman::p_load('rvest', 'stringi', 'dplyr', 'tidyr', 'measurements', 'reshape2','foreach','doParallel','raster','curl','httr','Iso','lambda.tools','RJSONIO','xml2','XML',"tabulizer",'janitor','stringi','stringr')

#setting proxy server


#set up the link to PGA website
owgr_url <- "http://www.owgr.com/about/?tabID={BBE32113-EBCB-4AD1-82AA-E3FE9741E2D9}&year="


#set year duration
year=2009:2019

#function to extract the url links to all pdf files with the time period set above
pdf_links<-function(index)
{
  repeat
  {
    res=try(owgr_url_links<-read_html(paste0(owgr_url,year[index]))%>%html_nodes("[class='item ranking']")%>%html_nodes("a")%>%html_attr("href"))
    if(inherits(res,"try-error"))
    {
      Sys.sleep(10)
    }
    else
    {
      break
    }
  }
  return(owgr_url_links)
}

#extract the tables from all links using multiprocessing or parallel computation
core=detectCores(1)
cluster=makeCluster(core[1])
registerDoParallel(cluster)
clusterExport(cluster,c("%>%","html_nodes","read_html","pdf_links","html_attr","owgr_url","year"))
system.time(pdf_tables_links<-parLapply(cluster,1:length(year),pdf_links))
stopCluster(cluster)


#combine the list of url links
all_pdf_links<-pdf_tables_links%>%unlist()


#extract tables from pdf
tables<-function(link_index)
{
  tables<-extract_tables(all_pdf_links[link_index])
}

core=detectCores(1)
cluster=makeCluster(core[1])
registerDoParallel(cluster)
clusterExport(cluster,c("tables","tables","extract_tables","all_pdf_links"))
system.time(pdf_tables<-parLapply(cluster,1:length(all_pdf_links),tables))
stopCluster(cluster)

new_pdftabl<-pdf_tables


unlisted<-unlist(pdf_tables,recursive = FALSE)

for (i in 1:length(unlisted)) {
  tryCatch({
    if (ncol(unlisted[[i]])==11) 
      unlisted[[i]]=unlisted[[i]][,-9]
  }, error=function(e){})
}

column_names<-list()
for (index in 1:length(unlisted)) 
{
  if(unlisted[[index]][4,1] == "") 
  {
    column_names[[index]]<-sapply(as.data.frame(unlisted[[index]][1:4, ]), paste, collapse = "")
  }
  else
  {
    column_names[[index]]<-sapply(as.data.frame(unlisted[[index]][1:3, ]), paste, collapse = "")
  }
}


for (i in 1:length(unlisted))
{
  if(unlisted[[i]][4,1]=="")
  {
    unlisted[[i]]=unlisted[[i]][-c(1:4),]%>%data.frame()
  }
  else
  {
    unlisted[[i]]=unlisted[[i]][-c(1:3),]%>%data.frame()
  }
}




for(i in 1:length(unlisted))
{
  tryCatch({
      colnames(unlisted[[i]])<-column_names[[i]]
  }, error=function(e){})
}



count=0
for(i in 1:length(pdf_tables))
{
  count[i]=length(pdf_tables[[i]])
}


for(i in 1:length(unlisted))
{
    names(unlisted[[i]])<-unlisted[[i]]%>%names()%>% stringr::str_replace_all(c("\\s"="_","\\("="_","\\)"="_","\\-"="_"))
}


#split_links<-setNames(type.convert(data.frame(
 # str_match(all_pdf_links, '), c('year', 'week'))
  

split_links<-str_match(all_pdf_links,'.*owgr(\\d+)[a-z]+(\\d+)')%>%data.frame()
colnames(split_links)<-c("link","Week","Year")

cnt1 <- cumsum(count)
index<-Map(`:`, c(1, cnt1[-length(cnt1)]+1), cnt1)

for(i in 1:length(index))
{
  for(j in 1:length(index[[i]]))
  {
    unlisted[[index[[i]][j]]]<-unlisted[[index[[i]][j]]]%>%data.frame()%>%mutate(Year=rep(split_links$Year[i],nrow(unlisted[[index[[i]][j]]])),
                                                               Week=(rep(split_links$Week[i],nrow(unlisted[[index[[i]][j]]]))),link=rep(all_pdf_links[i],nrow(unlisted[[index[[i]][j]]])))

    pages[index[[i]][j]]=j
    years[index[[i]][j]]=rep(split_links$Year[i],index[[i]][j])
    weeks[index[[i]][j]]=rep(split_links$Week[i],index[[i]][j])
    
    write.csv(unlisted[[index[[i]][j]]],paste0("C:/Users/SurafelTilahun/Luma Analytics/Lumineers - Documents/5 Users/Surafel/players_rank_data/reanked_data_",'_',years[index[[i]][j]],'_','week_',weeks[index[[i]][j]],'_',"page_",pages[index[[i]][j]],'.csv',rep=""))
  

    
  }
}




library(data.table)
library(dplyr)
library(tidyr)
library(lubridate)
setwd('U:/APP/temperature')

career = NULL
for ( y in c(2010:2019)) {
  game_info_name <- paste('game_info_', y, '.csv',sep = "")
  data <- read.csv(game_info_name, row.names = 1)
  
  game_info <- transpose(data)
  colnames(game_info) <- rownames(data)
  rownames(game_info) <- colnames(data)
  game_info <- setDT(game_info, keep.rownames = TRUE)[]
  colnames(game_info)[1] <- 'game_pk'
  
  game_info$game_pk <- gsub("X","",as.character(game_info$game_pk))
  game_info <- game_info[,c('game_pk','game date','Att','First pitch','T','Venue','Weather','Wind')]
  
  #############################merge#############################################
  bcr_name <- paste('game_bcr_', y , '.csv',sep = "")
  bcr <- read.csv(bcr_name, row.names = 1)
  bcr_game_info <- merge(bcr, game_info, by.x = 'game', by.y = 'game_pk')
  
  ############################split weather/wind columns###################################
  bcr_game_info <- bcr_game_info %>% separate(Weather, c('temperature','weather'), ", ")
  bcr_game_info$temperature <- as.numeric(gsub(" degrees", "",as.character(bcr_game_info$temperature)))
  bcr_game_info$weather <- as.character(sub("[.]$", "", bcr_game_info$weather))
  bcr_game_info$weather <- gsub("clear", "Clear", bcr_game_info$weather)
  
  bcr_game_info <- bcr_game_info %>% separate(Wind, c('wind_speed(mph)','wind_direction'), ", ")
  bcr_game_info$`wind_speed(mph)` <- as.numeric(gsub(" mph", "",as.character(bcr_game_info$`wind_speed(mph)`)))
  bcr_game_info$wind_direction <- as.character(sub("[.]$", "", bcr_game_info$wind_direction))
  
  bcr_game_info$Att <- as.character(sub("[.]$", "", bcr_game_info$Att))
  bcr_game_info$Att <- as.numeric(gsub(",","", bcr_game_info$Att))
  bcr_game_info$`First pitch` <- as.character(sub("[.]$", "", bcr_game_info$`First pitch`))
  bcr_game_info$T <- as.character(sub("[.]$", "", bcr_game_info$T))
  bcr_game_info$T <- gsub(" .*", "",bcr_game_info$T)
  bcr_game_info$Venue <- as.character(sub("[.]$", "", bcr_game_info$Venue))
  
  ############################convert game length into minutes#############################
  bcr_game_info$game_length <- hour(hm(bcr_game_info$T))* 60 + minute(hm(bcr_game_info$T))
  
  career <- rbind(career, bcr_game_info)
  
  ###############################write csv#################################################
  filename <- paste('bcr&game_info_', y, '.csv', sep = "")
  write.csv(bcr_game_info, filename)
}


###############################bcr vs. temperature#########################################
degree <- unique(career$temperature)
umplist <- unique(career$ump)

bcr_temp <- NULL
for (ump in umplist) {
  
  
  total_call_temp1 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(min(degree):39)])
  bad_call_temp1 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(min(degree):39)])
  bcr_temp1 <- bad_call_temp1 / total_call_temp1
  
  total_call_temp2 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(40:49)])
  bad_call_temp2 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(40:49)])
  bcr_temp2 <- bad_call_temp2 / total_call_temp2
  
  total_call_temp3 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(50:59)])
  bad_call_temp3 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(50:59)])
  bcr_temp3 <- bad_call_temp3 / total_call_temp3
  
  total_call_temp4 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(60:69)])
  bad_call_temp4 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(60:69)])
  bcr_temp4 <- bad_call_temp4 / total_call_temp4
  
  total_call_temp5 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(70:79)])
  bad_call_temp5 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(70:79)])
  bcr_temp5 <- bad_call_temp5 / total_call_temp5
  
  total_call_temp6 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(80:89)])
  bad_call_temp6 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(80:89)])
  bcr_temp6 <- bad_call_temp6 / total_call_temp6
  
  total_call_temp7 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(90:99)])
  bad_call_temp7 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(90:99)])
  bcr_temp7 <- bad_call_temp7 / total_call_temp7
  
  total_call_temp8 <- sum(career$total_call[career$ump == ump & career$temperature %in% c(100:max(degree))])
  bad_call_temp8 <- sum(career$bad_call[career$ump == ump & career$temperature %in% c(100:max(degree))])
  bcr_temp8 <- bad_call_temp8 / total_call_temp8
  
  bcrlist <- list(bcr_temp1, bcr_temp2,bcr_temp3,bcr_temp4,bcr_temp5,bcr_temp6,bcr_temp7,bcr_temp8)
  templist <- c('<40', '40-49','50-59','60-69','70-79','80-89','90-99','>=100')
  worst_temp <- templist[which.max(bcrlist)]
  best_temp <- templist[which.min(bcrlist)]
  
  id <- unique(career$id[career$ump == ump])
  
  temp <- data.frame(ump, id, worst_temp, best_temp, bcr_temp1, bcr_temp2, bcr_temp3,bcr_temp4,bcr_temp5,bcr_temp6,bcr_temp7,bcr_temp8)
  bcr_temp <- rbind(bcr_temp, temp)
  
}
colnames(bcr_temp)[5:12] <- paste('bcr_temp_',templist, sep = "")

write.csv(bcr_temp, 'bcr_temperature.csv', row.names = F)

#################################bcr vs. weather#######################################
weather <- unique(career$weather)
bcr_weather <- NULL

for (ump in umplist) {
  total_call_weather1 <- sum(career$total_call[career$ump == ump & career$weather == weather[1]])
  bad_call_weather1 <- sum(career$bad_call[career$ump == ump & career$weather == weather[1]])
  bcr_weather1 <- bad_call_weather1 / total_call_weather1
  
  total_call_weather2 <- sum(career$total_call[career$ump == ump & career$weather == weather[2]])
  bad_call_weather2 <- sum(career$bad_call[career$ump == ump & career$weather == weather[2]])
  bcr_weather2 <- bad_call_weather2 / total_call_weather2
  
  total_call_weather3 <- sum(career$total_call[career$ump == ump & career$weather == weather[3]])
  bad_call_weather3 <- sum(career$bad_call[career$ump == ump & career$weather == weather[3]])
  bcr_weather3 <- bad_call_weather3 / total_call_weather3
  
  total_call_weather4 <- sum(career$total_call[career$ump == ump & career$weather == weather[4]])
  bad_call_weather4 <- sum(career$bad_call[career$ump == ump & career$weather == weather[4]])
  bcr_weather4 <- bad_call_weather4 / total_call_weather4
  
  total_call_weather5 <- sum(career$total_call[career$ump == ump & career$weather == weather[5]])
  bad_call_weather5 <- sum(career$bad_call[career$ump == ump & career$weather == weather[5]])
  bcr_weather5 <- bad_call_weather5 / total_call_weather5
  
  total_call_weather6 <- sum(career$total_call[career$ump == ump & career$weather == weather[6]])
  bad_call_weather6 <- sum(career$bad_call[career$ump == ump & career$weather == weather[6]])
  bcr_weather6 <- bad_call_weather6 / total_call_weather6
  
  total_call_weather7 <- sum(career$total_call[career$ump == ump & career$weather == weather[7]])
  bad_call_weather7 <- sum(career$bad_call[career$ump == ump & career$weather == weather[7]])
  bcr_weather7 <- bad_call_weather7 / total_call_weather7
  
  total_call_weather8 <- sum(career$total_call[career$ump == ump & career$weather == weather[8]])
  bad_call_weather8 <- sum(career$bad_call[career$ump == ump & career$weather == weather[8]])
  bcr_weather8 <- bad_call_weather8 / total_call_weather8
  
  total_call_weather9 <- sum(career$total_call[career$ump == ump & career$weather == weather[9]])
  bad_call_weather9 <- sum(career$bad_call[career$ump == ump & career$weather == weather[9]])
  bcr_weather9 <- bad_call_weather9 / total_call_weather9
  
  total_call_weather10 <- sum(career$total_call[career$ump == ump & career$weather == weather[10]])
  bad_call_weather10 <- sum(career$bad_call[career$ump == ump & career$weather == weather[10]])
  bcr_weather10 <- bad_call_weather10 / total_call_weather10
  
  bcr_weather_list <- list(bcr_weather1, bcr_weather2,bcr_weather3,bcr_weather4,bcr_weather5,bcr_weather6,bcr_weather7,bcr_weather8,bcr_weather9,bcr_weather10)
  worst_weather <- weather[which.max(bcr_weather_list)]
  best_weather <- weather[which.min(bcr_weather_list)]
  
  id <- unique(career$id[career$ump == ump])
  
  df_weather <- data.frame(ump, id, worst_weather, best_weather,
                           bcr_weather1, bcr_weather2,bcr_weather3,bcr_weather4,bcr_weather5,bcr_weather6,bcr_weather7,bcr_weather8,bcr_weather9,bcr_weather10)
  bcr_weather <- rbind(bcr_weather, df_weather)
  
}
colnames(bcr_weather)[5:14] <- paste('bcr_weather_', weather,sep="")
write.csv(bcr_weather, 'bcr_weather.csv', row.names = F)

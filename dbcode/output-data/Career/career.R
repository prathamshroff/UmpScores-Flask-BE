library(dplyr)

############# career ##################
setwd("D:/RefRatings/APP/0814")
path <- "D:/RefRatings/APP/0801/"

years <- as.character(c(2010:2019))
#years <- as.character(c(2010:2018))

######################### season BCR #############################################
for (y in years) {
  file <- paste(path,y,"/pitcher_",y,".csv", sep = "")
  rawdata <- read.csv(file,stringsAsFactors = FALSE)
  
  calls <- data.frame(subset(rawdata, description %in% c('ball','called_strike') ))
  calls$bad_call <- ifelse(calls$description == 'ball' & calls$zone <= 9 | 
                             calls$description == 'called_strike' & calls$zone > 9, 1, 0)
  
  bad_calls = calls[calls$bad_call == 1,]
  colnames(calls)[colnames(calls) == 'Name'] <- 'ump'
  colnames(calls)[colnames(calls) == 'ID'] <- 'ump_id'
  colnames(calls)[colnames(calls) == 'fullName'] <- 'mlb_name'
  colnames(calls)[colnames(calls) == 'id'] <- 'mlb_id'
  colnames(calls)[colnames(calls) == 'team'] <- 'mlb_team_long'
  
  ########################################
  ##### for 2010, 2011 only############
  calls$mlb_team_long[calls$mlb_team_long == 'Florida Marlins'] <- 'Miami Marlins'
  
  team_name <- read.csv('team_name.csv')
  pitcher <- merge(calls,team_name, by.x = "mlb_team_long", by.y = "long")
  colnames(pitcher)[colnames(pitcher) == 'short'] <- 'mlb_team'
  pitcher <- pitcher[!is.na(pitcher$bad_call),]
  
  #############################################
  
  umplist <- unique(pitcher$ump)
  teams <- unique(pitcher$mlb_team)
  games <- unique(pitcher$game_pk)
  player_list <- unique(pitcher$mlb_id)
  #pitch_type <- unique(pitcher$pitch_type)
  pitch_type <- c('SL', 'FT', 'CU', 'FF', 'SI', 'CH', 'FC', 'EP', 'KC', 'FS', 'PO', 'KN', 'SC', 'FO', 'UN', 'FA', 'IN', '')
  pitcher$home_pitcher <- ifelse(pitcher$mlb_team == pitcher$home_team, 1,0)
  
  df = NULL
  for (name in umplist) {
    id <- unique(pitcher$ump_id[pitcher$ump == name])
    games <- length(unique(pitcher$game_pk[pitcher$ump == name]))
    
    total_call <- sum(pitcher$ump == name)
    bad_call <- sum(pitcher$ump == name & pitcher$bad_call == 1)
    bad_call_ratio <- bad_call / total_call
    
    call_ball <- sum(pitcher$ump == name & pitcher$description == "ball")
    call_strike <- sum(pitcher$ump == name & pitcher$description == "called_strike")
    
    bc_ball <- sum(pitcher$ump == name & pitcher$description == "ball" & pitcher$bad_call == 1)
    bc_strike <- sum(pitcher$ump == name & pitcher$description == "called_strike" & pitcher$bad_call == 1)
    
    bc_ball_ratio <- bc_ball/call_ball
    bc_strike_ratio <- bc_strike/call_strike
    
    ########## crucial potentially game-changing calls ################
    
    # 2 strikes
    call_when_2strikes <- sum(pitcher$ump == name & pitcher$strikes == 2)
    bc_when_2strikes <- sum(pitcher$ump == name & pitcher$strikes == 2 & pitcher$bad_call == 1)
    bcr_when_2strikes <- bc_when_2strikes / call_when_2strikes
    
    call_strike_when_2strikes <- sum(pitcher$ump == name & pitcher$strikes == 2 & pitcher$description == 'called_strike')
    ball_called_strike_when_2strikes <- sum(pitcher$ump == name & pitcher$strikes == 2 & pitcher$description == 'called_strike' & pitcher$bad_call == 1)
    ball_called_strike_ratio_when_2strikes <- ball_called_strike_when_2strikes / call_strike_when_2strikes
    
    call_ball_when_2strikes <- sum(pitcher$ump == name & pitcher$strikes == 2 & pitcher$description == 'ball')
    strike_called_ball_when_2strikes <- sum(pitcher$ump == name & pitcher$strikes == 2 & pitcher$description == 'ball' & pitcher$bad_call == 1)
    strike_called_ball_ratio_when_2strikes <- strike_called_ball_when_2strikes / call_ball_when_2strikes
    
    # 3 balls
    call_when_3balls <- sum(pitcher$ump == name & pitcher$balls == 3)
    bc_when_3balls <- sum(pitcher$ump == name & pitcher$balls == 3 & pitcher$bad_call == 1)
    bcr_when_3balls <- bc_when_3balls / call_when_3balls
    
    call_strike_when_3balls <- sum(pitcher$ump == name & pitcher$balls == 3 & pitcher$description == 'called_strike')
    ball_called_strike_when_3balls <- sum(pitcher$ump == name & pitcher$balls == 3 & pitcher$description == 'called_strike' & pitcher$bad_call == 1)
    ball_called_strike_ratio_when_3balls <- ball_called_strike_when_3balls / call_strike_when_3balls
    
    call_ball_when_3balls <- sum(pitcher$ump == name & pitcher$balls == 3 & pitcher$description == 'ball')
    strike_called_ball_when_3balls <- sum(pitcher$ump == name & pitcher$balls == 3 & pitcher$description == 'ball' & pitcher$bad_call == 1)
    strike_called_ball_ratio_when_3balls <- strike_called_ball_when_3balls / call_ball_when_3balls
    
    
    #left/right-handed
    call_lefthanded_pitcher <- sum(pitcher$ump == name & pitcher$p_throws == 'L')
    bc_lefthanded_pitcher <- sum(pitcher$ump == name & pitcher$p_throws == 'L' & pitcher$bad_call == 1)
    bcr_lefthanded_pitcher <- bc_lefthanded_pitcher / call_lefthanded_pitcher
    
    call_righthanded_pitcher <- sum(pitcher$ump == name & pitcher$p_throws == 'R')
    bc_righthanded_pitcher <- sum(pitcher$ump == name & pitcher$p_throws == 'R' & pitcher$bad_call == 1)
    bcr_righthanded_pitcher <- bc_righthanded_pitcher / call_righthanded_pitcher
    
    #innings
    df_inning = NULL
    colname = NULL
    col <- c('total_call_in_','bad_call_in_','BCR_in_')
    for (i in 1:10) {
      tc_inning <- sum(pitcher$ump == name & pitcher$inning == i)
      bc_inning <- sum(pitcher$ump == name & pitcher$inning == i & pitcher$bad_call == 1)
      bcr_inning <- bc_inning / tc_inning
      df_inning <- cbind(df_inning,tc_inning, bc_inning, bcr_inning)
      col_new <- paste(col, i, sep='')
      colname <- cbind(colname, col_new)
    }
    colnames(df_inning) <- colname
    
    #strike zone
    zones = c(1,2,3,4,5,6,7,8,9,11,12,13,14)
    df_zone = NULL
    colname = NULL
    col <- c('total_call_z','bad_call_z','BCR_z')
    for (j in zones) {
      tc_zone <- sum(pitcher$ump == name & pitcher$zone == j)
      bc_zone <- sum(pitcher$ump == name & pitcher$zone == j & pitcher$bad_call == 1)
      bcr_zone <- bc_zone / tc_zone
      df_zone <- cbind(df_zone, tc_zone, bc_zone, bcr_zone)
      col_new <- paste(col, j, sep='')
      colname <- cbind(colname, col_new)
    }
    colnames(df_zone) <- colname
    
    #pitch type
    df_ptype = NULL
    colname = NULL
    col <- c('total_call_','bad_call_','BCR_')
    for (p in pitch_type) {
      tc_ptype <- sum(pitcher$ump == name & pitcher$pitch_type == p)
      bc_ptype <- sum(pitcher$ump == name & pitcher$pitch_type == p & pitcher$bad_call == 1)
      bcr_ptype <- bc_ptype / tc_ptype
      df_ptype <- cbind(df_ptype, tc_ptype, bc_ptype, bcr_ptype)
      col_new <- paste(col, p, sep='')
      colname <- cbind(colname, col_new)
    }
    
    colnames(df_ptype) <- colname
    
    
    
    df = rbind(df, data.frame(name, id, games, call_ball, bc_ball, bc_ball_ratio, 
                              call_strike, bc_strike, bc_strike_ratio,
                              total_call,bad_call,bad_call_ratio,
                              ###
                              call_when_2strikes, bc_when_2strikes, bcr_when_2strikes,
                              call_ball_when_2strikes, strike_called_ball_when_2strikes, strike_called_ball_ratio_when_2strikes,
                              call_strike_when_2strikes, ball_called_strike_when_2strikes, ball_called_strike_ratio_when_2strikes,
                              ###
                              call_when_3balls, bc_when_3balls, bcr_when_3balls,
                              call_ball_when_3balls, strike_called_ball_when_3balls, strike_called_ball_ratio_when_3balls,
                              call_strike_when_3balls, ball_called_strike_when_3balls, ball_called_strike_ratio_when_3balls,
                              ###
                              call_lefthanded_pitcher, bc_lefthanded_pitcher, bcr_lefthanded_pitcher,
                              call_righthanded_pitcher, bc_righthanded_pitcher, bcr_righthanded_pitcher,
                              df_inning,df_zone,df_ptype))
  }
  filename = paste('season_bcr_',y,'.csv', sep = '')
  write.csv(df, filename)
}

#######################################################################################
setwd("D:/RefRatings/APP/0814")
years <- as.character(c(2010:2019))

path2 <- "D:/RefRatings/APP/0814"
dfs <- list()
i = 1
df_season = NULL
for (y in years) {
  file <- paste(path2,"/season_bcr_",y,".csv", sep = "")
  rawdata <- read.csv(file,stringsAsFactors = FALSE)
  dfs[[i]] <- rawdata
  i = i + 1
  df_season <- rbind(df_season, rawdata)
}

namelist <- unique(df_season$name)
idlist <- unique(df_season$id) 

df_career = NULL
for (ump in namelist) {
  total_call <- sum(df_season$total_call[df_season$name == ump])
  total_bad_call <- sum(df_season$bad_call[df_season$name == ump])
  career_bcr <- total_bad_call / total_call
  total_games <- sum(df_season$games[df_season$name == ump])
  id <- unique(df_season$id[df_season$name == ump])
  df_career = rbind(df_career, data.frame(ump, id, total_games, total_call, total_bad_call, career_bcr))
} 
write.csv(df_career, 'career.csv')



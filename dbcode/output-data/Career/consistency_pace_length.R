setwd('U:/APP/temperature')

game_info <- read.csv('10years_game_info.csv')
umplist <- unique(game_info[,c('ump','id')])
game_info$year <- gsub("/.*", "", game_info$date)
game_info$year <- gsub("-.*", "", game_info$year)


average_game_length_career <- aggregate(game_info$game_length, list(game_info$ump), mean)
colnames(average_game_length_career) <- c('ump', 'average_game_length_career')
df_gamelength = data.frame(average_game_length_career)

for ( y in c(2010:2019)) {
  average_game_length <- aggregate(game_info$game_length[game_info$year == y], list(game_info$ump[game_info$year == y]), mean)
  colnames(average_game_length) <- c('ump', paste('average_game_length_', y , sep = ""))
  df_gamelength <- merge(df_gamelength, average_game_length, by = 'ump', all = TRUE)
}
df_gamelength <- merge(df_gamelength, umplist)
df_gamelength <- df_gamelength[,c(1,13,2:12)]

write.csv(df_gamelength,'average_game_length.csv')
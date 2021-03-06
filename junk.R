

getwd()
source("functions.R")


library(tidyverse)
library(data.table)
library(glue)
library(janitor)
library(feather)


#
#' https://statsapi.web.nhl.com/api/v1/people/8475848/stats/?stats=careerRegularSeason
#'
#'
#'
#' https://statsapi.web.nhl.com/api/v1/people/8475848/stats/?stats=yearByYear
#'
#'
#' https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22goals%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22assists%22,%22direction%22:%22DESC%22%7D%5D&start=0&limit=50&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20192020%20and%20seasonId%3E=20192020
#'
#' https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=2019020001
#'
#' https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D%5D&start=0&limit=50&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20192020%20and%20seasonId%3E=20192020
#'
#' https://api.nhle.com/stats/rest/en/leaders/skaters/points?cayenneExp=season=20192020%20and%20gameType=2
#'
#'
#'





sch <- nhl_schedule(st_dt = "2018-07-02", en_dt = "2020-09-01")



gms <- sch %>%
    #sample_n(10) %>%
    pull(gamePk) %>%
    unique() %>%
    sample()

plays <- gms %>% nhl_games_plays()
actions <- gms %>% nhl_games_actions()
pressure <- gms %>% nhl_games_pressure()
shifts <- gms %>% nhl_Shifts_many()

NHL_TBL_CACHE_GLOBAL <- list()

nhl_cache_tables(gms)
nhl_cache_tables <- function(gms,
                             game_plays = T,
                             game_actions = T,
                             game_pressure = T,
                             game_shifts = T,
                             game_officials= T,
                             game_summary = T,
                             game_Player_on_ice = T,
                             playerprofiles = T#,
                             #venues = T
                             ){

    NHL_TBL_CACHE_tmp <- list()
    if (game_plays){
        NHL_TBL_CACHE_tmp[["game_plays"]] <- gms %>% nhl_games_plays()
    }
    if (game_actions){
        NHL_TBL_CACHE_tmp[["game_actions"]] <- gms %>% nhl_games_actions()
    }
    if (game_pressure){
        NHL_TBL_CACHE_tmp[["game_pressure"]] <- gms %>% nhl_games_pressure()
    }
    if (game_shifts){
        NHL_TBL_CACHE_tmp[["game_shifts"]] <- gms %>% nhl_games_shifts()
    }
    if(game_officials){
        NHL_TBL_CACHE_tmp[["game_officials"]] <- gms %>% nhl_games_officials()
        # NHL_TBL_CACHE_tmp[["game_officials"]]$link[[1]] %>%
        #     unique() %>%
        #     nhl_link()
    }
    if(game_Player_on_ice){
        NHL_TBL_CACHE_tmp[["game_Player_on_ice"]] <- nhl_players_on_ice_at_plays(plays = NHL_TBL_CACHE_tmp[["game_plays"]],
                                                                                 shifts = NHL_TBL_CACHE_tmp[["game_shifts"]]
                                                                                 )
    }
    if(game_summary){
        NHL_TBL_CACHE_tmp[["game_summary"]] <- gms %>% nhl_games_summary()
    }
    if(playerprofiles){
        NHL_TBL_CACHE_tmp[["playerprofiles"]] <-
            lapply(NHL_TBL_CACHE_tmp, function(x){
                x[["player_id"]]
            })  %>% unlist() %>% unique() %>%
                nhl_player_profiles() %>%
                tidyr::separate(height, into = c("feet", "inches"), convert = T) %>% mutate(height_cm = 2.54*(feet*12 + inches))

    }

    # combine the two caches
    lapply(names(NHL_TBL_CACHE_tmp), function(nm){
        NHL_TBL_CACHE_GLOBAL[[nm]] <<-
            bind_rows(
                NHL_TBL_CACHE_tmp[[nm]] %>%
                    mutate(time_generated = Sys.time()),
                NHL_TBL_CACHE_GLOBAL[[nm]]
            ) %>%
                arrange(desc(time_generated)) %>%
                distinct_at(vars(-time_generated), .keep_all = TRUE)

    })

    write_rds(x = NHL_TBL_CACHE_GLOBAL, file = "NHL_TBL_CACHE_GLOBAL.rds", compress = F)
}




tic = Sys.time()
actions <- gms %>% nhl_games_actions()
toc = Sys.time()
print(toc-tic)
nhl_games_actions



sch_games_without_pressure <-
    sch %>%
    rename(game_id := gamePk) %>%
    anti_join(pressure %>% count(game_id), by = "game_id")
pressure %>% count(game_id) %>% view()




# checks this wierd thing that sometime happens where dataframes are corrupt
lapply(colnames(plays), function(x){
    length(plays[[x]])

}) %>% unlist() %>% table()


#write_feather(plays, "plays.feather")
#nhl_cache_persist_to_disk()






sch_games_without_plays <-
    sch %>%
    rename(game_id := gamePk) %>%
    anti_join(plays %>% count(game_id), by = "game_id")
shifts <- NULL




sch_games_without_shifts <-
    sch %>%
    rename(gameId := gamePk) %>%
    anti_join(shifts %>% count(gameId), by = "gameId")



write_feather(shifts, "shifts.feather")
players_on_ice <- nhl_players_on_ice_at_plays(plays, shifts)
write_feather(players_on_ice, "players_on_ice.feather")



lapply(colnames(player_doing_play), function(x){
    length(player_doing_play[[x]])
}) %>% unlist() %>% table()
write_feather(player_doing_play, "player_doing_play.feather")


assertthat::assert_that(length(shifts$endTime) == nrow(shifts))



print(glue("game_id={game_id}, same={length(shifts$endTime) == nrow(shifts)} , shifts={nrow(shifts)}, endtime = {length(shifts$endTime)}...."))


write_feather(shifts, "shifts.feather")





#######################################
shifts <- read_feather("shifts.feather")
plays <- read_feather("plays.feather")


shifts %>% filter(game_id == 2018020417)

shifts
nhl_cache_persist_to_disk()


sch_games_without_shifts <-
    sch %>%
    rename(gameId := gamePk) %>%
    anti_join(shifts %>% count(gameId), by = "gameId")

shifts %>% count(gameId) %>% ggplot(aes(x = n)) + geom_density() + scale_x_continuous(limits = c(600,1000)) + geom_point(aes(y=0), alpha = 0.1)

shifts %>% count(gameId)



plays_players <- gms %>% nhl_games_plays_players()
nhl_persist_cache_to_disk()




plays %>%
    filter(event == "Shot") %>%
    ggplot(aes(x = coord_x_st, y = coord_y_st , color = is_home)) + geom_point()






players %>%
    filter(eventTypeId == "FACEOFF") %>%
    count( player_Type, player_name ) %>%
    pivot_wider(names_from = player_Type, values_from = n, values_fill = 0) %>%
    mutate(fraction_won = Winner/(Winner+Loser)) %>%
    arrange( desc(fraction_won), desc(Winner)) %>% view()


goals <-
    map_dfr(gms, function(game_id){
    this_game <- nhl_play_by_play(game_id)
    nhl_game_goal_info(this_game)
})



officials <-
    map_dfr(gms, function(game_id){
        this_game <- nhl_play_by_play(game_id)
        nhl_game_officials(this_game)
    })


officials %>%
    count(id, fullName, link, officialType, sort = T) %>%
    pivot_wider(names_from = officialType, values_from = n, values_fill = 0)


gm_summary <-
    map_dfr(gms, function(game_id){
        this_game <- nhl_play_by_play(game_id)
        nhl_game_summary(this_game)
    })



nhl_playerprofile



goals %>%
    ggplot(aes(x = coord_x_st, y = coord_y_st , color = is_home)) + geom_point()



shifts <- map_dfr(gms, function(game_id){nhl_Shifts(game_id)})

shifts %>% count(game_id)


pp <-
    shifts$playerId %>%
    nhl_playerprofiles()

pp %>% select(id, primaryPosition_name) %>% rename(playerID := id)
shifts %>%
    select(playerId, duration) %>%
    left_join(pp %>% select(id, primaryPosition_name, currentTeam_name) %>% rename(playerId := id)) %>%
    group_by(primaryPosition_name) %>%
    mutate(duration_mean = mean(duration, na.rm = T)) %>%
    filter(primaryPosition_name != "Goalie") %>%
    ggplot(aes(x = duration, y = primaryPosition_name, fill = primaryPosition_name, color = primaryPosition_name)) + geom_jitter(alpha = 0.1) +
    geom_violin(alpha = 0.5,  color = "black", draw_quantiles = c(0.25,0.5, 0.75))



geom_vline(xintercept = vars(duration_mean))


    group_by(primaryPosition_name, currentTeam_name) %>%
    summarise(n = n(),
              duration = mean(duration, na.rm = T)) %>%

     %>%

    facet_grid(rows = vars(primaryPosition_name), scales = "free")


nhl_Shifts



players %>% count(player_Type, sort = T)



this_game <- nhl_play_by_play(game_id)
shifts <- nhl_Shifts(game_id)
nhl_plays_df(this_game)
nhl_goal_info(this_game)
nhl_game_plays_players(this_game)
nhl_game_summary(this_game)
nhl_game_officials(this_game)



nhl_game_highlights <- function(game_id = 2020020017){
    url <- "https://statsapi.web.nhl.com/api/v1/game/{game_id}/content?site=en_nhl"
    raw_data <- url %>% glue() %>% fromJSON()
    names(raw_data)
    names(raw_data$editorial)
    names(raw_data$editorial$preview)
    tibble(raw_data$editorial$preview$items)
}





func_get_game_plays_full(this_game)





nhl_goal_info(this_game)
nhl_game_playes_players(this_game)


this_game <- nhl_play_by_play(game_id)

    all_pls <- raw_data$liveData$plays$allPlays

    all_pls[[1]]$strength <- all_pls[[1]]$strength$name

    all_pls[[2]] <-
        all_pls[[2]]$goals %>% setNames(paste0('goals_', names(.))) %>% bind_cols(all_pls[[2]] %>% select(-goals))

    all_pls[[4]] <- all_pls[[4]]%>% tibble() %>% setNames("players")



    bind_cols(
        all_pls[[1]] %>% tibble(),
        all_pls[[2]] %>% tibble(),
        all_pls[[3]] %>% tibble(),
        all_pls[[4]] %>% tibble(),
        all_pls[[5]] %>% tibble()
    ) %>% clean_names()


}


data <- read.csv("https://raw.githubusercontent.com/bigdatacup/Big-Data-Cup-2021/main/hackathon_scouting.csv")


game_id = 2020020017




8477933
raw_data <- "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=playerid=8477933"  %>% fromJSON()
nhl_Shifts(game_id)$playerId



https://statsapi.web.nhl.com/api/v1/game/2016020001/content?site=en_nhl

https://statsapi.web.nhl.com/api/v1/schedule?startDate=2016-10-01&endDate=2017-06-01


url <- "http://www.espn.com/nhl/gamecast/data/masterFeed?lang=en&isAll=true&gameId={game_id}"


resp <- url %>% glue() %>% GET()
a <- content(resp, "text", encoding = "ISO-8859-1")

resp

nhl_Shifts(game_id)
a <- nhl_p_by_p(game_id)




a <- url_gameData_prefix %>% paste0() %>% fromJSON()
a$teams %>% tibble()


a <- "https://statsapi.web.nhl.com/api/v1/schedule/" %>% paste("?date=2021-01-15") %>% fromJSON()


b <- a$dates %>% tibble()
c <- b$games[[1]] %>% tibble()
game_id <- c$gamePk[[1]]


z <- url_gameData_prefix %>% paste0(game_id, "/feed/live") %>% fromJSON()

z$liveData %>% names()

z$liveData$linescore%>% names()


z$liveData$decisions%>% names()


z$liveData$decisions$firstStar


z$liveData$decisions$firstStar


z$liveData$plays %>% names()


z$liveData$plays$allPlays %>% tibble() %>% view()
z$liveData$plays$allPlays %>% extract(1)

z$liveData$plays$allPlays

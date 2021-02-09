

#'
#'
#'
#'
#' some needs to be credited to Martin Ellis (martinellis)
#' https://www.kaggle.com/martinellis/where-the-data-comes-from
#'
#' Some other code comes from
#' https://github.com/SimonCoulombe/nhl_play_by_play
#'
#' additional credit for some documentation of the API to
#'
#' https://gitlab.com/dword4/nhlapi
#'
#'
#'


rm(list=ls())
gc()



library(tidyverse)
library(jsonlite)
library(data.table)
library(glue)
library(janitor)
library(curl)
library(httr)
library(roxygen2)
library(digest)
library(ggforce)
library(sqldf)


NHL_ICE_FEATURES = list(
    # ice features
    faceoff_circles = data.frame(
        x0 = c(0, 69, 69, 20, 20),
        y0 = c(0, 22,-22, 22,-22),
        r = c(15, 15, 15, 1, 1)
    ) #neutral should be 1, but too small for my liking
,
    goal_rectangle = data.frame(
        xmin = 89,
        xmax = 91,
        ymin = -3,
        ymax = 3
    )
,
    blue_line= data.frame(x = c(25, 25),
                            y = c(-40, 40))
,
    goal_line = data.frame(x = c(89, 89),
                            y = c(-40, 40)),
    center_line = data.frame(x = c(0,0),
                          y = c(-40, 40))
)

NHL_gg_add_ice_elments <-function(gg_object = ggplot(data = tibble(), mapping = aes())){

    gg_object +
        geom_line(data = NHL_ICE_FEATURES$blue_line, aes(x = x, y = y), color = "blue", size = 3) +
        geom_line(data = NHL_ICE_FEATURES$blue_line, aes(x = -x, y = y), color = "blue", size = 3) +
        geom_line(data = NHL_ICE_FEATURES$center_line, aes(x = x, y = y), color = "red", size = 3) +
        geom_line(data = NHL_ICE_FEATURES$goal_line, aes(x = x, y = y), color = "red") +
        geom_line(data = NHL_ICE_FEATURES$goal_line, aes(x = -x, y = y), color = "red") +
        geom_rect(
            data = NHL_ICE_FEATURES$goal_rectangle,
            aes(
                xmin = xmin,
                xmax = xmax,
                ymin = ymin,
                ymax = ymax
            ),
            fill = "blue",
            alpha = 0.1,
            color = "red"
        ) +
        geom_rect(
            data = NHL_ICE_FEATURES$goal_rectangle,
            aes(
                xmin = -xmin,
                xmax = -xmax,
                ymin = ymin,
                ymax = ymax
            ),
            fill = "steelblue2",
            alpha = 0.1,
            color = "red"
        ) +
        geom_circle(
            data = NHL_ICE_FEATURES$faceoff_circles,
            aes(x0 = x0, y0 = y0, r = r),
            fill = "steelblue2",
            alpha = 0.1,
            color = "red"
        ) +
        geom_circle(
            data = NHL_ICE_FEATURES$faceoff_circles,
            aes(x0 = -x0, y0 = y0, r = r),
            fill = "steelblue2",
            alpha = 0.1,
            color = "red"
        ) + ggplot2::theme_void()
}



###################################
#' global variable that records the last time we hit the NHL API
#'
NHL_LAST_API_CALL <- Sys.time()

###################################
#' global variable that says how long to wait in seconds between API calls
#'
NHL_DELAY_TIME_API = 0.25

###################################
#' global variable that hold the NHL Cache it is a list with a hash of the URL as the key
#'
#'
nhl_cache_get_blank <- function(){
tibble(key = character(),
       url = character(),
       fetched =  as.POSIXct(NA),
       val = list())
}

NHL_API_CACHE = nhl_cache_get_blank()



nhl_cache_persist_to_disk <- function(){
    write_rds(x = NHL_API_CACHE, file = "nhl_cache.rds")
}

nhl_cache_read_from_disk <- function(){

    NHL_API_CACHE <<- tryCatch(read_rds("nhl_cache.rds"),
                    warning = function(w){
                        print("warning on reading NHL cache")
                        return(nhl_cache_get_blank())
                        },
                    error = function(e) {
                        print("Error on read disk cache NHL API")
                        return(nhl_cache_get_blank)
                    })
}

nhl_cache_read_from_disk()
#full_url= "https://statsapi.web.nhl.com/api/v1/game/2019021033/feed/live?site=en_nhl"
#full_url="https://statsapi.web.nhl.com/api/v1/game/2019030123/feed/live?site=en_nhl"
#full_url="https://statsapi.web.nhl.com/api/v1/game/2018010001/feed/live?site=en_nhl"
###################################
#'cache wrapper around fromJSON function
#' @param url the url to call
fromJSON_cache <- function(full_url){
    curr_key <- digest(full_url)

    cache_key <- NHL_API_CACHE %>% filter(key == curr_key)


    if(nrow(cache_key) > 1){
        print("more than one key found, deleting all cache for that key.")
        NHL_API_CACHE <<-
            NHL_API_CACHE %>%
            subset(key != curr_key)
    }


    cache_key <- NHL_API_CACHE %>% filter(key == curr_key)


    if(nrow(cache_key) == 0){

        ntime <- difftime(Sys.time(), NHL_LAST_API_CALL, units = "secs") %>% as.double()
        if(ntime <= NHL_DELAY_TIME_API){
            cat(glue("pause API for {NHL_DELAY_TIME_API}s..."))
            Sys.sleep(NHL_DELAY_TIME_API)
        }

        cat(glue("NHL API call `{full_url}`..."))

        val <- tryCatch(fromJSON(full_url),
                 warning = function(w){
                     print("warning on API")
                     },
                 error = function(e) {
                     print("Error on API")
                     return(e)
                     })

        NHL_LAST_API_CALL<<- Sys.time()

        if (is(val, "error")){
            print("cacheing error ...")
        }else{
            NHL_API_CACHE <<-
                NHL_API_CACHE %>%
                add_row(key = curr_key,
                        url = full_url,
                        fetched = NHL_LAST_API_CALL,
                        val = list(wrap = val))
        }

    }else{
        #print(glue("using cash for `{full_url}`"))
    }

    NHL_API_CACHE %>% filter(key == curr_key) %>% pull(val) %>% nth(1)
}



nhl_players_on_ice_at_plays <- function(plays, shifts){

    shifts <-
        shifts %>%
        rename(game_id := gameId) %>%
        rename(shift_id := id) %>%
        rename(team_id := teamId) %>%
        rename(team_name := teamName) %>%

    s <- shifts %>% select(shift_id, game_id, period, playerId, startTime, endTime, team_id, team_name) %>%
        rename(shift_team_id := team_id, shift_team_name := team_name)

    pp <- plays %>% select(play_id, game_id, period, periodTime, event, team_id, team_name, is_home) %>%
        rename(play_team_id := team_id, play_team_name := team_name, play_is_home := is_home)

    p_need_start <-
        pp %>%
        filter(event %in% c("Game Scheduled", "Period Ready", "Period Start", "Faceoff", "Early Intermission End" ))

    p_need_end <-
        pp %>%
        filter(event %in% c("Stoppage", "Penalty" , "Period End", "Period Official","Game End","Game Official", "Official Challenge", "Early Intermission Start", "Goal" ) )

    p_rest <- pp %>% anti_join(p_need_start, by = c("play_id")) %>% anti_join(p_need_end, by = c("play_id"))




    sp_rest <-
    sqldf ("
        SELECT s.shift_id  , s.playerId , s.startTime, s.endTime , p.*
        FROM s AS s
        INNER JOIN p_rest as p ON
    s.game_id = p.game_id AND
    s.period = p.period AND
    s.startTime <= p.periodTime AND
    s.endTime >= p.periodTime") %>% tibble()

    sp_need_start <-
        sqldf ("
        SELECT s.shift_id  , s.playerId , s.startTime, s.endTime , p.*
        FROM s AS s
        INNER JOIN p_need_start as p ON
    s.game_id = p.game_id AND
    s.period = p.period AND
    s.startTime <= p.periodTime AND
    s.endTime > p.periodTime") %>% tibble()

    sp_need_end <-
        sqldf ("
        SELECT s.shift_id  , s.playerId , s.startTime, s.endTime , p.*
        FROM s AS s
        INNER JOIN p_need_end as p ON
    s.game_id = p.game_id AND
    s.period = p.period AND
    s.startTime < p.periodTime AND
    s.endTime >= p.periodTime") %>% tibble()


    shifts_plays <-
        rbind(sp_rest,
              sp_need_start,
              sp_need_end)
    shifts_plays
}




###################################
#' takes a game object and returns a table with 4 columns, showing the players in involved in each play
#' @param this_game
nhl_game_plays_players2 <- function(this_game){

    plays_players_list <- this_game$liveData$plays$allPlays$players



    map_dfr(1:length(plays_players_list), function(row_i){
        if(!is.null(plays_players_list[[row_i]])){
            tibble(
                play_id=paste(this_game$gamePk, row_i, sep="_"),
                game_id=this_game$gamePk %>% as.character(),
                eventTypeId=this_game$liveData$plays$allPlays$result$eventTypeId[[row_i]],
                player_id=plays_players_list[[row_i]]$player$id %>% as.character(),
                player_Type=plays_players_list[[row_i]]$playerType,
                player_name=plays_players_list[[row_i]]$player$fullName,
                player_link=plays_players_list[[row_i]]$player$link
                )
        }
    })


}


###################################
#' returns either either HOME|AWAY|TBC depending on who won the game
#' @param this_game game object
nhl_game_winner <- function(this_game){
    if(this_game$liveData$linescore$teams$home$goals >
       this_game$liveData$linescore$teams$away$goals){
        return("HOME")
    } else if(this_game$liveData$linescore$teams$away$goals >
              this_game$liveData$linescore$teams$home$goals){
        return("AWAY")
    } else{
        print(paste0("ERROR: result undetermined.   game_id: ", this_game$gamePk))
        return("TBC")
    }
}

###################################
#' returns either REG|OT|TBC
#' @param this_game game object
nhl_game_settled_in <- function(this_game){
    if(this_game$liveData$linescore$currentPeriod == 3){
        return("REG")
    } else  if(this_game$liveData$linescore$currentPeriod > 3){
        return("OT")
    } else{
        return("TBC")
    }
}

###################################
#' returns either if away or home won the game
#' @param this_game game object
nhl_game_result <- function(this_game){
    paste(nhl_game_winner(this_game), "win", nhl_game_settled_in(this_game))
}





###################################
#' returns tibble with a very high level summary of the game
#' @param this_game game object
nhl_game_summary <- function(this_game){

    table_game <- tibble(

        game_id=this_game$gamePk %>% as.character(),
        season=this_game$gameData$game$season,
        type=this_game$gameData$game$type,
        date_time_GMT=this_game$gameData$datetime$dateTime,
        away_team_id=this_game$gameData$teams$away$id %>% as.character(),
        away_team_name=this_game$gameData$teams$away$name %>% as.character(),
        home_team_id=this_game$gameData$teams$home$id %>% as.character(),
        home_team_name=this_game$gameData$teams$home$name %>% as.character(),
        away_goals=this_game$liveData$linescore$teams$away$goals %>% as.integer,
        home_goals=this_game$liveData$linescore$teams$home$goals %>% as.integer,
        outcome=nhl_game_result(this_game),
        home_rink_side_start=
            ifelse(is.null(this_game$liveData$linescore$periods$home$rinkSide[1]),
                   NA, this_game$liveData$linescore$periods$home$rinkSide[1]),
        venue=this_game$gameData$venue$name,
        venue_link=this_game$gameData$venue$link,
        venue_time_zone_id=this_game$gameData$teams$home$venue$timeZone$id,
        venue_time_zone_offset=this_game$gameData$teams$home$venue$timeZone$offset,
        venue_time_zone_tz=this_game$gameData$teams$home$venue$timeZone$tz
    )

    return(table_game)
}


nhl_games_plays_players <- function(ids){
    ids %>%
        unique() %>%
        map_dfr(nhl_game_plays_players)
}


nhl_game_plays_players <- function(id){
    id %>%
        nhl_play_by_play() %>%
        nhl_game_plays_players2()
}




nhl_games_plays <- function(ids){
    tmp <-
        ids %>%
        unique() %>%
        lapply(nhl_game_plays)

    #Remove blank tibbles
    tmp <- tmp[lapply(tmp, function(x){nrow(x)}) %>% unlist() != 0] #%>% length()

    #bind the tibbles
    tmp <- tmp %>% bind_rows()

    tmp
}



nhl_game_plays <- function(id){
    id %>%
        nhl_play_by_play() %>%
        nhl_game_plays2()
}

#this_game <- nhl_play_by_play(2018010002)
#this_game <- nhl_play_by_play(2019010084)
###################################
#' takes a game object and returns a dataframe of information about the plays
#' @param this_game game object
nhl_game_plays2 <- function(this_game){

    if(is_null(this_game)){
        return(tibble())
    }


    ap <- this_game$liveData$plays$allPlay %>% tibble()

    if (nrow(ap) == 0){
        #print("Game has no plays")
        return(tibble())
    }

    game_id <- this_game$gamePk

    home_team_id <- this_game$gameData$teams$home$id
    game_plays <- bind_cols(ap$about %>% tibble() %>%
                                mutate(goals_away = goals$away,
                                       goals_home = goals$home) %>%
                                select(-goals),
                            ap$result %>% tibble() %>%
                                mutate(strength = strength$name),
                            ap$coordinates %>% tibble() %>%
                                rename_all(~ paste0("coord_", .x)),
                            ap$team %>% tibble() %>%
                                rename_all(~ paste0("team_", .x))
    ) %>%
        mutate(game_id = game_id) %>%
        mutate(play_id = paste0(game_id,"_", eventIdx)) %>%
        mutate(is_home = home_team_id == team_id) %>%
        mutate(periodTime = as.ITime(periodTime, format="%M:%OS")) %>%
        mutate(periodTimeRemaining = as.ITime(periodTimeRemaining, format="%M:%OS"))


    game_plays %>%
        nhl_game_x_y_normalize() %>%
        return()


}


###################################
#' returns a dataframe of information about the goals in the game
#' @param this_game game object
nhl_game_goal_info <- function(this_game){
    game_plays <- nhl_game_plays2(this_game)

    goal_plays <- game_plays %>%
        filter(event == "Goal") %>%
        select(any_of(c("play_id", "period", "periodTime", "strength", "gameWinningGoal", "emptyNet", "is_home", "coord_x", "coord_y", "coord_x_st", "coord_y_st")))

    return(goal_plays)
}


###################################
#' takes a game object and returns an officials table for the data
#' @param this_game game object
nhl_game_officials <- function(this_game){

    ret_val <- tibble()
    if(!is.null(this_game$liveData$boxscore$officials$official)){

        officials <- this_game$liveData$boxscore$officials %>% as_tibble()

    ret_val <-
        bind_cols(
            select(officials, -official),
            officials$official
        ) %>%
        mutate(game_id = this_game$gamePk)

    }

    ret_val
}


###################################
#' Returns plays with two either added or replaced columns coord_x_st, coord_y_st, event, period, thes values are standardized so that home is always shooting to the right and away always shoots to the left in all periods
#' @param plays dataframe of plays, must have these columns coord_x, coord_y
nhl_game_x_y_normalize <- function(plays){
    #x,y coordinates are based on actual cartesian coordiantes, establish a
    #standard x&y based on relative to a team's atacking & defending zone.
    #this is made more difficult as the NHL records them relative to which side
    #the off-ice officials sit, which can vary. This is not related to team's bench
    #rink-side



    if(!("coord_x" %in% colnames(plays) &
       "coord_y" %in% colnames(plays) &
       "team_id" %in% colnames(plays) &
       "event" %in% colnames(plays) &
       "period" %in% colnames(plays)
       )){
        return(plays)
    }



    plays_loc <-
        plays %>%
        filter(!is.na(coord_x) & !is.na(coord_y) & !is.na(team_id))

    needed_wider_cols <- tibble(game_id=integer(), is_home = logical(), true= integer(), false= integer())

    plays_loc %>%
        filter(event == "Shot" & period == 1) %>%
        #select(coord_x, is_home) %>%
        mutate(x_is_positive = coord_x > 0) %>%
        count(game_id, is_home, x_is_positive, sort = T) %>%
        pivot_wider(names_from = x_is_positive, values_from = n, values_fill = 0) %>%
        clean_names() %>%
        bind_rows(needed_wider_cols, .) %>%
        replace_na(list(true = 0, false = 0)) %>%
        mutate(switch = (false > TRUE & is_home) |
               (true > FALSE & !is_home)
               )%>%
        mutate(needs_switch_count = ifelse(is_home,  false-true, true-false)) %>%
        group_by(game_id) %>%
        summarise(needs_switch_count = sum(needs_switch_count)) %>%
        mutate(needs_switch = needs_switch_count > 0) %>%
        select( game_id, needs_switch) %>%
        full_join(.,
            select(plays_loc,play_id, game_id, period, coord_y, coord_x) ,
            by = "game_id") %>%
        mutate(coord_x_st = ifelse((needs_switch & period %in% c(2,4,6,8)) |
                                    (!needs_switch & period %in% c(1,3,5,7))  ,
               coord_x,
               -coord_x)
               ) %>%
        mutate(coord_y_st = ifelse((needs_switch & period %in% c(2,4,6,8)) |
                                       (!needs_switch & period %in% c(1,3,5,7))  ,
                                   coord_y,
                                   -coord_y)

        ) %>%
        select(play_id, coord_x_st, coord_y_st) %>%
        left_join(select(plays, -one_of("coord_x_st", "coord_y_st")), .,  by = c("play_id"))

        #count(event)
    # %>%
        #filter(event == "Goal") %>%
        #ggplot(aes(x = coord_x_st, y = coord_y_st, color = is_home)) + geom_point() + facet_wrap(facets = vars(period))

}






###################################
#' Returns a object, by checking with the NHL's API
#' It returns a tibble, that is named from the one thing you passed in
#' @param full_url url to look up
#' @param id only used in error messegaes
#' @param name2get we do something like this tibble(raw_data[[name2get]]), to make sure to get get get the sub data you need
nhl_generic_data <- function(full_url, id, name2get) {
    raw_data <- fromJSON_cache(full_url)

    ret_val <-
        tibble(raw_data[[name2get]]) %>%
        mutate(id = id)

    if(nrow(ret_val) == 0){
        #print(glue("id '{id}' has no '{name2get}' at '{full_url}'"))
        return(tibble())
    }

    ret_val
}





###################################
#' Returns a game object, by checking with the NHL's API
#' @param game_id game id
nhl_play_by_play <- function(game_id, url = "https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live?site=en_nhl"){
    raw_data <- url %>%
        glue() %>%
        fromJSON_cache()
    return(raw_data)
}



###################################
#' does nhl_Shifts many times
#' @param game_ids vector of game_ID
nhl_Shifts_many <- function(game_ids){
    tmp <-
    game_ids %>%
        unique() %>%
        #map_dfr(nhl_Shifts)
        lapply(nhl_Shifts)

    #Remove blank tibbles
    tmp <- tmp[lapply(tmp, function(x){nrow(x)}) %>% unlist() != 0] #%>% length()

    #bind the tibbles
    tmp <- tmp %>% bind_rows()
    #lapply(tmp, function(x){nrow(x) == length(x$endTime)}) %>% unlist() %>% table()
    # nrow(tmp)
    # length(tmp$endTime)
    # lapply(colnames(tmp), function(x){
    #     print(length(tmp[[x]]))
    #
    # })
    tmp
}

###################################
#' Returns a shifts object, by checking with the NHL's API
#' @param game_id game id
nhl_Shifts <- function(game_id, url = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"){
    shifts <-
        url %>% glue() %>%
        nhl_generic_data(game_id, "data")

    if (nrow(shifts) == 0){
        # print(glue("game_id={game_id}, same={length(shifts$endTime) == nrow(shifts)} , shifts={nrow(shifts)}, endtime = {length(shifts$endTime)}...."))
        # if (length(shifts$endTime) != nrow(shifts)){
        #     print(glue("endtime = {length(shifts$endTime)}"))
        #     print(glue("shifts = {nrow(shifts)}"))
        #     assertthat::assert_that(length(shifts$endTime) == nrow(shifts))
        #     stop()
        # }

        return(shifts)
    }
    # print(glue("game_id={game_id}, same={length(shifts$endTime) == nrow(shifts)} , shifts={nrow(shifts)}, endtime = {length(shifts$endTime)}...."))
    # if (length(shifts$endTime) != nrow(shifts)){
    #     print(glue("game_id={game_id}, same={length(shifts$endTime) == nrow(shifts)} , shifts={nrow(shifts)}, endtime = {length(shifts$endTime)}...."))
    #     print(glue("endtime = {length(shifts$endTime)}"))
    #     print(glue("shifts = {nrow(shifts)}"))
    #     assertthat::assert_that(length(shifts$endTime) == nrow(shifts))
    #     stop()
    # }

    shifts <-
        shifts %>%
            mutate(duration = as.ITime(duration, format="%M:%OS")) %>%
            mutate(endTime = as.ITime(endTime, format="%M:%OS")) %>%
            mutate(startTime = as.ITime(startTime, format="%M:%OS")) %>%
            mutate(duration = minute(duration)*60 + second(duration))

    # print(glue("game_id={game_id}, same={length(shifts$endTime) == nrow(shifts)} , shifts={nrow(shifts)}, endtime = {length(shifts$endTime)}...."))
    # if (length(shifts$endTime) != nrow(shifts)){
    #     print(glue("game_id={game_id}, same={length(shifts$endTime) == nrow(shifts)} , shifts={nrow(shifts)}, endtime = {length(shifts$endTime)}...."))
    #     print(glue("endtime = {length(shifts$endTime)}"))
    #     print(glue("shifts = {nrow(shifts)}"))
    #     assertthat::assert_that(length(shifts$endTime) == nrow(shifts))
    #     stop()
    # }
    shifts
}




###################################
#' Returns players tombstone information by checking with the NHL's API
#' @param game_id game id
nhl_playerprofiles <- function(id){
    id %>%
        unique() %>%
        map_dfr(nhl_playerprofile)
}

###################################
#' Returns a player tombstone information by checking with the NHL's API
#' @param game_id game id
nhl_playerprofile <- function(player_id = 8477474, url = "http://statsapi.web.nhl.com/api/v1/people/{player_id}"){
    playerprofile <-
        url %>% glue() %>%
        nhl_generic_data(player_id, "people")

    if (nrow(playerprofile) == 0){
        return(playerprofile)
    }

    playerprofile <- nhl_expand_df_many(playerprofile, c("primaryPosition", "currentTeam"))


    playerprofile
}




###################################
#' Does nhl_teamprofile many times for a vector
#' @param venue_id venue_id
nhl_teamprofiles <- function(team_id){
    team_id %>%
        unique() %>%
        map_dfr(nhl_teamprofile)
}


###################################
#'
nhl_expand_df_many <- function(raw_data, col_nms){
    for(col_nm in col_nms){
        raw_data <- nhl_expand_df(raw_data, col_nm)
    }
    return(raw_data)
}


###################################
#'
nhl_expand_df <- function(raw_data, col_nm){
    if(col_nm %in% colnames(raw_data)){
        raw_data <-
            bind_cols(
                select(raw_data, -col_nm),
                raw_data[[col_nm]] %>% rename_all( ~ paste0(col_nm, "_", .x))
            )
    }
    return(raw_data)
}

###################################
#' Returns a teams tombstone information by checking with the NHL's API
#' @param team_id team_id
nhl_teamprofile <- function(team_id = 1, url = "https://statsapi.web.nhl.com/api/v1/teams/{team_id}"){
    raw_data <- url %>% glue() %>%
        nhl_generic_data(team_id, "teams")


    raw_data <- nhl_expand_df_many(raw_data, c("venue", "division","conference","franchise"))
    raw_data <- nhl_expand_df_many(raw_data, c("venue_timeZone"))
    return(raw_data)
}


###################################
#' Does nhl_venue many times for a vector
#' @param venue_id venue_id
nhl_venues <- function(venue_id){
    venue_id %>%
        unique() %>%
        map_dfr(nhl_venue)
}

#nhl_venues(sch$venue_id[1:5])

###################################
#' Returns a venue tombstone information by checking with the NHL's API
#' @param venue_id venue_id
nhl_venue <- function(venue_id = 1, url = "https://statsapi.web.nhl.com/api/v1/venues/{venue_id}"){
    url %>% glue() %>%
        nhl_generic_data(venue_id, "venues")

}


###################################
#' Returns a schedule of activity between two dates
#' @param st_dt 2018-07-01
#' @param en_dt 2020-09-01
nhl_schedule <- function(st_dt = "2018-07-01", en_dt= "2020-09-01", url = "https://statsapi.web.nhl.com/api/v1/schedule?startDate={st_dt}&endDate={en_dt}"){
    raw_data <- url %>% glue() %>% fromJSON_cache()

    dts <- raw_data$dates
    map_dfr(1:nrow(dts), function(irow){

        curr_gms <-
        dts$games[[irow]] %>%
            select(gamePk, link, gameType, season,gameDate) %>%
            tibble()

        curr_tms <- dts$games[[irow]]$teams


        bind_cols(curr_gms,
                  dts$games[[irow]]$status,
                  dts$games[[irow]]$venue %>%
                      rename_all( ~ paste0("venue_", .x) )
                  ,
                  dts$games[[irow]]$teams$home$team %>%
                      rename_all( ~ paste0("home_team_", .x) ),

                  dts$games[[irow]]$teams$away$team %>%
                      rename_all( ~ paste0("away_team_", .x) )
                  )
    })
}





###################################
#' Returns a what ever is at the "link" as many parts of NHL API return a link column you can feed that in here to get a new object from the API
#' @param link link as given in another part of the API
nhl_link <- function(link, url = "https://statsapi.web.nhl.com{link}"){
    url %>% glue() %>% fromJSON_cache()
}

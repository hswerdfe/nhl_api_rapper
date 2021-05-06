

library(MASS)
library(akima)
library(RColorBrewer)
library(colorspace)
library(lubridate)
library(stringr)
#nhl_cache_mns()



################################
#'
#'
#'
#'
NHL_ICE_LIM <-
list(
 x_lim = c(-100, 100),
 y_lim = c(-43, 43)
)
# col_nm = "player_id"
# col_val = players_of_pos


nhl_playing_time <- function(...){
    nhl_cache_game_shifts(...) %>%
        pull(duration) %>%
        sum(na.rm = T)
}



################################
#'
#'
#'
#'
nhl_attack_on_right <- function(dat,
                               x = "coord_x_st",
                               y = "coord_y_st",
                               not_flip = "is_home"
                               ){
    dat[[x]] <- if_else(dat[[flip]], dat[[x]], -dat[[x]])
    dat[[y]] <- if_else(dat[[flip]], dat[[y]], -dat[[y]])
    return(dat)
}




################################
#'
#'
#'
#'
nhl_side_filter <- function(dat,
                           side = "right",
                           x = "coord_x_st",
                           y = "coord_y_st"){

    if(side == "right"){
        return(dat[dat[[x]] >0,])
    }
    if(side == "left"){
        return(dat[dat[[x]] <0,])
    }
    return(dat)

}




nhl_2D_grid_density_playTime <- function(){
    nhl_2D_grid_density()
}
################################
#'
#'
#'
#'
nhl_2D_grid_density <- function(dat,
                                x = "coord_x_st",
                                y = "coord_y_st",
                                n = c(200, 100),
                                limits = c(NHL_ICE_LIM$x_lim, NHL_ICE_LIM$y_lim),
                                mulli_by_rows = T
){
    if(nrow(dat) <= 1){
        return(tibble(x = as.double(), y = as.double(), z = as.double()))
    }
    if (var(dat[[x]], na.rm = T) <= 0  | var(dat[[y]], na.rm = T) <= 0 ){
        return(tibble(x = as.double(), y = as.double(), z = as.double()))
    }
    nhl_playing_time

    ndat <- nrow(dat)

    dat <- dat[!is.na(dat[[x]]),]
    dat <- dat[!is.na(dat[[y]]),]

    print(glue("{nrow(dat)}"))


    kde2d(dat[[x]], dat[[y]], n = n, lims = limits) %>%
        akima::interp2xyz(data.frame=TRUE) %>%
        {if(mulli_by_rows) mutate(., z = ndat * z) else . } #%>% pull(z) %>% max()
}




################################
#'
#'
#'
#'NHL_grid_events(a_player_id = 8474564)
#'
#' a_player_id <- nhl_cache("playerprofiles") %>% pull(id) %>% sample(1)
NHL_grid_events <- function(a_player_id = NULL ,
                                     a_event_type_id = "GOAL",
                                     a_player_type = "Scorer",
                                     at_home = NULL,
                                     side = "right"
){
    actions = nhl_cache("game_actions")
    #playerprofiles = nhl_cache("playerprofiles")
    game_plays = nhl_cache("game_plays")

    nhl_playing_time(a_player_id)
    actions %>%
        filter(event_type_id == a_event_type_id) %>%
        filter(player_type == a_player_type) %>%
        {if(!is.null(a_player_id)) filter(., player_id == a_player_id) else .} %>%
        left_join(game_plays %>%
                      filter(event_type_id == a_event_type_id) %>%
                      #filter(player_type == a_player_type) %>%
                      dplyr::select(play_id,coord_x_st,coord_y_st,is_home), by = "play_id") %>%
        nhl_attack_on_right() %>%
        nhl_side_filter(side = side) %>%
        nhl_2D_grid_density()
}



################################
#'
#'
#'
#'
nhl_player_str <- function(a_player_id){
    player <- nhl_cache("playerprofiles") %>% filter(id == a_player_id)
    a <- floor(lubridate::time_length(difftime(Sys.Date(), as.Date(player$birth_date)), "years"))


    glue::glue("{player$full_name} ({player$primary_position_code}), age {a}, of {player$current_team_name}")

}



################################
#'
#'
#'
#'
nhl_x_limit<-function(side){
    if(side == "right"){
        return(c(0, 100))
    }
    if(side == "left"){
        return(c(-100, 0 ))
    }
    return(c(-100, 100 ))

}

NHL_actions_num <- function(a_player_id,
                            a_event_type_id = "GOAL",
                            a_player_type = "Scorer"){

    nhl_cache("game_actions") %>%
        filter(player_id == a_player_id) %>%
        filter(event_type_id == a_event_type_id)%>%
        filter(player_type == a_player_type) %>%
        distinct() %>%
        nrow()
}



################################
#'
#'
#'
#'
NHL_grid_events_plot <- function(a_player_id,
                                 a_event_type_id = "GOAL",
                                 a_player_type = "Scorer",
                                 at_home = NULL,
                                 side = "right",
                                 color_brewer_palette = "Greens"){
    dat <- NHL_grid_events(a_player_id= a_player_id,
                           a_event_type_id = a_event_type_id,
                           a_player_type = a_player_type,
                           at_home = at_home,
                           side = side)



    acts_num <- NHL_actions_num(a_player_id= a_player_id,
                    a_event_type_id = a_event_type_id,
                    a_player_type = a_player_type)


    p<-
        dat %>%
        ggplot(aes(x = x, y = y, z = z, fill = z)) +
        geom_tile(alpha = 0.99)+
        scale_fill_gradientn(colors = brewer.pal(n = 9,name = color_brewer_palette)) +
        #scale_fill_continuous_divergingx(palette = 'PiYG', mid = 0) +
        scale_x_continuous(limits = nhl_x_limit(side)) +
        guides(fill = FALSE, z = FALSE) +
        labs(title = stringr::str_to_title(glue("{nhl_player_str(a_player_id)}")),
             subtitle = stringr::str_to_title(glue("{a_player_type} of {acts_num} {a_event_type_id}"))
             )
    NHL_gg_add_ice_elments(gg_object = p)
}


nhl_cache("playerprofiles") %>% pull(id) %>% sample(1) %>% NHL_grid_events_plot(a_player_id = .)
nhl_cache("playerprofiles") %>% pull(id) %>% sample(1) %>% NHL_grid_events_plot(a_event_type_id = "HIT", a_player_type = "Hitter", side="both")
nhl_cache("playerprofiles") %>% pull(id) %>% sample(1) %>% NHL_grid_events_plot(a_event_type_id = "GIVEAWAY", a_player_type = "PlayerID", side="both")
nhl_cache("playerprofiles") %>% pull(id) %>% sample(1) %>% NHL_grid_events_plot(a_event_type_id = "TAKEAWAY", a_player_type = "PlayerID", side="both")

nhl_cache("playerprofiles") %>% pull(id) %>% sample(1) %>% NHL_grid_events_plot(a_event_type_id = "SHOT", a_player_type = "Shooter", side="both")



actions %>% count( event_type_id, player_type)


    denom_dat <-
        shifts_plays %>%
        {if(!is.null(at_home)) filter(., play_is_home  == at_home) else .} %>%
        #{if(!is.null(a_emptyNet)) filter(., play_is_home  == a_emptyNet) else .} %>%
        filter(event == a_event) %>%
        filter(is.na(emptyNet) | emptyNet  == FALSE) %>%
        mutate(coord_x_st = ifelse(play_is_home, coord_x_st, -coord_x_st)) %>%
        mutate(coord_y_st = ifelse(play_is_home, coord_y_st, -coord_y_st)) %>%
        {if(side == "right")filter(., coord_x_st > 0)else if(side == "left")filter(., coord_x_st < 0) else . }

    numer_dat <-
        denom_dat %>%
        {if(!is.null(a_playerId)) filter(., playerId  == a_playerId) else .}



    d <- kde2d(denom_dat$coord_x_st, denom_dat$coord_y_st, n = 100, lims = c(range(denom_dat$coord_x_st), range(denom_dat$coord_y_st)))
    n <- kde2d(numer_dat$coord_x_st, numer_dat$coord_y_st, n = 100, lims = c(range(denom_dat$coord_x_st), range(denom_dat$coord_y_st)))

    inner_join(
        d %>% interp2xyz(data.frame=TRUE) ,
        n %>% interp2xyz(data.frame=TRUE) ,by = c("x","y"),   suffix = c("_d", "_n")) %>%
        mutate(z_relative = z_d- z_n)

}

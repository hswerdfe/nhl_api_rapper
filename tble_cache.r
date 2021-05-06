


source("functions.R")




NHL_TBL_CACHE_GLOBAL <- list()


##############################################
#'
#'
#' filters a df for a single condition
#' Does not do any filtering under some circumstances
#'
nhl_filter_cond <- function(dat,
                            nm,
                            val,
                            col_cond = (!is.null(val) | length(val) >= 1)
){
    if (!col_cond ){
        return(dat)
    }else if (length(val) == 1){
        filter(dat, !!sym(nm) == val)
    }else if (length(val) > 1){
        filter(dat, !!sym(nm) %in% val)
    }else{
        dat
    }
}




##############################################
#'
#'
#'
#' nhl_cache_tables(gms)
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
        print("game_plays")
        NHL_TBL_CACHE_tmp[["game_plays"]] <- gms %>% nhl_games_plays()
    }
    if (game_actions){
        print("game_actions")
        NHL_TBL_CACHE_tmp[["game_actions"]] <- gms %>% nhl_games_actions()
    }
    if (game_pressure){
        print("game_pressure")
        NHL_TBL_CACHE_tmp[["game_pressure"]] <- gms %>% nhl_games_pressure()
    }
    if (game_shifts){
        print("game_shifts")
        NHL_TBL_CACHE_tmp[["game_shifts"]] <- gms %>% nhl_games_shifts()
    }
    if(game_officials){
        print("game_officials")
        NHL_TBL_CACHE_tmp[["game_officials"]] <- gms %>% nhl_games_officials()
        # NHL_TBL_CACHE_tmp[["game_officials"]]$link[[1]] %>%
        #     unique() %>%
        #     nhl_link()
    }
    if(game_Player_on_ice){
        print("game_Player_on_ice")
        NHL_TBL_CACHE_tmp[["game_Player_on_ice"]] <- nhl_players_on_ice_at_plays(plays = NHL_TBL_CACHE_tmp[["game_plays"]],
                                                                                 shifts = NHL_TBL_CACHE_tmp[["game_shifts"]]
        )
    }
    # if(game_summary){
    #     print("game_summary")
    #     NHL_TBL_CACHE_tmp[["game_summary"]] <- gms[1:2] %>% nhl_games_summary()
    # }
    if(playerprofiles){
        NHL_TBL_CACHE_tmp[["playerprofiles"]] <-
            lapply(NHL_TBL_CACHE_tmp, function(x){
                x[["player_id"]]
            })  %>% unlist() %>% unique() %>%
            nhl_player_profiles() %>%
            tidyr::separate(height, into = c("feet", "inches"), convert = T) %>% mutate(height_cm = 2.54*(feet*12 + inches))

    }

    # combine the two caches
    #NHL_TBL_CACHE_GLOBAL <- list()
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


    lapply(NHL_TBL_CACHE_GLOBAL, function(x){
        nhl_check_df(x)
    })




    write_rds(x = NHL_TBL_CACHE_GLOBAL, file = "NHL_TBL_CACHE_GLOBAL.rds")
}

# nhl_cache_nms()
# NHL_TBL_CACHE_GLOBAL[["game_plays"]] <- nhl_cache_game_plays() %>% NHL_derive_zone()
# NHL_TBL_CACHE_GLOBAL[["game_Player_on_ice"]] <- nhl_cache_game_Player_on_ice() %>% NHL_derive_zone()





##############################################
#'
#'
#'
#'
nhl_cache <- function(nm, filt_list = list()){
    NHL_TBL_CACHE_GLOBAL[[nm]] %>%
        nhl_filters_cond(filt_list)
}



##############################################
#'
#' takes a filter and returns a subset of the filter that can be used on the df object
#'
#'
nhl_filters_cond_can <- function(dat, filt_list= list()){
    nms <- intersect(names(filt_list), colnames(dat))
    filt_list[nms]
}






##############################################
#'
#' opposite of nhl_filters_cond_can
#'
#'
nhl_filters_cond_can_not <- function(dat, filt_list= list()){
    filt_list[setdiff(names(filt_list), colnames(dat))]
}
##############################################
#'
#' applies all conditions in filt_list that it can to dat
#'
#'
#'filt_list = list(event = c("Shot", "Hit"))
#'filt_list = list(primary_number  = 37, QQQ= 999, current_age = 35)
nhl_filters_cond <- function(dat,
                             filt_list = list(),
                             filter_list_useable = nhl_filters_cond_can(dat, filt_list)){
    dat2 <- dat
    nms <- intersect(names(filter_list_useable), colnames(dat2))

    lapply(nms, function(nm){
        print(nm)
        val <- filter_list_useable[[nm]]
        #print(val)
        dat2 <<-
            dat2 %>%
            nhl_filter_cond(nm = nm, val = val, col_cond = (val != "NULL"))
        return(TRUE)

    })
    dat2
}




##############################################
#'
#'
#'
#'
nhl_player_ids <- function(filt_list = list()){
    nhl_cache("playerprofiles", filt_list) %>%
        pull(player_id) %>%
        unique()
}

#
# nhl_cache_game_shifts(filt_list = list(primary_position_code = "D"))
# dat <- nhl_cache_game_shifts()
##############################################
#'
#' @param
#' attach_extras can be TRUE | FALSE | vector of column names
#'
nhl_cache_filter_further <- function(dat,
                                     other_nm = "playerprofiles",
                                     filt_list = list(),
                                     common_col = "player_id",
                                     attach_extras = names(filt_list)
                                     ){
    filt_list_new <- nhl_filters_cond_can_not(dat, filt_list)

    if (length(filt_list_new) == 0 ){
        return(dat)
    }
    other_dat <- nhl_cache(nm = other_nm, filt_list = filt_list_new)
    #filt_list_new <- nhl_filters_cond_can_not(playerprofiles, filt_list_new)

    dat2 <-
        dat %>%
        inner_join(other_dat %>% distinct(!!sym(common_col), !!sym(attach_extras)), by = common_col)

    # if (length (filt_list_new) > 0 ){
    #     warning(glue("Not sure how to filter by game_shifts by columns = {names(filt_list_new)}"))
    # }
    dat2


}

##############################################
#'nhl_cache_game_plays() %>% count(event)
#'
#'nhl_cache_game_plays(list(period = 4))
nhl_cache_game_plays <- function(filt_list = list()){
    dat = nhl_cache("game_plays", filt_list)
    nhl_cache_filter_further(dat, "playerprofiles", filt_list, "player_id")
}







train_goal_model <- function(){
    actions <-
        nhl_cache_game_actions( list(event_type_id  = c("SHOT", "GOAL"),
                                     player_type  = c("Goalie", "Scorer", "Shooter")
                                     )) %>% #count(player_type) %>%
        mutate(player_type = if_else(player_type == "Scorer", "Shooter", player_type))


    filter_players <- actions %>% distinct(player_id) %>% as.list()
    pp <- nhl_cache_playerprofiles(filter_players) %>%
        dplyr::select(player_id, shoots_catches, primary_position_code)


    filter_plays <- actions %>% distinct(play_id) %>% as.list()

    dat <-
        actions %>% left_join(pp, by = "player_id") %>%
        distinct(play_id, event_type_id, player_type, shoots_catches , primary_position_code) %>%
        mutate(is_goal = event_type_id == "GOAL", .keep = "unused") %>%
        pivot_wider(names_from = c("player_type", "player_type"),
                    values_from = c("shoots_catches", "primary_position_code"),
                    names_prefix = ""
        ) %>%
        left_join(nhl_cache_game_plays(filter_plays), by = "play_id") %>%
        dplyr::select(play_id, is_goal, is_home,shoots_catches_Shooter, shoots_catches_Goalie, primary_position_code_Shooter, primary_position_code_Goalie, period_time, period, secondary_type,coord_x_st , coord_y_st) %>%
        nhl_derive_polar_coords()


    library(tidymodels)
    boots <- bootstraps(dat, times = 15, apparent = TRUE)
    vfold_cv(data = ames_train, strata = Sale_Price)


    split <- boots$splits[[1]]


    rand_forest(mtry = 10, trees = 2000) %>%
        set_engine("ranger", importance = "impurity") %>%
        set_mode("regression")


    boost_tree() %>%
        set_engine("xgboost") %>%
        set_mode("regression") %>%
        translate()


    mod <-
        boost_tree(sample_size = tune()) %>%
        set_engine("xgboost") %>%
        set_mode("classification")

    # update the parameters using the `dials` function
    mod_param <-
        mod %>%
        parameters() %>%
        update(sample_size = sample_prop(c(0.4, 0.9)))

    model <- boost_tree(mtry = 10, min_n = 3)
    param_values <- tibble::tibble(mtry = 10, tree_depth = 5)
    model %>% update(param_values)
    fit(model)

    mdl <- lm(formula =   is_goal ~ coord_r + coord_alpha + abs(coord_alpha), data = analysis(split))
    rand_forest() %>% set_engine("randomForest")

    fit_nls_on_bootstrap <- function(split) {
        lm(  is_goal ~ coordr + coord_alpha, analysis(split), start = list(k = 1, b = 0))
    }

    analysis(boots$splits[[1]])
    analysis

    show_engines("logistic_reg")

    %>%
        pivot_wider(names_from = c("player_type"),
                values_from = c("shoots_catches"),
                names_prefix = "shoots_catches_"
        )
        mutate(s_C = paste0(player_type, "_", shoots_catches), .keep = "unused") %>%
        #dplyr::select(play_id, player_type, event_type_id, shoots_catches) %>%
        pivot_wider(names_from = c("s_C"),
                    values_from = c("s_C")
                    )
    , "player_type", "shoots_catches" , "primary_position_code"

    nhl_cache_game_plays(filter_plays) %>%
        dplyr::select(play_id, period, period_type, period_time, secondary_type, event_type_id, coord_x_st, coord_y_st, is_home) %>%
        nhl_derive_polar_coords() %>%
        left_join(actions, by = "play_id")

        #distinct( play_id)
        #TODO:
}




########################################
#'
#' The penalty section of the NHL API does not seem to work correctly for all types of events, I think it is just recored for goals
#' TODO:
nhl_derive_strength <- function(){
    nhl_cache_game_plays(list(event_type_id = "PENALTY")) %>%
        dplyr::select(play_id, game_id, period, period_time, penalty_severity, penalty_minutes, team_id)
}



########################################
#'
#' derives polar coordinates for the play, relative to the net.
#' works with coord_x_st coord_y_st which are standardized with home team shooting on the right
#'
#' this standardizes to the indicated team for the play is on the left with that net at (0,0)
#' then calculate angle and R from that
#' adds them to the DF and returns
#'
#'
#'@example
#' nhl_derive_polar_coords(dat) %>%
#' #filter(event == "Shot" | event == "Goal") %>%
#' #filter(event == "Shot") %>%
#' filter(event == "Goal") %>%
#'    filter(empty_net == FALSE | is.na(empty_net)) %>%
#'    sample_n(50000)%>%
#'    ggplot(aes(x = coord_r, y = coord_alpha)) + geom_hex() + facet_grid(rows = vars(event))
#'    #ggplot(aes(x = x_tmp, y = y_tmp)) + geom_hex() + facet_grid(rows = vars(event))
#' #'
nhl_derive_polar_coords <- function(dat,
                                    home_col = grep(x = colnames(dat), pattern = "is_home", value = T),
                                    coord_x = "coord_x_st",
                                    coord_y = "coord_y_st",
                                    GOAL_LINE_OFFEST = 89) {


    dat %>%
        mutate(x_tmp = if_else(!!sym(home_col),-!!sym(coord_x),!!sym(coord_x)) + GOAL_LINE_OFFEST) %>%
        mutate(y_tmp = if_else(!!sym(home_col),-!!sym(coord_y),!!sym(coord_y))) %>%
        mutate(coord_r = sqrt(x_tmp ^2 + y_tmp^2)) %>%
        mutate(coord_alpha = atan(y_tmp / x_tmp) * 180/pi) %>%
        dplyr::select(-x_tmp, -y_tmp)
}



#####################################
#'
#' Derived the zone offensive, defensive, neutral as well as if play is behind net and
#' returns dat with appended columns
#'
#' dat = nhl_cache_game_Player_on_ice()
#'
#'
#'
NHL_derive_zone <- function(dat){

    home_col <- grep(x = colnames(dat), pattern = "is_home", value = T)
    dat %>%
        mutate(x_tmp = if_else(!!sym(home_col),coord_x_st,-coord_x_st)) %>%
        mutate(zone = if_else(x_tmp > 25,"offensive",
                        if_else(x_tmp < 25,"defensive",
                               "neutral"))) %>% #count(zone)
    mutate(behind_goal_line = abs(coord_x_st)>89) %>%
        dplyr::select(-x_tmp)

#
#     89
#
#     %>% pull(zone)
#     blue_line
#     nhl_cache_nms_tbl_cols() %>% filter(grepl(pattern = "coord_x_st",x = cols))
#     nhl_cache_nms_tbl_cols() %>% filter(grepl(pattern = "is_home",x = cols))
#     nhl_cache_game_Player_on_ice()
#     nhl_cache_nms_tbl_cols() %>% filter(cols  == "is_home")
}


##############################################
#'
#' nhl_cache_playerprofiles()
#'
nhl_cache_playerprofiles <- function(filt_list = list()){
    nhl_cache("playerprofiles", filt_list)
}



##############################################
#'
#'nhl_cache_game_Player_on_ice() %>% count(event)
#'nhl_cache_game_Player_on_ice(filt_list = list(event = c("Shot", "Blocked Shot", "Goal"), primary_position_code = "D", shoots_catches = "L"))
#'nhl_cache_game_Player_on_ice(filt_list = list(event = c("Shot", "Hit"), primary_position_code = "D", shoots_catches = "L"))
#'nhl_cache_game_Player_on_ice(filt_list = list(event = c("Hit"), primary_position_code = "D", shoots_catches = "L"))

nhl_cache_game_Player_on_ice <- function(filt_list = list()){
    dat = nhl_cache("game_Player_on_ice", filt_list)
    nhl_cache_filter_further(dat, "playerprofiles", filt_list, "player_id")
}

##############################################
#'
#'nhl_cache_game_actions()
#'nhl_cache_game_actions(filt_list = list(event_type_id = "SHOT", player_type = "Shooter", primary_number = 37, primary_position_code = "D", shoots_catches = "L"))
#'nhl_cache_game_actions(filt_list = list(event_type_id = "SHOT", player_type = "Shooter", primary_number = 37))
nhl_cache_game_actions <- function(filt_list = list()){

    dat = nhl_cache("game_actions", filt_list)
    nhl_cache_filter_further(dat, "playerprofiles", filt_list, "player_id")


}
##############################################
#'
#'
#' nhl_cache("playerprofiles")
#'nhl_cache_game_shifts(filt_list = list(primary_number = 37, primary_position_code = "D")) %>% count(first_name , last_name, team_abbrev)
nhl_cache_game_shifts <- function(filt_list = list(), ...){
    dat <- nhl_cache("game_shifts", filt_list, ...)
    nhl_cache_filter_further(dat, "playerprofiles", filt_list, "player_id", ...)

}



##############################################
#'
#'
#'@example
#' nhl_cache_nms()
nhl_cache_nms <- function(){
    names(NHL_TBL_CACHE_GLOBAL)
}



##############################################
#'a <- nhl_cache_nms_tbl_cols()
#'
#' nhl_cache_nms_tbl_cols() %>% count(tbl, sort =T)
#'
nhl_cache_nms_tbl_cols <- function(){
    map_dfr(nhl_cache_nms(), function(nm){
        tibble(tbl = nm,
               cols = colnames(nhl_cache(nm))
        )
    })
}

##############################################
#'nhl_cache_nms_cols(list())
#'nhl_cache_nms_cols(list(period = 2))
nhl_cache_nms_cols <- function(filt_list_new){
    nhl_cache_nms_tbl_cols() %>%
        filter(cols %in% names(filt_list_new))
}


##############################################
#'
#'
#'
#'
nhl_cache_load <- function(){
    NHL_TBL_CACHE_GLOBAL <<- read_rds(file = "NHL_TBL_CACHE_GLOBAL.rds")

}




##############################################
#'
#'
#'
#'
nhl_cache_load()


-module(map_data).

-include("role.hrl").
-include("cache.hrl").

-export([tabs/0, map/1]). 

tabs() -> 
	[ 
	  role,
	  account
        ].

%%----------------------------map data-------------------------------------------



map(role) -> 
    #map{
        fields          = record_info(fields,role),
        key_fields      = [account_id, role_id],
        term_fields     = [],
        ignored_fields  = [career,sex,name,star,hp,max_hp,mp,max_mp,
                           att,def,p_def_rate,m_def_rate,
                           mingzhong,shanbi,baoji,kangbao,speed,buffer]
	};

map(account) -> 
    #map{
        fields          = record_info(fields,account),
        key_fields      = [player_id],
        string_fields     = [name],
        ignored_fields  = [career,sex,star,hp,max_hp,mp,max_mp,
                           att,def,p_def_rate,m_def_rate,
                           mingzhong,shanbi,baoji,kangbao,speed,buffer]
	}.
%%--------------------------------------------------------------------------------




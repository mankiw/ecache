-module(cache_app).

-include("cache.hrl").

-behaviour(application).

-export([
     start/2,
     stop/1
     ]).
-export([start/0]).

start() ->
    application:start(crypto),
    application:start(emysql),
    application:start(cache).

start(_,_) ->
    case cache_sup:start_link() of  %% 启动服务器根监督者cache_sup
      {ok, Pid} ->
          start_cache(),        %% 根监督者cache_sup下开启工作者
          {ok, Pid};
      Other ->
          {error, Other}
    end.

stop(_State) ->
    ok.


%%-------------------local fun--------------------------------------
start_cache() ->
  sql_operate:add_pool(),  %% creat connecting pool
  sql_operate:init_tabs(), %% creat db tables
  Tabs = map_data:tabs(),
    ets:new(ets_tmp_cache_for_pre, [named_table,{keypos, 1},public]),
  Map_data_num = length(Tabs)*?DB_HASH_NUM,
  TotUpdateTime = ?DB_UPDATE_INTERVAL,
  Interval = TotUpdateTime div Map_data_num,
  Fun = fun({Tab, Index},Num) -> 
    sql_operate:prepare(Tab),  %% sql prepare
    {EtsRef,UpdateEtsRef} = init_ets(Tab, Index),

    DBState = #db_state{tab = Tab,
              ref = cache_util:make_db_hash_ref(Tab,Index),
              index = Index,
              ets = EtsRef,
              update_ets = UpdateEtsRef,
              update_interval = ?DB_UPDATE_INTERVAL},


    {ok, _APid} = supervisor:start_child(cache_sup, [{DBState,Interval*Num}]),

    Num+1
  end,
  List = [{Tab, Index} || Index <- lists:seq(1,?DB_HASH_NUM),Tab <- Tabs],
  lists:foldl(Fun,1,List).

%% init: ets,ets_index,update_ets,db_null_ets
init_ets(Tab, Index) ->
  UpdateEtsRef = cache_util:make_hash_update_ref(Tab,Index),
  EtsRef = cache_util:make_hash_ref(Tab,Index),
  DBNullEtsRef = cache_util:make_hash_dbnull_ref(Tab,Index),
  Map = map_data:map(Tab),
  case length(Map#map.key_fields) of
    1 -> void;
    _ ->
      EtsIndexRef = cache_util:get_key_index_ets(Tab,Index),
      ets:new(EtsIndexRef, [named_table, public, set, {keypos,1}])
  end,
  ets:new(EtsRef, [named_table, public, set, {keypos,2}]),
  ets:new(UpdateEtsRef, [named_table, public, set, {keypos, #update_index.key}]),
  ets:new(DBNullEtsRef,[named_table,public,set,{keypos,#db_null_index.key}]),
  {EtsRef,UpdateEtsRef}.

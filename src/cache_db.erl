-module(cache_db).

-include("log.hrl").
-include("cache.hrl").

-export([start_link/1]).

-export([init/1, handle_cast/2, terminate/2]).

-compile(export_all).

 % #db_state{tab = Tab,
	% 		index = Index,
	% 		ets = Ets_ref,
	% 		update_ets = Update_ets_ref,
	%		update_timer = none},

start_link({DBState,Time_start_update}) ->
	gen_server:start_link({local, DBState#db_state.ref}, ?MODULE, [DBState,Time_start_update], []).

init([DBState,Time_start_update]) ->
	erlang:process_flag(trap_exit, true),
	erlang:process_flag(priority, high), 

	{ok,TRef} = timer:apply_after(Time_start_update, ?MODULE, 
					  update_to_db, [DBState#db_state.ref]),
    {ok, DBState#db_state{update_timer = TRef}}.

update_to_db(Ref) ->
	gen_server:cast(Ref,update_to_db).

insert_to_db(Ref,Record) ->
	gen_server:cast(Ref,{insert_to_db,Record}).

delete_in_db(Ref,Key) ->
	gen_server:cast(Ref,{delete_in_db,Key}).

cache_update_notify(Ref,Key) ->
	gen_server:cast(Ref,{cache_update_notify,Key}).

handle_cast({cache_update_notify,Key},State) ->
	UpdateIndex = #update_index{key = Key},
	ets:insert(State#db_state.update_ets,UpdateIndex),
	{noreply,State};


handle_cast(update_to_db,State) ->
	?DBG(db,"start update to db:~w",[State#db_state.ets]),
	UpdateList = ets:tab2list(State#db_state.update_ets),
	ets:delete_all_objects(State#db_state.update_ets),
	gen_server:cast(self(),{do_update, UpdateList}),
	timer:cancel(State#db_state.update_timer),
	{ok,TRef} = timer:apply_after(State#db_state.update_interval, ?MODULE, 
					  update_to_db, [State#db_state.ref]),
    {noreply, State#db_state{update_timer = TRef}};

handle_cast({do_update, UpdateList},State) ->
	case UpdateList of
		[] ->
			?DBG(db,"finish update to db:~w",[State#db_state.ets]),
			void;
		[#update_index{key = Key}|Res] ->
			case ets:lookup(State#db_state.ets,Key) of
				[] ->
					void;
				[Record] ->
					sql_operate:sql_update(Record)
			end,
			gen_server:cast(self(),{do_update, Res})
	end,
	{noreply,State};

handle_cast({insert_to_db,Record},State) ->
	sql_operate:sql_insert(Record),
	{noreply,State};

handle_cast({delete_in_db,Key},State) ->
	sql_operate:sql_delete(State#db_state.tab,Key),
	{noreply,State}.


terminate(_Reason,_State) ->
	ok.

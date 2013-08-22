-module(cache).

-include("cache.hrl").
-include("log.hrl").

-compile(export_all).
% -export([lookup/2,  update_element/3, update_record/1, insert/1, 
% 		 remove_cache_data/2, delete/2, update_counter/3, tab2list/1,delete_all_data/1,
% 		 lookup_help_db/3, delete_by_key/2]).

% -export([start_link/1,init/1]).


lookup(Tab, Key) ->
	Map = map_data:map(Tab),
	Index = cache_util:do_key_hash(Key),
	EtsRef = cache_util:make_hash_ref(Tab,Index),
	DBNullEtsRef = cache_util:make_hash_dbnull_ref(Tab,Index),
	case length(Map#map.key_fields) of
		1 ->
			case ets:lookup(EtsRef,Key) of
				[] ->
					case ets:lookup(DBNullEtsRef,Key) of
						[] ->
							case sql_operate:sql_select(Tab,[Key]) of
								not_found ->
									ets:insert(DBNullEtsRef,#db_null_index{key = Key}),
									[];
								Records ->
									Fun2 = fun(Record) ->
										ets:insert(EtsRef,Record)
									end,
									lists:map(Fun2,Records),
									Records
							end;
						_DBNullIndex ->
							[]
					end;
				Records ->
					Records
			end;
		_ ->
			EtsIndexRef = cache_util:get_key_index_ets(Tab,Index),
			case is_tuple(Key) of
				true ->
					[Id | _] = erlang:tuple_to_list(Key);
				false ->
					Id = Key
			end,
			LookupInedx = ets:lookup(EtsIndexRef,Id),
			case LookupInedx of	
				[{Id,KeyTupleList,1}] ->
					FilterKeyTuples = cache_util:filterKeyTuples(Id,Key,KeyTupleList,Map#map.key_fields),
					Fun1 = fun(KeyTuple,Records) ->
						case ets:lookup(EtsRef,KeyTuple) of
							[] ->
								Records;
							[Record] ->
								[Record|Records]
						end
					end, 
					lists:foldl(Fun1,[],FilterKeyTuples);
				_ ->
					case LookupInedx of
						[] ->
							EtsKeyTuples = [];
						[{Id,EtsKeyTuples,0}] ->
							void
					end,
					case ets:lookup(DBNullEtsRef,Id) of
						[] ->
							case sql_operate:sql_select(Tab,[Id]) of
								not_found ->
									ets:insert(DBNullEtsRef,#db_null_index{key = Id}),
									ets:insert(EtsIndexRef,{Id,EtsKeyTuples,1}),
									[];
								Records ->
									?DBG(sql_operate,"Record = ~w",[Records]),
									Fun = fun(Record) ->
										[Tab,KeyTuple|_Res] = tuple_to_list(Record),
										case lists:member(KeyTuple,EtsKeyTuples) of
											true ->
												[];
											false ->
												ets:insert(EtsRef,Record),
												KeyTuple
										end
									end,
									KeyTupleList = lists:flatten(lists:map(Fun,Records)),
									ets:insert(EtsIndexRef,{Id,KeyTupleList++EtsKeyTuples,1}),
									case Id == Key of
										true ->
											Records;
										false ->
											case length(tuple_to_list(Key)) == length(Map#map.key_fields) of
												true ->
													case lists:keyfind(Key,2,Records) of
														false ->
															[];
														Records1 ->
															[Records1]
													end;
												false ->
													{Id1,Id2} = Key,
													Fun1 = fun(Record) ->
														[Tab,{Key1,Key2,_Key3}|_Res] = tuple_to_list(Record),
														Id1 == Key1 andalso Id2 == Key2
													end,
													lists:filter(Fun1,Records)
											end
									end
							end;
						_DBNullIndex ->
							[]
					end
			end
	end.


insert(Record) ->
	[Tab,Key|_Res] = tuple_to_list(Record),
	Map = map_data:map(Tab),
	Index = cache_util:do_key_hash(Key),
	EtsRef = cache_util:make_hash_ref(Tab,Index),
	DBRef = cache_util:make_db_hash_ref(Tab,Index),
	ets:insert(EtsRef,Record),
	cache_db:insert_to_db(DBRef,Record),
	case length(Map#map.key_fields) of
		1 ->
			void;
		_ ->
			EtsIndexRef = cache_util:get_key_index_ets(Tab,Index),
			[Id | _] = erlang:tuple_to_list(Key),
			case ets:lookup(EtsIndexRef,Id) of
				[] ->
					ets:insert(EtsIndexRef,{Id,[Key],0});
				[{Id,KeyTuples,IsSearchDB}] ->
					ets:insert(EtsIndexRef,{Id,lists:umerge(KeyTuples,[Key]),IsSearchDB})
			end
	end.

update(Record) ->
	[Tab,Key|_Res] = tuple_to_list(Record),
	Index = cache_util:do_key_hash(Key),
	EtsRef = cache_util:make_hash_ref(Tab,Index),
	DBRef = cache_util:make_db_hash_ref(Tab,Index),
	case ets:lookup(EtsRef,Key) of
		[] ->
			void;
		_ ->
			ets:insert(EtsRef,Record)
	end,
	cache_db:cache_update_notify(DBRef,Key).


delete(Record) ->
	[Tab,Keys|_Res] = tuple_to_list(Record),
	delete(Tab,Keys).

delete(Tab,Keys) ->
	Map = map_data:map(Tab),
	Index = cache_util:do_key_hash(Keys),
	EtsRef = cache_util:make_hash_ref(Tab,Index),
	DBRef = cache_util:make_db_hash_ref(Tab,Index),
	case length(Map#map.key_fields) of
		1 ->
			ets:delete(EtsRef,Keys);
		_ ->
			EtsIndexRef = cache_util:get_key_index_ets(Tab,Index),
			case is_integer(Keys) of
				true ->
					Id = Keys;
				false ->
					[Id | _] = erlang:tuple_to_list(Keys)
			end,
			case ets:lookup(EtsIndexRef,Id) of
				[] ->
					?ERR(cache,"cant find index to delete,Tab = ~w,Key = ~w",[Tab,Keys]);
				[{Id,KeyTuples,IsSearchDB}] ->
					FilterKeyTuples = cache_util:filterKeyTuples(Id,Keys,KeyTuples,Map#map.key_fields),
					ets:insert(EtsIndexRef,{Id,KeyTuples--FilterKeyTuples,IsSearchDB}),
					Fun = fun(KeyTuple) ->
						ets:delete(EtsRef,KeyTuple)
					end,
					lists:map(Fun,FilterKeyTuples)
			end
	end,
	DBKeys = 
	case is_integer(Keys) of
		true ->
			[Keys];
		false ->
			tuple_to_list(Keys)
	end,
	cache_db:delete_in_db(DBRef,DBKeys).

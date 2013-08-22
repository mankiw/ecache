-module(sql_operate).

-include("cache.hrl").
-include("log.hrl").

-export([prepare/1]). %% sql prepare

-export([sql_select/2,sql_delete/2,sql_insert/1,sql_update/1]). %% sql operation

-export([add_pool/0,init_tabs/0,do_execute/1]).

-export([make_sql_tab/1]).

add_pool() ->
	{ok, User} = application:get_env(cache, db_user),
	{ok, Password} = application:get_env(cache, db_pass),
	{ok, Host} = application:get_env(cache, db_host),
	{ok, Port} = application:get_env(cache, db_port),
	{ok, Database} = application:get_env(cache, db_name),
	{ok, Encoding} = application:get_env(cache, db_encode),
	emysql:add_pool(oceanus_pool, 100, User, Password, Host, Port, Database, Encoding).
	% emysql:add_pool(oceanus_pool, 100,
	% 	"root", "123456", "192.168.24.159", 3306,
	% 	"zs_server", utf8).

do_execute(Sql) ->
	case emysql:execute(oceanus_pool,Sql) of
		{result_packet,_,_,Data,_} ->
			{ok,Data};
		Else ->
			?ERR(sql,"sql error when execute:~w,Info:~w",[Sql,Else])
	end.


init_tabs() ->
	Tabs = map_data:tabs(),
	Fun = fun(Tab) ->
		Map = map_data:map(Tab),
		SqlTabName = make_sql_tab(Tab),
		Sql = erlang:list_to_bitstring(io_lib:format("SELECT * from ~w Limit 1", [SqlTabName])),
		case emysql:execute(oceanus_pool,Sql) of
			{error_packet,_,_,<<"42S02">>,_Msg} ->
				creat_db_table(Tab,Map);
			_ ->
				void
		end
	end,
	lists:map(Fun,Tabs).


prepare(Tab) ->
	Map = map_data:map(Tab),
	SqlTabName = make_sql_tab(Tab),
	do_insert_prepare(Tab,Map,SqlTabName,Map#map.key_fields,Map#map.ignored_fields),
	do_update_prepare(Tab,Map,SqlTabName,Map#map.key_fields,Map#map.ignored_fields),
	case Map#map.key_fields of
		[ID1] ->
			do_prepare(Tab,SqlTabName,ID1);
		[ID1,ID2] ->
			do_prepare(Tab,SqlTabName,ID1),
			do_prepare(Tab,SqlTabName,ID1,ID2);
		[ID1,ID2,ID3] ->
			do_prepare(Tab,SqlTabName,ID1),
			do_prepare(Tab,SqlTabName,ID1,ID2),
			do_prepare(Tab,SqlTabName,ID1,ID2,ID3)
	end.

sql_select(Tab,Args) ->
	{result_packet,_,_,Data,_} = 
	case length(Args) of
		1 ->
			emysql:execute(oceanus_pool, make_select_stmt_1(Tab), Args);
		2 ->
			emysql:execute(oceanus_pool, make_select_stmt_2(Tab), Args);
		3 ->
			emysql:execute(oceanus_pool, make_select_stmt_3(Tab), Args)
	end,
	case Data of
		[] ->
			not_found;
		_ ->
			Map = map_data:map(Tab),
			[_KeyField|Fields] = Map#map.fields,
			fill_record(Tab,Data,Map,Fields,[])
	end.

sql_delete(Tab,Args) ->
	case length(Args) of
		1 ->
			emysql:execute(oceanus_pool, make_delete_stmt_1(Tab), Args);
		2 ->
			emysql:execute(oceanus_pool, make_delete_stmt_2(Tab), Args);
		3 ->
			emysql:execute(oceanus_pool, make_delete_stmt_3(Tab), Args)
	end.

sql_insert(Record) ->
	[Tab,Keys1|Res] = tuple_to_list(Record),
	Map = map_data:map(Tab),
	Keys2 =
	case is_tuple(Keys1) of
		true -> 
			tuple_to_list(Keys1);
		false ->
			[Keys1]
	end,
	Keys = lists:map(fun(Key) -> integer_to_list(Key) end,Keys2),
	ResFields = Map#map.fields -- Map#map.key_fields,
        io:format("~nfields is ~w~n", [Map#map.fields]),
        io:format("RestFields is ~w~n", [ResFields]),
	FieldValues = get_field_values(Res,ResFields,Map#map.ignored_fields,Map#map.term_fields,Map#map.string_fields,[]),
        io:format("Keys is ~w, Tab is ~w~n", [Keys, make_insert_stmt(Tab)]),
	emysql:execute(oceanus_pool, make_insert_stmt(Tab), Keys++FieldValues).

sql_update(Record) ->
	[Tab,Keys1|Res] = tuple_to_list(Record),
	Map = map_data:map(Tab),
	Keys2 =
	case is_tuple(Keys1) of
		true -> 
			tuple_to_list(Keys1);
		false ->
			[Keys1]
	end,
	Keys = lists:map(fun(Key) -> integer_to_list(Key) end,Keys2),
	[_KeyField|ResFields] = Map#map.fields,
	FieldValues = get_field_values(Res,ResFields,Map#map.ignored_fields,Map#map.term_fields,Map#map.string_fields,[]),
	emysql:execute(oceanus_pool, make_update_stmt(Tab), FieldValues++Keys).

%%------------------------------------------local fun-------------------------------------------------

list_to_atom_helper(List) ->
	% try erlang:list_to_existing_atom(List)
	% catch _:_ -> erlang:list_to_atom(List)
	% end.
	erlang:list_to_atom(List). %% qq说不会重复堆原子

make_sql_tab(Tab) ->
	list_to_atom_helper("gd_"++ erlang:atom_to_list(Tab)).

make_select_stmt_1(Tab) ->
	list_to_atom_helper("select_"++ erlang:atom_to_list(Tab)++"_1").

make_select_stmt_2(Tab) ->
	list_to_atom_helper("select_"++ erlang:atom_to_list(Tab)++"_2").

make_select_stmt_3(Tab) ->
	list_to_atom_helper("select_"++ erlang:atom_to_list(Tab)++"_3").

make_delete_stmt_1(Tab) ->
	list_to_atom_helper("delete_"++ erlang:atom_to_list(Tab)++"_1").

make_delete_stmt_2(Tab) ->
	list_to_atom_helper("delete_"++ erlang:atom_to_list(Tab)++"_2").

make_delete_stmt_3(Tab) ->
	list_to_atom_helper("delete_"++ erlang:atom_to_list(Tab)++"_3").

make_insert_stmt(Tab) ->
	list_to_atom_helper("insert_"++ erlang:atom_to_list(Tab)).

make_update_stmt(Tab) ->
	list_to_atom_helper("update_"++ erlang:atom_to_list(Tab)).

make_field_list(Map,KeyFields,IgnoreFields) ->
	Fields = Map#map.fields,
	Fields--IgnoreFields.

make_fields([],S) ->
	S;
make_fields([Field|Res],"") ->
	make_fields(Res,atom_to_list(Field));
make_fields([Field|Res],S) ->
	make_fields(Res,S++", "++atom_to_list(Field)).

make_set([],S) ->
	S;
make_set([Field|Res],"") ->
	make_set(Res,atom_to_list(Field)++"=?");
make_set([Field|Res],S) ->
	make_set(Res,S++", "++atom_to_list(Field)++"=?").

make_where([],S) ->
	S;
make_where([Field|Res],"") ->
	make_where(Res,atom_to_list(Field)++"=? ");
make_where([Field|Res],S) ->
	make_where(Res,S++"AND "++atom_to_list(Field)++"=? ").

get_field_values([],[],_IgnoreFields,_TermFields,_StringFields,NewValus) ->
	NewValus;
get_field_values([Value1|Res],[Field|ResFields],IgnoreFields,TermFields,StringFields,NewValus) ->
	case lists:member(Field,IgnoreFields) of
		true ->
			get_field_values(Res,ResFields,IgnoreFields,TermFields,StringFields,NewValus);
		false ->
			Value =
			case lists:member(Field,TermFields) of
				true ->
					cache_util:term_to_string(Value1); %% term
				false ->
					case lists:member(Field,StringFields) of
						true ->
							Value1; %% string
						false ->
							integer_to_list(Value1) %% integer
					end
			end,
			get_field_values(Res,ResFields,IgnoreFields,TermFields,StringFields,NewValus++[Value])
	end.

fill_record(_Tab,[],_Map,_Fields,Records) ->
	Records;
fill_record(Tab,[Data|ResData],Map,Fields,Records) ->
	KeyNum = length(Map#map.key_fields),
	{Keys,ResData1} = lists:split(KeyNum,Data),
	case KeyNum of
		1 ->
			[Key] = Keys;
		_ ->
			Key = list_to_tuple(Keys)
	end,
	Rec = value_to_rec(ResData1,Fields,Map#map.ignored_fields,Map#map.term_fields,Map#map.string_fields,[Tab]++[Key]),
	fill_record(Tab,ResData,Map,Fields,[Rec|Records]). 

value_to_rec([],_,_IgnoreFields,_TermFields,_StringFields,NewValus) ->
	list_to_tuple(NewValus);
value_to_rec(Value,[Field|ResFields],IgnoreFields,TermFields,StringFields,NewValus) ->
	case lists:member(Field,IgnoreFields) of
		true ->
			case lists:member(Field,TermFields) of
				true ->
					value_to_rec(Value,ResFields,IgnoreFields,TermFields,StringFields,NewValus++[[]]);
				false ->
					case lists:member(Field,TermFields) of
						true ->
                            value_to_rec(Value,ResFields,IgnoreFields,TermFields,StringFields,NewValus++[<<"">>]);
						false ->
							value_to_rec(Value,ResFields,IgnoreFields,TermFields,StringFields,NewValus++[0])
					end
			end;
		false ->
  			case Value of
		            [Value1] ->
				Res = [];
			    [Value1|Res] ->
				ok
			end,
			Val = 
			case lists:member(Field,TermFields) of
				true ->
					 cache_util:bitstring_to_term(Value1); %% term
				false ->
					case lists:member(Field,StringFields) of
						true ->
							Value1; %% string
						false ->
							Value1 %% integer
					end
			end,
			value_to_rec(Res,ResFields,IgnoreFields,TermFields,StringFields,NewValus++[Val])
	end.


do_prepare(Tab,SqlTabName,ID1) ->
	SelectStatement = erlang:list_to_bitstring(io_lib:format("SELECT * from ~w WHERE ~w = ?", [SqlTabName,ID1])),
	DeleteStatement = erlang:list_to_bitstring(io_lib:format("DELETE from ~w WHERE ~w = ?", [SqlTabName,ID1])),
	emysql:prepare(make_select_stmt_1(Tab),SelectStatement),
	emysql:prepare(make_delete_stmt_1(Tab),DeleteStatement).
do_prepare(Tab,SqlTabName,ID1,ID2) ->
	SelectStatement = erlang:list_to_bitstring(io_lib:format("SELECT * from ~w WHERE ~w = ? and ~w = ?", [SqlTabName,ID1,ID2])),
	DeleteStatement = erlang:list_to_bitstring(io_lib:format("DELETE from ~w WHERE ~w = ? and ~w = ?", [SqlTabName,ID1,ID2])),
	emysql:prepare(make_select_stmt_2(Tab),SelectStatement),
	emysql:prepare(make_delete_stmt_2(Tab),DeleteStatement).
do_prepare(Tab,SqlTabName,ID1,ID2,ID3) ->
	SelectStatement = erlang:list_to_bitstring(io_lib:format("SELECT * from ~w WHERE ~w = ? and ~w = ? and ~w = ?", [SqlTabName,ID1,ID2,ID3])),
	DeleteStatement = erlang:list_to_bitstring(io_lib:format("DELETE from ~w WHERE ~w = ? and ~w = ? and ~w = ?", [SqlTabName,ID1,ID2,ID3])),
	emysql:prepare(make_select_stmt_3(Tab),SelectStatement),
	emysql:prepare(make_delete_stmt_3(Tab),DeleteStatement).

do_insert_prepare(Tab,Map,SqlTabName,KeyFields,IgnoreFields) ->
	FieldsList = make_field_list(Map,KeyFields,IgnoreFields), %% put key on
        io:format("~w~n", [FieldsList]),
	Fields = make_fields(FieldsList,""),
        io:format("~w~n", [Fields]),
	ValuesFormat = make_fields(lists:map(fun(_Any) -> '?' end,FieldsList),""),
        FormatStr = io_lib:format("INSERT INTO ~w (~s) VALUES (~s)", [SqlTabName,Fields,ValuesFormat]),
	InsertStatement = erlang:list_to_bitstring(
		FormatStr
		),
        io:format("InsertStatement is ~s", [FormatStr]),
	emysql:prepare(make_insert_stmt(Tab),InsertStatement).

do_update_prepare(Tab,Map,SqlTabName,KeyFields,IgnoreFields) ->
	FieldsList = make_field_list(Map,[],IgnoreFields),  %% no key
	Set = make_set(FieldsList,""),
	Where = make_where(KeyFields,""),
	InsertStatement = erlang:list_to_bitstring(
		io_lib:format("UPDATE ~w SET ~s WHERE ~s", [SqlTabName,Set,Where])
		),
	emysql:prepare(make_update_stmt(Tab),InsertStatement).

creat_db_table(Tab,Map) ->
	SqlTabName = make_sql_tab(Tab),
        KeyFields = Map#map.key_fields,
        NormalFields = Map#map.fields -- KeyFields,
	PrimaryFieldInfo = make_sql_key_field_info(KeyFields,"")++
                    "PRIMARY KEY ("++make_fields(KeyFields,"")++")",
        NormalFiledInfo = make_sql_field_info(NormalFields ,Map#map.ignored_fields,Map#map.term_fields,Map#map.string_fields,""),
        FieldInfo = 
            case NormalFiledInfo == "" of
               true ->
                   PrimaryFieldInfo;
               _ ->
                   PrimaryFieldInfo ++ "," ++ NormalFiledInfo
            end,
	Sql = erlang:list_to_bitstring(io_lib:format("CREATE TABLE ~w (~s)", [SqlTabName,FieldInfo])),
	case emysql:execute(oceanus_pool,Sql) of
		{ok_packet,_,_,_,_,_,_} ->
			?WRN(sql,"Creat table successful! New Tab = ~w, If you want "
                 "to change the default config, please open your "
                 "sql management tool",[SqlTabName]);
		Msg ->
			Fun = fun(Arg) ->
				case is_list(Arg) of
					true ->
						list_to_atom(Arg);
					false ->
						Arg
				end
			end,
			Msg1 = lists:map(Fun,tuple_to_list(Msg)),
			?ERR(sql,"Creat table fail! Tab = ~w,Message is:~w",[SqlTabName,Msg1])
	end.

make_sql_key_field_info([],FieldInfo) ->
	FieldInfo;
make_sql_key_field_info([KeyField|Res],FieldInfo) ->
	make_sql_key_field_info(Res,FieldInfo++atom_to_list(KeyField)++" INT,").

make_sql_field_info([],_IgnoreFields,_TermFields,_StringFields,S) ->
	S;
make_sql_field_info([Field|Res],IgnoreFields,TermFields,StringFields,"") ->
	case lists:member(Field,IgnoreFields) of
		true ->
			make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,"");
		false ->
			case lists:member(Field,TermFields) of
				true ->
					make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,atom_to_list(Field)++" VARCHAR(256) NOT NULL DEFAULT '[]'"); %% term
				false ->
					case lists:member(Field,StringFields) of
						true ->
							make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,atom_to_list(Field)++" VARCHAR(512) NOT NULL DEFAULT '""'"); %% string
						false ->
							make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,atom_to_list(Field)++" INT NOT NULL DEFAULT 0") %% integer
					end
			end
	end;
make_sql_field_info([Field|Res],IgnoreFields,TermFields,StringFields,S) ->
	case lists:member(Field,IgnoreFields) of
		true ->
			make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,S);
		false ->
			case lists:member(Field,TermFields) of
				true ->
					make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,S++","++atom_to_list(Field)++" VARCHAR(256) NOT NULL DEFAULT '[]'"); %% term
				false ->
					case lists:member(Field,StringFields) of
						true ->
							make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,S++","++atom_to_list(Field)++" VARCHAR(512) NOT NULL DEFAULT '""'"); %% string
						false ->
							make_sql_field_info(Res,IgnoreFields,TermFields,StringFields,S++","++atom_to_list(Field)++" INT NOT NULL DEFAULT 0") %% integer
					end
			end
	end.

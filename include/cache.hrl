-record(map, {ets_tab = none,   %% 对应的ets表
              sql_tab = "",     %% 对应的sql
              key_classic = 1,  %% 表关键字的类别
              key_fields = [],  %% slq关键字的名称列表
              term_fields = [],
              string_fields = [],
              fields = [],      %% ets表中的所有字段名
              fields_spec,      %% 该map中的记录所对应的xxx_types记录
              ignored_fields = [] %% fields中该忽略掉的字段
              }).

-define(DB_HASH_NUM, 8).

-define(DB_UPDATE_INTERVAL, 60 * 1000).

-record(db_state, {record,               %% gen_cache对应的ets中的record，如player
                          mapper,               %% 对应于map记录
                          tab,
                          ets,
                          update_ets,
                          ref,
                          cache_ref,            %% 该缓存的注册名称，没有注册则为pid(TODO：目前还每支持飞注册进程)
                          index,         %% gen_cache对应的ets中做了修改了记录的索引
                          update_interval = 5*60000, %% 缓存数据更新到数据库的间隔(ms)
                          call_back,                %% gen_cache增删查改4个方法对应的call_back函数
                          update_timer = undefined,
                          cache_index = 0,
                          lookup_counter = 0,       %%查找次数
                          insert_counter = 0,        %%插入次数
                          update_counter = 0,       %%更新次数
                          delete_counter = 0        %%删除次数
                          }).
%% 更新索引的记录，key就是对于的记录数据的唯一key
-record(update_index, {key              
                       }).

-record(db_null_index, {
                        tab,
                        key}).  

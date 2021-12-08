-module(erflux_http).

-behaviour(gen_server).

-export([start_link/0, start_link/1, start_link/2, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).
-export([
  ping/0,
  get_databases/0,
  create_database/1,
  delete_database/1,
  get_database_users/1,
  create_user/3,
  delete_database_user/2,
  get_database_user/2,
  update_database_user/3,
  authenticate_database_user/3,
  get_cluster_admins/0,
  delete_cluster_admin/1,
  create_cluster_admin/2,
  update_cluster_admin/2,
  get_continuous_queries/1,
  delete_continuous_query/2,
  get_cluster_servers/0,
  get_cluster_shard_spaces/0,
  get_cluster_shards/0,
  create_cluster_shard/4,
  delete_cluster_shard/2,
  get_interfaces/0,
  q/2,
  w/2,
  a2b/1,
  ping/1,
  get_databases/1,
  create_database/2,
  delete_database/2,
  get_database_users/2,
  create_user/4,
  delete_database_user/3,
  get_database_user/3,
  update_database_user/4,
  authenticate_database_user/4,
  get_cluster_admins/1,
  delete_cluster_admin/2,
  create_cluster_admin/3,
  update_cluster_admin/3,
  get_continuous_queries/2,
  delete_continuous_query/3,
  get_cluster_servers/1,
  get_cluster_shard_spaces/1,
  get_cluster_shards/1,
  create_cluster_shard/5,
  delete_cluster_shard/3,
  get_interfaces/1,
  bin_to_timestamp/1,
  q/3,
  w/3,
  get_database_sets/1,
  get_series_columns/1,
  terms_to_dbstyle/2,
  terms_to_dbstyle/3 ]).

-include("erflux.hrl").

-type status_code() :: integer().
-type json_parse_error_reason() :: atom().

%% @doc Starts erflux_http worker.
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Configuration) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Configuration], []).

start_link(Name, Configuration) ->
  gen_server:start_link({local, Name}, ?MODULE, [Name, Configuration], []).

%% @doc Requests erflux_http worker stop.
stop() -> gen_server:cast(?MODULE, stop).

%% @doc Worker init.
init([]) ->
  {ok, {http, self(), erflux_conf:configure()}};
init([ Configuration ]) ->
  {ok, {http, self(), Configuration}};
init([ _Name, Configuration ]) ->
  {ok, {http, self(), Configuration}}.

%% General

-spec ping() -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Check if InfluxDB server is online.
ping() ->
  ping( ?MODULE ).

-spec ping( Pid :: pid() | atom() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Check if InfluxDB server is online.
ping(Pid) when is_pid(Pid) orelse is_atom(Pid) ->
  get_( Pid, path( Pid, <<"ping">> ) ).

%% Databases:

-spec get_databases() -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists available databases.
get_databases() ->
  get_databases( ?MODULE ).

-spec get_databases( Pid :: pid() | atom() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists available databases.
get_databases(Pid) when is_pid(Pid) orelse is_atom(Pid) ->
  get_( Pid, path( Pid, <<"query">>, [ { q, <<"SHOW DATABASES">> } ] ) ).

-spec create_database( DatabaseName :: atom() | binary() ) -> ok | { error, status_code() } | { error, status_code() }.
%% @doc Creates a database.
create_database(DatabaseName) when is_atom(DatabaseName) ->
  create_database( ?MODULE, a2b( DatabaseName ) );
create_database(DatabaseName) when is_binary(DatabaseName) ->
  create_database( ?MODULE, DatabaseName ).

-spec create_database( Pid :: pid() | atom(), DatabaseName :: atom() | binary() ) -> ok | { error, status_code() } | { error, status_code() }.
%% @doc Creates a database.
create_database(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                     andalso is_atom(DatabaseName) ->
  create_database( Pid, a2b( DatabaseName ) );
create_database(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                     andalso is_binary(DatabaseName) ->
  post( Pid, path( Pid, <<"query">> ), <<"q=CREATE DATABASE ", DatabaseName/binary>> ).

-spec delete_database( DatabaseName :: atom() | binary() ) -> ok.
%% @doc Deletes a database.
delete_database(DatabaseName) when is_atom(DatabaseName) ->
  delete_database( ?MODULE, a2b( DatabaseName ) );
delete_database(DatabaseName) when is_binary(DatabaseName) ->
  delete_database( ?MODULE, DatabaseName ).

-spec delete_database( Pid :: pid() | atom(), DatabaseName :: atom() | binary() ) -> ok.
%% @doc Deletes a database.
delete_database(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                     andalso is_atom(DatabaseName) ->
  delete_database( Pid, a2b( DatabaseName ) );
delete_database(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                     andalso is_binary(DatabaseName) ->
  delete( Pid, path( Pid, <<"db/", DatabaseName/binary>> ) ).

%% Database users:

-spec get_database_users( DatabaseName :: atom() | binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists users of a given database.
get_database_users(DatabaseName) when is_atom(DatabaseName) ->
  get_database_users( ?MODULE, a2b( DatabaseName ) );
get_database_users(DatabaseName) when is_binary(DatabaseName) ->
  get_database_users( ?MODULE, DatabaseName ).

-spec get_database_users( Pid :: pid() | atom(), DatabaseName :: atom() | binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists users of a given database.
get_database_users(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                        andalso is_atom(DatabaseName) ->
  get_database_users( Pid, a2b( DatabaseName ) );
get_database_users(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                        andalso is_binary(DatabaseName) ->
  get_( Pid, path( Pid, <<"query">>, [ { db, DatabaseName }, { q, <<"SHOW USERS">> } ] ) ).

-spec create_user( DatabaseName :: atom() | binary(), Username :: atom() | binary(), Password :: atom() | binary() ) -> ok | { error, status_code() }.
%% @doc Creates a database database user.
create_user(DatabaseName, Username, Password) when is_atom(DatabaseName)
                                                andalso is_atom(Username)
                                                andalso is_atom(Password) ->
  create_user( ?MODULE, a2b( DatabaseName ), a2b( Username ), a2b( Password ) );
create_user(DatabaseName, Username, Password) when is_binary(DatabaseName)
                                                andalso is_binary(Username)
                                                andalso is_binary(Password) ->
  create_user( ?MODULE, DatabaseName, Username, Password ).

-spec create_user( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Username :: atom() | binary(), Password :: atom() | binary() ) -> ok | { error, status_code() }.
%% @doc Creates a database database user.
create_user(Pid, DatabaseName, Username, Password) when (is_pid(Pid) orelse is_atom(Pid))
                                                     andalso is_atom(DatabaseName)
                                                     andalso is_atom(Username)
                                                     andalso is_atom(Password) ->
  create_user( Pid, a2b( DatabaseName ), a2b( Username ), a2b( Password ) );
create_user(Pid, DatabaseName, Username, Password) when (is_pid(Pid) orelse is_atom(Pid))
                                                     andalso is_binary(DatabaseName)
                                                     andalso is_binary(Username)
                                                     andalso is_binary(Password) ->
  %% post( Pid, path( Pid, <<"db/", DatabaseName/binary, "/users">> ), [ { name, Username }, { password, Password } ] ).
  post( Pid, path( Pid, <<"query">> ), <<"q=CREATE USER ", Username/binary, " WITH PASSWORD '", Password/binary, "'; CREATE DATABASE ", DatabaseName/binary, "; GRANT ALL ON ", Username/binary, " TO ", DatabaseName/binary, ";">> ).

-spec delete_database_user( DatabaseName :: atom() | binary(), Username :: atom() | binary() ) -> ok.
%% @doc Deletes a database user.
delete_database_user(DatabaseName, Username) when is_atom(DatabaseName)
                                               andalso is_atom(Username) ->
  delete_database_user( ?MODULE, a2b( DatabaseName ), a2b( Username ) );
delete_database_user(DatabaseName, Username) when is_binary(DatabaseName)
                                               andalso is_binary(Username) ->
  delete_database_user( ?MODULE, DatabaseName, Username ).

-spec delete_database_user( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Username :: atom() | binary() ) -> ok.
%% @doc Deletes a database user.
delete_database_user(Pid, DatabaseName, Username) when (is_pid(Pid) orelse is_atom(Pid))
                                                    andalso is_atom(DatabaseName)
                                                    andalso is_atom(Username) ->
  delete_database_user( Pid, a2b( DatabaseName ), a2b( Username ) );
delete_database_user(Pid, DatabaseName, Username) when (is_pid(Pid) orelse is_atom(Pid))
                                                    andalso is_binary(DatabaseName)
                                                    andalso is_binary(Username) ->
  delete( Pid, path( Pid, <<"db/", DatabaseName/binary, "/users/", Username/binary>> ) ).

-spec get_database_user( DatabaseName :: atom() | binary(), Username :: atom() | binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Gets database user details.
get_database_user(DatabaseName, Username) when is_atom(DatabaseName)
                                            andalso is_atom(Username) ->
  get_database_user( ?MODULE, a2b( DatabaseName ), a2b( Username ) );
get_database_user(DatabaseName, Username) when is_binary(DatabaseName)
                                            andalso is_binary(Username) ->
  get_database_user(?MODULE, DatabaseName, Username).

-spec get_database_user( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Username :: atom() | binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Gets database user details.
get_database_user(Pid, DatabaseName, Username) when (is_pid(Pid) orelse is_atom(Pid))
                                                 andalso is_atom(DatabaseName)
                                                 andalso is_atom(Username) ->
  get_database_user( Pid, a2b( DatabaseName ), a2b( Username ) );
get_database_user(Pid, DatabaseName, Username) when (is_pid(Pid) orelse is_atom(Pid))
                                                 andalso is_binary(DatabaseName)
                                                 andalso is_binary(Username) ->
  get_( Pid, path( Pid, <<"db/", DatabaseName/binary, "/users/", Username/binary>> ) ).

%% @doc Updates a database user.
-spec update_database_user( DatabaseName :: atom() | binary(), Username :: atom() | binary(), Params :: [ { binary(), any() } ] ) -> ok | { error, status_code() }.
update_database_user(DatabaseName, Username, Params) when is_atom(DatabaseName)
                                                       andalso is_atom(Username)
                                                       andalso is_list(Params) ->
  update_database_user( ?MODULE, a2b( DatabaseName ), a2b( Username ), Params );
update_database_user(DatabaseName, Username, Params) when is_binary(DatabaseName)
                                                       andalso is_binary(Username)
                                                       andalso is_list(Params) ->
  update_database_user( ?MODULE, DatabaseName, Username, Params ).

%% @doc Updates a database user.
-spec update_database_user( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Username :: atom() | binary(), Params :: [ { binary(), any() } ] ) -> ok | { error, status_code() }.
update_database_user(Pid, DatabaseName, Username, Params) when (is_pid(Pid) orelse is_atom(Pid))
                                                            andalso is_atom(DatabaseName)
                                                            andalso is_atom(Username)
                                                            andalso is_list(Params) ->
  update_database_user( Pid, a2b( DatabaseName ), a2b( Username ), Params );
update_database_user(Pid, DatabaseName, Username, Params) when (is_pid(Pid) orelse is_atom(Pid))
                                                            andalso is_binary(DatabaseName)
                                                            andalso is_binary(Username)
                                                            andalso is_list(Params) ->
  post( Pid, path( Pid, <<"db/", DatabaseName/binary, "/users/", Username/binary>> ), Params ).

%% @doc Authenticates a database user.
-spec authenticate_database_user( DatabaseName :: atom() | binary(), Username :: atom() | binary(), Password :: atom() | binary() ) -> {ok, status_code()}.
authenticate_database_user(DatabaseName, Username, Password) when is_atom(DatabaseName)
                                                               andalso is_atom(Username)
                                                               andalso is_atom(Password) ->
  authenticate_database_user( ?MODULE, a2b( DatabaseName ), a2b( Username ), a2b( Password ) );
authenticate_database_user(DatabaseName, Username, Password) when is_binary(DatabaseName)
                                                               andalso is_binary(Username)
                                                               andalso is_binary(Password) ->
  authenticate_database_user( ?MODULE, DatabaseName, Username, Password ).

%% @doc Authenticates a database user.
-spec authenticate_database_user( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Username :: atom() | binary(), Password :: atom() | binary() ) -> {ok, status_code()}.
authenticate_database_user(Pid, DatabaseName, Username, Password) when (is_pid(Pid) orelse is_atom(Pid))
                                                                    andalso is_atom(DatabaseName)
                                                                    andalso is_atom(Username)
                                                                    andalso is_atom(Password) ->
  authenticate_database_user( Pid, a2b( DatabaseName ), a2b( Username ), a2b( Password ) );
authenticate_database_user(Pid, DatabaseName, Username, Password) when (is_pid(Pid) orelse is_atom(Pid))
                                                                    andalso is_binary(DatabaseName)
                                                                    andalso is_binary(Username)
                                                                    andalso is_binary(Password) ->
  get_( Pid, path( Pid, <<"query">>, [ { db, DatabaseName }, { u, Username }, { p, Password }, { q, <<"SHOW MEASUREMENTS">> } ] ) ).

%% Cluster admins:

-spec get_cluster_admins() -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists cluster admin users.
get_cluster_admins() ->
  get_cluster_admins( ?MODULE ).

-spec get_cluster_admins( Pid :: pid() | atom() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists cluster admin users.
get_cluster_admins( Pid ) when is_pid(Pid) orelse is_atom(Pid) ->
  get_( Pid, path( Pid, <<"cluster_admins">> ) ).

-spec delete_cluster_admin( Username :: atom() | binary() ) -> ok.
%% @doc Deletes a cluster admin.
delete_cluster_admin(Username) when is_atom(Username) ->
  delete_cluster_admin( ?MODULE, a2b( Username ) );
delete_cluster_admin(Username) when is_binary(Username) ->
  delete_cluster_admin( ?MODULE, Username ).

-spec delete_cluster_admin( Pid :: pid() | atom(), Username :: atom() | binary() ) -> ok.
%% @doc Deletes a cluster admin.
delete_cluster_admin(Pid, Username) when (is_pid(Pid) orelse is_atom(Pid))
                                      andalso is_atom(Username) ->
  delete_cluster_admin( Pid, a2b( Username ) );
delete_cluster_admin(Pid, Username) when (is_pid(Pid) orelse is_atom(Pid))
                                      andalso is_binary(Username) ->
  delete( Pid, path( Pid, <<"cluster_admins/", Username/binary>> ) ).

-spec create_cluster_admin( Username :: atom() | binary(), Password :: atom() | binary() ) -> ok | { error, status_code() }.
%% @doc Creates a cluster admin user.
create_cluster_admin(Username, Password) when is_atom(Username)
                                           andalso is_atom(Password) ->
  create_cluster_admin( ?MODULE, a2b( Username ), a2b( Password ) );
create_cluster_admin(Username, Password) when is_binary(Username)
                                           andalso is_binary(Password) ->
  create_cluster_admin( ?MODULE, Username, Password ).

-spec create_cluster_admin( Pid :: pid() | atom(), Username :: atom() | binary(), Password :: atom() | binary() ) -> ok | { error, status_code() }.
%% @doc Creates a cluster admin user.
create_cluster_admin(Pid, Username, Password) when (is_pid(Pid) orelse is_atom(Pid))
                                                andalso is_atom(Username)
                                                andalso is_atom(Password) ->
  create_cluster_admin( Pid, a2b( Username ), a2b( Password ) );
create_cluster_admin(Pid, Username, Password) when (is_pid(Pid) orelse is_atom(Pid))
                                                andalso is_binary(Username)
                                                andalso is_binary(Password) ->
  post( Pid, path( Pid, <<"cluster_admins">> ), [ { name, Username }, { password, Password } ] ).

-spec update_cluster_admin( Username :: atom() | binary(), Params :: [ { binary(), any() } ] ) -> ok | { error, status_code() }.
%% @doc Updates a cluster admin.
update_cluster_admin(Username, Params) when is_atom(Username)
                                         andalso is_list(Params) ->
  update_cluster_admin( ?MODULE, a2b( Username ), Params );
update_cluster_admin(Username, Params) when is_binary(Username)
                                         andalso is_list(Params) ->
  update_cluster_admin( ?MODULE, Username, Params ).

-spec update_cluster_admin( Pid :: pid() | atom(), Username :: atom() | binary(), Params :: [ { binary(), any() } ] ) -> ok | { error, status_code() }.
%% @doc Updates a cluster admin.
update_cluster_admin(Pid, Username, Params) when (is_pid(Pid) orelse is_atom(Pid))
                                              andalso is_atom(Username)
                                              andalso is_list(Params) ->
  update_cluster_admin( Pid, a2b( Username ), Params );
update_cluster_admin(Pid, Username, Params) when (is_pid(Pid) orelse is_atom(Pid))
                                              andalso is_binary(Username)
                                              andalso is_list(Params) ->
  post( Pid, path( Pid, <<"cluster_admins/", Username/binary>> ), Params ).

%% Continous queries:

-spec get_continuous_queries( DatabaseName :: atom() | binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists continous queries for a database.
get_continuous_queries(DatabaseName) when is_binary(DatabaseName) ->
  get_continuous_queries( ?MODULE, a2b( DatabaseName ) );
get_continuous_queries(DatabaseName) when is_binary(DatabaseName) ->
  get_continuous_queries( ?MODULE, DatabaseName ).

-spec get_continuous_queries( Pid :: pid() | atom(), DatabaseName :: atom() | binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists continous queries for a database.
get_continuous_queries(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                            andalso is_binary(DatabaseName) ->
  get_continuous_queries( Pid, a2b( DatabaseName ) );
get_continuous_queries(Pid, DatabaseName) when (is_pid(Pid) orelse is_atom(Pid))
                                            andalso is_binary(DatabaseName) ->
  get_( Pid, path( Pid, <<"db/", DatabaseName/binary, "/continuous_queries">> ) ).

-spec delete_continuous_query( DatabaseName :: atom() | binary(), Id :: atom() | binary() ) -> ok.
%% @doc Deletes a continous query.
delete_continuous_query(DatabaseName, Id) when is_binary(DatabaseName)
                                            andalso is_binary(Id) ->
  delete_continuous_query( ?MODULE, a2b( DatabaseName ), a2b( Id ) );
delete_continuous_query(DatabaseName, Id) when is_binary(DatabaseName)
                                            andalso is_binary(Id) ->
  delete_continuous_query( ?MODULE, DatabaseName, Id ).

-spec delete_continuous_query( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Id :: atom() | binary() ) -> ok.
%% @doc Deletes a continous query.
delete_continuous_query(Pid, DatabaseName, Id) when (is_pid(Pid) orelse is_atom(Pid))
                                                 andalso is_binary(DatabaseName)
                                                 andalso is_binary(Id) ->
  delete_continuous_query( Pid, a2b( DatabaseName ), a2b( Id ) );
delete_continuous_query(Pid, DatabaseName, Id) when (is_pid(Pid) orelse is_atom(Pid))
                                                 andalso is_binary(DatabaseName)
                                                 andalso is_binary(Id) ->
  delete( Pid, path( Pid, <<"db/", DatabaseName/binary, "continuous_queries/", Id/binary>> ) ).

%% Cluster servers and shards

-spec get_cluster_servers() -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists servers of a cluster.
get_cluster_servers() ->
  get_cluster_servers( ?MODULE ).

-spec get_cluster_servers(Pid :: pid() | atom()) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists servers of a cluster.
get_cluster_servers(Pid) when is_pid(Pid) orelse is_atom(Pid) ->
  get_( Pid, path( Pid, <<"cluster/servers">> ) ).

-spec get_cluster_shards() -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists shards of a cluster.
get_cluster_shards() ->
  get_cluster_shards( ?MODULE ).

-spec get_cluster_shards(Pid :: pid() | atom()) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists shards of a cluster.
get_cluster_shards(Pid) when is_pid(Pid) orelse is_atom(Pid) ->
  get_( Pid, path( Pid, <<"cluster/shards">> ) ).

-spec create_cluster_shard( StartTime :: integer(), EndTime :: integer(), LongTerm :: boolean(), ServerIds :: list() ) -> ok | { error, status_code() }.
%% @doc Creates a shard.
create_cluster_shard(StartTime, EndTime, LongTerm, ServerIds) ->
  create_cluster_shard( ?MODULE, StartTime, EndTime, LongTerm, ServerIds ).

-spec create_cluster_shard( Pid :: pid() | atom(), StartTime :: integer(), EndTime :: integer(), LongTerm :: boolean(), ServerIds :: list() ) -> ok | { error, status_code() }.
%% @doc Creates a shard.
create_cluster_shard(Pid, StartTime, EndTime, LongTerm, ServerIds) ->
  post( Pid, path( Pid, <<"cluster/shards">> ),
              [
                { startTime, StartTime },
                { endTime, EndTime },
                { longTerm, LongTerm },
                { shards, [ { serverIds, ServerIds } ] } ] ).

-spec delete_cluster_shard( Id :: binary(), ServerIds :: list() ) -> ok.
%% @doc Deletes a shard.
delete_cluster_shard(Id, ServerIds) when is_binary(Id)
                                      andalso is_list(ServerIds) ->
  delete_cluster_shard( ?MODULE, Id, ServerIds ).

-spec delete_cluster_shard( Pid :: pid() | atom(), Id :: binary(), ServerIds :: list() ) -> ok.
%% @doc Deletes a shard.
delete_cluster_shard(Pid, Id, ServerIds) when (is_pid(Pid) orelse is_atom(Pid))
                                           andalso is_binary(Id)
                                           andalso is_list(ServerIds) ->
  delete( Pid, path( Pid, <<"cluster/shards/", Id/binary>> ), { serverIds, ServerIds } ).

-spec get_cluster_shard_spaces() -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists cluster shard spaces.
get_cluster_shard_spaces() ->
  get_cluster_shard_spaces( ?MODULE ).

-spec get_cluster_shard_spaces( Pid :: pid() | atom() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists cluster shard spaces.
get_cluster_shard_spaces( Pid ) when is_pid(Pid) orelse is_atom(Pid) ->
  get_( Pid, path( Pid, <<"cluster/shard_spaces">> ) ).

%% Interfaces:

-spec get_interfaces() -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists interfaces.
get_interfaces() ->
  get_interfaces( ?MODULE ).

-spec get_interfaces(Pid :: pid() | atom()) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Lists interfaces.
get_interfaces(Pid) when is_pid(Pid) orelse is_atom(Pid) ->
  get_( Pid, path( Pid, <<"interfaces">> ) ).

%% Reading data:

-spec q( DatabaseName :: atom() | binary(), Query :: binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Executes an arbitrary query.
q(DatabaseName, Query) when is_atom(DatabaseName)
                         andalso is_binary(Query) ->
  q( ?MODULE, a2b( DatabaseName ), Query );
q(DatabaseName, Query) when is_binary(DatabaseName)
                         andalso is_binary(Query) ->
  q( ?MODULE, DatabaseName, Query ).

-spec q( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Query :: binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Executes an arbitrary query.
q(Pid, DatabaseName, Query) when (is_pid(Pid) orelse is_atom(Pid))
                              andalso is_atom(DatabaseName)
                              andalso is_binary(Query) ->
  q( Pid, a2b( DatabaseName ), Query );
q(Pid, DatabaseName, Query) when (is_pid(Pid) orelse is_atom(Pid))
                              andalso is_binary(DatabaseName)
                              andalso is_binary(Query) ->
  get_( Pid, path( Pid, <<"query">>, [ { db, DatabaseName }, { q, Query } ] ) ).

%% Writing data

-spec w( DatabaseName :: atom() | binary(), Query :: binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Insert data to database.
w(DatabaseName, Query) when is_atom(DatabaseName)
                         andalso is_binary(Query) ->
  w( ?MODULE, a2b( DatabaseName ), Query );
w(DatabaseName, Query) when is_binary(DatabaseName)
                         andalso is_binary(Query) ->
  w( ?MODULE, DatabaseName, Query ).

-spec w( Pid :: pid() | atom(), DatabaseName :: atom() | binary(), Query :: binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Insert data to database.
w(Pid, DatabaseName, Query) when (is_pid(Pid) orelse is_atom(Pid))
                              andalso is_atom(DatabaseName)
                              andalso is_binary(Query) ->
  w( Pid, a2b( DatabaseName ), Query );
w(Pid, DatabaseName, Query) when (is_pid(Pid) orelse is_atom(Pid))
                              andalso is_binary(DatabaseName)
                              andalso is_binary(Query) ->
  post( Pid, path( Pid, <<"write">>, [ { db, DatabaseName } ] ), Query ).

%% Utilities:

get_series(DatabaseResult, DefaultKey) ->
    [ Results ] = maps:get(<<"results">>, DatabaseResult),
    SeriesKey = <<"series">>,
    case maps:is_key(SeriesKey, Results) of
	false ->
	    #{DefaultKey => []};
	true ->
    	    [ Series ] = maps:get(SeriesKey, Results),
	    Series
    end.
get_series_columns(DatabaseResult) ->
    ColumnsKey = <<"columns">>,
    Series = get_series(DatabaseResult, ColumnsKey),
    maps:get(ColumnsKey, Series).
get_series_values(DatabaseResult) ->
    ValuesKey = <<"values">>,
    Series = get_series(DatabaseResult, ValuesKey),
    maps:get(ValuesKey, Series).

-spec get_database_sets( DatabaseName :: binary() ) -> list().
%% @doc Get tag and field keys from IndluxDB.
get_database_sets(DatabaseName) ->
    TKResult = q(DatabaseName, <<"SHOW TAG KEYS">>),
    TSKeys = lists:flatten(get_series_values(TKResult)),
    FKResult = q(DatabaseName, <<"SHOW FIELD KEYS">>),
    FSKeys = lists:flatten([
	lists:reverse(
	  tl(lists:reverse(KeyName))) ||
	KeyName <- get_series_values(FKResult) ]),
    [ TSKeys, FSKeys ].

-spec terms_to_dbstyle( Terms :: term(), DatabaseSets :: list() ) -> list().
%% @doc Converts DB terms to line format chain.
terms_to_dbstyle( Terms, DatabaseSets ) ->
    terms_to_dbstyle( Terms, DatabaseSets, no_timestamps ).
-spec terms_to_dbstyle( Terms :: term(), DatabaseSets :: list(), Opts :: atom() ) -> list().
%% @doc Converts DB terms to line format chain.
terms_to_dbstyle( Terms, DatabaseSets, Opts ) ->
    [ TSKeys, FSKeys ] = DatabaseSets,
    [ Results ] = maps:get(<<"results">>, Terms),
    [ Series ] = maps:get(<<"series">>, Results),
    DBFields = maps:get(<<"columns">>, Series),
    Values = maps:get(<<"values">>, Series),
    SeriesName = maps:get(<<"name">>, Series),
    lists:foldl(fun(DBValues, AccLists) ->
	DBTagSet = lists:flatten(lists:zipwith(fun(Field, Value) ->
	    case lists:member(Field, TSKeys) of
		false when Value =/= null -> [];
		false when Value =:= null -> [];
		true when Value =:= null -> [];
		_ -> <<Field/binary, "=", Value/binary>>
	    end
	end, DBFields, DBValues)),
	DBFieldSet = lists:flatten(lists:zipwith(fun(Field, Value) ->
	    case lists:member(Field, FSKeys) of
		false when Value =:= null -> [];
		false when Value =/= null -> [];
		true when Value =:= null -> [];
		_ when is_binary(Value) ->
		    BinValue = list_to_binary(lists:flatten(
			io_lib:format('"~s"', [ Value ]))),
		    <<Field/binary, "=", BinValue/binary>>;
		_ ->
		    BinValue = list_to_binary(lists:flatten(
			io_lib:format("~p", [ Value ]))),
		    <<Field/binary, "=", BinValue/binary>>
	    end
	end, DBFields, DBValues)),
	TagSet = lists:join(<<",">>, lists:flatten([
			Col || Col <- DBTagSet,
			       Col /= []
		])),
	FieldSet = lists:join(<<",">>, lists:flatten([
			Col || Col <- DBFieldSet,
			       Col /= []
		])),
	LineFormatRow = case TagSet of
	    [] when Opts =:= [ preserve_timestamps ] ->
		RowTime = integer_to_binary(hd(DBValues)),
		[[SeriesName], [<<" ">>],
		 	FieldSet, [<<" ">>], [RowTime]];
	    [] ->
		[[SeriesName], [<<" ">>], FieldSet];
	    _ when Opts =:= [ preserve_timestamps ] ->
		RowTime = integer_to_binary(hd(DBValues)),
		[[SeriesName], [<<",">>], TagSet, [<<" ">>],
		 	FieldSet, [<<" ">>], [RowTime]];
	    _ -> [[SeriesName], [<<",">>], TagSet, [<<" ">>],
		  	FieldSet]
	end,
	Row = lists:concat(LineFormatRow),
	AccLists ++ [ lists:concat([ binary_to_list(Item) || Item <- Row ]) ]
    end, [], Values).

%% Internals:

-spec path( Pid :: pid() | atom(), Action :: binary() ) -> binary().
%% @doc Given an action, returns full URI for the command.
path( Pid, Action ) ->
  path( Pid, Action, [] ).

-spec path( Pid :: pid() | atom(), Component :: binary(), Options :: list() ) -> binary().
%% @doc Given an action, returns full URI for the command.
path( Pid, Action, Options ) ->
  gen_server:call(Pid, { path, Action, Options }).

-spec get_( Pid :: pid() | atom(), Action :: binary() ) -> list() | { error, json_parse, json_parse_error_reason() } | { error, status_code() }.
%% @doc Executes a GET request.
get_( Pid, Action ) ->
  gen_server:call(Pid, { get, Action }).

-spec post( Pid :: pid() | atom(), Action :: binary(), Data :: list() ) -> ok | { error, status_code() }.
%% @doc Executes a POST.
post( Pid, Action, Data ) ->
  gen_server:call(Pid, { post, Action, Data }).

-spec delete( Pid :: pid() | atom(), Action :: binary() ) -> ok.
%% @doc Executes a DELETE request.
delete( Pid, Action ) ->
  delete(Pid,  Action, [] ).

-spec delete( Pid :: pid() | atom(), Action :: binary(), Data :: list() ) -> ok.
%% @doc Executes a delete request with data.
delete( Pid, Action, Data ) ->
  gen_server:call(Pid, { delete, Action, Data }).

-spec a2b( Atom :: atom() ) -> binary().
%% @doc Given an atom, returns binary.
a2b( Atom ) ->
  list_to_binary( atom_to_list( Atom ) ).

-spec bin_to_timestamp( Datetime :: binary() ) -> integer().
%% @doc (OBSOLETE) Converts binary datetime to timestamp (RFC3339).
bin_to_timestamp(Datetime) ->
    BaseSecs = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    case binary:split(Datetime, <<".">>) =:= [Datetime] of
	false ->
	    {ok, [Year,Month,Day,Hours,Minutes,Seconds,Milliseconds], _} =
		io_lib:fread("~d-~d-~dT~d:~d:~d.~dZ",
		     binary_to_list(Datetime));
	true ->
	    Milliseconds = 0,
	    {ok, [Year,Month,Day,Hours,Minutes,Seconds], _} =
		io_lib:fread("~d-~d-~dT~d:~d:~dZ",
		     binary_to_list(Datetime))
    end,
    ((calendar:datetime_to_gregorian_seconds(
       {{Year,Month,Day},{Hours,Minutes,Seconds}}
    ) - BaseSecs) * 1000000000) + Milliseconds.

handle_call( { path, Action, Options }, From, { http, Pid, Config } ) ->
  Username = case lists:keyfind(u, 1, Options) of
    { u, GivenUsername } ->
      GivenUsername;
    false ->
      Config#erflux_config.username
  end,
  Password = case lists:keyfind(p, 1, Options) of
    {p, GivenPassword} ->
      GivenPassword;
    false ->
      Config#erflux_config.password
  end,
  case lists:keyfind( q, 1, Options ) of
    false ->
      case lists:keyfind( db, 1, Options ) of
	false ->
          gen_server:reply(From, <<Action/binary, "?u=", Username/binary, "&p=", Password/binary>>);
	{ db, DatabaseName } ->
          gen_server:reply(From, <<Action/binary, "?epoch=ns&db=", DatabaseName/binary, "&u=", Username/binary, "&p=", Password/binary>>)
      end;
    { q, Value } ->
      EscapedValue = list_to_binary( edoc_lib:escape_uri( binary_to_list( Value ) ) ),
      case lists:keyfind( db, 1, Options ) of
	false ->
	  gen_server:reply(From, <<Action/binary, "?u=", Username/binary, "&p=", Password/binary, "&q=", EscapedValue/binary>>);
	{ db, DatabaseName } ->
	  gen_server:reply(From, <<Action/binary, "?epoch=ns&db=", DatabaseName/binary, "&u=", Username/binary, "&p=", Password/binary, "&q=", EscapedValue/binary>>)
      end
  end,
  { noreply, { http, Pid, Config } };

handle_call( { get, Path }, From, { http, Pid, Config } ) ->
  spawn(fun() ->
    TimedResponse = gen_server:call( Pid, { get, timeout, Path }, Config#erflux_config.timeout ),
    gen_server:reply( From, TimedResponse )
  end),
  { noreply, { http, Pid, Config } };
handle_call( { get, timeout, Path }, From, { http, Pid, Config } ) ->
  Protocol = Config#erflux_config.protocol,
  Host = Config#erflux_config.host,
  Port = list_to_binary(integer_to_list( Config#erflux_config.port )),
  Uri = <<Protocol/binary, "://", Host/binary, ":", Port/binary, "/", Path/binary>>,
  {ok, {{_Version, StatusCode, _ReasonPhrase}, _Headers, Body}} =
  	httpc:request(get, {binary_to_list(Uri), []}, [], []),
  case trunc(StatusCode/100) of
    2 ->
      case list_to_binary(Body) of
        <<"">> ->
          gen_server:reply(From, { ok, StatusCode });
        _ ->
          try
            case jsx:decode( list_to_binary(Body), [{return_maps, true}] ) of
              JsonData ->
                gen_server:reply(From, JsonData)
            end
          catch
            _Error:Reason ->
              gen_server:reply(From, { error, json_parse, Reason })
          end
      end;
    _ ->
      gen_server:reply(From, { error, StatusCode })
  end,
  { noreply, { http, Pid, Config } };

handle_call( { post, Path, Data }, From, { http, Pid, Config } ) ->
  spawn_link(fun() ->
    TimedResponse = gen_server:call( Pid, { post, timeout, Path, Data }, Config#erflux_config.timeout ),
    gen_server:reply( From, TimedResponse )
  end),
  { noreply, { http, Pid, Config } };
handle_call( { post, timeout, Path, Data }, From, { http, Pid, Config } ) ->
  Protocol = Config#erflux_config.protocol,
  Host = Config#erflux_config.host,
  Port = list_to_binary(integer_to_list( Config#erflux_config.port )),
  Uri = <<Protocol/binary, "://", Host/binary, ":", Port/binary, "/", Path/binary>>,
  {ok, {{_Version, StatusCode, _ReasonPhrase}, _Headers, _Body}} =
  	httpc:request(post, {binary_to_list(Uri), [], "application/x-www-form-urlencoded", Data}, [], []),
  case trunc(StatusCode/100) of
    2 ->
      gen_server:reply(From, ok);
    _ ->
      gen_server:reply(From, { error, StatusCode } )
  end,
  { noreply, { http, Pid, Config } };

handle_call( { delete, Path, Data }, From, { http, Pid, Config } ) ->
  spawn_link(fun() ->
    TimedResponse = gen_server:call( Pid, { delete, timeout, Path, Data }, Config#erflux_config.timeout ),
    gen_server:reply( From, TimedResponse )
  end),
  { noreply, { http, Pid, Config } };
handle_call( { delete, timeout, Path, Data }, From, { http, Pid, Config } ) ->
  Protocol = Config#erflux_config.protocol,
  Host = Config#erflux_config.host,
  Port = list_to_binary(integer_to_list( Config#erflux_config.port )),
  Uri = <<Protocol/binary, "://", Host/binary, ":", Port/binary, "/", Path/binary>>,
  BinaryData = case Data of
    [] ->
      <<"">>;
    _ ->
      jsx:encode( Data )
  end,
  {ok, _, _, _} = hackney:request(delete, Uri, [], BinaryData),
  gen_server:reply( From, ok ),
  { noreply, { http, Pid, Config } }.

%% gen_server behaviour:

handle_cast(_Msg, LoopData) ->
  {noreply, LoopData}.

handle_info(_Msg, LoopData) ->
  {noreply, LoopData}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _LoopData) ->
  ok.

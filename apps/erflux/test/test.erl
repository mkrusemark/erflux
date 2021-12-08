-module(test).
-include_lib("eunit/include/eunit.hrl").

-export([start/0]).

start() ->
    application:start(crypto),
    application:start(erflux),
    application:load(erflux),
    application:set_env(erflux, host, <<"127.0.0.1">>),
    application:set_env(erflux, port, 8086),
    application:set_env(erflux, username, <<"admin">>),
    application:set_env(erflux, password, <<"mysecret">>),
    application:set_env(erflux, ssl, false),
    application:set_env(erflux, timeout, infinity),
    application:start(erflux),
    application:ensure_all_started(erflux),
    erflux_sup:add_erflux(erflux_http),

    CreateDatabase = erflux_http:create_database(erfluxtest),
    io:format("CreateDatabase=~p~n", [CreateDatabase]),

    DatabaseSets = erflux_http:get_database_sets(erfluxtest),
    io:format("DatabaseSets=~p~n", [DatabaseSets]),

    WriteSyntax = <<"testseries,taga=777,tagb=999,tagc=333 field002=0.23,field007=999 1636733333914173508">>,
    WriteResult = erflux_http:w(erfluxtest, WriteSyntax),
    io:format("WriteResult=~p~n", [WriteResult]),

    ReadSyntax = <<"SELECT * FROM testseries ORDER BY time DESC">>,
    ReadResult = erflux_http:q(erfluxtest, ReadSyntax),

    ListNoTimestamps = erflux_http:terms_to_dbstyle(ReadResult, DatabaseSets),
    io:format("== DEFAULT DB ON WRITE (NO TIMESTAMPS) ==~n~p~n", [ ListNoTimestamps ]),

    ListPreserveTimestamps = erflux_http:terms_to_dbstyle(ReadResult,
					DatabaseSets, [ preserve_timestamps ]),
    io:format("== PRESERVE TIMESTAMPS ==~n~p~n~n", [ ListPreserveTimestamps ]).

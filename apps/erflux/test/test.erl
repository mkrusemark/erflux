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

    %% A simple example: WRITE TO DB
    Data = <<"testseries,a=777,b=999,c=333 value=0.998801 1636732718658895378">>,
    io:format(">>> Write data: ~p~n", [ Data ]),
    W = erflux_http:w(erfluxtest, Data),
    io:format(">>> Write result: ~p~n~n", [ W ]),

    %% A simple example: READ FROM DB
    InfluxQuery = <<"SELECT a,b,c,value FROM testseries WHERE value > 0 LIMIT 1000">>,
    DBTerms = erflux_http:q(erfluxtest, InfluxQuery),
    ListPreserveTimestamps = erflux_http:terms_to_dbstyle(DBTerms, preserve_timestamps),
    ListNoTimestamps = erflux_http:terms_to_dbstyle(DBTerms),
    io:format("== PRESERVE TIMESTAMPS ==~n~p~n~n", [ ListPreserveTimestamps ]),
    io:format("== DEFAULT DB ON WRITE (NO TIMESTAMPS) ==~n~p~n", [ ListNoTimestamps ]).

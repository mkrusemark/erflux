-module(test).
-include_lib("eunit/include/eunit.hrl").

-export([start/0]).

lnread(File) ->
    case file:read_line(File) of
        {ok, Data} -> [
		lists:reverse(
		  tl(lists:reverse(Data))) | lnread(File)];
        eof        -> []
    end.

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

    DeleteDatabase = erflux_http:delete_database(erfluxtest_other),
    io:format("DeleteDatabase=~p~n", [DeleteDatabase]),

    CreateDatabase = erflux_http:create_database(erfluxtest),
    io:format("CreateDatabase=~p~n", [CreateDatabase]),

    DatabaseSets = erflux_http:get_database_sets(erfluxtest),
    io:format("DatabaseSets=~p~n", [DatabaseSets]),

    CountResult = erflux_http:get_field_values_count(erfluxtest, testseries),
    io:format("CountResult=~p~n", [CountResult]),

    WriteSyntax = <<"testseries,taga=777,tagb=999,tagc=333 field002=0.23,field007=999 1636733333914173508">>,
    WriteResult = erflux_http:w(erfluxtest, WriteSyntax),
    io:format("WriteResult=~p~n", [WriteResult]),

    ReadSyntax = <<"SELECT * FROM testseries ORDER BY time DESC">>,
    ReadResult = erflux_http:q(erfluxtest, ReadSyntax),

    ListNoTimestamps = erflux_http:terms_to_dbstyle(ReadResult, DatabaseSets),
    io:format("== DEFAULT DB ON WRITE (NO TIMESTAMPS) ==~n~p~n", [ ListNoTimestamps ]),

    ListPreserveTimestamps = erflux_http:terms_to_dbstyle(ReadResult,
					DatabaseSets, [ preserve_timestamps ]),
    io:format("== PRESERVE TIMESTAMPS ==~n~p~n~n", [ ListPreserveTimestamps ]),

    {ok, W} = file:open("result-line-format.txt", [write]),
    ResW = [ io:format(W, "~s~n", [Line]) || Line <- ListPreserveTimestamps ],
    file:close(W),
    io:format("Write lines to file: ~p~n", [ResW]),

    {ok, R} = file:open("result-line-format.txt", [read]),
    Lines = lnread(R),
    file:close(R),
    io:format("Lines: ~p~n", [Lines]),

    CreateOtherDatabase = erflux_http:create_database(erfluxtest_other),
    io:format("CreateOtherDatabase=~p~n", [CreateOtherDatabase]),

    ResDb = [ erflux_http:w(erfluxtest_other, list_to_binary(Line)) || Line <- Lines ],
    io:format("Insert lines to other db: ~p~n", [ResDb]),

    %% TEST OTHER DB (only with preserve_timestamps argument testing)
    ReadSyntaxOther = <<"SELECT * FROM testseries ORDER BY time DESC">>,
    ReadResultOther = erflux_http:q(erfluxtest_other, ReadSyntaxOther),

    ListPreserveTimestampsOther = erflux_http:terms_to_dbstyle(ReadResultOther,
					DatabaseSets, [ preserve_timestamps ]),
    io:format("== OTHERDB PRESERVE TIMESTAMPS ==~n~p~n~n", [ ListPreserveTimestampsOther ]).

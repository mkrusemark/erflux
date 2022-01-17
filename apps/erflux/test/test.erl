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

    DeleteDatabase = erflux_http:delete_database(database_test08_other),
    io:format("DeleteDatabase=~p~n", [DeleteDatabase]),

    CreateDatabase = erflux_http:create_database(database_test08),
    io:format("CreateDatabase=~p~n", [CreateDatabase]),

    ShowMeasurements = erflux_http:show_measurements(database_test08),
    io:format("ShowMeasurements=~p~n", [ShowMeasurements]),

    IsMeasurementExists = erflux_http:is_measurement_exists(database_test08, prueba999),
    io:format("IsMeasurementExists=~p~n", [IsMeasurementExists]),

    CountResult = erflux_http:get_field_values_count(database_test08, testseries),
    io:format("CountResult=~p~n", [CountResult]),

    Rand1 = integer_to_binary(rand:uniform(100000000)),
    Rand2 = integer_to_binary(rand:uniform(10000)),
    WriteSyntax = <<"testseries,taga=", Rand2/binary, ",tagb=999,tagc=333 field002=0.23,field007=", Rand1/binary, ",str01=\"Hola Alberto\" 1636733333914173508">>,
    WriteResult = erflux_http:w(database_test08, WriteSyntax),
    io:format("WriteResult=~p~n", [WriteResult]),
    WriteSyntax2 = <<"testseries,taga=", Rand2/binary, ",tagb=999,tagc=333 field002=0.23,field007=", Rand1/binary, ",str01=\"Hola Alberto\"">>,
    WriteResult2 = erflux_http:w(database_test08, WriteSyntax2),
    io:format("WriteResult2=~p~n", [WriteResult2]),

    ReadSyntax = <<"SELECT * FROM testseries ORDER BY time DESC">>,
    ReadResult = erflux_http:q(database_test08, ReadSyntax),
    % io:format("== RESULT == ~n~p~n", [ReadResult]),

    DatabaseSets = erflux_http:get_database_sets(database_test08),
    io:format("DatabaseSets=~p~n", [DatabaseSets]),

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

    CreateOtherDatabase = erflux_http:create_database(database_test08_other),
    io:format("CreateOtherDatabase=~p~n", [CreateOtherDatabase]),

    ResDb = [ erflux_http:w(database_test08_other, list_to_binary(Line)) || Line <- Lines ],
    io:format("Insert lines to other db: ~p~n", [ResDb]),

    %% TEST OTHER DB (only with preserve_timestamps argument testing)
    ReadSyntaxOther = <<"SELECT * FROM testseries ORDER BY time DESC">>,
    ReadResultOther = erflux_http:q(database_test08_other, ReadSyntaxOther),

    ListPreserveTimestampsOther = erflux_http:terms_to_dbstyle(ReadResultOther,
					DatabaseSets, [ preserve_timestamps ]),
    io:format("== OTHERDB PRESERVE TIMESTAMPS ==~n~p~n~n", [ ListPreserveTimestampsOther ]).

%%%-------------------------------------------------------------------
%% @doc erflux public API
%% @end
%%%-------------------------------------------------------------------

-module(erflux_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    erflux_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

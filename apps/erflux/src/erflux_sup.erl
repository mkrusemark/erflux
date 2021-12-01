%%%-------------------------------------------------------------------
%% @doc erflux top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(erflux_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-export([
  add_erflux/1,
  add_erflux/3,
  add_erflux/4,
  add_erflux/5,
  add_erflux/6,
  add_erflux/7,
  remove_erflux/1
]).

-include("erflux.hrl").

-define(ATTEMPT_MODULE_USE, true).
-define(CUSTOM_PID, fasle).
-define(CONF_MODULE, erflux_conf).

-type child() :: undefined | pid().
-type config() :: #erflux_config{}.
-type statup_result() :: {ok, Child :: child()}
                          | {ok, Child :: child(), Info :: term()}
                          | {error, already_present}
                          | {error, {already_started, Child :: child()}}
                          | {error, term()}.

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

-spec add_erflux( Name :: atom() ) -> statup_result().
%% @doc Starts instance of erflux with a given name. If name other than erflux_http, pid() versions of erflux_http functions have to be used.
add_erflux(Name) ->
  Config = ?CONF_MODULE:configure(),
  add_erflux_internal( Name, Config#erflux_config{} ).

-spec add_erflux( Name :: atom(), Username :: binary(), Password :: binary() ) -> statup_result().
%% @doc Starts instance of erflux with a given name. If name other than erflux_http, pid() versions of erflux_http functions have to be used.
add_erflux(Name, Username, Password) ->
  Config = ?CONF_MODULE:configure(),
  add_erflux_internal( Name, Config#erflux_config{
                              username = Username,
                              password = Password } ).

-spec add_erflux( Name :: atom(), Username :: binary(), Password :: binary(), Host :: binary() ) -> statup_result().
%% @doc Starts instance of erflux with a given name. If name other than erflux_http, pid() versions of erflux_http functions have to be used.
add_erflux(Name, Username, Password, Host ) ->
  Config = ?CONF_MODULE:configure(),
  add_erflux_internal( Name, Config#erflux_config{
                              username = Username,
                              password = Password,
                              host = Host } ).

-spec add_erflux( Name :: atom(), Username :: binary(), Password :: binary(), Host :: binary(), Port :: binary() ) -> statup_result().
%% @doc Starts instance of erflux with a given name. If name other than erflux_http, pid() versions of erflux_http functions have to be used.
add_erflux(Name, Username, Password, Host, Port) ->
  Config = ?CONF_MODULE:configure(),
  add_erflux_internal( Name, Config#erflux_config{
                              username = Username,
                              password = Password,
                              host = Host,
                              port = Port } ).

-spec add_erflux( Name :: atom(), Username :: binary(), Password :: binary(), Host :: binary(), Port :: binary(), Protocol :: binary() ) -> statup_result().
%% @doc Starts instance of erflux with a given name. If name other than erflux_http, pid() versions of erflux_http functions have to be used.
add_erflux(Name, Username, Password, Host, Port, Protocol) ->
  Config = ?CONF_MODULE:configure(),
  add_erflux_internal( Name, Config#erflux_config{
                              username = Username,
                              password = Password,
                              host = Host,
                              port = Port,
                              protocol = Protocol } ).

-spec add_erflux( Name :: atom(), Username :: binary(), Password :: binary(), Host :: binary(), Port :: binary(), Protocol :: binary(), Timeout :: integer() ) -> statup_result().
%% @doc Starts instance of erflux with a given name. If name other than erflux_http, pid() versions of erflux_http functions have to be used.
add_erflux(Name, Username, Password, Host, Port, Protocol, Timeout) ->
  Config = ?CONF_MODULE:configure(),
  add_erflux_internal( Name, Config#erflux_config{
                              username = Username,
                              password = Password,
                              host = Host,
                              port = Port,
                              protocol = Protocol,
                              timeout = Timeout } ).

-spec add_erflux_internal(Name :: atom(), Configuration :: config()) -> statup_result().
%% @doc Used internally to start erflux instance.
add_erflux_internal(Name, Configuration) ->
  case Name of
    erflux_http ->
      supervisor:start_child(?MODULE, {
        Name,
        { erflux_http, start_link, [ Configuration ]},
        permanent,
        brutal_kill,
        supervisor,
        []
      });
    _ ->
      supervisor:start_child(?MODULE, {
        Name,
        { erflux_http, start_link, [Name, Configuration]},
        permanent,
        brutal_kill,
        supervisor,
        []
      })
  end.

-spec remove_erflux( Name :: atom() ) -> {ok, Child :: child()}
                                         | {ok, Child :: child(), Info :: term()}
                                         | {error, running}
                                         | {error, restarting}
                                         | {error, not_found}
                                         | {error, simple_one_for_one}
                                         | {error, term()}.
%% @doc Removing an instance of erflux.
remove_erflux(Name) ->
  case supervisor:terminate_child(?MODULE, Name) of
    ok ->
      supervisor:delete_child(?MODULE, Name);
    {error, Reason} ->
      {error, Reason}
  end.

%%%-------------------------------------------------------------------
%% @doc qmqec top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(qmqec_broker_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).


%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link(?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    Broker =
    	{ qmqec_broker_conn_server
    	, { qmqec_broker_conn_server, start_link, [] }
        , temporary
        , brutal_kill
        , worker
        , [ qmqec_broker_conn_server ]
        },
    Children = [Broker],
    RestartStrategy = {simple_one_for_one, 0, 1},
    {ok, {RestartStrategy, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

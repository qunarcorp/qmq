%%%-------------------------------------------------------------------
%% @doc qmqec
%% @end
%%%-------------------------------------------------------------------

-module(qmqec).


%% API
-export([ start/2
        , start/3
        , start_link/2
        , start_link/3
        , publish/3
        ]).

-define(MOD, qmqec_control_server).

-include("qmqec.hrl").

%%====================================================================
%% API functions
%%====================================================================

%% Options = [option()].
%% option() = {app_code, binary()}
%%            
start(MetaHttpUrl, Options) ->
    gen_server:start(?MOD, {MetaHttpUrl, Options}, []).

start(ServerName, MetaHttpUrl, Options) ->
    gen_server:start(ServerName, ?MOD, {MetaHttpUrl, Options}, []).

start_link(MetaHttpUrl, Options) ->
    gen_server:start_link(?MOD, {MetaHttpUrl, Options}, []).


start_link(ServerName, MetaHttpUrl, Options) ->
    gen_server:start_link(ServerName, ?MOD, {MetaHttpUrl, Options}, []).

%% -> ok
%%  | {uncompleted, [#producer_message{}]}
%%  | error
publish(QmqecPid, Subject, KVList) ->
	Reqeust = #meta_request{ subject = Subject
	                       , client_type_code = producer
	                       },
	MessageList = [ #producer_message{ kv_list = KVList } ],
	do_publish(QmqecPid, Reqeust, MessageList).


%%====================================================================
%% Internal functions
%%====================================================================

do_publish(QmqecPid, MetaRequest, MessageList) ->
    ?MOD:publish(QmqecPid, MetaRequest, MessageList).
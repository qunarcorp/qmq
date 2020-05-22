-module(qmqec_utils).

%% API
-export([ local_ip_v4/0
        , local_ip_bin/0
        , random_select/1
        , timestamp/0
        , get_pid/0
        , binary_to_ip/1
        ]).


-include("qmqec.hrl").

%%====================================================================
%% API functions
%%====================================================================

%% 获取当前主机ip
local_ip_v4() ->
    {ok, Addrs} = inet:getifaddrs(),
    hd([
         Addr || {_, Opts} <- Addrs, {addr, Addr} <- Opts, size(Addr) == 4, Addr =/= {127, 0, 0, 1}
    ]).

local_ip_bin() ->
    {A, B, C, D} = local_ip_v4(),
    << (integer_to_binary(A))/binary, "."
     , (integer_to_binary(B))/binary, "."
     , (integer_to_binary(C))/binary, "."
     , (integer_to_binary(D))/binary >>.


random_select([]) ->
    undefined;
random_select(List) ->
    {_, _, M} = os:timestamp(),
    lists:nth(((M rem length(List)) + 1), List).


timestamp() ->
    {MS, S, US} = os:timestamp(),
    ( MS * 1000000000 ) + ( S * 1000 ) + ( US div 1000 ).


get_pid() ->
    list_to_binary(os:getpid()).


binary_to_ip(Bin) when is_binary(Bin) ->
    [A, B, C, D] = lists:map( fun erlang:binary_to_integer/1
                            , string:split(Bin, <<".">>, all)
                            ),
    {A, B, C, D}.


%%====================================================================
%% API functions about record
%%====================================================================



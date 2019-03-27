-module(qmqec_test).


%% API
-export([ start/0
        ]).


%%====================================================================
%% API functions
%%====================================================================

start() ->
    {ok, Pid} = qmqec:start_link( "http://localhost:8080/meta/address"
                                , [{app_code, <<"gibbon">>}]
                                ),
    lists:foreach(
        fun(N) ->
            Result = qmqec:publish(
                Pid, <<"gibbon_test">>,
                [{<<"message">>, <<"gibbon is a good boy">>}]
            ),
            io:format("~p: ~p~n", [N, Result])
        end,
        lists:seq(1, 10)
    ).

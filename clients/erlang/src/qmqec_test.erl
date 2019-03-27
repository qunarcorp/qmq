-module(qmqec_test).

-record(state, { count = 100000
               , ok_count = 0
               , uncompleted_count = 0
               , error_count = 0
               , concurrent = 0
               , max_concurrent = 1000
               , pid
               , start_timestamp
               }).

%% API
-export([ start/0
        ]).

%% 使用的我的intel-computer-stick跑的客户端测试代码,
%% 配置:
%%     cpu: 四核凌动 x5-Z8300
%%     mem: 2GB
%%     eMMC: 32GB
%% 测试publish 10w个消息要耗时多少, 并发1000个进程
%% 测试结果:
%%     timespan: 23069
%%     ok: 100000
%%     uncompleted: 0
%%     error: 0
%% 折算为qps为4335


%%====================================================================
%% API functions
%%====================================================================

start() ->
    {ok, Pid} = qmqec:start_link( "http://localhost:8080/meta/address"
                                , [{app_code, <<"gibbon">>}]
                                ),
    loop(#state{pid = Pid, start_timestamp = qmqec_utils:timestamp()}).


loop( State = #state{ ok_count = OkCount
                    , uncompleted_count = UncompletedCount
                    , error_count = ErrorCount
                    , concurrent = MaxConcurrent
                    , max_concurrent = MaxConcurrent
                    }
    ) ->
    receive
        {result, N, Result} ->
            State1 = State#state{concurrent = MaxConcurrent-1},
            State2 =
                case Result of
                    ok -> State1#state{ok_count = OkCount+1};
                    {uncompleted, _} -> State1#state{uncompleted_count = UncompletedCount+1};
                    error -> State1#state{error_count = ErrorCount+1}
                end,
            case N of
                1 -> show_result(State2);
                _ -> loop(State2)
            end
    end;
loop( State = #state{ count = Count
                    , concurrent = Concurrent
                    , max_concurrent = MaxConcurrent
                    , pid = Pid
                    }
    ) when Concurrent < MaxConcurrent ->
    Me = self(),
    spawn(
        fun() ->
            Result = qmqec:publish(
                Pid, <<"gibbon_test">>,
                [{<<"message">>, <<"gibbon is a good boy">>}]
            ),
            Me ! {result, Count, Result}
        end
    ),
    % io:format("spawn ~p~n", [Count]),
    loop(State#state{ count = Count-1
                    , concurrent = Concurrent+1
                    }).


show_result( #state{ ok_count = OkCount
                   , uncompleted_count = UncompletedCount
                   , error_count = ErrorCount
                   , start_timestamp = Start
                   }
           ) ->
    End = qmqec_utils:timestamp(),
    io:format("timespan: ~p~nok: ~p~nuncompleted: ~p~nerror: ~p~n", [End - Start, OkCount, UncompletedCount, ErrorCount]).
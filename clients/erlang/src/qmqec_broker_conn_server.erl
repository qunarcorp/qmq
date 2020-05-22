-module(qmqec_broker_conn_server).
-behaviour(gen_server).

%% API
-export([ start_link/2
        , publish/3
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, { sock
               , broker                    %% #broker{}
               , monitor_pid
               , request_tab
               , local_ip
               , process_id                %% erlang运行时的进程编号
               , id_acc = 0                %% message自增id
               , tref                      %% 上个定时器, 做一些事情: 
                                           %%   1. 检查是不是很久没发request了,如果是,就关闭连接
                                           %%   2. 重置id_acc
               , request_id_counter = 0    %% request_id的计数器
               , last_time
               }).
% {ReqId, ReqHeader, ReqArg, Action, _Time}
-record(request, { request_id
                 , header
                 , arg
                 , action
                 , time
                 }).

-include("qmqec.hrl").

%%====================================================================
%% API functions
%%====================================================================

start_link(Broker, MonitorPid) ->
    gen_server:start_link(?MODULE, {Broker, MonitorPid}, []).


%% -> {ok, FailedMsgList}
publish(Pid, MetaRequest, MessageList) ->
    gen_server:call(Pid, {publish, MetaRequest, MessageList}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({ Broker = #broker{ server_ip   = Ip
                       , server_port = Port
                       } 
     , MonitorPid
    }) ->
    {ok, Sock} = gen_tcp:connect( Ip
                                , Port
                                , [ binary
                                  , {active, true}
                                  , {packet, 4}
                                  ]
                                ),
    RequestTab = ets:new(request_tab, [set, private, {keypos, 2}]),
    %% local ip
    LocalIp = qmqec_utils:local_ip_bin(),
    %% pid
    ProcId = qmqec_utils:get_pid(),
    %% 6秒触发一次
    TRef = timer:send_interval(6000, tick),
    %% 自己启动了, 通知meta_tcp_server一声
    MonitorPid ! {broker_started, {Ip, Port, self()}},
    {ok, #state{ sock         = Sock
               , broker       = Broker
               , monitor_pid  = MonitorPid
               , request_tab  = RequestTab
               , local_ip     = LocalIp
               , process_id   = ProcId
               , tref         = TRef
               , last_time   = qmqec_utils:timestamp()
               }}.


handle_call( { publish
             , _MetaRequest = #meta_request{ subject   = Subject
                                           , app_code  = AppCode
                                           }
             , MessageList
             }
           , _From
           , State = #state{ sock               = Sock
                           , request_tab        = RequestTab
                           , local_ip           = LocalIp
                           , process_id         = ProcId
                           , id_acc             = Id
                           , request_id_counter = RequestId
                           }
           ) ->
    %% check message

    %% 需要考虑到请求的数据如果服务一直不相应,会造成客户端内存泄漏的问题,
    %% 所以需要给每个请求加上时间戳属性, 定时扫描超时时使用.
    CreateTime = qmqec_utils:timestamp(),
    Fun =
        fun( Msg, {MsgList, IdAcc} ) ->
            Default = #producer_message{ begin_ts = qmqec_utils:timestamp()
                                       , subject  = Subject
                                       , id       = build_msg_id(LocalIp, ProcId, IdAcc)
                                       },
            %% 根据余昭辉的要求, 需要在每个msg的kv_list中加入qmq_appCode和
            %% qmq_createTIme这两个k来兼容历史问题.
            NewMsgKVList = [ {<<"qmq_appCode">>, AppCode}
                           , {<<"qmq_createTIme">>, integer_to_binary(CreateTime)}
                           | Msg#producer_message.kv_list
                           ],
            NewMsg = combine_producer_message(Default, Msg#producer_message{ kv_list = NewMsgKVList }),
            {[NewMsg | MsgList], IdAcc + 1}
        end,
    {MessageListReversed, NewId} = lists:foldl(Fun, {[], Id}, MessageList),
    NewMessageList = lists:reverse(MessageListReversed),
    Header = #header{ code         = send_message
                    , request_id   = RequestId
                    , flag         = request
                    , request_code = send_message
                    },
    SendData = qmqec_protocol:build_producer_request(Header, NewMessageList),
    ok = gen_tcp:send(Sock, SendData),
    ets:insert(RequestTab, #request{ request_id = RequestId
                                   , header     = Header
                                   , arg        = NewMessageList
                                   , action     = {reply, _From}
                                   , time       = CreateTime
                                   }),
    % ets:insert(RequestTab, {RequestId, Header, NewMessageList, {reply, _From}, CreateTime}),
    {noreply, State#state{ id_acc             = NewId
                         , request_id_counter = RequestId + 1
                         , last_time         = qmqec_utils:timestamp()
                         }}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info( {tcp, Sock, Data}
           , State = #state{ sock        = Sock
                           , request_tab = _RequestTab
                           }
           ) ->
    {ok, Header, Body} = qmqec_protocol:parse_data(Data),
    handle_resp({Header, Body}, State);

handle_info(tick, State = #state{ request_tab = Tab
                                , last_time = Last 
                                }) ->
    Now = qmqec_utils:timestamp(),
    %% 检查是否有超时请求需要清理
    N = ets:info(Tab, size),
    case N of
        N when N > 1000 ->
            %% 因为绝大部份情况,请求都会有响应, 所以没有必要每次都扫描请求超时,
            %% 拍了一个数, 当积压的请求上下文超过1000时, 才进行扫描
            %% 删除10秒之前的请求
            delete_timeout(Tab, Now - 10000);
        _ ->
            pass
    end,
    %% 检查是否长时间空闲
    case (Now - Last) of
        N when N >= 180000 ->
            %% 暂时设置3分钟为超时, 如果3分钟内没有任何操作, 就主动关闭连接
            {stop, normal, State};
        _ ->
            {noreply, State}
    end.


terminate( _Reason
         , #state{ broker      = #broker{ server_ip   = Ip
                                        , server_port = Port
                                        }
                 , monitor_pid = MonitorPid
                 , tref        = TRef
                 }
         ) ->
    MonitorPid ! {broker_stopped, {Ip, Port}},
    timer:cancel(TRef),
    ok.

code_change(_OldVsn, State, _Ectra) ->
    {ok, State}.



%%====================================================================
%% Internal functions
%%====================================================================

get_datetime() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:local_time(),
    DateList = [ io_lib:format("~2.10.0B", [T rem 100]) || T <- [Year, Month, Day, Hour] ],
    TimeList = [ io_lib:format("~2.10.0B", [T rem 100]) || T <- [Hour, Minute, Second] ],
    DateBin = list_to_binary(DateList),
    TimeBin = list_to_binary(TimeList),
    << DateBin/binary, ".", TimeBin/binary >>.


build_msg_id(LocalIp, ProcId, IdAcc) ->
    list_to_binary([get_datetime(), ".", LocalIp, ".", ProcId, ".", integer_to_binary(IdAcc)]).

combine_producer_message(Old, New) ->
    %% flag有默认值, 不用处理
    BeginTs =
        case New#producer_message.begin_ts of
            undefined -> Old#producer_message.begin_ts;
            BeginTsValue -> BeginTsValue
        end,
    %% end_ts有默认值, 不用处理
    %% subject 使用Subject来兜底
    Subject =
        case New#producer_message.subject of
            undefined -> Old#producer_message.subject;
            SubjectValue -> SubjectValue
        end,
    Id =
        case New#producer_message.id of
            undefined -> Old#producer_message.id;
            IdValue -> IdValue
        end,
    New#producer_message{ begin_ts = BeginTs
                        , subject  = Subject
                        , id       = Id
                        }.


handle_resp( { #header{ code = heartbeat }, _ }, State ) ->
    {noreply, State};
handle_resp( { RespHeader = #header{ code       = _RespCode
                                   , request_id = ReqId 
                                   }
             , Body
             }
           , State = #state{ request_tab = ReqTab }
           ) ->
    case ets:lookup(ReqTab, ReqId) of
        [] ->
            %% 查不到对应的request, 也不知道能干什么, 忽略吧
            pass;
        [ #request{ request_id = _RequestId
                  , header     = ReqHeader
                  , arg        = ReqArg
                  , action     = Action
                  , time       = _CreateTime
                  } ] ->
        % [{ReqId, ReqHeader, ReqArg, Action, _Time}] ->
            ets:delete(ReqTab, ReqId),
            handle_resp_body(ReqHeader, ReqArg, RespHeader, Body, Action)
    end,
    {noreply, State}.


handle_resp_body( #header{ code = send_message}
                , ReqMsgList
                , #header{ code = success }
                , Body
                , {reply, From}
                ) ->
    {ok, RespMsgList} = qmqec_protocol:parse_producer_publish_response_body(Body),
    ReqMsgDict = dict:from_list([ {Id, ReqMsg} || ReqMsg = #producer_message{id = Id} <- ReqMsgList ]),
    {LeftReqMsgDict, RespFailedMsgList} = lists:foldl(
        fun( RespMsg = #producer_message_result{ id     = RespId
                                               , code   = RespCode
                                               , remark = _Remark
                                               }
           , {Dict, FailedList}
           ) ->
            case RespCode of
                success -> { dict:erase(RespId, Dict), FailedList };
                _ ->
                    {ReqMsg, NewDict} = dict:take(RespId, Dict),
                    {NewDict, [{ReqMsg, RespMsg} | FailedList]}
            end
        end,
        {ReqMsgDict, []},
        RespMsgList
    ),
    %% 对于LeftDict中的消息,都按照失败来处理,且code都为unknown
    OtherRespFailedMsgList =
        [ { ReqMsg
          , #producer_message_result{ id     = ReqMsgId
                                    , code   = unknown
                                    , remark = <<>>
                                    }
          } || {ReqMsgId, ReqMsg} <- dict:to_list(LeftReqMsgDict)
        ],
    gen_server:reply(From, {ok, RespFailedMsgList ++ OtherRespFailedMsgList});

handle_resp_body( #header{ code = send_message}
                , ReqMsgList
                , #header{ }  %% 非success的其他值,认为所有msg都发送失败
                , _Body
                , {reply, From}
                ) ->
    FailedMsgList =
        [ { ReqMsg
          , #producer_message_result{ id     = ReqMsgId
                                    , code   = unknown
                                    , remark = <<>>
                                    }
          } || ReqMsg = #producer_message{id = ReqMsgId} <- ReqMsgList
        ],
    gen_server:reply(From, {ok, FailedMsgList}).


delete_timeout(Tab, Timeout) ->
    ets:select_delete(
        Tab,
        [{ #request{ time = '$1'
                   , _ = '_'
                   }
         , [{'<', '$1', Timeout}]
         , [true]
        }]
    ).
    
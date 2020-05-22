-module(qmqec_meta_tcp_server).
-behaviour(gen_server).

%% API
-export([ start/4
        , get_broker_list/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, { addr                      %% {ip(), port()}
               , sock
               , broker_list_tab
               , request_tab
               , monitor
               , local_ip                  %% 本地ip,用来填充client_id用的
               , request_id_counter = 0    %% request_id的计数器
               , tref                      %% 定时器,用来看看自己多久没有被访问了
                                           %%     如果自己长时间(1分钟)没有被访问, 就主动关闭
               , last_time
               }).

-include("qmqec.hrl").

%%====================================================================
%% API functions
%%====================================================================

start(Ip, Port, Monitor, BrokerListTab) ->
    gen_server:start(?MODULE, {Ip, Port, Monitor, BrokerListTab}, []).


get_broker_list(Pid, Request = #meta_request{}) ->
    gen_server:call(Pid, {get_broker_list, Request}).


refresh(Pid, Request) ->
    gen_server:cast(Pid, {refresh, Request}).



%%====================================================================
%% gen_server callbacks
%%====================================================================

init( _Arg = {Ip, Port, Monitor, BrokerListTab} ) ->
    % io:format("~p init arg: ~p~n", [?MODULE, _Arg]),
    {ok, Sock} = gen_tcp:connect(Ip, Port, [ binary
                                           , {active, true}
                                           , {packet, 4}
                                           ]),
    RequestTab = ets:new(request_tab, [set]),
    LocalIp = qmqec_utils:local_ip_bin(),
    TRef = timer:send_interval(60000, tick),
    {ok, #state{ addr               = {Ip, Port}
               , sock               = Sock
               , broker_list_tab    = BrokerListTab
               , request_tab        = RequestTab
               , monitor            = Monitor
               , local_ip           = LocalIp
               , tref               = TRef
               , last_time          = qmqec_utils:timestamp()
               }}.


handle_call( { get_broker_list
             , MetaRequest = #meta_request{ subject          = Subject
                                          , client_type_code = Type
                                          }
             }
           , _From
           , State = #state{ sock               = Sock
                           , broker_list_tab    = BrokerListTab
                           , request_tab        = RequestTab
                           , request_id_counter = Counter
                           }
           ) ->
    %% 能够复用meta response的key是{subject, client_type_code}
    case ets:lookup(BrokerListTab, {Subject, Type}) of
        [] ->
            %% 没有, 需要去请求meta
            %% 此时 request_type 为 register
            DefaultMetaRequest = build_defaut_meta_request(State),
            %% 修正的Request应该存储备用
            Request = combine_meta_request( DefaultMetaRequest
                                          , MetaRequest#meta_request{ request_type = register }
                                          ),
            Header = #header{ code          = client_register
                            , request_id    = Counter
                            , flag          = request
                            , request_code  = client_register
                            },
            % io:format("meta_request: ~p~n", [Request]),

            SendData = qmqec_protocol:build_meta_request(Header, Request),
            % io:format("meta_request send_data:~p~n", [SendData]),
            ok = gen_tcp:send(Sock, SendData),
            ets:insert(RequestTab, {Counter, Header, Request, {reply, _From}}),
            {noreply, State#state{ request_id_counter = Counter + 1
                                 , last_time = qmqec_utils:timestamp()
                                 }};

        [{{Subject, Type}, _OldRequest, BrokerList, _Timestamp}] ->
            %% 用Timestamp判断一下这个Subject有多久没有refresh了,
            %% 如果超过1分钟, 就默默的刷一次
            Now = qmqec_utils:timestamp(),
            case (Now - _Timestamp) of
                N when N > 60000 ->
                    refresh( self()
                           , combine_meta_request( _OldRequest
                                                 , MetaRequest#meta_request{ request_type = heartbeat}
                                                 )
                           );
                _ ->
                    pass
            end,
            { reply
            , BrokerList
            , State#state{ last_time = qmqec_utils:timestamp() }
            }
    end.


handle_cast( { refresh
             , Request = #meta_request{}
             }
           , State = #state{ sock                = Sock
                           , request_tab         = RequestTab
                           , request_id_counter  = Counter
                           }
           ) ->
    Header = #header{ code          = client_register
                    , request_id    = Counter
                    , flag          = request
                    , request_code  = client_register
                    },
    SendData = qmqec_protocol:build_meta_request(Header, Request),
    ok = gen_tcp:send(Sock, SendData),
    ets:insert(RequestTab, {Counter, Header, Request, refresh}),
    {noreply, State#state{request_id_counter = Counter + 1}}.

handle_info( {tcp, Sock, Data}
           , #state{ sock              = Sock
                   , broker_list_tab   = BrokerListTab
                   , request_tab       = RequestTab
                   } = State
           ) ->
    % io:format("~p handle_info, addr:~p, data:~p~n", [?MODULE, State#state.addr, Data]),
    { ok
    , _Header = #header{ code          = Code
                       , version       = _Version
                       , request_id    = RequestId
                       , flag          = _Flag
                       , request_code  = _RequestCode
                       }
    , Body
    } = qmqec_protocol:parse_data(Data),
    case Code of
        heartbeat ->
            pass;
        success -> 
            %% 剩下应该只有broker list的响应了, 具体code找有空找余昭辉确认
            %% 余昭辉确认: meta server的响应只有一个状态吗,就是success
            %% 这里应该能从RequestTab中查到对应的请求,
            %% 要是查不到就日了鬼了
            [{_, _RequestHeader, Request, Action}] = ets:lookup(RequestTab, RequestId),
            %% 清理request tab
            ets:delete(RequestTab, RequestId),
            { ok
            , #meta_response{ subject          = Subject
                            , client_type_code = ClientType
                            , broker_list      = BrokerList
                            }
            } = qmqec_protocol:parse_meta_response_body(Body),
            case BrokerList of
                [] ->
                    %% 这里把返回broker_list为[]的情况屏蔽了
                    pass;
                _  ->
                    %% 空闲的refresh是基于这样的理由的： 
                    %%      比如现在消息量很高,我们可能会给这个主题加一台server,
                    %%      如果你一直发消息是成功的，然后又不主动refresh,
                    %%      那你一直不会发现这台新的server
                    %% 基于上述理由,写MetaTab时要附加写上insert的时间戳, 这样在下次
                    %% 撞到这个Subject时, 可以判断是不是需要refresh了
                    ets:insert(BrokerListTab, { {Subject, ClientType}
                                              , Request
                                              , BrokerList
                                              , qmqec_utils:timestamp()
                                              })
            end,
            case Action of
                {reply, From} -> gen_server:reply(From, BrokerList);
                refresh       -> pass
            end
    end,
    {noreply, State};

handle_info(tick, State = #state{last_time = Last}) ->
    Now = qmqec_utils:timestamp(),
    case Now - Last of
        N when N >= 60000 ->
            {stop, normal, State};
        _ ->
            {noreply, State}
    end.


terminate( _Reason
         , #state{ addr    = Addr
                 , tref    = TRef
                 , monitor = Monitor
                 }
         ) ->
    timer:cancel(TRef),
    Monitor ! {meta_tcp_stopped, Addr},
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

build_defaut_meta_request( #state{ local_ip = LocalIp } ) ->
    AppCode = <<"qmqec">>,
    ClientId = << LocalIp/binary, "@", AppCode/binary >>,
    #meta_request{ app_code = AppCode
                 , client_id = ClientId
                 }.


combine_meta_request(Old, New) ->
    %% subject和client_type_code必须一致才能结合, 所以这两个字段不处理
    AppCode =
        case New#meta_request.app_code of
            undefined -> Old#meta_request.app_code;
            AppCodeValue -> AppCodeValue
        end,
    %% request_type按照New的来,不处理
    ClientId =
        case New#meta_request.client_id of
            undefined -> Old#meta_request.client_id;
            ClientIdValue -> ClientIdValue
        end,
    Room =
        case New#meta_request.room of
            undefined -> Old#meta_request.room;
            RoomValue -> RoomValue
        end,
    Env =
        case New#meta_request.env of
            undefined -> Old#meta_request.env;
            EnvValue -> EnvValue
        end,
    New#meta_request{ app_code  = AppCode
                    , client_id = ClientId
                    , room      = Room
                    , env       = Env
                    }.
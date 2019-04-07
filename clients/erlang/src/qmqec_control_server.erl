-module(qmqec_control_server).
-behaviour(gen_server).

%% API
-export([ publish/3
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(meta_http, { url                        %% 请求meta_http的url, 初始化meta_http_server需要
                   , pid                        %% meta_http_server的pid
                   }).

-record(meta_tcp, { addr                        %% meta_tcp的地址,{ip(), port()}格式
                  , pid                         %% meta_tcp_server的pid
                  , broker_list_tab             %% 保存meta_tcp响应结果的tab
                  }).

-record(broker_conn, { sup_pid                   %% broker_sup_pid
                     , broker_conn_tab           %% 保存broker连接的tab
                     }).

-record(state, { meta_http_state
               , meta_tcp_state
               , broker_conn_state
               , option
               }).

-include("qmqec.hrl").

%%====================================================================
%% API functions
%%====================================================================

publish(ControlPid, MetaRequest, MsgList) ->
    { ok
    , State = #state{ meta_tcp_state = #meta_tcp{ pid             = MetaTcpPid
                                                , broker_list_tab = _BrokerListTab
                                                },
                      option = #option{ app_code = AppCode }
                    }
    } = gen_server:call(ControlPid, get_state),
    %% option中的app_code, 要渲染到meta_request中去
    NewMetaRequest = MetaRequest#meta_request{ app_code = AppCode },
    case qmqec_meta_tcp_server:get_broker_list(MetaTcpPid, NewMetaRequest) of
        [] ->
            error;
        BrokerList ->
            RandomBrokerList = get_random_broker_list(BrokerList, [], 3),
            do_publish_with_broker_list(ControlPid, State, NewMetaRequest, MsgList, RandomBrokerList)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================


init({MetaHttpUrl, Options}) ->
    %% MetaTcpServer使用start, 因为可能会重启
    %% 在terminate方法中通知到controller
    %% MetaHttpServer, BrokerSupvisor不太会挂,用start_link
    Option = deal_options(Options, #option{}),
    Me = self(),
    BrokerListTab = ets:new( broker_list_tab, [ set, public ] ),
    %% BrokerConnTab由于存在并发lookup的操作, 所以设置可以并发读取
    BrokerConnTab = ets:new( broker_conn_tab, [ set, public, {read_concurrency, true} ] ),
    {ok, MetaHttpPid} = qmqec_meta_http_server:start_link(MetaHttpUrl, Me),
    {ok, {MetaTcpIp, MetaTcpPort}} = qmqec_meta_http_server:get_addr(MetaHttpPid),
    {ok, MetaTcpPid} = qmqec_meta_tcp_server:start(MetaTcpIp, MetaTcpPort, Me, BrokerListTab),
    {ok, BrokerSupPid} = qmqec_broker_sup:start_link(),

    {ok, #state{ meta_http_state = #meta_http{ url = MetaHttpUrl
                                             , pid = MetaHttpPid
                                             }
               , meta_tcp_state = #meta_tcp{ addr            = {MetaTcpIp, MetaTcpPort}
                                           , pid             = MetaTcpPid
                                           , broker_list_tab = BrokerListTab
                                           }
               , broker_conn_state = #broker_conn{ sup_pid         = BrokerSupPid
                                                 , broker_conn_tab = BrokerConnTab
                                                 }
               , option = Option
               }}.


handle_call( get_state
           , _From
           , State
           ) ->
    {reply, {ok, State}, State}.
    

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info( {broker_started, {Ip, Port, BrokerPid}}
           , State = #state{
                broker_conn_state = #broker_conn{
                    broker_conn_tab = BrokerConnTab
                }
             }
           ) ->
    ets:insert(BrokerConnTab, {{Ip, Port}, BrokerPid}),
    {noreply, State};
handle_info( {broker_stopped, {Ip, Port}}
           , State = #state{
                broker_conn_state = #broker_conn{
                    broker_conn_tab = BrokerConnTab
                }
             }
           ) ->
    ets:delete(BrokerConnTab, {Ip, Port}),
    {noreply, State};
handle_info( {meta_tcp_stopped, Addr}
           , State = #state{
                meta_http_state = MetaHttpState,
                meta_tcp_state  = MetaTcpState
             }
           ) ->
    case MetaTcpState#meta_tcp.addr of
        Addr ->
            %% 确认meta_tcp_server停了, 要重新启动一个
            {ok, {MetaTcpIp, MetaTcpPort}} = qmqec_meta_http_server:get_addr(MetaHttpState#meta_http.pid),
            {ok, MetaTcpPid} = qmqec_meta_tcp_server:start(MetaTcpIp, MetaTcpPort, self(), MetaTcpState#meta_tcp.broker_list_tab),
            { noreply
            , State#state{ meta_tcp_state = MetaTcpState#meta_tcp{
                    addr = {MetaTcpIp, MetaTcpPort},
                    pid  = MetaTcpPid
              }}
            };
        _OtherAddr ->
            {noreply, State}
    end;
handle_info( {meta_tcp_addr_changed, NewAddr}
           , State = #state{
                meta_tcp_state  = MetaTcpState
             }
           ) ->
    case MetaTcpState#meta_tcp.addr of
        NewAddr ->
            {noreply, State};
        _OldAddr ->
            {NewIp, NewPort} = NewAddr,
            {ok, MetaTcpPid} = qmqec_meta_tcp_server:start(NewIp, NewPort, self(), MetaTcpState#meta_tcp.broker_list_tab),
            { noreply
            , State#state{ meta_tcp_state = MetaTcpState#meta_tcp{
                    addr = NewAddr,
                    pid  = MetaTcpPid
              }}
            }
    end.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Ectra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

deal_options([], Option) ->
    Option;
deal_options([{app_code, AppCode} | Left], Option) ->
    deal_options(Left, Option#option{app_code = AppCode}).


get_random_broker_list(_, RandomBrokerList, 0) ->
    RandomBrokerList;
get_random_broker_list(BrokerList, RandomBrokerList, N) ->
    case qmqec_utils:random_select(BrokerList) of
        undefined ->
            RandomBrokerList;
        Broker ->
            get_random_broker_list( lists:delete(Broker, BrokerList)
                                  , [Broker | RandomBrokerList]
                                  , N - 1
                                  )
    end.



do_publish_with_broker_list(_, _State, _, MsgList, []) ->
    {uncompleted, MsgList};
do_publish_with_broker_list(
      ControlPid
    , State = #state{
        broker_conn_state = #broker_conn{ sup_pid         = BrokerSupPid
                                        , broker_conn_tab = BrokerConnTab
                                        }
      }
    , MetaRequest = #meta_request{}
    , MsgList
    , [Broker | LeftBroker]
) ->
    #broker{ server_ip = Ip, server_port = Port } = Broker,
    BrokerPid =
        case ets:lookup(BrokerConnTab, {Ip, Port}) of
            [] ->
                {ok, Pid} = supervisor:start_child(BrokerSupPid, [Broker, ControlPid]),
                Pid;
            [{{Ip, Port}, Pid}] ->
                Pid
        end,
    case qmqec_broker_conn_server:publish(BrokerPid, MetaRequest, MsgList) of
        {ok, []} -> ok;
        {ok, FailedMsgList} ->
            RetryMsgList = [ ReqMsg || {ReqMsg, _RespMsgResult} <- FailedMsgList ],
            do_publish_with_broker_list(ControlPid, State, MetaRequest, RetryMsgList, LeftBroker)

    end.
 

-module(qmqec_meta_http_server).
-behaviour(gen_server).

%% API
-export([ start_link/2
        , get_addr/1
        , flush_addr/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, { meta_http_url
               , meta_tcp_ip
               , meta_tcp_port
               , tref
               , monitor
               }).

-define(FLUSH_INTERVAL, 60000).


%%====================================================================
%% API functions
%%====================================================================

%% MetaHttpUrl是string类型.
%% 比如: “http://gibbon.is.a.good.boy/”
start_link(MetaHttpUrl, Monitor) ->
    gen_server:start_link(?MODULE, {MetaHttpUrl, Monitor}, []).

%% -> { ok, { ip(), port() } }
get_addr(Pid) ->
    gen_server:call(Pid, get_addr).


flush_addr(Pid) ->
    MetaHttpUrl = gen_server:call(Pid, get_url),
    case request_meta_tcp_addr(MetaHttpUrl) of
        {ok, {Ip, Port}} ->
            {ok, {Ip, Port}} = gen_server:call(Pid, {update_addr, {Ip, Port}});
        Error ->
            Error
    end.


%%====================================================================
%% gen_server callbacks
%%====================================================================

init({MetaHttpUrl, Monitor}) ->
    %% enabel httpc
    ok = application:ensure_started(inets),
    %% 同步获取meta地址,成功才继续哦
    {ok, {MetaIp, MetaPort}} = request_meta_tcp_addr(MetaHttpUrl),
    %% 60秒刷新一次meta server地址
    {ok, TRef} = timer:send_interval(?FLUSH_INTERVAL, async_flush_addr),
    {ok, #state{ meta_http_url = MetaHttpUrl
               , meta_tcp_ip   = MetaIp
               , meta_tcp_port = MetaPort
               , tref          = TRef
               , monitor       = Monitor
               }}.


handle_call( get_addr
           , _From
           , #state{ meta_tcp_ip    = Ip
                   , meta_tcp_port  = Port
                   } = State
           ) ->
    {reply, {ok, {Ip, Port}}, State};
handle_call( get_url
           , _From
           , #state{ meta_http_url = Url } = State
           ) ->
    {reply, Url, State};
handle_call( {update_addr, {Ip, Port}}
           , _From
           , #state{tref = TRef} = State
           ) ->
    {ok, cancel} = timer:cancel(TRef),
    {ok, NewTRef} = timer:send_interval(?FLUSH_INTERVAL, async_flush_addr),
    { reply
    , {ok, {Ip, Port}}
    , State#state{ meta_tcp_ip    = Ip
                 , meta_tcp_port  = Port
                 , tref           = NewTRef
                 }
    }.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info( async_flush_addr
           , #state{ meta_http_url = MetaHttpUrl
                   , meta_tcp_ip   = Ip
                   , meta_tcp_port = Port
                   , monitor       = Monitor
                   } = State
           ) ->
    Pid = self(),
    spawn(
        fun() ->
            case request_meta_tcp_addr(MetaHttpUrl) of
                {ok, {Ip, Port}} ->
                    pass;
                {ok, {NewIp, NewPort}} ->
                    gen_server:call(Pid, {update_addr, {NewIp, NewPort}}),
                    Monitor ! {meta_tcp_addr_changed, {NewIp, NewPort}};
                {error, _} ->
                    pass
            end
        end
    ),
    {noreply, State}.

terminate(_Reason, _State) ->
    timer:cancel(_State#state.tref),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% 同步获取meta server地址
request_meta_tcp_addr(MetaHttpUrl) ->
    Request =
        { MetaHttpUrl
        , []
        },
    case httpc:request(get, Request, [{timeout, 3000}], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Resp}} ->
            [IpBin, PortBin] = string:split(Resp, <<":">>),
            {ok, {qmqec_utils:binary_to_ip(IpBin), binary_to_integer(PortBin)}};
        {ok, {{_, Code, _}, _, _}} ->
            {error, {http_code, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

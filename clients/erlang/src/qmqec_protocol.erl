-module(qmqec_protocol).


%% API
-export([ parse_data/1
        , build_meta_request/2
        , parse_meta_response_body/1
        , build_producer_request/2
        , parse_producer_publish_response_body/1
        , parse_kv_binary/1
        ]).


-include("qmqec.hrl").


%%====================================================================
%% API functions
%%====================================================================

parse_data( << HeaderLen:16/big
             , Header:HeaderLen/binary
             , Body/binary >> = _Data
          ) ->
    %% <<222,193,10,222, 0,51, 0,3,     0,0,0,0,     0,0,0,1,   0,35>>
    %%   magic_number    code  version  request_id   flag       quest_code
    << 16#DE, 16#C1, 16#0A, 16#DE
     , Code:16/big
     , Version:16/big
     , RequestId:32/big
     , Flag:32/big
     , RequestCode:16/big >> = Header,
    { ok
    , #header{ code          = parse_code(Code)
             , version       = Version
             , request_id    = RequestId
             , flag          = parse_flag(Flag)
             , request_code  = RequestCode
             }
    , Body
    };
parse_data(_) ->
    error.


build_meta_request( Header = #header{}
                  , Body = #meta_request{}
                  ) ->
    HeaderBin = build_header(Header),
    BodyBin = build_meta_request_body(Body),
    % DataLen = size(HeaderBin) + size(BodyBin),
    % << DataLen:32/big, HeaderBin/binary, BodyBin/binary >>.
    << HeaderBin/binary, BodyBin/binary >>.


parse_meta_response_body(
    << SubjectLen:16/big
     , Subject:SubjectLen/binary
     , ClientTypeCode
     , ServersSize:16/big
     , Brokers/binary >>
     ) ->
    CtcAtom =
        case ClientTypeCode of
            1 -> producer;
            2 -> consumer;
            4 -> delay_producer
        end,
    Fun =
        fun( BrokerGroupName, ServerIp, ServerRoom, UpdateTs, State ) ->
            [IpBin, PortBin] = string:split(ServerIp, <<":">>),
            Ip = qmqec_utils:binary_to_ip(IpBin),
            StateAtom =
                case State of
                  1 -> 'RW';
                  2 -> 'R';
                  3 -> 'W';
                  4 -> 'NRW'
                end,
            #broker{ broker_group_name = BrokerGroupName
                   , server_ip         = Ip
                   , server_port       = binary_to_integer(PortBin)
                   , server_room       = ServerRoom
                   , update_ts         = UpdateTs
                   , state             = StateAtom
                   }
        end,
    BrokerList =
        [  Fun(BrokerGroupName, ServerIp, ServerRoom, UpdateTs, State)
        || << BrokerGroupNameLen:16/big
            , BrokerGroupName:BrokerGroupNameLen/binary
            , ServerIpLen:16/big
            , ServerIp:ServerIpLen/binary
            , ServerRoomLen:16/big
            , ServerRoom:ServerRoomLen/binary
            , UpdateTs:64/big
            , State >>
        <= Brokers     
        ],
    ServersSize = length(BrokerList),
    {ok, #meta_response{ subject          = Subject
                       , client_type_code = CtcAtom
                       , broker_list      = BrokerList
                       }}.





build_producer_request( Header = #header{}
                      , MsgList
                      ) ->
    HeaderBin = build_header(Header),
    BodyBin = build_producer_publish_request_body(MsgList),
    % DataLen = size(HeaderBin) + size(BodyBin),
    % << DataLen:32/big, HeaderBin/binary, BodyBin/binary >>.
    << HeaderBin/binary, BodyBin/binary >>.


parse_producer_publish_response_body(Bin) ->
    { ok
    , [  #producer_message_result{ id     = Id
                                 , code   = parse_producer_response_code(Code)
                                 , remark = Remark
                                 }
      || << IdLen:16/big
          , Id:IdLen/binary
          , Code:32/big
          , RemarkLen:16/big
          , Remark:RemarkLen/binary >>
      <= Bin
      ]
    }.



%%====================================================================
%% Internal functions
%%====================================================================



build_header( #header{ code             = Code
                     , version          = Version
                     , request_id       = RequestId
                     , flag             = Flag
                     , request_code     = RequestCode
                     }
            ) ->
    Content = << 16#DE, 16#C1, 16#0A, 16#de
               , (build_code(Code)):16/big
               , Version:16/big
               , RequestId:32/big
               , (build_flag(Flag)):32/big
               , (build_code(RequestCode)):16/big >>,
    << (size(Content)):16/big, Content/binary >>.



parse_code(0)   -> success;                      %% 响应成功
parse_code(3)   -> unknown_code;                 %% 未知状态
parse_code(4)   -> no_message;                   %% 无消息(consumer拉消息)
parse_code(10)  -> send_message;                 %% 发送消息
parse_code(11)  -> pull_message;                 %% 拉消息
parse_code(12)  -> ack_request;                  %% ack请求
parse_code(20)  -> sync_request;                 %% 同步请求
parse_code(30)  -> broker_register;              %% 注册broker
parse_code(35)  -> client_register;              %% 注册client
parse_code(40)  -> broker_acquire_meta;          %% 获取broker meta
parse_code(51)  -> broker_error;                 %% broker异常(强制路由)
parse_code(52)  -> broker_reject;                %% broker拒绝此消息(强制路由)
parse_code(100) -> heartbeat.                    %% 心跳包



build_code(send_message)         -> 10;          %% 发送消息
build_code(pull_message)         -> 11;          %% 拉消息
build_code(ack_request)          -> 12;          %% ack请求
build_code(sync_request)         -> 20;          %% 同步请求
build_code(broker_register)      -> 30;          %% 注册broker
build_code(client_register)      -> 35;          %% 注册client
build_code(broker_acquire_meta)  -> 40;          %% 获取broker meta
build_code(heartbeat)            -> 100.         %% 心跳


parse_flag(1)        -> response;                %% 响应
parse_flag(0)        -> request.                 %% 请求

build_flag(request)  -> 0;                       %% 请求
build_flag(response) -> 1.                       %% 响应


build_message_flag(high)  -> 0;                  %% 高可靠
build_message_flag(low)   -> 1.                  %% 低可靠



build_meta_request_body(
    #meta_request{ subject           = Subject
                 , client_type_code  = ClientTypeCode
                 , app_code          = AppCode
                 , request_type      = RequestType
                 , client_id         = ClientId
                 , room              = Room
                 , env               = Env
                 }) ->
    Fun =
        fun
            ({<<"room">>, undefined}, {Bin, Count}) ->
                {Bin, Count};
            ({<<"env">>, undefined}, {Bin, Count}) ->
                {Bin, Count};
            ({<<"clientTypeCode">> = KBin, VAtom}, {Bin, Count}) ->
                %% 1 - producer, 2 - consumer, 4 - delay_producer
                VBin =
                    case VAtom of
                        producer       -> <<"1">>;
                        consumer       -> <<"2">>;
                        delay_producer -> <<"4">>
                    end,
                { << (size(KBin)):16/big, KBin/binary, (size(VBin)):16/big, VBin/binary, Bin/binary >>
                , Count + 1
                };
            ({<<"requestType">> = KBin, VAtom}, {Bin, Count}) ->
                %% ( 1 - 注册, 2 - 心跳, 只有第一次的时候使用1 )
                VBin =
                    case VAtom of
                        register       -> <<"1">>;
                        heartbeat      -> <<"2">>
                    end,
                { << (size(KBin)):16/big, KBin/binary, (size(VBin)):16/big, VBin/binary, Bin/binary >>
                , Count + 1
                };
            ({K, V}, {Bin, Count}) when is_binary(K), is_binary(V) ->
                { << (size(K)):16/big, K/binary, (size(V)):16/big, V/binary, Bin/binary >>
                , Count + 1
                }
        end,
    {BodyContent, Count} =
        lists:foldr( Fun
                   , { << >>, 0 }
                   , [ {<<"subject">>,         Subject}
                     , {<<"clientTypeCode">>,  ClientTypeCode}
                     , {<<"appCode">>,         AppCode}
                     , {<<"requestType">>,     RequestType}
                     , {<<"consumerId">>,      ClientId}
                     , {<<"room">>,            Room}
                     , {<<"env">>,             Env}
                     ]
                   ),
    <<Count:16/big, BodyContent/binary>>.


build_producer_publish_request_body(MsgList) ->
    Fun =
        fun( #producer_message{ flag         = Flag
                              , begin_ts     = Begin
                              , end_ts       = End
                              , subject      = Subject
                              , id           = Id
                              , kv_list      = KVList
                              } = _Msg
           , Bin
           ) ->
            % io:format("build_publish_msg: ~p~n", [_Msg]),
            FlagInt = build_message_flag(Flag),
            KVBin = build_kv_binary(KVList),
            << FlagInt, Begin:64/big, End:64/big
             , (size(Subject)):16/big, Subject/binary
             , (size(Id)):16/big, Id/binary
             , (size(KVBin)):32/big, KVBin/binary
             , Bin/binary >>
        end,
    lists:foldr(Fun, << >>, MsgList).


parse_producer_response_code(0) -> success;
parse_producer_response_code(1) -> busy;
parse_producer_response_code(2) -> duplicated;
parse_producer_response_code(3) -> unregistered;
parse_producer_response_code(4) -> slave;
parse_producer_response_code(5) -> intercepted;
parse_producer_response_code(6) -> unstored.
%% 7 unknown错误是qmqec另外添加的, 不在原生协议中,
%% 是为了表示publish的相应中,result列表个数不够的情况.

             

parse_kv_binary(Bin) when is_binary(Bin) ->
    [ {KBin, VBin}
    || << KBinLen:16/big
        , KBin:KBinLen/binary
        , VBinLen:16/big
        , VBin:VBinLen/binary >> <= Bin
    ].

build_kv_binary(KVList) ->
    BinList =
        [ << (size(K)):16/big, K/binary
           , (size(V)):16/big, V/binary >>
        || {K, V} <- KVList
        ],
    list_to_binary(BinList).





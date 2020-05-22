%% option
-record(option, { app_code = <<"qmqec">>
                }).



%% header
-record(header, { code                       %% atom:
                                             %%     | atom                | integer | remark                     |
                                             %%     | :------------------ | :------ | :------------------------- |
                                             %%     | success             | 0       | 响应成功                   |
                                             %%     | unknown_code        | 3       | 未知状态                   |
                                             %%     | no_message          | 4       | 无消息(consumer拉消息)     |
                                             %%     | send_message        | 10      | 发送消息                   |
                                             %%     | pull_message        | 11      | 拉消息                     |
                                             %%     | ack_request         | 12      | ack请求                    |
                                             %%     | sync_request        | 20      | 同步请求                   |
                                             %%     | broker_register     | 30      | 注册broker                 |
                                             %%     | client_register     | 35      | 注册client                 |
                                             %%     | broker_acquire_meta | 40      | 获取broker meta            |
                                             %%     | broker_error        | 51      | broker异常(强制路由)       |
                                             %%     | broker_reject       | 52      | broker拒绝此消息(强制路由) |
                                             %%     | heartbeat           | 100     | 心跳包                     |
                , version = 3                %% integer
                , request_id                 %% integer
                , flag                       %% atom: request | response
                , request_code               %% integer
                }).

%% 请求meta的body结构
-record(meta_request, { subject              %% binary: 消息的主题, 必填
                      , client_type_code     %% atom: 生产消费者, producer | consumer | delay_producer
                                             %%       ( 1 - producer, 2 - consumer, 4 - delay_producer )
                                             %%       必填
                      , app_code             %% binary: 必填
                      , request_type         %% atom: register | heartbeat
                                             %%       ( 1 - 注册, 2 - 心跳, 只有第一次的时候使用1 )
                                             %%       必填
                      , client_id            %% binary: 客户端唯一编号 (可以使用ip@appcode的形式), 必填
                      , room                 %% binary: 机房, 非必填
                      , env                  %% binary: 环境(prod, uat, fat), 非必填
                      }).


%% meta response
-record(meta_response, { subject             %% binary: 消息的主题, 可以理解为topic. 
                                             %% 和 #meta_request.subject 值一致
                       , client_type_code    %% atom: 和 #meta_request.client_type_code 值一致
                       , broker_list         %% #broker的列表
                       }).




%% broker info
-record(broker, { broker_group_name          %% binary: qmq server组名称, 每一组的名称都不相同, server的ip会发生变化, 但是组名称不会变
                , server_ip                  %% {0..255, 0..255, 0..255, 0..255}
                , server_port                %% integer: 端口号
                , server_room                %% binary: 机房
                , update_ts                  %% integer: 时间戳
                , state                      %% atom: 'RW': 可读可写 |'R': 只读 |'W': 只写 |'NRW': 不可用
                }).


%% producer publish
-record( producer_message
       , { flag = low                        %% atom: high | low, 可靠性, 0 - 高可靠, 1 - 低可靠
         , begin_ts                          %% integer: 创建时间戳
         , end_ts = 0                        %% integer: 过期时间戳, 0表示永不过期
         , subject                           %% binary: 消息主题
         , id                                %% binary: 消息id
         , kv_list = []                      %% {K, V}列表, K,V都是binary类型
         }
       ).


% producer publish response
-record( producer_message_result
       , { id                                %% binary: 同 #producer.id
         , code                              %% atom:
                                             %%     | atom          | integer | remark                     |
                                             %%     | :------------ | :------ | :------------------------- |
                                             %%     | success       | 0       | 成功(只有该状态是认为发送成功,其他状态都是失败)
                                             %%     | busy          | 1       | 繁忙
                                             %%     | duplicated    | 2       | 重复投递的消息
                                             %%     | unregistered  | 3       | 消息主题未注册
                                             %%     | slave         | 4       | 消息投递到slave broker了
                                             %%     | intercepted   | 5       | 被broker拦截了
                                             %%     | unstored      | 6       | 存储失败,但broker成功接收
                                             %%     | unknown       | 7       | 未知错误
         , remark                            %% binary
         }
       ).

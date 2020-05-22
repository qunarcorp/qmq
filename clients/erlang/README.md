qmqec
=====

An OTP library

Build
-----

    $ rebar3 compile


cd _build/default/lib/qmqec
erl -pa ebin -s qmqec_test


Usage
-----

```erlang

{ok, Pid} = qmqec:start_link( "http://url-to-meta-http-server"
                            , [{app_code, <<"gibbon">>}]
                            ),
case qmqec:publish( Pid
                  , <<"topic">>
                  , [ {<<"key">>, <<"value">>}
                    , {<<"info">>, <<"gibbon is a good boy">>}
                    ]
                  ) of
    ok ->
        ok;
    {uncompleted, _FailedMsgList} ->
        io:format("uncompleted~n", []);
    error ->
        io:format("error~n", [])
end,

```


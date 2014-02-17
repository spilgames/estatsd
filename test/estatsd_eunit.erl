-module(estatsd_eunit).

-include_lib("eunit/include/eunit.hrl").

%% ====================================================================
%% Helper functions for setup/cleanup
%% ====================================================================

setup() ->
    error_logger:tty(false),
    Port = 2449,
    FlushInterval = 1000,
	ok = application:set_env(estatsd, vm_metrics, false),
	ok = application:set_env(estatsd, graphite_port, Port),
	ok = application:set_env(estatsd, flush_interval, FlushInterval),
	estatsd_receiver:start(Port),
    application:start(estatsd),
    [fun() -> application:stop(estatsd) end].

cleanup(CleaningFuns) ->
	lists:foreach(fun (F) -> F() end, CleaningFuns).

%% ====================================================================
%% Tests
%% ====================================================================

application_test_() ->
    {setup,
        fun setup/0,
        fun cleanup/1,
        fun (_) ->
            [
                ?_assertEqual(ok, estatsd:increment("key1")),
                ?_assertEqual(ok, estatsd:increment("key2")),
                ?_assertEqual(ok, estatsd:increment("key2")),
                ?_assertEqual(ok, estatsd:timing("key3", 1.0)),
                ?_assertEqual(ok, estatsd:timing("key3", 2.0)),
                ?_assertEqual(ok, timer:sleep(2000)),

                ?_assertMatch({ok, [
						{"stats.timers.key3.count","2",_},
                        {"stats.timers.key3.lower","1",_},
                        {"stats.timers.key3.upper_90","2",_},
                        {"stats.timers.key3.upper","2",_},
                        {"stats.timers.key3.mean","1.5",_},
                		{"stats.key2", "2.0" ,_},
                		{"stats.key1", "1.0" ,_}
                	]},
                	estatsd_receiver:get_stats())
            ]
        end
    }.

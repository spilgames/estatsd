-module(estatsd_eunit).

-include_lib("eunit/include/eunit.hrl").

%% ====================================================================
%% Helper functions for setup/cleanup
%% ====================================================================

setup_no_vmstats() ->
    error_logger:tty(false),
    Port = 2449,
    FlushInterval = 1000,
	ok = application:set_env(estatsd, vm_metrics, false),
	ok = application:set_env(estatsd, graphite, {"127.0.0.1", Port}),
	ok = application:set_env(estatsd, flush_interval, FlushInterval),
	estatsd_receiver:start(Port),
    ok = application:start(estatsd),
    [fun() -> estatsd_receiver:stop() end, fun() -> application:stop(estatsd) end].

setup_vmstats() ->
    error_logger:tty(false),
    Port = 2449,
    FlushInterval = 1000,
    ok = application:set_env(estatsd, vm_statistics, [{additional, [scheduler_wall_time]}]),
    ok = application:set_env(estatsd, vm_memory, [{disabled, [ets]}]),
    ok = application:set_env(estatsd, vm_metrics, true),
	ok = application:set_env(estatsd, graphite, {"127.0.0.1", Port}),
    ok = application:set_env(estatsd, flush_interval, FlushInterval),
    estatsd_receiver:start(Port),
    ok = application:start(estatsd),
    [fun() -> estatsd_receiver:stop() end, fun() -> application:stop(estatsd) end].

cleanup(CleaningFuns) ->
	lists:foreach(fun (F) -> F() end, CleaningFuns).

%% ====================================================================
%% Tests
%% ====================================================================

application_novmstats_test_() ->
    {setup,
        fun setup_no_vmstats/0,
        fun cleanup/1,
        fun (_) ->
            [
                ?_assertEqual(ok, estatsd:increment("key0", 2.5, 1)),
                ?_assertEqual(ok, estatsd:increment("key1")),
                ?_assertEqual(ok, estatsd:increment("key1")),
                ?_assertEqual(ok, estatsd:increment("key1")),
                ?_assertEqual(ok, estatsd:increment("key1",1)),
                ?_assertEqual(ok, estatsd:increment("key1",1,1)),
                ?_assertEqual(ok, estatsd:decrement("key1")),
                ?_assertEqual(ok, estatsd:decrement("key1",1)),
                ?_assertEqual(ok, estatsd:decrement("key1",1,1)),
                ?_assertEqual(ok, estatsd:increment("key2")),
                ?_assertEqual(ok, estatsd:timing("key3", 1.0)),
                ?_assertEqual(ok, estatsd:timing("key3", 3.0)),
                ?_assertEqual(ok, estatsd:timing("key3", 2)),
                ?_assertEqual(ok, estatsd:timing("key4", erlang:now())),
                ?_assertEqual(ok, estatsd:gauge("key5", 2)),
                ?_assertEqual(ok, estatsd:gauge("key5", 4)),
                ?_assertEqual(ok, timer:sleep(2000)),

                %%?_assertMatch(ok, ?debugFmt("~p", [estatsd_receiver:get_stats()])),
                ?_assertMatch([
                        {"stats.gauges.key5","2",_},
                        {"stats.gauges.key5","4",_},
                        {"stats.key0", _, _}, %% "2.5"
                        {"stats.key1", _, _}, %% "2.0"
                        {"stats.key2", _, _}, %% "1.0"
                        {"stats.timers.key3.count","3",_},
                        {"stats.timers.key3.lower","1",_},
                        {"stats.timers.key3.mean", _, _}, %% "2.0"
                        {"stats.timers.key3.upper","3",_},
                        {"stats.timers.key3.upper_90","3",_},
                        {"stats.timers.key4.count","1",_},
                        {"stats.timers.key4.lower","0",_},
                        {"stats.timers.key4.mean", _, _}, %% "0.0"
                        {"stats.timers.key4.upper","0",_},
                        {"stats.timers.key4.upper_90","0",_}
                    ],
                    begin
                        {ok, R} = estatsd_receiver:get_stats(),
                        lists:sort(R)
                    end)
            ]
        end
    }.

application_vmstats_test_() ->
    {setup,
        fun setup_vmstats/0,
        fun cleanup/1,
        fun (_) ->
            [
                % wait till after the flush time
                ?_assertEqual(ok, timer:sleep(1500)),
                %?_assertMatch(ok, ?debugFmt("~p", [estatsd_receiver:get_stats()])),
                ?_assertMatch({ok,[
                    {"stats.erlangvm.memory.total.nonode.nohost",_,_},
                    {"stats.erlangvm.memory.processes.nonode.nohost",_,_},
                    {"stats.erlangvm.memory.binary.nonode.nohost",_,_},
                    {"stats.erlangvm.memory.atom.nonode.nohost",_,_},
                    {"stats.erlangvm.process_count.nonode.nohost",_,_},
                    {"stats.erlangvm.reductions.nonode.nohost",_,_},
                    {"stats.erlangvm.run_queue.nonode.nohost",_,_},
                    {"stats.erlangvm.scheduler_wall_time.scheduler.1.nonode.nohost",_, _},
                    {"stats.erlangvm.scheduler_wall_time.scheduler.2.nonode.nohost",_, _}
                    | _ % some other schedulers, but depends on system how many cores
                ]}, estatsd_receiver:get_stats())
            ]
        end
    }.

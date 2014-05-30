-module(estatsd_benchmark).

-export([benchmark/2]).

%% @doc CountKeys = how many unique keys
%% CountRequests = how many unique requests
-spec benchmark(integer(), integer()) -> ok.
benchmark(CountKeys, CountRequests) ->
	Port = 2993,
	FlushInterval = 4000,
	estatsd_receiver:start(Port),
	application:set_env(estatsd, vm_metrics, true),
	application:set_env(estatsd, graphite, {"127.0.0.1", Port}),
	application:set_env(estatsd, flush_interval, FlushInterval),
	ok = application:start(estatsd),
	Keys = [ lists:flatten(io_lib:format("dummmy.key.~p.demo", [Index])) || Index <- lists:seq(0,CountKeys-1)],
	Before = get_reductions(),
	[ estatsd:increment(lists:nth((Counter rem CountKeys)+1, Keys)) || Counter <- lists:seq(1, CountRequests)],
	%[ estatsd:timing(lists:nth((Counter rem CountKeys)+1, Keys), 1) || Counter <- lists:seq(1, CountRequests)],
	timer:sleep(FlushInterval + 500),
	After = get_reductions(),
	{ok, Stats} = estatsd_receiver:get_stats(),
	io:format("Stats: ~p\n",[Stats]),
	io:format("Reductions used: ~p\n",[After-Before]),
	ok = application:stop(estatsd),
	ok.

get_reductions() ->
	{TotalReductions, _} = erlang:statistics(exact_reductions),
    TotalReductions.

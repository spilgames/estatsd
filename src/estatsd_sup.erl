-module(estatsd_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1, start_link/3]).

%% Supervisor callbacks
-export([init/1]).

%% Flush once every 10 seconds by default.
-define(FLUSH_INTERVAL, appvar(flush_interval, 10000)).
-define(GRAPHITE_HOST,  appvar(graphite_host,  "127.0.0.1")).
-define(GRAPHITE_PORT,  appvar(graphite_port,  2003)).
%% Toggle VM stats on (default) or off.
-define(VM_METRICS,     appvar(vm_metrics,     true)).


%% ===================================================================
%% API functions
%% ===================================================================


start_link() ->
    start_link( ?FLUSH_INTERVAL, ?GRAPHITE_HOST, ?GRAPHITE_PORT, ?VM_METRICS).

start_link(FlushIntervalMs) ->
    start_link( FlushIntervalMs, ?GRAPHITE_HOST, ?GRAPHITE_PORT, ?VM_METRICS).

start_link(FlushIntervalMs, GraphiteHost, GraphitePort) ->
    start_link( FlushIntervalMs, GraphiteHost, GraphitePort, ?VM_METRICS).

start_link(FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics) ->
    supervisor:start_link({local, ?MODULE},
                          ?MODULE,
                          [FlushIntervalMs, GraphiteHost, GraphitePort,
                          {VmMetrics, get_all_stats()}]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([FlushIntervalMs, GraphiteHost, GraphitePort, {VmMetrics, UsedStats}]) ->
    Children = [
        {estatsd_server,
         {estatsd_server, start_link,
             [FlushIntervalMs, GraphiteHost, GraphitePort, {VmMetrics, UsedStats}]},
         permanent, 5000, worker, [estatsd_server]}
    ],
    {ok, { {one_for_one, 10000, 10}, Children} }.


%% ===================================================================
%% Internal
%% ===================================================================

appvar(K, Def) ->
    case application:get_env(estatsd, K) of
        {ok, Val} -> Val;
        undefined -> Def
    end.


%% @doc get all stats (vm_statistics + vm_memory).
get_all_stats() ->
    get_stats(vm_statistics) ++ get_stats(vm_memory).


%% @doc Determine which statistics are used
-spec get_stats( vm_statistics | vm_memory ) ->
  [{vm_statistics | vm_memory, [atom()]}].
%% @end
get_stats(Name) ->
    Stats = appvar(Name, []),
    Additional = proplists:get_value(additional, Stats, []),
    Disabled = proplists:get_value(disabled, Stats, []),
    EnabledSet = sets:from_list(Additional ++ default_enabled_stats(Name)),
    DisabledSet = sets:from_list(Disabled),
    UsedStats = lists:usort(sets:to_list(sets:subtract(EnabledSet, DisabledSet))),
    [{Name, UsedStats}].

%% @doc Get the default enabled statistics for this key.
-spec default_enabled_stats(vm_statistics | vm_memory) -> [atom()].
%% @end
default_enabled_stats(vm_statistics) ->
    [
        %used_fds,      %% Number of FileDescriptors in use (Linux only)
        process_count,  %% Number of Erlang processes
        reductions,     %% Reductions since last invocation
        run_queue       %% Current run queue length
    ];
default_enabled_stats(vm_memory) ->
    [
        total,          %% Total memory currently allocated
        binary,         %% Currently allocated for binaries
        atom,           %% Currently allocated for atom
        ets,            %% Currently allocated for ets
        processes       %% Amount of memory currently allocated by the Erlang processes
    ].

%% Stats aggregation process that periodically dumps data to graphite
%% Will calculate 90th percentile etc.
%% Inspired by etsy statsd:
%% http://codeascraft.etsy.com/2011/02/15/measure-anything-measure-everything/
%%
%% This could be extended to take a callback for reporting mechanisms.
%% Right now it's hardcoded to stick data into graphite.
%%
%% Richard Jones <rj@metabrew.com>
%%
-module(estatsd_server).
-behaviour(gen_server).

-export([start_link/4]).

-export([set_state_data/2]).

-export([key2str/1]). %% export for debugging

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(COUNTER_KEY, ek_counter).
-define(GAUGE_KEY, ek_gauge).

-record(state, {timers, % GB_tree of timer data
    flush_interval,     % Ms interval between stats flushing
    flush_timer,        % TRef of interval timer
    graphite_host,      % Graphite server host
    graphite_port,      % Graphite server port
    vm_metrics,         % Flag to enable sending VM metrics on flush
    vm_used_stats,      % which stats are used
    vm_key_prefix,      % Needs to end with a . (period). Default "stats.erlangvm."
    vm_key_postfix      % Needs to start with a . (period). Default ".NODENAME.SHORTHOSTNAME"
}).

-spec set_state_data(atom(), string()) -> ok.
set_state_data(Key, Value) ->
    gen_server:call(?MODULE, {set_state_data, Key, Value}).

-spec start_link(pos_integer(), string(), pos_integer(), {boolean(), list()}) ->
    {ok, Pid::pid()} | {error, Reason::term()}.
start_link(FlushIntervalMs, GraphiteHost, GraphitePort, {VmMetrics, UsedStats}) ->
    gen_server:start_link({local, ?MODULE},
                          ?MODULE,
                          [FlushIntervalMs, GraphiteHost, GraphitePort, {VmMetrics, UsedStats}],
                          []).

init([FlushIntervalMs, GraphiteHost, GraphitePort, {VmMetrics, UsedStats}]) ->
    error_logger:info_msg("estatsd will flush stats to ~p:~w every ~wms\n",
                          [ GraphiteHost, GraphitePort, FlushIntervalMs ]),
    %% Flush out stats to graphite periodically
    {ok, Tref} = timer:apply_interval(FlushIntervalMs, gen_server, cast,
                                                       [?MODULE, flush]),
    State = #state{ timers          = gb_trees:empty(),
                    flush_interval  = FlushIntervalMs,
                    flush_timer     = Tref,
                    graphite_host   = GraphiteHost,
                    graphite_port   = GraphitePort,
                    vm_metrics      = VmMetrics,
                    vm_used_stats   = UsedStats,
                    vm_key_prefix   = "stats.erlangvm.",
                    vm_key_postfix  = "." ++ statsnode()
                  },
    {ok, State}.

handle_cast({gauge, Key0, Value0}, State) ->
    Value = {Value0, unixtime()},
    Key = {?GAUGE_KEY, Key0},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, [Value]);
        Values ->
            erlang:put(Key, [Value | Values])
    end,
    {noreply, State};

handle_cast({increment, Key0, Delta0, Sample}, State) when Sample >= 0, Sample =< 1 ->
    Delta = Delta0 * ( 1 / Sample ), %% account for sample rates < 1.0
    Key = {?COUNTER_KEY, Key0},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, Delta);
        Tot ->
            erlang:put(Key, Tot+Delta)
    end,
    {noreply, State};

handle_cast({timing, Key, Duration}, State) ->
    case gb_trees:lookup(Key, State#state.timers) of
        none ->
            {noreply, State#state{timers = gb_trees:insert(Key, [Duration], State#state.timers)}};
        {value, Val} ->
            {noreply, State#state{timers = gb_trees:update(Key, [Duration|Val], State#state.timers)}}
    end;

handle_cast(flush, State) ->
    ProcessDict = get(),
    spawn( fun() -> do_report(ProcessDict, State) end ),
    %% Erase all keys from the process dictionary!
    erlang:erase(),
    %% Hack to fix initial_call
    erlang:put('$initial_call',{estatsd_server,init,1}),
    NewState = State#state{timers = gb_trees:empty()},
    {noreply, NewState}.

handle_call({set_state_data, vm_key_prefix, Value}, _, State) ->
    {reply, ok, State#state{vm_key_prefix=Value}};
handle_call({set_state_data, vm_key_postfix, Value}, _, State) ->
    {reply, ok, State#state{vm_key_postfix=Value}};
handle_call(_,_,State)      ->
    {reply, ok, State}.

handle_info(_Msg, State)    -> {noreply, State}.

code_change(_, _, State)    -> {ok, State}.

terminate(_, _)             -> ok.

%% Make a new TCP connection to the Graphite cluster for every flush:
%%  - resilient to errors; cluster can go down and no complex
%%    reconnect logic is required
%%  - flush interval is only once every 10 seconds by default
%%    so it doesn't happen all the time
%%  - tcp ensures we don't need to worry about size of data and makes
%%    debugging easier (tcpdump, nc)
send_to_graphite(Msg, State) ->
    case gen_tcp:connect(State#state.graphite_host,
                         State#state.graphite_port,
                         [list, {packet, 0}]) of
        {ok, Sock} ->
            gen_tcp:send(Sock, Msg),
            gen_tcp:close(Sock),
            ok;
        E ->
            error_logger:error_msg("Failed to connect to graphite: ~p~n~p",
                [E, State]),
            E
    end.


%% Format: " VALUE 123456789\n"
val_time_nl(Val, TsStr) ->
    [" ", io_lib:format("~w", [Val]), " ", TsStr, "\n"].

%% Faster implementation compared to original which used
%% re:compile (everytime). TODO: we can just skip this
%% check for the keys if you are sure the keys are correct.
-spec key2str(Key :: atom() | binary() | string()) -> string().
key2str(K) when is_atom(K) ->
    atom_to_list(K);
key2str(K) when is_binary(K) ->
    key2str(binary_to_list(K));
key2str(K) when is_list(K) ->
    key2str_chars(K, "").

key2str_chars([], Acc) ->
    lists:reverse(Acc);
key2str_chars([C | Rest], Acc) when C >= $a, C =< $z ->
    key2str_chars(Rest, [C | Acc]);
key2str_chars([C | Rest], Acc) when C >= $A, C =< $Z ->
    key2str_chars(Rest, [C | Acc]);
key2str_chars([C | Rest], Acc) when C >= $0, C =< $9 ->
    key2str_chars(Rest, [C | Acc]);
key2str_chars([C | Rest], Acc) when C == $_; C == $-; C == $. ->
    key2str_chars(Rest, [C | Acc]);
key2str_chars([$/ | Rest], Acc) ->
    key2str_chars(Rest, [$- | Acc]);
key2str_chars([C | Rest], Acc) when C == 9; C == 32; C == 10->
    key2str_chars(Rest, [$_ | Acc]);
key2str_chars([_ | Rest], Acc) ->
    key2str_chars(Rest, Acc).

num2str(NN) -> lists:flatten(io_lib:format("~w",[NN])).

%% Returns unix timestamp as a string.
unixtime()  ->
    {Meg,S,_Mic} = os:timestamp(),
    integer_to_list(Meg*1000000 + S).

%% Aggregate the stats and generate a report to send to graphite
do_report(ProcessDict, State) ->
    {All, Gauges} = lists:foldl(
            fun({{?COUNTER_KEY, K}, V}, {CoAcc, GaAcc}) ->
                    {[{K, V} | CoAcc], GaAcc};
                ({{?GAUGE_KEY, K}, V}, {CoAcc, GaAcc}) ->
                    {CoAcc, [{K, V} | GaAcc]};
                (_, Acc) ->
                    Acc
            end,
        {[], []},
        ProcessDict),

    % One time stamp string used in all stats lines:
    TsStr = unixtime(),
    {MsgCounters, NumCounters}         = do_report_counters(All, TsStr, State),
    {MsgTimers,   NumTimers}           = do_report_timers(TsStr, State),
    {MsgGauges,   NumGauges}           = do_report_gauges(Gauges),
    {MsgVmMetrics,NumVmMetrics}        = do_report_vm_metrics(TsStr, State),
    %% REPORT TO GRAPHITE
    case NumTimers + NumCounters + NumGauges + NumVmMetrics of
        0 -> nothing_to_report;
        _NumStats ->
            FinalMsg = [ MsgCounters,
                         MsgTimers,
                         MsgGauges,
                         MsgVmMetrics
                       ],
            send_to_graphite(FinalMsg, State)
    end.

do_report_counters(All, TsStr, #state{flush_interval=FlushInterval}) ->
    FlushIntervalSec = FlushInterval/1000,
    Msg = lists:foldl(
                fun({Key, Val0}, Acc) ->
                        KeyS = key2str(Key),
                        Val = Val0 / FlushIntervalSec,  % Per second
                        %% Build stats string for graphite
                        Fragment = [ "stats.", KeyS | val_time_nl(Val, TsStr) ],
                        [ Fragment | Acc ]
                end, [], All),
    {Msg, length(All)}.

do_report_timers(TsStr, State) ->
    Timings = gb_trees:to_list(State#state.timers),
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
                KeyS = key2str(Key),
                Values          = lists:sort(Vals),
                Count           = length(Values),
                Min             = hd(Values),
                Max             = lists:last(Values),
                PctThreshold    = 90,
                ThresholdIndex  = erlang:round(((100-PctThreshold)/100)*Count),
                NumInThreshold  = Count - ThresholdIndex,
                Values1         = lists:sublist(Values, NumInThreshold),
                MaxAtThreshold  = lists:nth(NumInThreshold, Values),
                Mean            = lists:sum(Values1) / NumInThreshold,
                %% Build stats string for graphite
                Startl          = [ "stats.timers.", KeyS, "." ],
                Endl            = [" ", TsStr, "\n"],
                Fragment        = [ [Startl, Name, " ", num2str(Val), Endl] || {Name,Val} <-
                                  [ {"mean", Mean},
                                    {"upper", Max},
                                    {"upper_90", MaxAtThreshold},
                                    {"lower", Min},
                                    {"count", Count}
                                  ]],
                [ Fragment | Acc ]
        end, [], Timings),
    {Msg, length(Msg)}.

do_report_gauges(Gauges) ->
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
            KeyS = key2str(Key),
            Fragments = lists:foldl(
                fun ({Val, TsStr}, KeyAcc) ->
                    %% Build stats string for graphite
                    Fragment = [ "stats.gauges.", KeyS | val_time_nl(Val, TsStr) ],
                    [ Fragment | KeyAcc ]
                end, [], Vals
            ),
            [ Fragments | Acc ]
        end, [], Gauges
    ),
    {Msg, length(Gauges)}.

do_report_vm_metrics(TsStr, State) ->
    case State#state.vm_metrics of
        true ->
            UsedStats = State#state.vm_used_stats,

            %% Generic statistics
            VmUsedStats = proplists:get_value(vm_statistics, UsedStats),
            StatsData = [ {Key, stat(Key)} || Key <- VmUsedStats ],
            StatsMsg = lists:map(fun({Key, Val}) ->
                format_vm_key(State, "", Key) ++
                val_time_nl(Val, TsStr)
            end, StatsData),

            %% Memory specific statistics
            VmUsedMem = proplists:get_value(vm_memory, UsedStats),
            MemoryMsg = lists:map(fun({Key, Val}) ->
                format_vm_key(State, "memory.", Key) ++
                val_time_nl(Val, TsStr)
            end, erlang:memory(VmUsedMem)),
            Msg = StatsMsg ++ MemoryMsg;
        false ->
            Msg = []
    end,
    {Msg, length(Msg)}.


format_vm_key(#state{
                vm_key_prefix  = VmKeyPrefix,
                vm_key_postfix  = VmKeyPostfix
            }, Prefix, Key) ->
    [VmKeyPrefix, Prefix, key2str(Key), VmKeyPostfix].

%% @doc Statistics by key. Note that not all statistics are supported
%%  and we are preferring since-last-call data over absolute values.
-spec stat(atom()) -> non_neg_integer().
%% @end
stat(used_fds) ->
    case get_used_fd() of
        unknown -> 0;
        Used -> Used
    end;
stat(ports) ->
    length(erlang:ports());
stat(process_count) ->
    erlang:system_info(process_count);
stat(reductions) ->
    {_TotalReductions, Reductions} = erlang:statistics(reductions),
    Reductions;
stat(context_switches) ->
    {ContextSwitches, _} = erlang:statistics(context_switches),
    ContextSwitches;
stat(garbage_collection) ->
    {NumberofGCs, _, _} = erlang:statistics(garbage_collection),
    NumberofGCs;
stat(run_queue) ->
    erlang:statistics(run_queue);
stat(runtime) ->
    {_, Time_Since_Last_Call} = erlang:statistics(runtime),
    Time_Since_Last_Call;
stat(wall_clock) ->
    {_, Wallclock_Time_Since_Last_Call} = erlang:statistics(wall_clock),
    Wallclock_Time_Since_Last_Call;
stat(_) ->
    0.


%% @doc Determine the number of used file descriptors.
%% First attempts to read from /proc/PID/fd otherwise fallback to
%% using lsof to find open files
%% @end
get_used_fd() ->
    case file:list_dir("/proc/" ++ os:getpid() ++ "/fd") of
        {ok, Files} -> length(Files);
        {error, _} -> get_used_fd_lsof()
    end.

get_used_fd_lsof() ->
    case os:find_executable("lsof") of
        false -> unknown;
        Path -> Cmd = Path ++ " -d \"0-9999999\" -lna -p " ++ os:getpid(),
                 string:words(os:cmd(Cmd), $\n) - 1
    end.


%% @doc Returns a string like ErlangNodeName.ShortHostName
-spec statsnode() -> string().
statsnode() ->
    N=atom_to_list(node()),
    [Nodename, Hostname]=string:tokens(N, "@"),
    ShortHostName=lists:takewhile(fun (X) -> X /= $. end, Hostname),
    string:join([Nodename, ShortHostName], ".").

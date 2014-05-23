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

-include("defs.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/4]).
-export([set_state_data/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {timers, % GB_tree of timer data
    flush_interval,     % Ms interval between stats flushing
    flush_timer,        % TRef of interval timer
    graphite_host,      % Graphite server host
    graphite_port,      % Graphite server port
    vm_metrics,         % Flag to enable sending VM metrics on flush
    vm_used_stats,      % Which stats are used
    vm_key_prefix,      % Needs to end with a . (period). Default "stats.erlangvm."
    vm_key_postfix,     % Needs to start with a . (period). Default ".NODENAME.SHORTHOSTNAME"
    vm_previous,        % Dict that stores previous results (for stateful stats)
    vm_current          % Dict that stores current results
}).

%% @doc Ability to set vm_key_prefix and vm_key_postfix from
%% another library, such that you get custom graphite keys.
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


% ====================== GEN_SERVER CALLBACKS ===============================

init([FlushIntervalMs, GraphiteHost, GraphitePort, {VmMetrics, UsedStats}]) ->
    error_logger:info_msg("estatsd will flush stats to ~p:~w every ~wms\n",
                          [ GraphiteHost, GraphitePort, FlushIntervalMs ]),
    ets:new(?ETS_TABLE_COUNTERS, [named_table, set, public, {write_concurrency, true}]),
    ets:new(?ETS_TABLE_GAUGES, [named_table, set, private]),
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
                    vm_key_postfix  = "." ++ statsnode(),
                    vm_previous     = dict:new(),
                    vm_current      = dict:new()
                  },

    {ok, update_previous_current(State)}.

handle_cast({gauge, Key, {_, _} = Value}, State) ->
    case ets:lookup(?ETS_TABLE_GAUGES, Key) of
        [] ->
            ets:insert(?ETS_TABLE_GAUGES, {Key, [Value]});
        [{Key, Values}] ->
            ets:insert(?ETS_TABLE_GAUGES, {Key, [Value | Values]})
    end,
    {noreply, State};

handle_cast({gauge, Key, Value0}, State) ->
    handle_cast({gauge, Key, {Value0, unixtime()}}, State);

handle_cast({increment, Key, Delta0, Sample}, State) when Sample >= 0, Sample =< 1 ->
    Delta = case {Delta0, Sample} of
        {1,1} -> 1;                  %% ensure integer for ets:update_counter/3
        {-1,1} -> -1;
        _ -> Delta0 * ( 1 / Sample ) %% account for sample rates < 1.0
    end,
    case ets:lookup(?ETS_TABLE_COUNTERS, Key) of
        [] ->
            ets:insert(?ETS_TABLE_COUNTERS, {Key, Delta});
        [{Key,Tot}] ->
            ets:insert(?ETS_TABLE_COUNTERS, {Key, Tot+Delta})
    end,
    {noreply, State};

handle_cast({timing, Key, Duration}, State) ->
    case gb_trees:lookup(Key, State#state.timers) of
        none ->
            {noreply, State#state{timers = gb_trees:insert(Key, [Duration], State#state.timers)}};
        {value, Val} ->
            {noreply, State#state{timers = gb_trees:update(Key, [Duration|Val], State#state.timers)}}
    end;

handle_cast(flush, State0) ->
    State1 = update_previous_current(State0),
    All = ets:tab2list(?ETS_TABLE_COUNTERS),
    Gauges = ets:tab2list(?ETS_TABLE_GAUGES),
    spawn( fun() -> do_report(All, Gauges, State1) end ),
    %% WIPE ALL
    ets:delete_all_objects(?ETS_TABLE_COUNTERS),
    ets:delete_all_objects(?ETS_TABLE_GAUGES),
    State2 = State1#state{timers = gb_trees:empty()},
    {noreply, State2}.

handle_call({set_state_data, vm_key_prefix, Value}, _, State) ->
    {reply, ok, State#state{vm_key_prefix=Value}};
handle_call({set_state_data, vm_key_postfix, Value}, _, State) ->
    {reply, ok, State#state{vm_key_postfix=Value}};
handle_call(_,_,State)      ->
    {reply, ok, State}.

handle_info(_Msg, State)    -> {noreply, State}.

code_change(_, _, State)    -> {ok, State}.

terminate(_, _)             -> ok.



% ====================== INTERNAL ===============================

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
val_time_nl(Val, TsStr) when is_atom(Val)->
    [$\s, atom_to_list(Val), $\s, TsStr, $\n];
val_time_nl(Val, TsStr) when is_integer(Val)->
    [$\s, integer_to_list(Val), $\s, TsStr, $\n];
val_time_nl(Val, TsStr) when is_float(Val)->
    [$\s, float_to_list(Val), $\s, TsStr, $\n];
val_time_nl(Val, TsStr) ->
    [$\s, io_lib:format("~w", [Val]), $\s, TsStr, $\n].

num2str(NN) when is_integer(NN) ->
    integer_to_list(NN);
num2str(NN) when is_float(NN) ->
    float_to_list(NN);
num2str(NN) -> lists:flatten(io_lib:format("~w",[NN])).

%% Returns unix timestamp as a string.
unixtime()  ->
    {Meg,S,_Mic} = os:timestamp(),
    integer_to_list(Meg*1000000 + S).

%% Aggregate the stats and generate a report to send to graphite
do_report(All, Gauges, State) ->
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
                        Val = Val0 / FlushIntervalSec,  % Per second
                        %% Build stats string for graphite
                        %% Revert to old-style (no .counters. and dont log NumVals)
                        Fragment = [ "stats.", Key | val_time_nl(Val, TsStr) ],
                        [ Fragment | Acc ]
                end, [], All),
    {Msg, length(All)}.

do_report_timers(TsStr, State) ->
    Timings = gb_trees:to_list(State#state.timers),
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
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
                Startl          = [ "stats.timers.", Key, $. ],
                Endl            = [$\s, TsStr, "\n"],
                Fragment        = [ [Startl, Name, $\s, num2str(Val), Endl] || {Name,Val} <-
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
            Fragments = lists:foldl(
                fun ({Val, TsStr}, KeyAcc) ->
                    %% Build stats string for graphite
                    Fragment = [ "stats.gauges.", Key | val_time_nl(Val, TsStr) ],
                    [ Fragment | KeyAcc ]
                end, [], Vals
            ),
            [ Fragments | Acc ]
        end, [], Gauges
    ),
    {Msg, length(Gauges)}.

do_report_vm_metrics(TsStr, #state{vm_used_stats=UsedStats} = State) ->
    Msg = case State#state.vm_metrics of
        true ->
            %% Generic statistics
            VmUsedStats = proplists:get_value(vm_statistics, UsedStats),
            StatsMsgs = lists:flatten(lists:foldl(fun(VmUsedStat, StatsMsgAcc) ->
                    S0 = create_graphite_stats(VmUsedStat, TsStr, State),
                    S0 ++ StatsMsgAcc
                end,
                "",
                VmUsedStats)),
            %% Memory specific statistics
            VmUsedMem = proplists:get_value(vm_memory, UsedStats),
            MemoryMsg = lists:map(fun({Key, Val}) ->
                format_vm_key(State, "memory.", Key) ++
                val_time_nl(Val, TsStr)
            end, erlang:memory(VmUsedMem)),
            StatsMsgs ++ MemoryMsg;
        false ->
            ""
    end,
    {Msg, length(Msg)}.


format_vm_key(#state{
                vm_key_prefix  = VmKeyPrefix,
                vm_key_postfix  = VmKeyPostfix
            }, Prefix, Key) when is_atom(Key)->
    [VmKeyPrefix, Prefix, atom_to_list(Key), VmKeyPostfix];
format_vm_key(#state{
                vm_key_prefix  = VmKeyPrefix,
                vm_key_postfix  = VmKeyPostfix
            }, Prefix, Key) ->
    [VmKeyPrefix, Prefix, Key, VmKeyPostfix].

update_previous_current(#state{vm_used_stats=UsedStats} = State) ->
    lists:foldl(fun({Category, CategoryStats}, StateAcc) ->
            update_previous_current(Category, CategoryStats, StateAcc)
        end,
        State,
        UsedStats).


update_previous_current(vm_memory, _, State) ->
    State;
update_previous_current(vm_statistics, [], State) ->
    State;
update_previous_current(vm_statistics, [scheduler_wall_time=Key | T], #state{vm_previous=VMPrevious, vm_current=VMCurrent} = State) ->
    NewSched = stat(Key),
    % current value becomes the previous value, unless it was not set
    OldSched = case dict:find(scheduler_wall_time, VMCurrent) of
        error -> NewSched;
        {ok, Val} -> Val
    end,

    NewState = State#state{
        vm_previous = dict:store(Key, OldSched, VMPrevious),
        vm_current  = dict:store(Key, NewSched, VMCurrent)
     },
    update_previous_current(vm_statistics, T, NewState);
update_previous_current(vm_statistics, [_|T], State) ->
    update_previous_current(vm_statistics, T, State).

create_graphite_stats(scheduler_wall_time = Key, TsStr, #state{vm_previous=VMPrevious, vm_current=VMCurrent} = State ) ->
    {ok, NewSched} = dict:find(scheduler_wall_time, VMCurrent),
    {ok, OldSched} = dict:find(scheduler_wall_time, VMPrevious),
    SchedulerStats = lists:map(fun({{I, A0, T0}, {I, A1, T1}}) ->
        {I, (A1 - A0)/(T1 - T0)}
    end,
    lists:zip(OldSched,NewSched)),
    StatsMsgs = lists:foldl(fun({SchedulerId, SchedulerStat}, Acc) ->
        Val = erlang:trunc(SchedulerStat*1000),
        GraphiteKey = lists:flatten(io_lib:format("~p.scheduler.~p", [Key, SchedulerId])),
        StatsMsg = format_vm_key(State, "", GraphiteKey) ++ val_time_nl(Val, TsStr),
        StatsMsg ++ Acc
    end,
    "",
    SchedulerStats),
    StatsMsgs;
create_graphite_stats(Key, TsStr, State) ->
    StatsMsg = format_vm_key(State, "", Key) ++ val_time_nl(stat(Key), TsStr),
    StatsMsg.


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
stat(scheduler_wall_time) ->
    erlang:system_flag(scheduler_wall_time, true),
    lists:sort(erlang:statistics(scheduler_wall_time));
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

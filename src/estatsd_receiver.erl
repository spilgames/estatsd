%@doc Helper module to receive stats from Graphite.
%TODO: this should become a gen_server so you can query stats
-module(estatsd_receiver).
-behavior(gen_server).

-export([init/1,
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2]).

-export([accept_loop/1]).

-export([start/1,
    set_stats/1,
    get_stats/0]).

-define(TCP_OPTIONS, [list, {packet, line}, {active, false}, {reuseaddr, true}]).

-record(server_state, {
        port,
        received=[],
        ip=any,
        lsocket=null}).

start(Port) ->
    State = #server_state{port = Port},
    gen_server:start_link({local, ?MODULE}, ?MODULE, State, []).

set_stats(Stats) ->
    gen_server:cast(?MODULE, {set_stats, Stats}).

get_stats() ->
    gen_server:call(?MODULE, get_stats).


init(State = #server_state{port=Port}) ->
    case gen_tcp:listen(Port, ?TCP_OPTIONS) of
        {ok, LSocket} ->
            NewState = State#server_state{lsocket = LSocket},
            {ok, accept(NewState)};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_cast({set_stats, Stats}, #server_state{received = Received} = State) ->
    {noreply, State#server_state{received=[Stats|Received]}};
handle_cast({accepted, _Pid}, State=#server_state{}) ->
    {noreply, accept(State)}.

accept_loop({Server, LSocket}) ->
    {ok, Socket} = gen_tcp:accept(LSocket),
    gen_server:cast(Server, {accepted, self()}),
    loop(Socket).

% To be more robust we should be using spawn_link and trapping exits
accept(State = #server_state{lsocket=LSocket}) ->
    proc_lib:spawn(?MODULE, accept_loop, [{self(), LSocket}]),
    State.

% These are just here to suppress warnings.
handle_call(get_stats, _Caller, #server_state{received = Received} = State) ->
    {reply, {ok, Received}, State};
handle_call(_Msg, _Caller, State) ->
    {noreply, State}.
handle_info(_Msg, Library) ->
    {noreply, Library}.
terminate(_Reason, _Library) ->
    ok.
code_change(_OldVersion, Library, _Extra) ->
    {ok, Library}.

loop(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Line} ->
            Stats = parse(Line),
            set_stats(Stats),
            loop(Socket);
        {error, closed} ->
            ok
    end.

parse(Line) ->
    [Key, Count, Timestamp] = string:tokens(Line, " \n"),
    {Key, Count, list_to_integer(Timestamp)}.

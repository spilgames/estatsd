-module(estatsd).

-include("defs.hrl").

-export([
         gauge/2, gauge/3,
         increment/1, increment/2, increment/3,
         decrement/1, decrement/2, decrement/3,
         timing/2
        ]).

-define(SERVER, estatsd_server).

-type key() :: string() | binary() | atom().
-type delta() :: float() | integer().
-type sample() :: float() | integer().

-export_type([key/0, delta/0, sample/0]).

% Convenience: just give it the now() tuple when the work started
-spec timing(key(), erlang:timestamp() | delta()) -> ok.
timing(Key, StartTime = {_,_,_}) ->
    Dur = erlang:round(timer:now_diff(os:timestamp(), StartTime)/1000),
    timing(Key,Dur);
timing(Key, Duration) when is_integer(Duration) ->
    gen_server:cast(?SERVER, {timing, Key, Duration});
timing(Key, Duration) ->
    gen_server:cast(?SERVER, {timing, Key, erlang:round(Duration)}).

% Increments one or more stats counters
-spec increment(key()) -> ok.
increment(Key) ->
    % Note that this will fail if the value in the ETS table
    % is NOT an integer. So you shouldn't mix samplesizes or amounts
    % Assuming you only update a key in one place, this is fine.
    % To keep it safer, we only optimize increment/1, which is most
    % commonly used.

    % try-catch is required, because initially the key will not be set
    % and after flushing it is also removed.
    try
        _ = ets:update_counter(?ETS_TABLE_COUNTERS, Key, 1),
        ok
    catch
        error:badarg ->
            increment(Key,1,1)
    end.
-spec increment(key(), delta()) -> ok.
increment(Key, Amount) ->
    increment(Key, Amount, 1).
-spec increment(key(), delta(), sample()) -> ok.
increment(Key, Amount, Sample) when Sample >= 0, Sample =< 1 ->
    gen_server:cast(?SERVER, {increment, Key, Amount, Sample}).

-spec decrement(key()) -> ok.
decrement(Key) ->
    decrement(Key, 1, 1).
-spec decrement(key(), delta()) -> ok.
decrement(Key, Amount) ->
    decrement(Key, Amount, 1).
-spec decrement(key(), delta(), sample()) -> ok.
decrement(Key, Amount, Sample) ->
    increment(Key, 0 - Amount, Sample).

% Sets a gauge value
-spec gauge(key(), delta()) -> ok.
gauge(Key, Value) when is_number(Value) ->
    gen_server:cast(?SERVER, {gauge, Key, Value}).

-spec gauge(key(), integer(), delta()) -> ok.
gauge(Key, Time, Value) when is_number(Value), is_number(Time) ->
    gen_server:cast(?SERVER, {gauge, Key, {Value, Time}}).

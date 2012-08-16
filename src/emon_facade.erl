-module(emon_facade).

-export([increment/3, decrement/3, timing/2, tc/2, tc_pt/3]).

%%--------------------------------------------------------------------
%% @doc Increments the value of he given Key in the given Amount. Note that SampleRate smaller than 1 allows you to state that you are
%% only measuring a porcentage of the actual calls and you want your amounts to be updated as if you where measuring them
%% all. If in doubt on how to use this parameter, set it to 1.
-spec increment(string(), pos_integer(), float()) -> ok.
%% @end
%%--------------------------------------------------------------------
increment(Key, Amount, SampleRate) when Amount>0, SampleRate>0, SampleRate=<1->
    estatsd:increment(Key, Amount, SampleRate).

%%--------------------------------------------------------------------
%% @doc Decrements the value of he given Key in the given Amount. Note that SamplRate smaller than 1 allows you to state that you are
%% only measuring a porcentage of the actual calls and you want your amounts to be updated as if you where measuring them
%% all. If in doubt on how to use this parameter, set it to 1.
-spec decrement(string(), pos_integer(), float()) -> ok.
%% @end
%%--------------------------------------------------------------------
decrement(Key, Amount, SampleRate) when Amount>0, SampleRate>0, SampleRate=<1->
    estatsd:decrement(Key, Amount, SampleRate).

%%--------------------------------------------------------------------
%% @doc Updates a timing monitoring Key with the Duration value, assumed in microseconds.
-spec timing(string(), pos_integer()) -> ok.
%% @end
%%--------------------------------------------------------------------
timing(Key, Duration) when is_integer(Duration), Duration>=0 ->
    estatsd:timing(Key, Duration).

%%--------------------------------------------------------------------
%% @doc Updates a timing monitoring Key with the time consumed by he execution of F. Duration will be monitored in
%% milliseconds.
-spec tc(string(), fun(() -> term())) -> term().
%% @end
%%--------------------------------------------------------------------
tc(Key, F) ->
    {Duration, Result}=timer:tc(F),
    estatsd:timing(Key, round(Duration/1000)),
    Result.

%%--------------------------------------------------------------------
%% @doc Allow calling tc/2 via a parse_transform decorator
-spec tc_pt(fun(), any(), {string() | atom(), string() | atom()}) -> fun().
%% @end
%%--------------------------------------------------------------------
tc_pt(Fun, Input, {AppName, FuncName}) ->
    ResponseTimeKey=emon_utils:get_key_string("services", AppName, [FuncName, "responseTime"]),
    fun() -> 
            tc(ResponseTimeKey, fun() -> erlang:apply(Fun, [Input]) end ) 
    end.

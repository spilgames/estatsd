-module(estatsd_app).

-behaviour(application).

%% API
-export([start/0, stop/0]).

%% Application callbacks
-export([start/2, stop/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Starts the application
-spec start() -> ok | {error, term()}.
%% @end
%%--------------------------------------------------------------------
start() ->
    application:start(estatsd).

%%--------------------------------------------------------------------
%% @doc Stops the application
-spec stop() -> ok | {error, term()}.
%% @end
%%--------------------------------------------------------------------
stop() ->
    application:stop(estatsd).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    estatsd_sup:start_link().

stop(_State) ->
    ok.

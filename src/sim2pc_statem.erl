-module(sim2pc_statem).

-behavior(gen_statem).

-include_lib("kernel/include/logger.hrl").


%% API
-export([
    start_link/0
]).

%% gen_statem callbacks
-export([
    callback_mode/0,
    init/1,
    format_status/2,
    terminate/3,
    code_change/4
]).

%% states
-export([
    first_state/3
]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_statem:start_link(?MODULE, [], []).
%%%%    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================
callback_mode() ->
    state_functions.

init([]) ->
    {ok, first_state, #state{}}.

format_status(_Opt, [_PDict, State, Data]) ->
    [{data, [{"State", {State, Data}}]}].

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================
%%% state transitions
%%%===================
first_state({call, Sender}, {start_tx, _Param1}, _State) ->
    {next_state, second_state, _State, {reply, Sender, ok}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

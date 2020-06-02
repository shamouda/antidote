-module(sim2pc_statem_sup).

-behavior(supervisor).

-export([
    start_fsm/0,
    start_link/0
]).

-export([
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Starts a new transaction coordinator under this supervisor
start_fsm() ->
    % calls sim2pc_statem:start_link()
    supervisor:start_child(?MODULE, []).

%% Starts the coordinator of a transaction.
init([]) ->
    Worker = {undefined,
        {sim2pc_statem, start_link, []},
        temporary, 5000, worker, [sim2pc_statem]},
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.

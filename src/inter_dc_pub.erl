%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% InterDC publisher - holds a ZeroMQ PUB socket and makes it available for Antidote processes.
%% This vnode is used to publish interDC transactions.

-module(inter_dc_pub).

-behaviour(gen_server).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([
    broadcast/1
]).

%% Server methods
-export([
    init/1,
    start_link/0,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% State
-record(state, {socket :: any()}).

%%%% API --------------------------------------------------------------------+

-spec broadcast(interdc_txn()) -> ok.
broadcast(Txn) ->
    gen_server:call(?MODULE, {publish, inter_dc_txn:to_bin(Txn)}).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Port = application:get_env(antidote, pubsub_port, ?DEFAULT_PUBSUB_PORT),
    Socket = zmq_utils:create_bind_socket(pub, false, Port),
    ?LOG_ERROR("Publisher started on port ~p", [Port]),
    {ok, #state{socket = Socket}}.

handle_call({publish, Message}, _From, State) ->
    ?LOG_INFO("Publishing a message", []),

    Result = erlzmq:send(State#state.socket, Message),
    case Result of
        ok -> ok;
        Error -> ?LOG_ERROR("Could not publish a message: ~p", [Error])
    end,

    {reply, ok, State}.

terminate(_Reason, State) -> erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

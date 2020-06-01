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

-module(simple_kv_vnode).
-behaviour(riak_core_vnode).
-include_lib("kernel/include/logger.hrl").

%% Internal API
-export([
    init/1,
    start_vnode/1,
    handle_command/3,
    handle_coverage/4,
    handle_exit/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_command/3,
    handle_handoff_data/2,
    encode_handoff_item/2,
    is_empty/1,
    terminate/2,
    delete/1,
    handle_overload_command/3,
    handle_overload_info/2]).

%% VNode state
-record(state, {partition, kv_map :: map()}).

%%%% Internal API

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition = Partition, kv_map = #{}}}.

handle_command({get, ReqId, {Key}}, _Sender, State = #state{kv_map = KvMap}) ->
    Value = maps:get(Key, KvMap, not_assigned),
    {reply, {{request_id, ReqId}, {result, Value}}, State};

handle_command({put, ReqId, {Key, Value}}, _Sender, State = #state{kv_map = KvMap, partition = Partition}) ->
    Location = [Partition, node()],
    NewKvMap = maps:put(Key, Value, KvMap),
    {reply, {{request_id, ReqId}, {location, Location}}, State#state{kv_map = NewKvMap}};

handle_command({list_keys}, _Sender, State = #state{kv_map = KvMap, partition = _Partition}) ->
    {reply, {keys, maps:keys(KvMap)}, State};

handle_command({hello}, _Sender, State) ->
    {reply, ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.
handoff_starting(_TargetNode, State) ->
    {true, State}.
handoff_cancelled(State) ->
    {ok, State}.
handoff_finished(_TargetNode, State) ->
    {ok, State}.
handle_handoff_command( _Message , _Sender, State) ->
    {noreply, State}.
handle_handoff_data(_Data, State) ->
    {reply, ok, State}.
encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).
is_empty(State) ->
    {true, State}.
delete(State) ->
    {ok, State}.
terminate(_Reason, _State) ->
    ok.
handle_overload_command(_, _, _) ->
    ok.
handle_overload_info(_, _) ->
    ok.
    
%%%% Private Functions


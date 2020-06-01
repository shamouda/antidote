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

-module(simple_kv).
-include_lib("kernel/include/logger.hrl").

%% External API
-export([
    
]).


%% Internal API
-export([
	get/1,
	put/2
    ]).

-define(BUCKET, test_utils:bucket(simple_kv_bucket)).

%%%% External API
get(Key) ->
	?LOG_INFO("simple_kv.get called key = ~p", [Key]),
    ReqId = make_ref(),
    send_to_one(Key, {get, ReqId, {Key}}).

put(Key, Value) ->
	?LOG_INFO("simple_kv.put called key = ~p value = ~p", [Key, Value]),
    ReqId = make_ref(),
    send_to_one(Key, {put, ReqId, {Key, Value}}).

send_to_one(Key, Cmd) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, simple_kv),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, Cmd, simple_kv_vnode_master).
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


-module(simple_kv_SUITE).

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

%% tests
-export([
    simple_kv_test/1,
    simple_2pc_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(SOME_CONST, 3).

-define(BUCKET, test_utils:bucket(simple_kv_bucket)).


init_per_suite(Config) ->
	Suite = ?MODULE,
    ct:pal("[~p]", [Suite]),
    test_utils:at_init_testsuite(),
	Nodes = test_utils:set_up_multi_node_dc(Config, [dev5, dev6, dev7]),
    [{clusters, Nodes} | Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    %%simple_kv_test,
    simple_2pc_test
].
 
simple_kv_test(Config) ->
    Cluster = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3] = Cluster,
    ct:pal("Cluster ~p ~p ~p", [Node1, Node2, Node3]),
	
	Bucket = ?BUCKET,
    Key1 = key1,
    Value1 = value1,
    rpc:call(Node2, simple_kv, put, [Bucket, Key1, Value1]),
    GetResult = rpc:call(Node1, simple_kv, get, [Bucket, Key1]),
    ?assertMatch({_Request,{result,Value1}}, GetResult),

	ct:pal("testing get and set passed.", []),
    pass.
    
simple_2pc_test(_Config) ->
%%	{ok, Pid} = sim2pc_statem_sup:start_fsm(),
    {ok, Pid} = sim2pc_statem:start_link(),
	Param1 = 1,
	gen_statem:call(Pid, {start_tx, Param1}),
	pass.


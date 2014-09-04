%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(inter_dc_recvr).

-behaviour(gen_server).

%%public API
-export([start_link/0, replicate/3, stop/1]).

%%gen_server call backs
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
%%API

start_link() ->
    {ok, PID} = gen_server:start_link(?MODULE, [], []),
    register(inter_dc_recvr, PID),
    {ok, PID}.

replicate(Node, Payload, Origin) ->
    lager:info("Sending update ~p to ~p ~n",[Payload, Node]),
    gen_server:cast(Node, {replicate, Payload, from, Origin}).

stop(Pid)->
    gen_server:call(Pid, terminate).

%%Server functions
init([]) ->
    {ok, []}.

handle_cast({replicate, Payload, from, {PID,Node}}, _StateData) ->
    apply(Payload),
    gen_server:cast({inter_dc_recvr,Node},{acknowledge,Payload,PID}),
    {noreply,_StateData};
handle_cast({acknowledge, Payload, PID}, _StateData) ->
    inter_dc_repl:acknowledge(PID,Payload),
    {noreply, _StateData}.

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State}.

handle_info(Msg, State) ->
    lager:info("Unexpected message: ~p~n",[Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    lager:info("Inter_dc_repl_recvr stopping"),
    ok;
terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    %% No change planned. The function is there for the behaviour,
    %% but will not be used. Only a version on the next
    {ok, State}.

%%private

apply(Payload) ->
    lager:info("Recieved update ~p ~n",[Payload]),
    {Key, Op} = Payload,
    %%TODO: Replace this with proper replication protocol
    _ = floppy_coord_sup:start_fsm([self(), update, Key, Op]),
    receive
        {_, Result} ->
            lager:info("Updated ~p",[Result])
    after 5000 ->
            lager:info("Update failed!~n")
    end,
    lager:info("Updated operation ~p ~n", [Payload]),
    ok.

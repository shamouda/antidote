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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote module to filter database objects according to
%%%      the Antidote Query Language (AQL) schema.
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(query_optimizer).

-include("querying.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([query_filter/2,
         get_partial_object/2,
         get_partial_object/3,
         get_partial_object/5]).

%% TODO support more than one table per filter, for supporting join queries
query_filter(Filter, TxId) when is_list(Filter) ->
    TableName = table(Filter),
    Table = table_utils:table_metadata(TableName, TxId),
    TCols = table_utils:column_names(Table),
    Projection = projection(Filter),

    case validate_projection(Projection, TCols) of
        {error, Msg} ->
            {error, Msg};
        {ok, ProjectionCols} ->
            Conditions = conditions(Filter),

            FilteredResult =
                case Conditions of
                    [] ->
                        Index = indexing:read_index(primary, TableName, TxId),
                        Records = read_records(Index, TableName, TxId),
                        prepare_records(table_utils:column_names(Table), Table, Records);
                    _Else ->
                        apply_filter(Conditions, Table, TxId)
                end,
            ResultToList =
                case is_list(FilteredResult) of
                    false -> sets:to_list(FilteredResult);
                    true -> FilteredResult
                end,

            {ok, apply_projection(ProjectionCols, ResultToList)}
    end.

get_partial_object(Key, Type, Bucket, Filter, TxId) ->
    ObjectKey = querying_utils:build_keys(Key, Type, Bucket),
    [Object] = querying_utils:read_keys(value, ObjectKey, TxId),
    {ok, apply_projection(Filter, Object)}.
get_partial_object(ObjectKey, Filter, TxId) when is_tuple(ObjectKey) ->
    [Object] = querying_utils:read_keys(value, ObjectKey, TxId),
    {ok, apply_projection(Filter, Object)}.
get_partial_object(Object, Filter) when is_list(Object) ->
    {ok, apply_projection(Filter, Object)}.

table(Filter) ->
    {_, [TableName]} = lists:keyfind(tables, 1, Filter),
    TableName.
projection(Filter) ->
    {_, Columns} = lists:keyfind(projection, 1, Filter),
    Columns.
conditions(Filter) ->
    {_, Conditions} = lists:keyfind(conditions, 1, Filter),
    Conditions.

%% ====================================================================
%% Internal functions
%% ====================================================================
apply_filter(Conditions, Table, TxId) ->
    case ?is_disjunction(Conditions) of
        true ->
            ?DISJUNCTION(Conjunctions) = Conditions,
            lists:foldl(fun(Conjunction, FinalRes) ->
                {RemainConds, PartialRes} = read_subqueries(Conjunction, Table, TxId, [], nil),
                PartialResult = read_remaining(RemainConds, Table, PartialRes, TxId),

                ResultSet = sets:from_list(PartialResult),
                %io:format("ResultSet: ~p~n", [ResultSet]),
                sets:union(FinalRes, ResultSet)
            end, sets:new(), Conjunctions);
        false ->
            throw("The current condition is not valid")
    end.

%% The sub-queries mentioned here are conditions that are
%% surrounded by parenthesis.
read_subqueries([{sub, Conds} | Tail], Table, TxId, RemainConds, PartialRes) ->
    Result = apply_filter(Conds, Table, TxId),
    ResultSet = case is_list(Result) of
                       true -> sets:from_list(Result);
                       false -> Result
                end,

    Intersection = case PartialRes of
                       nil -> ResultSet;
                       _Else -> sets:intersection(PartialRes, ResultSet)
                   end,
    read_subqueries(Tail, Table, TxId, RemainConds, Intersection);
read_subqueries([Cond | Tail], Table, TxId, RemainConds, PartialRes) ->
    read_subqueries(Tail, Table, TxId, lists:append(RemainConds, [Cond]), PartialRes);
read_subqueries([], _Table, _TxId, RemainConds, nil) ->
    {RemainConds, nil};
read_subqueries([], _Table, _TxId, RemainConds, PartialRes) ->
    %AddFks = get_shadow_columns(Table, sets:to_list(PartialRes), TxId),
    {RemainConds, sets:to_list(PartialRes)}.

read_remaining(_Conditions, _Table, [], _TxId) -> [];
read_remaining([], _Table, CurrentData, _TxId) -> CurrentData;
read_remaining(Conditions, Table, CurrentData, TxId) ->
    %TableName = table_utils:table(Table),
    RangeQueries = range_queries:get_range_query(Conditions),
    case RangeQueries of
        nil -> [];
        _Else ->
            case CurrentData of
                nil ->
                    {Remain, Indexes} = read_indexes(RangeQueries, Table),
                    LeastKeys = lists:foldl(fun(Index, Curr) ->
                        ReadKeys = interpret_index(Index, Table, RangeQueries, TxId),
                        case Curr of
                            nil -> ReadKeys;
                            Curr -> ordsets:intersection(Curr, ReadKeys)
                        end
                    end, nil, Indexes),

                    case LeastKeys of
                        nil ->
                            iterate_ranges(RangeQueries, Table, TxId);
                        LeastKeys ->
                            KeyList = ordsets:to_list(LeastKeys),
                            Objects = read_records(KeyList, TxId),
                            PreparedObjs = prepare_records(table_utils:column_names(Table), Table, Objects),

                            RemainRanges = dict:filter(fun(Column, _Range) ->
                                lists:member(Column, Remain)
                            end, RangeQueries),

                            case dict:is_empty(RemainRanges) of
                                true -> PreparedObjs;
                                false -> iterate_ranges(RemainRanges, Table, PreparedObjs, TxId)
                            end
                    end;
                CurrentData ->
                    iterate_ranges(RangeQueries, Table, CurrentData, TxId)
            end
    end.

iterate_ranges(RangeQueries, Table, TxId) ->
    TableName = table_utils:table(Table),
    Keys = indexing:read_index(primary, TableName, TxId),
    Data = read_records(Keys, TxId),
    PreparedData = prepare_records(table_utils:column_names(Table), Table, Data),
    iterate_ranges(RangeQueries, Table, PreparedData, TxId).

iterate_ranges(RangeQueries, Table, Data, TxId) ->
    Predicates = dict:fold(fun(Column, Range, FunList) ->
        FilterFun = read_predicate(Range),
        Fun = fun(Record) ->
                satisfies_predicate({Column, FilterFun}, Table, Record, TxId)
              end,
        lists:append(FunList, [Fun])
    end, [], RangeQueries),

    lists:foldl(fun(Record, ObjAcc) ->
        ExecPreds = lists:foldl(fun(F, FunRes) -> FunRes andalso F(Record) end, true, Predicates),
        case ExecPreds of
            true -> lists:append(ObjAcc, [Record]);
            false -> ObjAcc
        end
    end, [], Data).

satisfies_predicate(_, _, [], _) ->
    false;
satisfies_predicate({Column, Predicate}, Table, Record, TxId) ->
    case ?is_function(Column) of
        true ->
            record_utils:satisfies_function({Column, Predicate}, Table, Record, TxId);
        false ->
            record_utils:satisfies_predicate(Column, Predicate, Record)
    end.

filter_keys(_Predicate, []) -> [];
filter_keys(Predicate, Keys) when is_list(Keys) ->
    lists:filter(Predicate, Keys).

%% TODO support this search to comprise indexes with multiple attributes
find_index_by_attribute(_Attribute, []) -> [];
find_index_by_attribute(Attribute, IndexList) when is_list(IndexList) ->
    lists:foldl(fun(Elem, Acc) ->
        ?INDEX(_IdxName, _TName, Cols) = Elem,
        case lists:member(Attribute, Cols) of
            true -> lists:append(Acc, [Elem]);
            false -> Acc
        end
     end, [], IndexList);
find_index_by_attribute(_Attribute, _Idx) -> [].

validate_projection(?WILDCARD, Columns) ->
    {ok, Columns};
validate_projection(Projection, Columns) ->
    Invalid = lists:foldl(fun(ProjCol, AccInvalid) ->
        case lists:member(ProjCol, Columns) of
            true ->
                AccInvalid;
            false ->
                lists:append(AccInvalid, [ProjCol])
        end
    end, [], Projection),
    case Invalid of
        [] ->
            {ok, Projection};
        _Else ->
            {error, {invalid_projection, Invalid}}
    end.

apply_projection([], Objects) ->
    Objects;
apply_projection(Projection, Objects) when is_list(Objects) ->
    apply_projection(Projection, Objects, []);
apply_projection(Projection, Object) ->
    apply_projection(Projection, [Object]).

apply_projection(Projection, [Object | Objs], Acc) ->
    FilteredObj = lists:foldl(fun(Col, ObjAcc) ->
        case record_utils:get_column(Col, Object) of
            ?ATTRIBUTE(Col, _Type, _Value) = Attr -> lists:append(ObjAcc, [Attr]);
            undefined ->
                ErrorMsg = io_lib:format("Invalid projection column: ~p", [Col]),
                throw(lists:flatten(ErrorMsg))
        end
    end, [], Projection),
    apply_projection(Projection, Objs, lists:append(Acc, [FilteredObj]));
apply_projection(_Projection, [], Acc) ->
    Acc.

%% This function reads all remaining data of each record.
%% A record may not have its full data if:
%% - At least one of its bounded counter columns has the initial value
%%   (which means to have the initial state of a bounded counter);
%% - Or it does not have the values (i.e. states) of its shadow columns.
prepare_records(Columns, Table, Records) ->
    TColumns = table_utils:columns(Table),
    BCounterCols = lists:foldl(fun(ColName, AccCols) ->
        case maps:get(ColName, TColumns) of
            {_, ?AQL_COUNTER_INT, _} = Col ->
                lists:append(AccCols, [Col]);
            _Else ->
                AccCols
        end
    end, [], Columns),
    prepare_records0(BCounterCols, Records, []).

prepare_records0(BCounterCols, [Record | Records], Acc) ->
    NewBCounter = ?CRDT_BCOUNTER_INT:new(),
    NewObj = lists:foldl(fun({BCounterName, _, _}, AccRecord) ->
        case record_utils:get_column(BCounterName, AccRecord) of
            undefined ->
                lists:append(AccRecord, [?ATTRIBUTE(BCounterName, ?CRDT_BCOUNTER_INT, NewBCounter)]);
            _Else ->
                AccRecord
        end
    end, Record, BCounterCols),

    NewObjsAcc = lists:append(Acc, [NewObj]),
    prepare_records0(BCounterCols, Records, NewObjsAcc);
prepare_records0(_, [], Acc) -> Acc.

read_records(PKey, TableName, TxId) ->
    record_utils:record_data(PKey, TableName, TxId).
read_records(Key, TxId) ->
    record_utils:record_data(Key, TxId).

range_type({{{greatereq, _Val}, {lessereq, _Val}}, _}) -> equality;
range_type({{{nil, infinity}, {nil, infinity}}, Excluded}) when length(Excluded) > 0 -> notequality;
range_type(_) -> range.

read_predicate(Range) ->
    case range_queries:to_predicate(Range) of
        {{{_, LPred}, infinity}, Inequality} ->
            fun(V) -> LPred(V) andalso Inequality(V) end;
        {{infinity, {_, RPred}}, Inequality} ->
            fun(V) -> RPred(V) andalso Inequality(V) end;
        {{{_, LPred}, {_, RPred}}, Inequality} ->
            fun(V) -> LPred(V) andalso RPred(V) andalso Inequality(V) end;
        {ignore, Pred} when is_function(Pred) ->
            Pred;
        {Pred1, Pred2} when is_function(Pred1) and is_function(Pred2) ->
            fun(V) -> Pred1(V) andalso Pred2(V) end
    end.

read_predicate_pk(Range) ->
    case range_queries:to_predicate(Range) of
        {{{_, LPred}, infinity}, Inequality} ->
            fun({V, _, _}) -> LPred(V) andalso Inequality(V) end;
        {{infinity, {_, RPred}}, Inequality} ->
            fun({V, _, _}) -> RPred(V) andalso Inequality(V) end;
        {{{_, LPred}, {_, RPred}}, Inequality} ->
            fun({V, _, _}) -> LPred(V) andalso RPred(V) andalso Inequality(V) end;
        {ignore, Pred} when is_function(Pred) ->
            fun({V, _, _}) -> Pred(V) end;
        {Pred1, Pred2} when is_function(Pred1) and is_function(Pred2) ->
            fun({V, _, _}) -> Pred1(V) andalso Pred2(V) end
    end.

read_pk_predicate(Range) ->
    {{{LB, Val1}, {RB, Val2}}, Excluded} = Range,
    NewRange = {{LB, querying_utils:to_atom(Val1)}, {RB, querying_utils:to_atom(Val2)}},
    read_predicate_pk({NewRange, Excluded}).

read_indexes(RangeQueries, Table) ->
    TableName = table_utils:table(Table),
    dict:fold(fun(Column, _Range, {RemainAcc, IdxAcc}) ->
        case ?is_function(Column) of
            true -> {lists:append(RemainAcc, [Column]), IdxAcc};
            false ->
                case table_utils:is_primary_key(Column, Table) of
                    true ->
                        {RemainAcc, lists:append(IdxAcc, [{primary, TableName}])};
                    false ->
                        SIndexes = table_utils:indexes(Table),
                        case find_index_by_attribute(Column, SIndexes) of
                            [] -> {lists:append(RemainAcc, [Column]), IdxAcc};
                            [SIndex] -> {RemainAcc, lists:append(IdxAcc, [{secondary, SIndex}])}
                        end
                end
        end
    end, {[], []}, RangeQueries).

interpret_index({primary, TableName}, Table, RangeQueries, TxId) ->
    IdxData = indexing:read_index(primary, TableName, TxId),
    [PKCol] = table_utils:primary_key_name(Table),

    GetRange = range_queries:lookup_range(PKCol, RangeQueries),
    FilterFun = read_pk_predicate(GetRange),
    ordsets:from_list(filter_keys(FilterFun, IdxData));
interpret_index({secondary, {Name, TName, [Col]}}, _Table, RangeQueries, TxId) -> %% TODO support more columns
    GetRange = range_queries:lookup_range(Col, RangeQueries),

    IdxData = case range_type(GetRange) of
                  equality ->
                      {{{_, Val}, {_, Val}}, _} = GetRange,
                      Res = indexing:read_index_function(secondary, {TName, Name}, {get, Val}, TxId),
                      case Res of
                          {error, _} -> [];
                          _Else -> [Res]
                      end;
                  notequality ->
                      {_, Excluded} = GetRange,
                      Aux = indexing:read_index(secondary, {TName, Name}, TxId),

                      lists:filter(fun({IdxVal, _Set}) ->
                          %% inequality predicate
                          not lists:member(IdxVal, Excluded)
                      end, Aux);
                  range ->
                      {{LeftBound, RightBound}, Excluded} = GetRange,
                      Index = indexing:read_index_function(secondary,
                          {TName, Name}, {range, {send_range(LeftBound), send_range(RightBound)}}, TxId),

                      lists:filter(fun({IdxCol, _PKs}) ->
                          %% inequality predicate
                          not lists:member(IdxCol, Excluded)
                      end, Index)
              end,

    lists:foldl(fun({_IdxCol, PKs}, Set) ->
        %% there's an assumption that the accumulator will never have repeated keys
        ordsets:union(Set, PKs)
    end, ordsets:new(), IdxData).

send_range({_, infinity}) -> infinity;
send_range({Bound, Val}) -> {Bound, Val}.

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

comp_to_predicate(Comparator, Value) ->
    case Comparator of
        equality -> fun(Elem) -> Elem == Value end;
        notequality -> fun(Elem) -> Elem /= Value end;
        greater -> fun(Elem) -> Elem > Value end;
        greatereq -> fun(Elem) -> Elem >= Value end;
        lesser -> fun(Elem) -> Elem < Value end;
        lessereq -> fun(Elem) -> Elem =< Value end
    end.

comparison_convert_test() ->
    ValueList = [0, 1, 2, 3, 4],
    Op1 = equality,
    Op2 = notequality,
    Op3 = greater,
    Val = 2,
    Res1 = filter_keys(comp_to_predicate(Op1, Val), ValueList),
    Res2 = filter_keys(comp_to_predicate(Op2, Val), ValueList),
    Res3 = filter_keys(comp_to_predicate(Op3, Val), ValueList),
    ?assertEqual(Res1, [2]),
    ?assertEqual(Res2, [0, 1, 3, 4]),
    ?assertEqual(Res3, [3, 4]).

projection_test() ->
    Objects = [
        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "B1"}, {{'Col3', crdt}, "C1"}],
        [{{'Col1', crdt}, "A2"}, {{'Col2', crdt}, "B2"}, {{'Col3', crdt}, "C2"}],
        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "B3"}, {{'Col3', crdt}, "C3"}]
    ],
    ExpectedResult1 = [
        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "B1"}],
        [{{'Col1', crdt}, "A2"}, {{'Col2', crdt}, "B2"}],
        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "B3"}]
    ],
    Result = apply_projection(['Col1', 'Col2'], Objects),
    ?assertEqual(Result, ExpectedResult1).

-endif.

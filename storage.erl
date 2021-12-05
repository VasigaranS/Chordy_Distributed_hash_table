%% @author vasigarans
%% @doc @todo Add description to storage.


-module(storage).

%% ====================================================================
%% API functions
%% ====================================================================
-compile(export_all).

create()->
    [].

add(Key,Value,Store)->
    [{Key,Value}|Store].

lookup(Key,Store)->
    lists:keyfind(Key,1,Store).
							 
split(From,To,Store)->
    lists:partition(fun({Key,_})->key:between(Key,From,To) end,Store).

merge(Entries,Store)->
    %[Entries|Store].
    lists:merge(Entries, Store).

%% ====================================================================
%% Internal functions
%% ====================================================================



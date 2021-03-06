%% @author vasigarans
%% @doc @todo Add description to key.


-module(key).
-export([generate/0, between/3]).
-define(MAX, 1000000000).
%% ====================================================================
%% API functions
%% ====================================================================


generate()->
	rand:uniform(?MAX).

between(Key,From,To) when From<To->
    From < Key andalso Key =< To;
between(Key,From,To) when From>To->
	Key=<To orelse From<Key;
between(_,From,To) when From==To->
	true.



%% ====================================================================
%% Internal functions
%% ====================================================================



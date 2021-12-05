%% @author vasigarans
%% @doc @todo Add description to node2.


-module(node2).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/1,start/2]).
-define(Timeout, 10000).
-define(Stabilize, 1000).
node(Id, Predecessor, Successor,Store) ->
	%io:format("Store: ~p~n", [Store]),
	%io:format('Id ~w   Predecessor~w  Successor~w~n',[Id, Predecessor, Successor]),
    receive
		% a peer needs to know our key
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor,Store);
        {notify, New} ->% a new node informs us of its existence
            {Pred,Store2} = notify(New, Id, Predecessor,Store),
            node(Id, Pred, Successor,Store2);
        {request, Peer} ->%a predecessor needs to know our predecessor
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor,Store);
        {status, Pred} ->%our successor informs us about its predecessor
            Succ = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ,Store);
		 stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor,Store);
		probe ->
	    	create_probe(Id, Successor),
	    	node(Id, Predecessor, Successor,Store);
		{probe, Id, Nodes, T} ->
	    	remove_probe(T, Nodes),
	    	node(Id, Predecessor, Successor,Store);
		{probe, Ref, Nodes, T} ->
	    	forward_probe(Ref, T, Nodes, Id, Successor),
	    	node(Id, Predecessor, Successor,Store);
        {add, Key, Value, Qref, Client} ->
            Added = add(Key, Value, Qref, Client,
                        Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Added);
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Store);
		{handover, Elements} ->
            Store3 = storage:merge(Store, Elements),
            node(Id, Predecessor, Successor, Store3);
		display ->
	    	io:format("ID: ~w~n", [Id]),
			io:format("Predecessor: ~p, Successor: ~p~n", [Predecessor, Successor]),
			io:format("Store: ~p~n", [Store]),
			node(Id, Predecessor, Successor, Store);
			
			stop ->
	    ok
end.

create_probe(Id, {_Skey, Spid}) ->
    Spid ! {probe, Id, [Id], erlang:system_time()}.

forward_probe(Ref, T, Nodes, Id, {_Skey, Spid}) ->
    Spid ! {probe, Ref, [Id | Nodes], T}.

remove_probe(T, Nodes) ->
    Diff = erlang:system_time()-T,
    io:format("Route: ~w~n", [lists:reverse(Nodes)]),
    io:format("Trip time: ~w milli~n", [Diff]).

stabilize(Pred, Id, Successor) ->
    {Skey, Spid} = Successor,
    case Pred of 
		nil ->
			%nil we should of course inform it about our existence.
			Spid!{notify, {Id, self()}},
				 Successor;
	  {Id,_}->%pointing back to us we don’t have to do anything.
			Successor;
		{Skey, Spid} ->%pointing to itself we should of course notify it about our existence.
				Spid ! {notify, {Id, self()}},
				Successor;
		{Xkey, Xpid} ->
				case key:between(Xkey, Id, Skey) of
							true -> %If the key of the predecessor of our successor (Xkey) is between us and our successor we should of course adopt this node as our successor and run stabilization again.
								Xpid!{request, self()},
								Pred;

								
							false -> 
								%If we should be in between the nodes we inform our successor of our existence
								Spid!{notify, {Id, self()}},
								Successor
						end
					end.

add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
    %io:format('~w~w~n',[Id,Store]),
    case key:between(Key, Pkey, Id) of
	true ->
	    Client ! {Qref, ok},
		Updated = storage:add(Key, Value, Store),
		%io:format('~w~w~n',[Id,Updated]),
		Updated;
	false ->
	    Spid ! {add, Key, Value, Qref, Client},
	    Store
    end.

lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
	case key:between(Key, Pkey, Id) of
		true ->
            Result = storage:lookup(Key, Store),
            Client ! {Qref, Result};
		 false ->
            {_, Spid} = Successor,
			Spid ! {lookup, Key, Qref, Client}
    end.
			




schedule_stabilize() ->
	%  stabilize procedure must be done with regular intervals so that new nodes are quickly linked into the ring.
%The procedure schedule stabilize/1 is called when a node is created. When the process receives a stabilize message it will call stabilize/1 procedure.
    timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
    Spid ! {request, self()}.

request(Peer, Predecessor) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil};
        {Pkey, Ppid} ->
            Peer ! {status, {Pkey, Ppid}}
end.




notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
	nil ->
	    Keep = handover(Id, Store, Nkey, Npid),%split our Store based on the NKey. Which part should be kept and which part should be handed over to the new predecessor
	    {{Nkey, Npid}, Keep};
	{Pkey, _} ->
	    case key:between(Nkey, Pkey, Id) of
		true ->
		    Keep = handover(Id, Store, Nkey, Npid),%split our Store based on the NKey. Which part should be kept and which part should be handed over to the new predecessor
		    {{Nkey, Npid}, Keep};
		false ->
		    {Predecessor, Store}
	    end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Rest, Keep} = storage:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    Keep.
					
		
			

start(Id) ->
    start(Id, nil).

start(Id, Peer) ->
    timer:start(),
    spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
    Predecessor = nil,
    {ok, Successor} = connect(Id, Peer),
    schedule_stabilize(),
    node(Id, Predecessor, Successor,[]).


connect(Id, nil) ->
	 {ok, {Id, self()}};
connect(Id, Peer) ->
	Qref = make_ref(),
	Peer! {key, Qref, self()},
	receive
		{Qref,Skey}->
			{ok, {Skey, Peer}}
		after ?Timeout ->
            io:format("Time out: no response~n")
end.



		
			


			









		



%% ====================================================================
%% Internal functions
%% ====================================================================



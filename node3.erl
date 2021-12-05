%% @author vasigarans
%% @doc @todo Add description to node2.


-module(node3).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/1,start/2]).
-define(Timeout, 10000).
-define(Stabilize, 1000).
node(Id, Predecessor, Successor,Next,Store) ->
	%io:format("Store: ~p~n", [Store]),
	%io:format('Id ~w   Predecessor~w  Successor~w~n',[Id, Predecessor, Successor]),
    receive
		% a peer needs to know our key
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor,Next,Store);
        {notify, New} ->% a new node informs us of its existence
            {Pred,Store2} = notify(New, Id, Predecessor,Store),
            node(Id, Pred, Successor,Next,Store2);
        {request, Peer} ->%a predecessor needs to know our predecessor
            request(Peer, Predecessor,Successor),
            node(Id, Predecessor, Successor,Next,Store);
        {status, Pred,Nx} ->%our successor informs us about its predecessor
            {Succ,Nxt} = stabilize(Pred,Nx, Id, Successor),
            node(Id, Predecessor, Succ,Nxt,Store);
		 stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor,Next,Store);
		probe ->
	    	create_probe(Id, Successor),
	    	node(Id, Predecessor, Successor,Next,Store);
		{probe, Id, Nodes, T} ->
	    	remove_probe(T, Nodes),
	    	node(Id, Predecessor, Successor,Next,Store);
		{probe, Ref, Nodes, T} ->
	    	forward_probe(Ref, T, Nodes, Id, Successor),
	    	node(Id, Predecessor, Successor,Next,Store);
        {add, Key, Value, Qref, Client} ->
            Added = add(Key, Value, Qref, Client,
                        Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor,Next,Added);
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor,Next, Store);
		{handover, Elements} ->
            Store3 = storage:merge(Store, Elements),
            node(Id, Predecessor, Successor,Next, Store3);
		display ->
	    	io:format("ID: ~w~n", [Id]),
			io:format("Predecessor: ~p, Successor: ~p~n, Next: ~p~n", [Predecessor, Successor,Next]),
			io:format("Store: ~p~n", [Store]),
			node(Id, Predecessor, Successor,Next, Store);
		{'DOWN', Ref, process, _, _} ->
			io:format("SOS ~n",[]),
            {Pred, Succ, Nxt}  = down(Ref, Predecessor, Successor, Next),
            node(Id, Pred, Succ, Nxt, Store);
			
			stop ->
	    ok
end.



down(Ref, {_, Ref, _}, Successor, Next) ->
          {nil,Successor,Next};
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}) ->
          Nref=monitor(process,Npid),
		  self()!stabilize,
    	  {Predecessor, {Nkey, Nref, Npid}, nil}.

create_probe(Id, {_Skey,_, Spid}) ->
    Spid ! {probe, Id, [Id], erlang:system_time()}.

forward_probe(Ref, T, Nodes, Id, {_Skey,_, Spid}) ->
    Spid ! {probe, Ref, [Id | Nodes], T}.

remove_probe(T, Nodes) ->
    Diff = erlang:system_time()-T,
    io:format("Route: ~w~n", [lists:reverse(Nodes)]),
    io:format("Trip time: ~w milli~n", [Diff]).

stabilize(Pred,Next, Id, Successor) ->
    {Skey,Sref, Spid} = Successor,
    case Pred of 
		nil ->
			%nil we should of course inform it about our existence.
			Spid!{notify, {Id, self()}},
				 {Successor,Next};
	  {Id,_}->%pointing back to us we don’t have to do anything.
			{Successor,Next};
		{Skey, Spid} ->%pointing to itself we should of course notify it about our existence.
				Spid ! {notify, {Id, self()}},
				{Successor,Next};
		{Xkey, Xpid} ->
				case key:between(Xkey, Id, Skey) of
							true -> %If the key of the predecessor of our successor (Xkey) is between us and our successor we should of course adopt this node as our successor and run stabilization again.
								demonitor(Sref),
								Xref=monitor(process,Xpid),
								Xpid!{request, self()},
								
								
								self()!stabilize,
								{{Xkey,Xref,Xpid},Successor};

								
							false -> 
								%If we should be in between the nodes we inform our successor of our existence
								Spid!{notify, {Id, self()}},
								{Successor,Next}
						end
					end.

add(Key, Value, Qref, Client, Id, {Pkey, _,_}, {_,_, Spid}, Store) ->
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

lookup(Key, Qref, Client, Id, {Pkey, _,_}, Successor, Store) ->
	case key:between(Key, Pkey, Id) of
		true ->
            Result = storage:lookup(Key, Store),
            Client ! {Qref, Result};
		 false ->
            {_,_,Spid} = Successor,
			Spid ! {lookup, Key, Qref, Client}
    end.
			




schedule_stabilize() ->
	%  stabilize procedure must be done with regular intervals so that new nodes are quickly linked into the ring.
%The procedure schedule stabilize/1 is called when a node is created. When the process receives a stabilize message it will call stabilize/1 procedure.
    timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_,_, Spid}) ->
    Spid ! {request, self()}.

request(Peer, Predecessor,Successor) ->
	%io:format("peer ~w predecessor~w Next~w~n",[Peer, Predecessor,Next]),
	{Skey,_,Spid}=Successor,

    case Predecessor of
        nil ->
            Peer ! {status, nil,{Skey,Spid}};
        {Pkey,_, Ppid} ->
            Peer ! {status, {Pkey, Ppid},{Skey,Spid}}
end.




notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
	nil ->
	    Keep = handover(Id, Store, Nkey, Npid),%split our Store based on the NKey. Which part should be kept and which part should be handed over to the new predecessor
	    Nref=monitor(process,Npid),
		{{Nkey,Nref, Npid}, Keep};
	{Pkey, Pref,_} ->
	    case key:between(Nkey, Pkey, Id) of
		true ->
		    
			
			Keep = handover(Id, Store, Nkey, Npid),%split our Store based on the NKey. Which part should be kept and which part should be handed over to the new predecessor
		    demonitor(Pref),
			Nref=monitor(process,Npid),
			
			
			{{Nkey,Nref, Npid}, Keep};
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
    node(Id, Predecessor, Successor,nil,[]).


connect(Id, nil) ->
	 {ok, {Id, nil,self()}};
connect(Id, Peer) ->
	Qref = make_ref(),
	Peer! {key, Qref, self()},
	receive
		{Qref,Skey}->
			Sref=monitor(process,Peer),
			{ok, {Skey,Sref, Peer}}
		after ?Timeout ->
            io:format("Time out: no response~n")
end.


monitor(Pid) ->
    monitor(process, Pid).
demonitor(nil) ->
    ok;
demonitor(Pid) ->
    demonitor(Pid, [flush]).


		
			


			









		



%% ====================================================================
%% Internal functions
%% ====================================================================



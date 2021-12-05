%% @author vasigarans
%% @doc @todo Add description to node1.


-module(node1).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/1,start/2]).
-define(Timeout, 10000).
-define(Stabilize, 1000).
node(Id, Predecessor, Successor) ->
	%io:format('Id ~w   Predecessor~w  Successor~w~n',[Id, Predecessor, Successor]),
    receive
		% a peer needs to know our key
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor);
        {notify, New} ->% a new node informs us of its existence
            Pred = notify(New, Id, Predecessor),
            node(Id, Pred, Successor);
        {request, Peer} ->%a predecessor needs to know our predecessor
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor);
        {status, Pred} ->%our successor informs us about its predecessor
            Succ = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ);
		 stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor);
		probe ->
	    	create_probe(Id, Successor),
	    	node(Id, Predecessor, Successor);
		{probe, Id, Nodes, T} ->
	    	remove_probe(T, Nodes),
	    	node(Id, Predecessor, Successor);
		{probe, Ref, Nodes, T} ->
	    	forward_probe(Ref, T, Nodes, Id, Successor),
	    	node(Id, Predecessor, Successor);

	
			
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
			Spid!{notify, {Id, self()}},%id:why donti be ur predecssor
				 Successor;
	  {Id,_}->%pointing back to us we don’t have to do anything.
			Successor;
		{Skey, Spid} ->%pointing to itself we should of course notify it about our existence.%id:why donti be ur predecssor
				Spid ! {notify, {Id, self()}},
				Successor;
		{Xkey, Xpid} ->
				case key:between(Xkey, Id, Skey) of
							true -> %If the key of the predecessor of our successor (Xkey) is between us and our successor we should of course adopt this node as our successor and run stabilization again.
								Xpid!{request, self()},%can i consider u  to be my successor
								Pred;

								
							false -> 
								%If we should be in between the nodes we inform our successor of our existence
								Spid!{notify, {Id, self()}},%id:why donti be ur predecssor
								Successor
						end
					end.


schedule_stabilize() ->
	%  stabilize procedure must be done with regular intervals so that new nodes are quickly linked into the ring.
%The procedure schedule stabilize/1 is called when a node is created. When the process receives a stabilize message it will call stabilize/1 procedure.
    timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
    Spid ! {request, self()}.% node sending a {request, self()} message to its successor and then expecting a {status, Pred} in return.

request(Peer, Predecessor) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil};
        {Pkey, Ppid} ->
            Peer ! {status, {Pkey, Ppid}}
end.


notify({Nkey, Npid}, Id, Predecessor) ->
    case Predecessor of
		nil->% If our own predecessor is set to nil then the new node can be predecessor
			{Nkey, Npid};
		{Pkey,  _} ->%if we already have a predecessor we of course have to check if the new node actually should be our predecessor or no
			case key:between(Nkey, Pkey, Id) of
				true->
					{Nkey, Npid};
				false->
					Predecessor
			end
end.
					
		
			

start(Id) ->
    start(Id, nil).

start(Id, Peer) ->
    timer:start(),
    spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
    Predecessor = nil,
    {ok, Successor} = connect(Id, Peer),
    schedule_stabilize(),
    node(Id, Predecessor, Successor).
			

connect(Id, nil) ->
	 {ok, {Id, self()}};
connect(Id, Peer) ->
	Qref = make_ref(),%unique reference
	Peer! {key, Qref, self()},% the first nde needs to know the key is added
	receive
		{Qref,Skey}->
			{ok, {Skey, Peer}}
		after ?Timeout ->
            io:format("Time out: no response~n")
end.
		
			


			


							 

		



%% ====================================================================
%% Internal functions
%% ====================================================================



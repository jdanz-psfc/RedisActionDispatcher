RedisActionDispatcher
===
New distributed dispatchless implementation of MDSplus action server based on REDIS
---
<br>
This is a new implementaion of the MDSplus action dispatcher based on REDIS. The dispatcher itself is no more existing and its functions are directly implemented by a set of Action Servers written in python. Action servers have access to the MDSplus tree and therefore every Action Server instance can derive all the information that is required for the execution of the actions assigned to that server class. <br> However some degree of overall synchronization is required namely:

  - Action server instances must receive commands for building internal dispatch tables and execute phases
  - Non sequential actions are triggered by the termination of other actions, possibly on different Action Servers
  - When more than one Action Server instance for a given server class is defined, it needs to synchronize with the other Action Server(s) of the same class in order to make sure that all actions with lower sequence number have been execute (possibly in another ActionServer instance of the same server class) before incrementing the sequence number.
  - Again, when more than one Action Server instance for a given server class is defined, they must make sure that a given action is executed only by a single action server. 
  - When an action needs to be aborted it is necessary to forward the request to the Action Server doing that action.
<br>
The above functions are now provided by REDIS via the following two REDIS mechanisms:

- *in memory hashes*
- *publish-subscribe channels*
<br>
The shared information on REDIS required for the proper Action Servers synchronization are the following:

 - Action Status Hash, indexed by **{experiment}:{shot}:ActionStatus:{server class}**. The hash key is the action nid and the corresponding value is the current actioin status (NOT_DISPATCHED, DOING, DONE, ERROR, TIMEOUT, ABORTED, STREAMING). This shared information reflects the current action execution status for a given experiment, shot and server class.  
 - AbortRequest, indexed by **{experiment}:{shot}:AbortRequest:{server class}**. The hash key is the action nid and the corresponding value is the abort request (1/0).
<br>
Other information is stored on REDIS, but it is not required by Action Servers, rather by the dispatch monitor (see below).
<br>
The REDIS publish-subscribe (pubsub) channels used for Action Server synchronization are indexed by COMMAND:{Server class} and every Action Server instance subscribes to the corresponding pubsub channel. Observe that Action Server instances of the same server class will subscribe to the same pubsub channel. The messages sent over these pubsub channels may originate from the central supervisor (issuing the phase execution commands) or by the Action Server instances themselves in case they just executed an action that potentially triggers another dependent action. The messages sent over pubsub channels can be:

- **QUIT** forces the Action Server to exit from listening to commands
- **BUILD_TABLES** createds the internal tables (see below). The target Action server will collect from the tree all related action information, including all actions for this server class and actions (for this or other server classes) that may be possibly triggered by such action (see local structure description below)
- **DO_PHASE** Request for phase execution. The target Action Server(s) shall start the execution of sequential actions in the order dictated by the sequence number. In order to share the load among Action ervers of the same server class, once the list of actions for a given sequence number has been selected, the next action to executed is picked choosing one action of the set that has not been yet dispatched, looking the shared REDIS action status information and updating it when an action has been chosen for execution. The current sequence (for a given sequence number) will terminate only when all the actions for that sequence number have DONE or ERROR or ABORTED or TIMEOUT status. In order to avoid race conditions that may arise when concurrently picking an action whose current shared state is NOT_DISPATCHED and updating it as DOING, the REDIS watch mechanism to ensure atomic test and set is used. When an action has been executed, a new UPDATE message is sent to the pubsub channels for all the server classes for which a potentially triggered dependent action is defined.
- **UPDATE:{triggering action nid}** When this message is received, the condition for all potentially affected local dependent actions (i.e. declared for this server class) is checked, looking at the currrent action status in the REDIS shared memory. Whenever the condition is satisfied, the action is executed, still using the REDIS test and set mechanism in order to make sure that only one istance of the Action Server for that server class will execute the dependent action. Observe that dependent actions are executed in parallel with sequential action (executed by the Action Server in sequence).
<br>  

### Local Action Server structures
In order to provide proper action dispatching, the Action Server will hold the following private python structures:
 - ***seqActions*** keeps track of the declared sequential actions for this server class for each phase. *Dictionary{Phase:Dictionary{seqNum:Nid list}}*
 - ***depActions*** keeps track of conditional action (those declared for this action server) and the associated condition  *Dictionary{nid: Dependency}*
 - ***inDependency*** keeps track for each local/global action nid the list of local potentially affected action nids
    *Dictionary{str(nid):list of affected nids}*
- ***outDependency*** keeps track for every local action the list of idents hosting potentially affected actions
*Dictionary{nid: list of affected idents (including self)}*
- ***actTimeout*** keeps track of possible (>0) action timeouts *Dictionary{nid: timeout}*
- ***completionEvent*** keeps track for every action nid for this action server the possible completion event *Dictionary{nid: event name}*

### Action execution: classic

Classic action execution is the mode supported so far by the old MDSplus Dispatcher
<br>
Even if the Action Server executes sequential actions in real sequence, it executes them in a separate thread, later joining the thread in order to continue with the next sequewntial action. A separate thread is required in order to check for action timeout (by using a timeout of .1 seconds in a join loop) and checking possible request for abort in REDIS memory (this check is performed every 0.5 seconds). Conditional actions are executed in a detached thread.

### Action execution: streamed

Streamed actions have been introduced with this version. In order to be streamed, an action must:

- Be declared as a device method
- Have the extended attribute "is_streamed" set to "yes"

If the above conditions are met, the execution occurs in a detached thread, and a set of methods are called for that device:
 ***{method}_init*** is called once first;  ***{method}_step*** is then repeatedly called. This method will return a Python dictionary including at least the key **'is_last'**. If the associated value is false, the loop continues, otherwise ***{method}_finish*** is called and the action terminates. 
 <br>
 Streamed action supports two mechanism for asynchronous dispatch notification:
 
 - **Status Update** triggered by the presence of the **'update'** key (with an associated update string) in the dictionary returned by ***{method}_step***. In this case the Action Server uses the same mechanism for conditional action triggering. Any declared conditional action in the tree can be declared as dependent on a given update for a given streamed action. For this purpose the *less than* operator is used in the condition definition (e.g. "my_update" \< {streamed action}) 
 - **Action Update** that may be triggered during streamed execution. Action updates are declared in the tree as members of the streamed action. The dispatch information for this actions can be of any supported type (conditional or sequential), but the task information shall be a string. This string will be used to define the specific method to be called whenever an action update is triggered. An update action will produce any effect only in the case it is triggered during the target streamed action execution. In this case, as soon as the current ***{method}_step*** terminates for the target device (without signaling termination via 'is_last' key in the returned dictionary), method ***{method}_{update string}*** shall be called once, before calling again ***{method}_step***.

### Web based Dispatch Monitor

A "Proof of Concept" dispatch monitor is provided in order to validate the candidate technology. This dispatch monitor is implemented in node.js and uses the nodejs REDIS interface to access REDIS data and to send commands over REDIS pubsub channels. The node.js application will then implement a Web server exporting dispatching information, using Server Sent Event (SSE) to update the current action status, and triggering the construction of the dispatch tables, the execution of phases and the abort of actions, via AJAX messages sent to the node.js Web server implementaion and then translated by the latter into REDIS memory access or pubsub channel messages. 
Even if not required by Action Servers, additional information is exported over REDIS memory in order to be displayed by the in the dispatch moinitor web page. 

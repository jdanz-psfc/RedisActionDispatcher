RedisActionDispatcher
===
New distributed dispatchless implementation of MDSplus action server based on REDIS
---
<br>
This is a new implementaion of the MDSplus action dispatcher based on REDIS. The dispatcher itself is no more existing and its functions are directly implemented by a set of Action Servers written in python. Action servers have access to the MDSplus tree and therefore every Action Server instance can derive all the information that is required for the execution of the actions assigned to that server class. <br> However som degree of overall synchronization is required namely:

  - Action server instances must receive commands for building internal dispatch tables and execute phases
  - Non sequential actions are triggered by the termination of other actions, possibly on different Action Servers
  - When more than one Action Server instance for a given server class is defined, it needs to synchronize with the other Action Server(s) of the same class in order to make sure that all actions with lower sequence number have been execute (possibly in anothert ActionServer instance) before incrementing the sequence number
  - Again, when more than one Action Server instance for a given server class is defined, they must make sure that a given action is executed by a single action server. 
<br>
The above functions are now provided by REDIS via the following two REDIS mechanisms:
in memory hashes
publish-subscribe channels
<br>

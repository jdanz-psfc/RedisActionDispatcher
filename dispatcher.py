import MDSplus
import redis
import time
import random
import threading
import traceback

    # Shared information on Redis
    # actionStatus : Dictionary {str(nid): status[:message]} indexed by <experiment>:<shot>:ActionStatus>:<ident>
    # abortRequest : Dictionary {str(nid): abort req(1/0) } indexed by <experiment>:<shot>:AbortRequest:<ident>
    # phaseInfo : Set indexed by <experiment>:<shot>:Phases
    # serverInfo :Dictionary {ident: number of instances}: indexed by ActionServers

    # PubSub channels on Redis: 
    # ident: shared by all instances of action servers sharing the same ident. Messages received:
    #  DO_SEQUENCE: starts executing the passed sequence number
    #  DEPEND_UPDATE: check eligibility for local action nids affected by the passed nid
    # dispatcher: unique, subscribed by every action server. Messages receives
    #  BUILD_TABLES: start local information collection based on passed exp+shot
    #  DO_PHASE: starts execution of the passed phase 


treeDEPENDENCY_AND = 10
treeDEPENDENCY_OR = 11
treeDEPENDENCY_OF = 12
treeLOGICAL_AND = 45
treeLOGICAL_OR = 267
treeLOGICAL_OF = 229

ACTION_NOT_DISPATCHED = 0
ACTION_DOING = 1
ACTION_DONE = 2
ACTION_ERROR = 3
ACTION_TIMEOUT = 4
ACTION_ABORT = 5
ACTION_STREAMING= 6


#red = redis.Redis(host='scdevail.rfx.local')
red = redis.Redis(host='localhost')



# return the list of Action nids potentially affected by the execution of this actionNode
def getDepActionNids(actionNode):
    action = actionNode.getData()
    dispatch = action.getDispatch()
    when = dispatch.getWhen()
    if isinstance(when, MDSplus.Compound):
        opcode = when.getOpcode()
        if opcode == treeDEPENDENCY_AND or opcode == treeLOGICAL_AND or opcode == treeDEPENDENCY_OR or opcode == treeLOGICAL_OR:
            if isinstance(when.getArgumentAt(0), MDSplus.TreeNode) or isinstance(when.getArgumentAt(0), MDSplus.TreePath):
                 leftSide = [when.getArgumentAt(0).getNid()]
            elif  isinstance(when.getArgumentAt(0), MDSplus.Compound):
                leftSide = getDepActionNids(when.getArgumentAt(0)) 
            else:
                leftSide = []
            if isinstance(when.getArgumentAt(1), MDSplus.TreeNode) or isinstance(when.getArgumentAt(1), MDSplus.TreePath):
                rightSide = [when.getArgumentAt(1).getNid()]
            elif  isinstance(when.getArgumentAt(1), MDSplus.Compound):
                rightSide = getDepActionNids(when.getArgumentAt(1)) 
            else:
                rightSide = []
            return leftSide + rightSide    
        if opcode ==  treeDEPENDENCY_OF or opcode == treeLOGICAL_OF:
            return [when.getArgumentAt(1).getNid()]
    if isinstance(when, MDSplus.TreeNode) or isinstance(when, MDSplus.TreePath):
        if when.getNid() != None:
            return [when.getNid()] 
    return []


def registerIdent(ident):
    red.hincrby('ActionServers', ident, 1)

def getActionServers():
    keys = red.hkeys('ActionServes')
    retServers = []
    for key in keys:
         retServer.append(key.decode('utf8'))
    return retServers


    # add action as not yet dispatched in redis
def addActionInShared(experiment, shot, ident, actionNid, actionPath, phase):
    red.hset(experiment+':'+str(shot)+':ActionStatus:'+ident, str(actionNid), str(ACTION_NOT_DISPATCHED))
    red.hset(experiment+':'+str(shot)+':ActionPathStatus:'+ident, actionPath, str(ACTION_NOT_DISPATCHED))
    red.hset(experiment+':'+str(shot)+':ActionInfo:'+ident, actionPath, str(actionNid))
    red.hset(experiment+':'+str(shot)+':ActionPhaseInfo:'+ident, actionPath, phase)
    red.hset(experiment+':'+str(shot)+':AbortRequest:'+ident, str(actionNid), 0)
    red.sadd(experiment+':'+str(shot)+':Phases', phase)



# get an undispatched action in the list. -1 is returned if non undispatched
def pickNotYetDispatched(experiment, shot, ident, actionNidList): 
    pending = False
    for actionNid in actionNidList:
        itemid = experiment+':'+str(shot)+':ActionStatus:'+ident
        while True:
            try:
                pipe = red.pipeline()
                pipe.watch(itemid)
                status = int(pipe.hget(itemid, str(actionNid)))
                if status == ACTION_NOT_DISPATCHED:
                    pipe.multi()
                    pipe.hset(itemid, str(actionNid), ACTION_DOING)
                    pipe.execute()
                    pipe.unwatch()
                    return actionNid
                pipe.unwatch()
                if status == ACTION_DOING:
                    pending = True
                break
            except redis.WatchError as exc:
                print('*****************Concurrent access for ident '+ident)
#                print(exc)

    if pending:
        return -1 #all dispatched but some not yet finished
    else: 
        return -2 #all dispatched and finished

def updateStatus(experiment, shot, ident, actionNid, actionPath, status):
    red.hset(experiment+':'+str(shot)+':ActionStatus:'+ident, str(actionNid), str(status))
    red.hset(experiment+':'+str(shot)+':ActionPathStatus:'+ident, actionPath, str(status))
    red.hset(experiment+':'+str(shot)+':AbortRequest:'+ident, str(actionNid), 0)

def checkAbort(experiment, shot, ident, actionNid):
    abortReq = int(red.hget(experiment+':'+str(shot)+':AbortRequest:'+ident, str(actionNid)))
    if abortReq == 1:
        abortReq = red.hset(experiment+':'+str(shot)+':AbortRequest:'+ident, str(actionNid), 0)
        return True
    return False


def clearShared(experiment):
    keys = red.keys(experiment.upper()+'*')
    for key in keys:
        red.delete(key)




class ActionServer:
    # local information
    # seqActions keeps track of the sequential actions for this ident
    # seqActions Dictionary{Phase:Dictionary{seqNum:Nid list}}
    #
    # depActions Dictionary{nid: Dependency}
    #
    # inDependency keeps track for each local/global action nid the list of local potentially affected action nids
    # inDependency Dictionary{str(nid):list of affected nids}
    # 
    # outDependency keeps track for every local action the list of idents hosting potentially affected actions
    # outDependency Dictionary{nid: list of affected idents (including self)}
    #
    # actTimeout keeps track of possible (>0) action timeouts
    # actTimeout Dictionary{nid: timeout}
    #
    # completionEvent keeps track for every action nid for this action server the possible completion event
    # completionEvent Dictionary{nid: event name}
    # 

    def __init__(self, ident):
        self.seqActions = {}
        self.inDependency = {}
        self.outDependency = {}
        self.actTimeout = {}
        self.completionEvent = {}
        self.ident = ident
        self.pubsub = red.pubsub()
        self.pubsub.subscribe('COMMAND:'+ident)
        self.actionExecutor = ActionExecutor(self)
        self.updateMutex = threading.Lock()
        registerIdent(ident)
 
    def buildTables(self, tree):
        print("BUILD TABLES "+ tree.name+'  '+str(tree.shot))
        self.tree = tree
        self.experiment = tree.name
        self.shot = tree.shot
        dd = tree.getNodeWild('***', 'ACTION')
        for d in dd:
            if d.isOn():
                try:
                    disp = d.getData().getDispatch()
                    when = disp.getWhen()
                    phase = disp.getPhase().data()
                    task = d.getData().getTask()
                    if not phase in self.seqActions.keys():
                        self.seqActions[phase] = {}
                    if disp.getIdent() == self.ident: #Consider only local sequential actions
                    #Record timeout
                        try:
                            self.actTimeout[d.getNid()] = float(task.getTimeout().data())
                            print("TIMEOUT FOR "+ d.getPath()+': '+str(self.actTimeout[d.getNid()]))
                        except:
                            self.actTimeout[d.getNid()] = 0.
                    #update shared action status EXCEPT FOR ACTION UPDATES     
                        if not isinstance(d.getData().getTask(), MDSplus.String):   
                            updateStatus(self.experiment, self.shot, self.ident, d.getNid(), d.getPath(), ACTION_NOT_DISPATCHED)

                        if isinstance(when, MDSplus.Scalar):
                            seqNum = int(when.data())
                            if not seqNum in self.seqActions[phase].keys():
                                self.seqActions[phase][seqNum] = []
                            self.seqActions[phase][seqNum].append(d.getNid())
                        addActionInShared(self.experiment, self.shot, self.ident,d.getNid(), d.getPath(), phase)
                    # record completion event if any
                    completionName = disp.getCompletion().getString()
                    if completionName != '':
                        self.completionEvent[d.getNid()] = completionName
                    if not isinstance(when, MDSplus.Scalar): #if it is a dependent action
                        depNids = getDepActionNids(d) #get Affecting nodes
                        if disp.getIdent() == self.ident: #if the affected node is local
                            for depNid in depNids:
                                if not str(depNid) in self.inDependency.keys():
                                    self.inDependency[str(depNid)] = []
                                self.inDependency[str(depNid)].append(d.getNid())

                        depIdent = disp.getIdent()
                        #if(depIdent != self.ident):
                        for depNid in depNids:
                            depNode = MDSplus.TreeNode(depNid, self.tree)
                                #if the affected node is on another action server and the affecting node is on this server
                            if(depNode.getData().getDispatch().getIdent().data() == self.ident): 
                                if not depNode.getNid() in self.outDependency.keys():
                                    self.outDependency[depNode.getNid()] = []
                                if not depIdent in self.outDependency[depNode.getNid()]:
                                    self.outDependency[depNode.getNid()].append(depIdent)
                except Exception as e:
                    print('Error collecting action ' + d.getPath()+ ': '+str(e))


    def handleCommands(self):
        while True:
            while True:
                msg = self.pubsub.get_message(timeout=1)
                if msg != None:
                    break
            if not isinstance(msg['data'], bytes):
                continue
            message = str(msg['data'], 'utf8')
            print('Received Command: '+message)
            if message == 'QUIT':
                return
            if message.startswith('DO_PHASE:'):
                phase = message[9:]
                t = threading.Thread(target=doPhase, args = (self, phase))
                t.start()
            elif message.startswith('UPDATE:'): #when any action potentially affecting a local action has been updated
                actionNidStr = message[7:]
                newTree = self.tree.copy()
                t1 = threading.Thread(target=doUpdate, args = (self, actionNidStr, newTree))
                t1.start()
            elif message.startswith('BUILD_TABLES:'):
                try:
                    cc = message.split(':')
                    treeName = cc[1]
                    shot = int(cc[2])
                    t = MDSplus.Tree(treeName, shot)
                    self.buildTables(t)
                except :
                    traceback.print_exc()


# return True if the dispatching confition is satisfied

    def checkDispatch(self, actionNode):
        action = actionNode.getData()
        dispatch = action.getDispatch()
        when = dispatch.getWhen()
        return self.checkDone(when)

    def checkDone(self, when):
        if isinstance(when, MDSplus.TreeNode):
            nid = when.getNid()
            itemId = self.tree.name+':'+str(self.shot)+':ActionStatus:'+self.ident
            status = (red.hget(itemId, str(nid))).decode('utf8')
            return status == str(ACTION_DONE)
        if isinstance(when, MDSplus.Compound):
            opcode = when.getOpcode()
            if opcode == treeDEPENDENCY_AND or opcode == treeLOGICAL_AND:
                return self.checkDone(when.getArgumentAt(0)) and self.checkDone(when.getArgumentAt(1))
            if opcode == treeDEPENDENCY_OR or opcode == treeLOGICAL_OR:
                return self.checkDone(when.getArgumentAt(0)) or self.checkDone(when.getArgumentAt(1))
            if opcode == treeDEPENDENCY_OF or opcode == treeLOGICAL_OF:
                nid = when.getArgumentAt(1).getNid()
                updateMsg = when.getArgumentAt(0).data()
                itemId = self.tree.name+':'+str(self.shot)+':ActionStatus:'+self.ident
                statuses = str(red.hget(itemId, str(nid)).decode('utf8')).split(':')
                return len(statuses) == 2 and statuses[0] == str(ACTION_STREAMING) and statuses[1] == updateMsg
        return False
                                

def doUpdate(dispatchTable, actionNidStr, tree):
    if actionNidStr in dispatchTable.inDependency.keys():
        affectedNids = dispatchTable.inDependency[actionNidStr]
        toDoNids = []
        actionUpdateNids = []
        for affectedNid in affectedNids:
            affectedNode = MDSplus.TreeNode(affectedNid, tree)
            if dispatchTable.checkDispatch(affectedNode):
# Action updates are not picked by the shared redis memory, but are always passed to the action executor
# only the action executor for the action server currently running it will delievr the action update
                if isinstance(affectedNode.getData().getTask(), MDSplus.String):   
                    actionUpdateNids.append(affectedNid)
                else:             
                    toDoNids.append(affectedNid)
        if len(toDoNids) + len(actionUpdateNids) > 0:
            while True:
                currActionNid = pickNotYetDispatched(dispatchTable.experiment, dispatchTable.shot, dispatchTable.ident, toDoNids)
                if currActionNid < 0:
                    break
                ########################
                currActionNode = MDSplus.TreeNode(currActionNid, dispatchTable.tree)
                dispatchTable.actionExecutor.doAction(dispatchTable.ident, dispatchTable.tree, currActionNode, dispatchTable.actTimeout[currActionNid])
                ###########################
                dispatchTable.updateMutex.acquire()
                if currActionNid in dispatchTable.outDependency.keys():
                    for outIdent in dispatchTable.outDependency[currActionNid]:
                        red.publish('COMMAND:'+dispatchTable.ident, 'UPDATE:'+str(currActionNid))
                dispatchTable.updateMutex.release()
            # do the same for action updates
            for currActionNid in actionUpdateNids:
                ########################
                currActionNode = MDSplus.TreeNode(currActionNid, dispatchTable.tree)
                dispatchTable.actionExecutor.doAction(dispatchTable.ident, dispatchTable.tree, currActionNode, dispatchTable.actTimeout[currActionNid])
                ###########################
                # action updates DO NOT TRIGGER other actions
                ###########################

def doPhase(dispatchTable, phase):
    seqNums = []
    for seqNum in dispatchTable.seqActions[phase].keys():
        seqNums.append(seqNum)
    seqNums.sort()
    for currSeqNum in seqNums:
        actionNids = dispatchTable.seqActions[phase][currSeqNum].copy()
        while True:
            currActionNid = pickNotYetDispatched(dispatchTable.experiment, dispatchTable.shot, dispatchTable.ident, actionNids)
            if currActionNid == -1: #all actions in the list dispatched but some not yet finished
                while currActionNid != -2:
                    time.sleep(0.1)
                    currActionNid = pickNotYetDispatched(dispatchTable.experiment, dispatchTable.shot, dispatchTable.ident, actionNids)
            if currActionNid == -2:
                break
            currActionNode = MDSplus.TreeNode(currActionNid, dispatchTable.tree)
            actionNids.remove(currActionNid)
            ######################
            dispatchTable.actionExecutor.doAction(dispatchTable.ident, dispatchTable.tree, currActionNode, dispatchTable.actTimeout[currActionNid])
            ###########################
            dispatchTable.updateMutex.acquire()
            if currActionNid in dispatchTable.outDependency.keys():
                for outIdent in dispatchTable.outDependency[currActionNid]:
                    red.publish('COMMAND:'+dispatchTable.ident, 'UPDATE:'+str(currActionNid))
            dispatchTable.updateMutex.release()


#################################################################
# aCTION STUFF
#################################################################
class ActionExecutor:
    class Worker(threading.Thread):
        def __init__(self, tree, node, dispatchTable):
            super().__init__()
            self.tree = tree
            self.node = node
            self.dispatchTable = dispatchTable

        def run(self):
            task = self.node.getData().getTask()
            if isinstance(task, MDSplus.Program) or isinstance(task, MDSplus.Procedure or isinstance(task, MDSplus.Routine)):
                self.tree.copy().tcl('do '+self.path)
                self.status = 1
            elif isinstance(task, MDSplus.Method):
                try:
                    self.status = int(task.getObject().doMethod(task.getMethod()).data())
                except Exception as exc:
                   # traceback.print_exception(exc)
                    self.status = 0
            else:
                try:
                    self.status = int(task.data())
                except Exception as exc:
                #    traceback.print_exception(exc)
                    self.status = 0

    class StreamedWorker(threading.Thread):
        def __init__(self, tree, device, actionName, actionNid, actionPath, dispatchTable, streamedWorkers, objectNid):
            super().__init__()
            self.tree = tree
            self.device = device
            self.actionName = actionName
            self.actionNid = actionNid
            self.actionPath = actionPath
            self.dispatchTable = dispatchTable
            self.streamedWorkers = streamedWorkers
            self.objectNid = objectNid
            self.updateAction = ''
        def run(self):
            updateStatus(self.tree.name, self.tree.shot, self.dispatchTable.ident, self.actionNid, self.actionPath, str(ACTION_STREAMING))
            try:
                status = int(self.device.doMethod(self.actionName+'_init'))
                if (status % 2) == 0: 
                    self.dispatchTable.updateMutex.acquire()
                    updateStatus(self.tree.name, self.tree.shot, self.dispatchTable.ident, self.actionNid, self.actionPath, ACTION_ERROR)
                    self.dispatchTable.updateMutex.release()
                    return
                if self.actionNid in self.dispatchTable.completionEvent.keys():
                    completionEvent = self.dispatchTable.completionEvent[self.actionNid]
                else:
                    completionEvent = ''
                while(True):
                    if self.updateAction != '':
                        try:
                            retDict = self.device.doMethod(self.actionName+'_'+self.updateAction)
                            if 'status' in retDict.keys() and retDict['status'] % 2 == 0:
                                self.dispatchTable.updateMutex.acquire()
                                updateStatus(self.tree.name, self.tree.shot, self.dispatchTable.ident, self.actionNid, self.actionPath, ACTION_ERROR)
                                self.dispatchTable.updateMutex.release()
                                return
                            self.updateAction = ''
                        except Exception as exc:
                            print('Error in Action update '+self.updateAction+': '+str(exc))
                            self.updateAction = ''
                            traceback.print_exception(exc)
                    retDict = self.device.doMethod(self.actionName+'_step')
                    if 'status' in retDict.keys() and retDict['status'] % 2 == 0:
                        self.dispatchTable.updateMutex.acquire()
                        updateStatus(self.tree.name, self.tree.shot, self.dispatchTable.ident, self.actionNid, self.actionPath, ACTION_ERROR)
                        self.dispatchTable.updateMutex.release()
                        return
                    if completionEvent != '':
                        MDSplus.Event.setevent(completionEvent)
                    if 'update' in retDict.keys():
                        self.dispatchTable.updateMutex.acquire()
                        updateStatus(self.tree.name, self.tree.shot, self.dispatchTable.ident, self.actionNid,self.actionPath,  str(ACTION_STREAMING)+':'+retDict['update'])
                        if self.actionNid in self.dispatchTable.outDependency.keys():
                            for outIdent in self.dispatchTable.outDependency[self.actionNid]:
                                red.publish('COMMAND:'+self.dispatchTable.ident, 'UPDATE:'+str(self.actionNid))
                        self.dispatchTable.updateMutex.release()

                    if 'is_last' in retDict.keys() and retDict['is_last']:
                        status = int(self.device.doMethod(self.actionName+'_finish')) 
                        self.dispatchTable.updateMutex.acquire()
                        if (status % 2) == 0:
                            updateStatus(self.tree.name, self.tree.shot, self.dispatchTable.ident, self.actionNid, self.actionPath, ACTION_ERROR)
                        else:
                            updateStatus(self.tree.name, self.tree.shot, self.dispatchTable.ident, self.actionNid, self.actionPath, ACTION_DONE)
                            if self.actionNid in self.dispatchTable.outDependency.keys():
                                for outIdent in self.dispatchTable.outDependency[self.actionNid]:
                                    red.publish('COMMAND:'+self.dispatchTable.ident, 'UPDATE:'+str(self.actionNid))
                            if completionEvent != '':
                                MDSplus.Event.setevent(completionEvent)
                        del self.streamedWorkers[int(self.objectNid)]
                        self.dispatchTable.updateMutex.release()
                        return    
            except Exception as exc:
                traceback.print_exception(exc)
 


    def __init__(self, dispatchTable):
        self.mutex = threading.Lock()
        self.dispatchTable = dispatchTable
        self.streamedWorkers = {}

    def isStreamed(self, tree, actionNode):
        if not isinstance(actionNode.getData().getTask(), MDSplus.Method):
            return False
        if actionNode.getExtendedAttribute('is_streamed') == 'yes':
            return True
        return False    

    def doAction(self, ident, tree, node, timeout = 0):
        if self.isStreamed(tree, node):
            self.doActionStreamed(ident, tree, node)
        elif isinstance(node.getData().getTask(), MDSplus.String):
# the action update will be executed only if the target action is currently running
            parentAct = node.getParent()
            if parentAct.getLength() > 0 and isinstance(parentAct.getData(), MDSplus.Action) and isinstance(parentAct.getData().getTask(), MDSplus.Method):
                targetActNid = (parentAct.getData().getTask().getObject()).getNid()
                if targetActNid in self.streamedWorkers.keys():
                    self.streamedWorkers[targetActNid].updateAction = node.getData().getTask().data()
                    updateStatus(tree.name, tree.shot, ident, node.getNid(), node.getPath(), ACTION_DONE)
               # else: --Do Not update status if not found singe another instance of this ident may do the stuff
               #     updateStatus(tree.name, tree.shot, ident, node.getNid(), node.getPath(), ACTION_ERROR)

        else:
            return self.doActionNonStreamed(ident, tree, node, timeout)
        
    def doActionStreamed(self, ident, tree, node):
        task = node.getData().getTask()
        worker = self.StreamedWorker(tree, task.getObject(), task.getMethod().data(), node.getNid(), node.getPath(), self.dispatchTable, self.streamedWorkers, task.getObject().getNid())
        self.streamedWorkers[task.getObject().getNid()] = worker
        worker.start()
        
    def doActionNonStreamed(self, ident, tree, node, timeout = 0):
        self.mutex.acquire()
        worker = self.Worker(tree, node, self.dispatchTable)
        self.dispatchTable.updateMutex.acquire()
        updateStatus(tree.name, tree.shot, ident, node.getNid(), node.getPath(), ACTION_DOING)
        self.dispatchTable.updateMutex.release()
        worker.start()
        actionTime = 0.
        abortTime = 0.5
        while True:
            worker.join(timeout = 0.1)
            if not worker.is_alive():
                self.dispatchTable.updateMutex.acquire()
                if (worker.status % 2) == 0: 
                    updateStatus(tree.name, tree.shot, ident, node.getNid(), node.getPath(), ACTION_ERROR)
                else:
                    updateStatus(tree.name, tree.shot, ident, node.getNid(), node.getPath(), ACTION_DONE)
                    if node.getNid() in self.dispatchTable.completionEvent.keys():
                        MDSplus.Event.setevent(self.dispatchTable.completionEvent[node.getNid()])
                self.dispatchTable.updateMutex.release()     
                self.mutex.release()
                return worker.status
            actionTime += 0.1
            if timeout > 0 and actionTime > timeout:
                print('TIMEOUT')
                self.dispatchTable.updateMutex.acquire()
                updateStatus(tree.name, tree.shot, ident, node.getNid(), node.getPath(), ACTION_TIMEOUT)
                self.dispatchTable.updateMutex.release()
                self.mutex.release()
                return 0
            if actionTime > abortTime:
                abortTime = actionTime + 0.5
                if checkAbort(tree.name, tree.shot, ident, node.getNid()):
                    print('ABORT')
                    self.dispatchTable.updateMutex.acquire()
                    updateStatus(tree.name, tree.shot, ident, node.getNid(), node.getPath(), ACTION_ABORT)
                    self.dispatchTable.updateMutex.release()
                    self.mutex.release()
                    return 0












def test():
#    t = MDSplus.Tree('action', -1)
#    t.createPulse(1)
#    t = MDSplus.Tree('action', 1)
#    red.delete('ACTION:1:ActionStatus:my_server')
    act = ActionServer('my_server')
#    act.buildTables(t) 
    act.handleCommands()




                        





                






package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Client.Response;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
    final static int T = 20; //Timeout in second for read and write
    private final Map<Integer, ActorRef> nodes = new TreeMap<Integer, ActorRef>(); //Map between key and their nodes in DKVS
    private final MapElements elements = new MapElements();  //Object maintained by the node
    private final Map<Integer, Boolean> busy = new HashMap<Integer, Boolean>(); //Map that indicates if a nodes is writing on an element or not
    private final Set<Integer> to_be_updated = new HashSet<>(); // set of the indexes that remain to be updated after join
    private final Map<Integer, Req> waitC = new HashMap<Integer, Req>(); //Map between the key and the waiting request of a client
    private final Map<Integer, Integer> replication_indexes = new HashMap<>();
    private final Map<Integer, ArrayList<Integer>> versionMap = new TreeMap<>();
    boolean isRecovering = false;
    boolean isJoining = false;
    int key; //Key of the object
    int count; //Counter of the request this node send as coordinator
    private int last_update_replication_indexes_hash = 0; // hash of the previous time the replication indexes changed

    public Node(int id) {
        this.key = id;
        this.count = 0;
    }

    static public Props props(int id) {
        return Props.create(Node.class, () -> new Node(id));
    }

    /**
     * Updates the Map of node with the given one. The nodes in the actual Map not found in the given one will be
     * removed. The nodes present in both Map will be updated in the local one. The nodes present in the new Map will be
     * added to the local one
     *
     * @param new_rout the new Map of nodes
     */
    public void update_remove_nodes(Map<Integer, ActorRef> new_rout) {
        Utils.update_remove(nodes, new_rout);
    }

    /**
     * Updates the Map of nodes with the given one. It is as update_remove, but it doesn't remove nodes from local Map
     *
     * @param new_rout the new Map of nodes
     */
    public void update_nodes(Map<Integer, ActorRef> new_rout) {
        Utils.update_remove(nodes, new_rout);
    }

    /**
     * Given the key of some data, find and return the reference of the node responsible for it
     *
     * @param key the key of the data to search the responsible node
     * @return the reference of the node responsible for the data
     */
    public ActorRef get_responsible_node(Integer key) {
        Integer neighbour_id = -1;
        List<Integer> ordered_id = new ArrayList<>(nodes.keySet());
        ordered_id.sort(Comparator.reverseOrder());
        for (Integer i : ordered_id) {
            if (key < i) {
                neighbour_id = i;
                break;
            }
        }
        if (neighbour_id == -1)
            neighbour_id = ordered_id.get(0);
        return this.nodes.get(neighbour_id);
    }

    /**
     * Get the neighbour of this node
     *
     * @return the reference of the neighbour node of this node
     */
    public ActorRef get_neighbour() {
        List<Integer> ordered_id = new ArrayList<>(nodes.keySet());
        ordered_id.sort(Comparator.naturalOrder());
        for (Integer i : ordered_id) {
            if (key < i) {
                return nodes.get(i);
            }
        }
        return nodes.get(0); // if there is no bigger node, you have to take the first one in the list (smaller)
        // end of ring
    }

    /**
     * Function that checks that the data stored in the node is updated w.r.t. the replication indexes.
     * If not, it asks the responsible node the data to replicate, and updates it.
     */
    public void update_replication() {
        if (last_update_replication_indexes_hash != 0 &&
                last_update_replication_indexes_hash == replication_indexes.hashCode()) {
            // if replciation indexes didn't change since last update, then do nothing
            return;
        }

        // remove all replicated data for simplicity
        remove_replicated_data();

        // Request new replicated data to responsible node. For each replication index ask data to node
        for (int index : replication_indexes.keySet()) {
            nodes.get(index).tell(new ReplicationRequest(), getSelf());
        }

        last_update_replication_indexes_hash = replication_indexes.hashCode();
    }

    /**
     * Removes all the replicated data from the element map, keeping only the data the node is responsible for.
     */
    public void remove_replicated_data() {
        MapElements responsibleElements = get_responsible_elements();

        elements.clear();
        elements.putAll(responsibleElements);
    }

    /**
     * Get the elements this node is responsible for, excluded replication elements
     *
     * @return a Map containing the elements this node is responsible for
     */
    public MapElements get_responsible_elements() {
        //System.out.println("responsible elements of node " + key + " are " + elements.get_range(get_preceding_id(), key).keySet().toString() );
        return elements.get_range(get_preceding_id(), key);
    }

    /**
     * Returns the reference of the node preceding this node in the ring
     *
     * @return the reference of the node preceding this node in the ring
     */
    public ActorRef get_preceding() {
        List<Integer> ordered_id = new ArrayList<>(nodes.keySet());
        ordered_id.sort(Comparator.reverseOrder());
        for (Integer i : ordered_id) {
            if (key > i) {
                return nodes.get(i);
            }
        }
        return nodes.get(nodes.size() - 1); // if there is no smaller node, you have to take the last one in the list
        // end of ring
    }

    /**
     * Get the node that precedes the given node id
     *
     * @param id the id of the node to get the preceding one
     * @return the id of the preceding node
     */
    public int get_preceding_id(int id) {
        List<Integer> ordered_id = new ArrayList<>(nodes.keySet());
        ordered_id.sort(Comparator.reverseOrder());
        for (Integer i : ordered_id) {
            if (id > i) {
                return i;
            }
        }
        return ordered_id.get(0); // if there is no smaller node, you have to take the last one in the list
        // end of ring
    }

    /**
     * Get n preceding nodes of this node
     *
     * @param count the number of preceding nodes to take
     * @return the list of preceding nodes
     */
    public List<Integer> get_precedings_id(int count) {
        if (count > main.N) {
            throw new RuntimeException("invalid input, bigger than N");
        }

        List<Integer> res = new ArrayList<>();

        Integer prec_id = get_preceding_id();
        res.add(prec_id);

        int c = count - 1;
        while (c != 0) {
            prec_id = get_preceding_id(prec_id);
            res.add(prec_id);
            c--;
        }

        return res;
    }

    /**
     * Returns the preceding node in the ring, returning its id
     *
     * @return the id of the preceding node
     */
    public int get_preceding_id() {
        List<Integer> ordered_id = new ArrayList<>(nodes.keySet());
        ordered_id.sort(Comparator.reverseOrder());
        for (Integer i : ordered_id) {
            if (key > i) {
                return i;
            }
        }
        return ordered_id.get(0); // if there is no smaller node, you have to take the last one in the list
        // end of ring
    }

    /**
     * Handles start message
     *
     * @param msg JoinGroupMsg
     */
    private void onJoinGroupMsg(JoinGroupMsg msg) {
        //Add map between key and their nodes in DKVS
        for (Map.Entry<Integer, ActorRef> entry : msg.group.entrySet()) {
            Integer key = entry.getKey();
            ActorRef value = entry.getValue();
            this.nodes.put(key, value);
        }

        // fill replication indexes
        update_replication_indexes();

        if (msg.start_elements != null) {
            this.elements.putAll(msg.start_elements);
            update_replication();
        }
    }

    /**
     * Handles a replication response message, by updating the elements Map with the received data.
     *
     * @param msg the ReplicationResponse msg
     */
    private void onReplicationResponse(ReplicationResponse msg) {
        elements.update(msg.new_elements);
        System.out.println("Node " + key + " received replication response with elements: " + msg.new_elements.keySet());
    }

    /**
     * Handles a replication request, by gathering the elements this node is responsible for and sending them to the
     * node that sent the request
     *
     * @param msg ReplicationRequest message
     */
    private void onReplicationRequest(ReplicationRequest msg) {
        // send this node's elements (no replicated data)
        sender().tell(new ReplicationResponse(get_responsible_elements()), getSelf());
    }

    /**
     * Handling message for write operation from client
     *
     * @param msg
     */
    private void onchange(Change msg) {
        waitC.put(count, new Req(getSender(), msg.key)); //Add new waiting request
        waitC.get(count).value = msg.value;
        //Handling to send read request to the N replicas
        //ActorRef va = null;
        int key;
        int i = 0; //Counter of replica found
        //boolean first = true; //Handling the first
        for (Map.Entry<Integer, ActorRef> entry : this.nodes.entrySet()) {
            key = entry.getKey();
            ActorRef value = entry.getValue();
            if (key >= msg.key) {
                /*if (first) {
                    va.tell(new Readforwrite(msg.key, count), getSelf());
                    waitC.get(count).repl.add(va);
                    i++;
                    first = false;
                }*/
                if (i < main.N) {
                    value.tell(new Readforwrite(msg.key, count), getSelf());
                    waitC.get(count).repl.add(value);
                    i++;
                } else {
                    break;
                }
            }
            //va = value;
        }
        //Handling the case it was visited all nodes and it wasn't covered all replicas
        if (msg.key < main.RANGE && i < main.N) {
            for (Map.Entry<Integer, ActorRef> entry : this.nodes.entrySet()) {
                if (i >= main.N) {
                    break;
                }
                key = entry.getKey();
                ActorRef value = entry.getValue();
                value.tell(new Readforwrite(msg.key, count), getSelf());
                waitC.get(count).repl.add(value);
                i++;
            }
        }
        count++; //Raise number of waiting request
        //Set the timeout to notify if after a period it isn't received W answers
        getContext().system().scheduler().scheduleOnce(
                Duration.create(T * 1000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutW(count - 1, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    /**
     * Handling message for read operation from client
     *
     * @param msg
     */
    private void onretrive(Retrive msg) {
        waitC.put(count, new Req(getSender(), msg.key)); //Add new waiting request
        //Handling to send read request to the N replicas
        //ActorRef va = null;
        int key;
        int i = 0; //Counter of replica found
        //boolean first = true;
        for (Map.Entry<Integer, ActorRef> entry : this.nodes.entrySet()) {
            key = entry.getKey();
            ActorRef value = entry.getValue();
            if (key >= msg.key) {
                /*if (first) {
                    va.tell(new read(msg.key, count), getSelf());
                    i++;
                    first = false;
                }*/
                if (i < main.N) {
                    value.tell(new Read(msg.key, count), getSelf());
                    i++;
                } else {
                    break;
                }
            }
            //va = value;
        }
        //Handling the case it was visited all nodes and it wasn't covered all replicas
        if (msg.key < main.RANGE && i < main.N) {
            for (Map.Entry<Integer, ActorRef> entry : this.nodes.entrySet()) {
                if (i >= main.N) {
                    break;
                }
                key = entry.getKey();
                ActorRef value = entry.getValue();
                value.tell(new Read(msg.key, count), getSelf());
                i++;
            }
        }
        count++; //Raise number of waiting request
        //Set the timeout to notify if after a period it isn't received R answers
        getContext().system().scheduler().scheduleOnce(
                Duration.create(T * 1000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutR(count - 1, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    /**
     * Handle message from coordinator to read the version of a certain object for write operation
     *
     * @param msg
     */
    private void onreadforwrite(Readforwrite msg) {
        if (this.busy.containsKey(msg.key)) { //Check if the object is already in other write operation
            if (this.busy.get(msg.key)) {
                return;
            }
        }
        Pair<String, Integer> e = null;
        if (this.elements.containsKey(msg.key)) {
            e = this.elements.get(msg.key);
        }
        this.busy.put(msg.key, true);
        if (e == null) {
            e = new Pair("prova", -1);
            //System.out.println("ON msg " + msg.key + " write " + " countreq " + msg.count + " vers " + e.getValue());
            //element.put(msg.key,e);
        }
        if (e != null) {
            getSender().tell(new ResponseRFW(e.getValue(), msg.count, msg.key), getSelf());
        }
    }

    /**
     * Handle message from coordinator to read a certain object for read operation
     *
     * @param msg
     */
    private void onread(Read msg) {
        if (this.busy.containsKey(msg.key)) {//Check if the object is already in other write operation
            if (this.busy.get(msg.key)) {
                return;
            }
        }
        Pair<String, Integer> e = null;
        if (this.elements.containsKey(msg.key)) {
            e = elements.get(msg.key);
        }

        if (e != null) {
            getSender().tell(new ResponseRead(e, msg.count, msg.key), getSelf());
        }
    }

    /**
     * Find in list of objects the one with the maximum version
     *
     * @param l
     * @return
     */
    private Pair<String, Integer> max(List<Pair<String, Integer>> l) {
        int max = -1;
        Pair<String, Integer> pa = new Pair("0", 0);
        for (Pair<String, Integer> p : l) {
            if (p.getValue() >= max) {
                pa.setValue(p.getValue());
                pa.setKey(p.getKey());
                max = p.getValue();
            }
        }
        return pa;
    }

    /**
     * Handling the answer from nodes for read operation
     *
     * @param msg
     */
    private void onresponseRead(ResponseRead msg) {
        if (waitC.get(msg.count) != null && waitC.get(msg.count).success && waitC.get(msg.count).key == msg.key) {
            System.out.println("msg " + msg.key + " count " + waitC.get(msg.count).count + " read " + " countreq " + msg.count);
            waitC.get(msg.count).respo.add(msg.e);
            if (waitC.get(msg.count).count >= main.R - 1) {
                waitC.get(msg.count).timeout = false;
                waitC.get(msg.count).success = false;
                waitC.get(msg.count).a.tell(new Response(max(waitC.get(msg.count).respo), true, msg.key, "read"), getSelf());
            }
            waitC.get(msg.count).count++;
        }
    }

    /**
     * Find in list of versions the one with maximum
     *
     * @param l
     * @return
     */
    private Integer maxI(List<Integer> l) {
        int max = -1;
        for (Integer i : l) {
            if (i > max) {
                max = i;
            }
        }
        return max;
    }

    /**
     * Handling the answer from nodes for write operation
     *
     * @param msg
     */
    private void onresponseRFW(ResponseRFW msg) {
        if (waitC.get(msg.count) != null && waitC.get(msg.count).success && waitC.get(msg.count).key == msg.key) {
            System.out.println("msg " + msg.key + " count " + waitC.get(msg.count).count + " write " + " countreq " + msg.count + " vers " + msg.ver);
            waitC.get(msg.count).version.add(msg.ver);
            if (waitC.get(msg.count).count >= main.W - 1) {
                waitC.get(msg.count).timeout = false;
                waitC.get(msg.count).success = false;
                int maxV = maxI(waitC.get(msg.count).version);
                maxV++;
                waitC.get(msg.count).a.tell(new Response(new Pair(waitC.get(msg.count).value, maxV), true, msg.key, "write"), getSelf());
                for (ActorRef r : waitC.get(msg.count).repl) {
                    r.tell(new Write(maxV, waitC.get(msg.count).value, msg.key), getSelf());
                }
            }
            waitC.get(msg.count).count++;
        }
    }

    /**
     * Handling the write operation from coordinator
     *
     * @param msg
     */
    private void onwrite(Write msg) {
        this.elements.put(msg.key, new Pair(msg.value, msg.ver));
        if (this.versionMap.containsKey(msg.key)) {
            this.versionMap.get(msg.key).add(msg.ver);
        } else {
            this.versionMap.put(msg.key, new ArrayList<Integer>());
            this.versionMap.get(msg.key).add(msg.ver);
        }
        this.busy.put(msg.key, false);
    }

    /**
     * Handling the timeout for read operation
     *
     * @param msg
     */
    private void onTimeoutR(TimeoutR msg) {
        if (waitC.get(msg.count).timeout) {
            System.out.println("TIMEOUTR");
            waitC.get(msg.count).a.tell(new Response(null, false, msg.key, "read"), getSelf());
            waitC.get(msg.count).success = false;
        }
    }

    /**
     * Handling the timeout for write operation
     *
     * @param msg
     */
    private void onTimeoutW(TimeoutW msg) {
        if (waitC.get(msg.count).timeout) {
            System.out.println("TIMEOUTW");
            waitC.get(msg.count).a.tell(new Response(null, false, msg.key, "write"), getSelf());
            waitC.get(msg.count).success = false;
            for (ActorRef a : waitC.get(msg.count).repl) { //UnLock every node from write operation
                a.tell(new Unlock(msg.key), getSelf());
            }
        }
    }

    /**
     * Msg sent by the main, to tell to a joining node who is his bootstrapper
     *
     * @param msg the JoinNode message
     */
    private void onJoinNode(JoinNode msg) {
        msg.bootstrapper.tell(new JoinRequest(), getSelf());
        this.replication_indexes.putAll(msg.replication_index);
    }

    /**
     * When a join request is received, this means that this node is now a bootstrapper for a new node
     *
     * @param msg
     */
    private void onJoinRequest(JoinRequest msg) {
        sender().tell(new JoinResponse(this.nodes), getSelf());
    }

    /**
     * Processing the Join Response sent by the bootstrapper. It finds the nearest neighbour, asking him the
     * keys this node is responsible for
     *
     * @param msg
     */
    private void onJoinResponse(JoinResponse msg) {
        update_remove_nodes(msg.nodes); // updates the list of all nodes from the bootstrapper Join Response
        ActorRef neighbour = get_neighbour();
        // ask data to neighbour
        neighbour.tell(new DataRequest(key), self());

        update_replication_indexes();
        // update replication done on on announce
    }

    /**
     * Handles a data request message. If received, a node needs some data, this function gather that data and responds
     * with it
     *
     * @param msg
     */
    private void onDataRequest(DataRequest msg) {
        // get preceding works because the new node is not present yet
        MapElements selected = elements.get_range(get_preceding_id(msg.id), msg.id);
        sender().tell(new DataResponse(selected), getSelf());
    }

    /**
     * Called when a data response message is received. It updates the joining node with the new elements of which he is
     * responsible. Then, for each element, he does a read, to take the updated values if any.
     *
     * @param msg
     */
    private void onDataResponse(DataResponse msg) {
        isJoining = true; // needed in onResponse to differentiate from a recovery
        elements.update_remove(msg.data);
        // if there are no elements skip the read and announce itself
        if (msg.data.isEmpty()) {
            // announce to all nodes
            for (ActorRef n : nodes.values()) {
                n.tell(new AnnounceNode(this.key, self()), self());
            }
            update_replication();
            return;
        }
        // check the data with a read
        for (Integer key : elements.keySet()) {
            ActorRef resp_node = get_responsible_node(key);
            resp_node.tell(new Retrive(key), self());
        }
        //add to a temporary list
        to_be_updated.addAll(msg.data.keySet());
    }

    /**
     * This handles responses received from a read request done after a node joins the circle and reads all the elements
     *
     * @param msg
     */
    private void onResponse(Client.Response msg) {
        if (!msg.success || msg.op.equals("read")) {
            // TODO ?
        } else {
            elements.put_if_newer(msg.key, msg.p);
        }
        to_be_updated.remove(msg.key);
        if (to_be_updated.isEmpty()) { // if all elements to be updated have been updated, then announce
            // announce node to others
            if (isJoining) {
                // Announce node to all the others
                for (ActorRef n : nodes.values()) {
                    n.tell(new AnnounceNode(this.key, self()), self());
                }
                update_replication();
                isJoining = false;
            } else if (isRecovering) {
                isRecovering = false;
            }

        }
    }

    /**
     * Handles the receiving of an AnnounceNode message. Upon this, the new node should be added to the list of nodes of
     * this node.
     *
     * @param msg
     */
    private void onAnnounceNode(AnnounceNode msg) {
        // add new joined node
        this.nodes.put(msg.key, msg.new_node); // not sender
        // Update the replication indexes:
        update_replication_indexes();
        update_replication(); // updates values

        /* if the preceding node is the one joining, this node is his neighbour, and should remove the elements
           it is not responsible anymore. This is implicitly accomplished in the update_replication function */
    }

    /**
     * Handling unlock of object from write operation because of the timeout
     *
     * @param msg
     */
    private void onunlock(Unlock msg) {
        busy.put(msg.key, true);
    }

    /**
     * Print objects maneged by the node
     *
     * @param msg
     */
    private void onprintElem(PrintElem msg) {
        System.out.println("ID node:" + this.key);
        System.out.println("Objects in the node: ------------");

        // print responsible nodes
        MapElements responsible = get_responsible_elements();
        System.out.println("Responsible elements:");
        for (Map.Entry<Integer, Pair<String, Integer>> entry : responsible.entrySet()) {
            System.out.println("idN:" + this.key + " idE:" + entry.getKey() + " value:" + entry.getValue().getKey() + " version:" + entry.getValue().getValue());
        }

        System.out.println("Replicated elements:");
        for (Map.Entry<Integer, Pair<String, Integer>> entry : elements.entrySet()) {
            if (!responsible.containsKey(entry.getKey())) {
                System.out.println("idN:" + this.key + " idE:" + entry.getKey() + " value:" + entry.getValue().getKey() + " version:" + entry.getValue().getValue());
            }
        }

        System.out.println("Version list:");
        for (Map.Entry<Integer, ArrayList<Integer>> entry : this.versionMap.entrySet()) {
            System.out.println("idN:" + this.key + " idE:" + entry.getKey());
            for (Integer version : entry.getValue()) {
                System.out.println("version:" + version);
            }
        }
        System.out.println("---------------------------------");
    }

    /**
     * Processes a leave request message
     *
     * @param msg
     */
    private void onLeaveRequest(LeaveRequest msg) {
        // announce to all nodes that the node is leaving

        for (ActorRef node : nodes.values()) {
            node.tell(
                    new NodeLeavingInfo(
                            get_responsible_elements(),
                            this.key),
                    getSelf()
            );
        }
    }

    /**
     * Processes a LeavingInfo message by removing the node from the list of nodes and updating the replication indexes
     *
     * @param msg
     */
    private void onNodeLeavingInfo(NodeLeavingInfo msg) {
        if (msg.key == key)
            // msg returned to leaving node
            return;

        nodes.remove(msg.key); // remove node that leaved

        if (msg.new_elements != null) {
            elements.update(msg.new_elements); // update the data of the old element (if present)
        }

        update_replication_indexes(); // update this node's replication indexes with the updates
        // received from the message

        update_replication(); // update replicated data
    }

    /**
     * Handles the RecoveryRequest message, upon receiving it the node updates wrt the ring and comes back from crash
     *
     * @param msg
     */
    public void onRecovery_request(RecoveryMsg msg) {
        //Return to normal behaviour
        getContext().become(createReceive());

        //CLear nodes map and add map between keys and their nodes in DKVS
        this.nodes.clear();
        for (Map.Entry<Integer, ActorRef> entry : msg.nodes.entrySet()) {
            Integer key = entry.getKey();
            ActorRef value = entry.getValue();
            this.nodes.put(key, value);
        }

        isRecovering = true;
        // update repl indexes
        update_replication_indexes();
        // update repl data
        update_replication();
        // read on all responsible data
        for (Integer el_id : get_responsible_elements().keySet()) {
            // retrieve could be done to any node ideally
            get_responsible_node(el_id).tell(new Retrive(el_id), getSelf());
        }
    }

    /**
     * Function that updates replication indexes locally in a node by using just the list of nodes (that has to be
     * updated)
     */
    private void update_replication_indexes() {
        List<Integer> precedings_id = get_precedings_id(main.N - 1);

        System.out.println("[DEBUG] node " + key + " new replication indexes: " + precedings_id.toString());
        replication_indexes.clear();
        for (Integer id : precedings_id) {
            replication_indexes.put(id, 0);
        }
    }

    /**
     * Handling the crash message
     *
     * @param msg
     */
    private void oncrash(Crashmsg msg) {
        //nodes.remove(this.key);
        crash();
    }

    /**
     * Change to crash state
     */
    private void crash() {
        getContext().become(crashed());
    }


    /**
     * Receive function in normal behaviour
     * @return the Receive Object
     */
    public Receive createReceive() {
        return receiveBuilder()
                //Write and read messages
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(Retrive.class, this::onretrive)
                .match(Change.class, this::onchange)
                .match(Read.class, this::onread)
                .match(ResponseRead.class, this::onresponseRead)
                .match(TimeoutR.class, this::onTimeoutR)
                .match(TimeoutW.class, this::onTimeoutW)
                .match(Readforwrite.class, this::onreadforwrite)
                .match(ResponseRFW.class, this::onresponseRFW)
                .match(Write.class, this::onwrite)
                .match(Unlock.class, this::onunlock)
                .match(PrintElem.class, this::onprintElem)
                // join messages
                .match(JoinNode.class, this::onJoinNode)
                .match(JoinRequest.class, this::onJoinRequest)
                .match(JoinResponse.class, this::onJoinResponse)
                .match(DataRequest.class, this::onDataRequest)
                .match(DataResponse.class, this::onDataResponse)
                .match(Client.Response.class, this::onResponse)
                .match(AnnounceNode.class, this::onAnnounceNode)
                // leave messages
                .match(LeaveRequest.class, this::onLeaveRequest)
                .match(NodeLeavingInfo.class, this::onNodeLeavingInfo)
                // replication
                .match(ReplicationResponse.class, this::onReplicationResponse)
                .match(ReplicationRequest.class, this::onReplicationRequest)
                //Crash message
                .match(Crashmsg.class, this::oncrash)
                .build();
    }

    /**
     * Receive function in crashed behaviour
     * @return the Receive Object
     */
    final AbstractActor.Receive crashed() {
        return receiveBuilder()
                .match(RecoveryMsg.class, this::onRecovery_request)
                .build();
    }

    /**
     * Response to the Replication request containing the elements requested by the node who issued the
     * replication request
     */
    public static class ReplicationResponse implements Serializable {
        MapElements new_elements;

        public ReplicationResponse(MapElements new_elements) {
            this.new_elements = new_elements;
        }
    }

    /**
     * Replication request message
     */
    public static class ReplicationRequest implements Serializable {

    }

    /**
     * Crash message issued by the main to the node that has to crash
     */
    public static class Crashmsg implements Serializable {
    }

    /**
     * "start" message issued by the main to a node to init it into the circle. Contains the nodes int the circle
     * and the start elements
     */
    public static class JoinGroupMsg implements Serializable {
        public final Map<Integer, ActorRef> group;   // a map of nodes
        public MapElements start_elements;

        /**
         * @param group
         * @param start_elements If you want to fill the node with initial elements
         */
        public JoinGroupMsg(Map<Integer, ActorRef> group, MapElements start_elements) {
            this.group = Collections.unmodifiableMap(new TreeMap<Integer, ActorRef>(group));
            this.start_elements = start_elements;
        }
    }

    /**
     * Message used by the client to read data
     */
    public static class Retrive implements Serializable {
        public final int key;   //Key of object Client wants to read

        public Retrive(int key) {
            this.key = key;
        }
    }

    /**
     * Message used by the Client to write data
     */
    public static class Change implements Serializable {
        public final int key; //Key of object client wants to insert
        public final String value; //Value of object client wants to insert

        public Change(int key, String val) {
            this.key = key;
            this.value = val;
        }
    }

    /**
     * Read message issued by the coordinator to specific nodes to read data
     */
    public static class Read implements Serializable {
        public final int key; //Key of object coordinator wants to read
        public final int count; //Key of waiting request associated with the read operation

        public Read(int key, int count) {
            this.key = key;
            this.count = count;
        }
    }

    /**
     * The read message issued by the coordinator before doing a write
     */
    public static class Readforwrite implements Serializable {
        public final int key; //Key of object coordinator wants to read
        public final int count; //Key of waiting request associated with the read operation

        public Readforwrite(int key, int count) {
            this.key = key;
            this.count = count;
        }
    }

    /**
     * Message for the timeout of read operations
     */
    public static class TimeoutR implements Serializable {
        public final int count; //Key of waiting request associated with the timeout
        public final int key; //Key of object associated with the request

        public TimeoutR(int count, int key) {
            this.count = count;
            this.key = key;
        }
    }

    /**
     * Message for the timeout in write operations
     */
    public static class TimeoutW implements Serializable {
        public final int count; //Key of waiting request associated with the timeout
        public final int key; //Key of object associated with the request

        public TimeoutW(int count, int key) {
            this.count = count;
            this.key = key;
        }
    }

    /**
     * Response messages from the nodes after a read message
     */
    public static class ResponseRead implements Serializable {
        public final Pair<String, Integer> e; //Object (value and version) requested
        public final int count; //Key of waiting request associated with the read operation
        public final int key; //Key of object associated with the read operation

        public ResponseRead(Pair<String, Integer> pair, int count, int key) {
            String ind = pair.getKey();
            Integer value = pair.getValue();
            this.e = new Pair<String, Integer>(ind, value);
            this.count = count;
            this.key = key;
        }
    }

    /**
     * Answer from nodes to the coordinator in write operation
     */
    public static class ResponseRFW implements Serializable {
        public final int count; //Key of waiting request associated with the write operation
        public final int key; //Key of object associated with the write operation
        public final int ver; //Version of object associated with the write operation

        public ResponseRFW(Integer ver, int count, int key) {
            this.ver = ver;
            this.count = count;
            this.key = key;
        }
    }

    /**
     * Write message from coordinator to specific nodes
     */
    public static class Write implements Serializable {
        public final String value; //Value of object coordinator wants to insert
        public final int key; //Key of object coordinator wants to insert
        public final int ver; //Value of object coordinator wants to insert

        public Write(Integer ver, String value, int key) {
            this.ver = ver;
            this.value = value;
            this.key = key;
        }
    }

    /**
     * Message issued by a joining node to its neighbour to retrieve the elements it is responsible for.
     */
    public static class DataRequest implements Serializable {
        public final Integer id;

        public DataRequest(Integer id) {
            this.id = id;
        }
    }

    /**
     * Response message to the Data Request message, containing the elements the new node is responsible for
     */
    public static class DataResponse implements Serializable {
        public final MapElements data;

        public DataResponse(MapElements data) {
            this.data = data;
        }
    }

    /**
     * Message sent by a new node when it joined the network
     */
    public static class AnnounceNode implements Serializable {
        public final Integer key;
        public final ActorRef new_node;

        public AnnounceNode(Integer key, ActorRef new_node) {
            this.key = key;
            this.new_node = new_node;
        }
    }

    public static class Unlock implements Serializable {
        public final int key; //Key of object coordinator wants to unlock

        public Unlock(int key) {
            this.key = key;
        }
    }

    /**
     * Message issued by the main to the new joining node, contains a reference to the bootstrapper and the actual
     * replication index for convenience
     */
    public static class JoinNode implements Serializable {
        ActorRef bootstrapper;
        Map<Integer, Integer> replication_index;

        public JoinNode(ActorRef bootstrapper, Map<Integer, Integer> replication_index) {
            this.bootstrapper = bootstrapper;
            this.replication_index = replication_index;
        }
    }

    /**
     * First message issued by a joining node to its coordinator.
     */
    public static class JoinRequest implements Serializable {
        public JoinRequest() {
        }
    }

    /**
     * Response message of a join request, contains the actual list of nodes in the circle
     */
    public static class JoinResponse implements Serializable {
        Map<Integer, ActorRef> nodes;

        public JoinResponse(Map<Integer, ActorRef> nodes) {
            this.nodes = nodes;
        }
    }

    /**
     * Message sent by the main to a node to ask it to print its elements
     */
    public static class PrintElem implements Serializable {
    }

    /**
     * Message sent by the main to a node to ask it to leave
     */
    public static class LeaveRequest implements Serializable {
    }

    /**
     * Message sent by the leaving node to inform others that it is leaving
     */
    public static class NodeLeavingInfo implements Serializable {
        MapElements new_elements;
        Integer key;

        public NodeLeavingInfo(MapElements new_elements, Integer key) {
            this.new_elements = new_elements;
            this.key = key;
        }
    }

    /**
     * Recovery message from the main to the crashed node. It contains the actual state of the circle
     */
    public static class RecoveryMsg implements Serializable {
        Map<Integer, ActorRef> nodes;

        public RecoveryMsg(Map<Integer, ActorRef> nodes) {
            this.nodes = Collections.unmodifiableMap(new TreeMap<Integer, ActorRef>(nodes));
        }
    }

    //Waiting request of a client
    public class Req {
        int count; //number of response to this request
        ActorRef a; //Client associated to this request
        boolean success; //Outcome of the request (successful or unsuccessful)
        boolean timeout; //Variable that indicates if a timeout can raised against this request
        int key; //Key of object associated to request
        String value; //Value associated to the request (write operation case)
        List<Pair<String, Integer>> respo; //List of object received in the request's answers (read operation case)
        List<ActorRef> repl; //List of replica nodes where this request is sent (write operation case)
        List<Integer> version; //List of object's version in request's answers (write operation case)

        public Req(ActorRef a, int key) {
            this.key = key;
            this.count = 0;
            this.a = a;
            this.success = true;
            this.timeout = true;
            respo = new ArrayList<Pair<String, Integer>>();
            version = new ArrayList<Integer>();
            repl = new ArrayList<ActorRef>();
            value = "";
        }
    }
}

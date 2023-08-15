package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Client.response;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
    private final Map<Integer, ActorRef> nodes = new TreeMap<Integer, ActorRef>(); //Map between key and their nodes in DKVS
    private final Map<Integer, Pair<String, Integer>> elements = new LinkedHashMap<Integer, Pair<String, Integer>>();  //Object mantained by the node
    private final Map<Integer, Boolean> busy = new HashMap<Integer, Boolean>(); //Map that indicates if a nodes is writing on an element or not

    private final Set<Integer> to_be_updated = new HashSet<>(); // set of the indexes that remain to be uptaded after join
    private final Map<Integer, Req> waitC = new HashMap<Integer, Req>(); //Map between the key and the waiting request of a client
    int key; //Key of the object
    int count; //Counter of the request this node send as coordinator

    public Node(int id) {
        this.key = id;
        this.count = 0;
    }

    static public Props props(int id) {
        return Props.create(Node.class, () -> new Node(id));
    }

    public void update_remove_nodes(Map<Integer, ActorRef> new_rout) {
        Utils.update_remove(nodes, new_rout);
    }

    public void update_remove_element(Map<Integer, Pair<String, Integer>> new_element) {
        Utils.update_remove(elements, new_element);
    }

    public void update_nodes(Map<Integer, ActorRef> new_rout) {
        Utils.update_remove(nodes, new_rout);
    }

    public void update_element(Map<Integer, Pair<String, Integer>> new_element) {
        Utils.update_remove(elements, new_element);
    }


    /**
     * Selects and removes all the elements given the new node id.
     *
     * @param id the node asking the data
     */
    public Map<Integer, Pair<String, Integer>> select_elements_and_remove(Integer id) {
        Map<Integer, Pair<String, Integer>> selected = new HashMap<>();

        // select keys
        for (Map.Entry<Integer, Pair<String, Integer>> el : elements.entrySet()) {
            if (el.getKey() <= id) {
                selected.put(el.getKey(), el.getValue());
            }
        }

        // Remove no-more responsible keys
        for (Integer key : selected.keySet()) {
            elements.remove(key);
        }

        return selected;
    }

    /**
     * Given the key, find the node responsible for it (can also be used to find a neighbour)
     *
     * @param key
     * @return
     */
    public ActorRef get_responsible_node(Integer key) {
        Integer neighbour_id = -1;

        List<Integer> ordered_id = new ArrayList<>(nodes.keySet());
        ordered_id.sort(Comparator.reverseOrder());

        for (Integer i : ordered_id) {
            if (key < i) {
                break;
            }
            neighbour_id = i; // TODO check
        }

        if (neighbour_id == -1)
            return null;

        return this.nodes.get(neighbour_id);
    }

    /**
     * Given a key and an element, put it in the Map if newer (if not present it will not be added)
     *
     * @param key
     * @param el
     */
    public void put_if_newer(Integer key, Pair<String, Integer> el) {
        Pair<String, Integer> old_el = elements.get(key);

        if (old_el.getValue() < el.getValue()) {
            elements.put(key, el);
        }
    }

    //Handling start message
    private void onJoinGroupMsg(JoinGroupMsg msg) {
        //Add map between key and their nodes in DKVS
        for (Map.Entry<Integer, ActorRef> entry : msg.group.entrySet()) {
            Integer key = entry.getKey();
            ActorRef value = entry.getValue();
            this.nodes.put(key, value);
        }


    }

    //Handling message for write operation from client
    private void onchange(change msg) {
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
                    va.tell(new readforwrite(msg.key, count), getSelf());
                    waitC.get(count).repl.add(va);
                    i++;
                    first = false;
                }*/
                if (i < main.N) {
                    value.tell(new readforwrite(msg.key, count), getSelf());
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
                value.tell(new readforwrite(msg.key, count), getSelf());
                waitC.get(count).repl.add(value);
                i++;


            }
        }
        count++; //Raise number of waiting request
        //Set the timeout to notify if after a period it isn't received W answers
        getContext().system().scheduler().scheduleOnce(
                Duration.create(20000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutW(count - 1, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );


    }

    //Handling message for read operation from client
    private void onretrive(retrive msg) {

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
                    value.tell(new read(msg.key, count), getSelf());
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
                value.tell(new read(msg.key, count), getSelf());
                i++;


            }
        }
        count++; //Raise number of waiting request
        //Set the timeout to notify if after a period it isn't received R answers
        getContext().system().scheduler().scheduleOnce(
                Duration.create(20000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutR(count - 1, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );


    }

    //Handle message from coordinator to read the version of a certain object for write operation
    private void onreadforwrite(readforwrite msg) {
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
            e = new Pair("BESTIALE", -1);
            System.out.println("ON msg" + msg.key + "write" + "countreq" + msg.count + "vers" + e.getValue());
            //element.put(msg.key,e);
        }
        if (e != null) {

            getSender().tell(new responseRFW(e.getValue(), msg.count, msg.key), getSelf());
        }
    }

    //Handle message from coordinator to read a certain object for read operation
    private void onread(read msg) {
        if (this.busy.containsKey(msg.key)) {//Check if the object is already in other write operation
            if (this.busy.get(msg.key)) {
                return;
            }
        }
        Pair<String, Integer> e = null;
        if (this.elements.containsKey(msg.key)) {
            e = elements.get(msg.key);
        }

        /*if (e == null) { // SOLO SCOPO DI TESTTTTTTTTTTTT !!!!!!!!!!!!!!!
            e = new Pair("BESTIALE", 0);
            element.put(msg.key, e);
        }*/
        //System.out.println("LEGGGERRRE");
        if (e != null) {

            getSender().tell(new responseRead(e, msg.count, msg.key), getSelf());
        }
    }

    //Find in list of objects the one with the maximum version
    private Pair<String, Integer> max(List<Pair<String, Integer>> l) {
        int max = -1;
        Pair<String, Integer> pa = new Pair("0", 0);
        for (Pair<String, Integer> p : l) {
            if (p.getValue() > max) {
                pa.setValue(p.getValue());
                pa.setKey(p.getKey());
                max = p.getValue();
            }
        }

        return pa;

    }

    //Handling the answer from nodes for read operation
    private void onresponseRead(responseRead msg) {
        if (waitC.get(msg.count) != null && waitC.get(msg.count).success && waitC.get(msg.count).key == msg.key) {
            System.out.println("msg" + msg.key + " count" + waitC.get(msg.count).count + "read" + "countreq" + msg.count);
            if (waitC.get(msg.count).count >= main.R - 1) {
                waitC.get(msg.count).timeout = false;
                waitC.get(msg.count).success = false;

                waitC.get(msg.count).a.tell(new response(max(waitC.get(msg.count).respo), true, msg.key, "read"), getSelf());
            }
            waitC.get(msg.count).respo.add(msg.e);
            waitC.get(msg.count).count++;

        }


    }

    //Find in list of versions the one with maximum
    private Integer maxI(List<Integer> l) {
        int max = -1;

        for (Integer i : l) {
            if (i > max) {
                max = i;
            }
        }

        return max;

    }

    //Handling the answer from nodes for write operation
    private void onresponseRFW(responseRFW msg) {

        if (waitC.get(msg.count) != null && waitC.get(msg.count).success && waitC.get(msg.count).key == msg.key) {
            System.out.println("msg" + msg.key + " count" + waitC.get(msg.count).count + "write" + "countreq" + msg.count + "vers" + msg.ver);
            if (waitC.get(msg.count).count >= main.W - 1) {
                waitC.get(msg.count).timeout = false;
                waitC.get(msg.count).success = false;
                int maxV = maxI(waitC.get(msg.count).version);
                maxV++;
                if (msg.key == 0 || msg.key == 1) {
                    System.out.println("maxV" + maxV + "count" + msg.count);

                }
                waitC.get(msg.count).a.tell(new response(new Pair(waitC.get(msg.count).value, maxV), true, msg.key, "write"), getSelf());
                for (ActorRef r : waitC.get(msg.count).repl) {
                    r.tell(new write(maxV, waitC.get(msg.count).value, msg.key), getSelf());
                }
            }
            waitC.get(msg.count).version.add(msg.ver);
            waitC.get(msg.count).count++;

        }


    }

    //Handling the write operation from coordinator
    private void onwrite(write msg) {

        this.elements.put(msg.key, new Pair(msg.value, msg.ver));
        this.busy.put(msg.key, false);
    }

    //Handling the timeout for read operation
    private void onTimeoutR(TimeoutR msg) {
        if (waitC.get(msg.count).timeout) {
            System.out.println("TIMEOOUTRRR");
            waitC.get(msg.count).a.tell(new response(null, false, msg.key, "read"), getSelf());
            waitC.get(msg.count).success = false;

        }
    }

    //Handling the timeout for write operation
    private void onTimeoutW(TimeoutW msg) {
        if (waitC.get(msg.count).timeout) {
            waitC.get(msg.count).a.tell(new response(null, false, msg.key, "write"), getSelf());
            waitC.get(msg.count).success = false;
            for (ActorRef a : waitC.get(msg.count).repl) { //UnLock every node from write operation
                a.tell(new unlock(msg.key), getSelf());
            }

        }
    }

    /**
     * Msg sent by the main, to tell to a node which is his bootsrapper
     *
     * @param msg
     */
    private void onJoinNode(JoinNode msg) {
        msg.bootstrapper.tell(new JoinRequest(), getSelf());
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
     * Processing the Join Response sent by the bootstrapper. It finds the nearest neighbour, asking to him the
     * keys this node is responsible for
     *
     * @param msg
     */
    private void onJoinResponse(JoinResponse msg) {
        update_remove_nodes(msg.nodes); // updates the list of all nodes from the bootstrapper Join Response
        ActorRef neighbour = get_responsible_node(key);

        // ask data to neighbour
        neighbour.tell(new DataRequest(key), self());
    }

    private void onDataRequest(DataRequest msg) {
        Map<Integer, Pair<String, Integer>> selected = select_elements_and_remove(msg.id);

        sender().tell(new DataResponse(selected), getSelf());
    }

    private void onDataResponse(DataResponse msg) {
        update_remove_element(msg.data);

        // TODO: check the data with a read
        for (Integer key : elements.keySet()) {
            ActorRef resp_node = get_responsible_node(key);
            resp_node.tell(new retrive(key), self());
        }

        to_be_updated.addAll(msg.data.keySet());
    }

    /**
     * This handles responses received from a read request done after a node joins the circle and reads all the elements
     *
     * @param msg
     */
    private void onResponse(Client.response msg) {
        to_be_updated.remove(msg.key);

        if (!msg.success || msg.op.equals("read")) {
            // TODO ?
            return;
        }

        put_if_newer(msg.key, msg.p);

        if (to_be_updated.isEmpty()) {
            // TODO announce node
        }
    }

    private void onAnnounceNode(AnnounceNode msg) {
        // TODO: add new joined node
        this.nodes.put(msg.key, sender());
    }

    //Handling unlock of object from write operation because of the timeout
    private void onunlock(unlock msg) {
        busy.put(msg.key, true);
    }

    private void onprintElem(printElem msg) {

        for (Map.Entry<Integer, Pair<String, Integer>> entry : this.elements.entrySet()) {
            System.out.println("idN" + this.key + " idE" + entry.getKey() + " value:" + entry.getValue().getKey() + " version:" + entry.getValue().getValue());
        }
    }

    private void onLeaveRequest(LeaveRequest msg) {
        // announce to every node that this is leaving
        // add the data items they are now responsible for
        for (Integer key : nodes.keySet()) {
            Map<Integer, Pair<String, Integer>> to_give = new HashMap<>();

            for (Map.Entry<Integer, Pair<String, Integer>> el : elements.entrySet()) {
                if (el.getKey() <= key)
                    to_give.put(el.getKey(), el.getValue());
            }

            nodes.get(key).tell(new NodeLeavingInfo(to_give, key), self());
        }
    }

    private void onNodeLeavingInfo(NodeLeavingInfo msg) {
        update_element(msg.new_elements); // update the data of the old element (if present)
        nodes.remove(msg.key); // remove node that leaved
    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(retrive.class, this::onretrive)
                .match(change.class, this::onchange)
                .match(read.class, this::onread)
                .match(responseRead.class, this::onresponseRead)
                .match(TimeoutR.class, this::onTimeoutR)
                .match(TimeoutW.class, this::onTimeoutW)
                .match(readforwrite.class, this::onreadforwrite)
                .match(responseRFW.class, this::onresponseRFW)
                .match(write.class, this::onwrite)
                .match(unlock.class, this::onunlock)
                .match(printElem.class,this::onprintElem)

                // join messages
                .match(JoinNode.class, this::onJoinNode)
                .match(JoinRequest.class, this::onJoinRequest)
                .match(JoinResponse.class, this::onJoinResponse)
                .match(DataRequest.class, this::onDataRequest)
                .match(DataResponse.class, this::onDataResponse)
                .match(Client.response.class, this::onResponse)
                .match(AnnounceNode.class, this::onAnnounceNode)

                // leave messages
                .match(LeaveRequest.class, this::onLeaveRequest)
                .match(NodeLeavingInfo.class, this::onNodeLeavingInfo)

                .build();
    }

    //Start message
    public static class JoinGroupMsg implements Serializable {
        public final Map<Integer, ActorRef> group;   // a map of nodes

        public JoinGroupMsg(Map<Integer, ActorRef> group) {
            this.group = Collections.unmodifiableMap(new TreeMap<Integer, ActorRef>(group));
        }
    }

    //Read message from Client
    public static class retrive implements Serializable {
        public final int key;   //Key of object Client wants to read

        public retrive(int key) {
            this.key = key;
        }
    }

    //Write message from Client
    public static class change implements Serializable {
        public final int key; //Key of object client wants to insert
        public final String value; //Value of object client wants to insert

        public change(int key, String val) {
            this.key = key;
            this.value = val;
        }
    }

    //Read message from coordinator to specific nodes
    public static class read implements Serializable {
        public final int key; //Key of object coordinator wants to read
        public final int count; //Key of waiting request associated with the read operation

        public read(int key, int count) {
            this.key = key;
            this.count = count;
        }
    }

    //Read message from coordinator for write operation purpose
    public static class readforwrite implements Serializable {
        public final int key; //Key of object coordinator wants to read
        public final int count; //Key of waiting request associated with the read operation

        public readforwrite(int key, int count) {
            this.key = key;
            this.count = count;
        }
    }

    //Timeout message (in read operation)
    public static class TimeoutR implements Serializable {

        public final int count; //Key of waiting request associated with the timeout

        public final int key; //Key of object associated with the request

        public TimeoutR(int count, int key) {

            this.count = count;
            this.key = key;


        }
    }

    //Timeout message (in write operation)
    public static class TimeoutW implements Serializable {

        public final int count; //Key of waiting request associated with the timeout

        public final int key; //Key of object associated with the request

        public TimeoutW(int count, int key) {

            this.count = count;
            this.key = key;


        }
    }

    //Answer from nodes to the coordinator in read operation
    public static class responseRead implements Serializable {
        public final Pair<String, Integer> e; //Object (value and version) requested
        public final int count; //Key of waiting request associated with the read operation

        public final int key; //Key of object associated with the read operation

        public responseRead(Pair<String, Integer> pair, int count, int key) {
            String ind = pair.getKey();
            Integer value = pair.getValue();
            this.e = new Pair<String, Integer>(ind, value);
            this.count = count;
            this.key = key;


        }
    }

    //Answer from nodes to the coordinator in write operation
    public static class responseRFW implements Serializable {
        public final int count; //Key of waiting request associated with the write operation
        public final int key; //Key of object associated with the write operation
        public final int ver; //Version of object associated with the write operation

        public responseRFW(Integer ver, int count, int key) {

            this.ver = ver;
            this.count = count;
            this.key = key;


        }
    }

    //Write message from coordinator to specific nodes
    public static class write implements Serializable {
        public final String value; //Value of object coordinator wants to insert
        public final int key; //Key of object coordinator wants to insert
        public final int ver; //Value of object coordinator wants to insert

        public write(Integer ver, String value, int key) {

            this.ver = ver;
            this.value = value;
            this.key = key;
        }
    }

    public static class DataRequest implements Serializable {
        public final Integer id;

        public DataRequest(Integer id) {
            this.id = id;
        }
    }

    public static class DataResponse implements Serializable {
        public final Map<Integer, Pair<String, Integer>> data;

        public DataResponse(Map<Integer, Pair<String, Integer>> data) {
            this.data = data;
        }
    }

    /**
     * Message sent by a new node when it joined the network
     */
    public static class AnnounceNode implements Serializable {
        public final Integer key;

        public AnnounceNode(Integer key) {
            this.key = key;
        }
    }

    public static class unlock implements Serializable {
        public final int key; //Key of object coordinator wants to unlock

        public unlock(int key) {
            this.key = key;
        }
    }

    public static class JoinNode implements Serializable {
        ActorRef bootstrapper;

        public JoinNode(ActorRef bootstrapper) {
            this.bootstrapper = bootstrapper;
        }
    }

    //Start message
    public static class JoinRequest implements Serializable {
        public JoinRequest() {
        }
    }

    public static class JoinResponse implements Serializable {
        Map<Integer, ActorRef> nodes;

        public JoinResponse(Map<Integer, ActorRef> nodes) {
            this.nodes = nodes;
        }
    }

    public static class printElem implements Serializable {
    }

    public static class LeaveRequest implements Serializable {
    }

    public static class NodeLeavingInfo implements Serializable {
        Map<Integer, Pair<String, Integer>> new_elements;
        Integer key;
        public NodeLeavingInfo(Map<Integer, Pair<String, Integer>> new_elements, Integer key) {
            this.new_elements = new_elements;
            this.key = key;
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

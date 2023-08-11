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
    private final List<ActorRef> peers = new ArrayList<ActorRef>(); //List of nodes in DKVS
    private final Map<Integer, ActorRef> rout = new HashMap<Integer, ActorRef>(); //Map between key and their nodes in DKVS
    private final Map<Integer, Pair<String, Integer>> element = new HashMap<Integer, Pair<String, Integer>>();  //Object mantained by the node
    private final Map<Integer, Boolean> busy = new HashMap<Integer, Boolean>(); //Map that indicates if a nodes is writing on an element or not

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
        public Integer ver; //Version of object associated with the write operation

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
        public Integer ver; //Value of object coordinator wants to insert

        public write(Integer ver, String value, int key) {

            this.ver = ver;
            this.value = value;
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

    //Handling start message
    private void onJoinGroupMsg(JoinGroupMsg msg) {
        //Add map between key and their nodes in DKVS
        for (Map.Entry<Integer, ActorRef> entry : msg.group.entrySet()) {
            Integer key = entry.getKey();
            ActorRef value = entry.getValue();
            this.rout.put(key, value);
        }
    }

    //Handling message for write operation from client
    private void onchange(change msg) {
        waitC.put(count, new Req(getSender(), msg.key)); //Add new waiting request
        waitC.get(count).value = msg.value;


        //Handling to send read request to the N replicas
        //ActorRef va = null;
        Integer key;
        int i = 0; //Counter of replica found
        //boolean first = true; //Handling the first
        for (Map.Entry<Integer, ActorRef> entry : this.rout.entrySet()) {

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
            for (Map.Entry<Integer, ActorRef> entry : this.rout.entrySet()) {
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

        //Set the timeout to notify if after a period it isn't received W answers
        getContext().system().scheduler().scheduleOnce(
                Duration.create(20000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutW(count, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );

        count++; //Raise number of waiting request

    }

    //Handling message for read operation from client
    private void onretrive(retrive msg) {

        waitC.put(count, new Req(getSender(), msg.key)); //Add new waiting request


        //Handling to send read request to the N replicas
        //ActorRef va = null;
        Integer key;
        int i = 0; //Counter of replica found
        //boolean first = true;
        for (Map.Entry<Integer, ActorRef> entry : this.rout.entrySet()) {

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
            for (Map.Entry<Integer, ActorRef> entry : this.rout.entrySet()) {
                if (i >= main.N) {
                    break;
                }
                key = entry.getKey();
                ActorRef value = entry.getValue();
                value.tell(new read(msg.key, count), getSelf());
                i++;


            }
        }

        //Set the timeout to notify if after a period it isn't received R answers
        getContext().system().scheduler().scheduleOnce(
                Duration.create(20000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutR(count, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
        count++; //Raise number of waiting request

    }

    //Handle message from coordinator to read the version of a certain object for write operation
    private void onreadforwrite(readforwrite msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if (busy.containsKey(msg.key)) { //Check if the object is already in other write operation
            if (busy.get(msg.key)) {
                return;
            }
        }
        this.busy.put(msg.key, true);
        if (e == null) {
            e = new Pair("BESTIALE", -1);
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
        Pair<String, Integer> e = element.get(msg.key);
        if (e == null) { // SOLO SCOPO DI TESTTTTTTTTTTTT !!!!!!!!!!!!!!!
            e = new Pair("BESTIALE", 0);
            element.put(msg.key, e);
        }
        System.out.println("LEGGGERRRE");
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
                pa = p;
            }
        }

        return pa;

    }

    //Handling the answer from nodes for read operation
    private void onresponseRead(responseRead msg) {
        if (waitC.get(msg.count) != null && waitC.get(msg.count).success && waitC.get(msg.count).key == msg.key) {
            System.out.println("msg" + msg.key + " count" + waitC.get(msg.count).count + "read");
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
            System.out.println("msg" + msg.key + " count" + waitC.get(msg.count).count + "write");
            if (waitC.get(msg.count).count >= main.W - 1) {
                waitC.get(msg.count).timeout = false;
                waitC.get(msg.count).success = false;
                int maxV = maxI(waitC.get(msg.count).version);
                maxV++;
                waitC.get(msg.count).a.tell(new response(new Pair(waitC.get(msg.count).value, maxV), true, msg.key, "write"), getSelf());
                for (ActorRef r : waitC.get(msg.count).repl) {
                    r.tell(new write(msg.key, waitC.get(msg.count).value, maxV), getSelf());
                }
            }
            waitC.get(msg.count).version.add(msg.ver);
            waitC.get(msg.count).count++;

        }


    }

    //Handling the write operation from coordinator
    private void onwrite(write msg) {
        this.element.put(msg.key, new Pair(msg.value, msg.ver));
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
        sender().tell(new JoinResponse(this.rout), getSelf());
    }

    private void onJoinResponse(JoinResponse msg) {
        this.rout = msg.nodes;

        Integer neighbour_id = -1;

        List<Integer> ordered_id = new ArrayList<>(rout.keySet());
        ordered_id.sort(Comparator.reverseOrder());

        for (Integer i : ordered_id) {
            if (this.key < i) {
                break;
            }
            neighbour_id = i; // TODO check
        }

        ActorRef neighbour = this.rout.get(neighbour_id);

        // TODO: ask data to neighbour
    }

    //Handling unlock of object from write operation because of the timeout
    private void onunlock(unlock msg) {
        busy.put(msg.key, true);
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
                .match(JoinNode.class, this::onJoinNode)
                .match(JoinRequest.class, this::onJoinRequest)
                .build();
    }
}

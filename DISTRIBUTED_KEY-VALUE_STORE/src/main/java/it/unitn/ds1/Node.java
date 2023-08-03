package it.unitn.ds1;

import java.io.Serializable;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Client.response;
import it.unitn.ds1.Node.Req;
import javafx.util.Pair;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
    private final List<ActorRef> peers = new ArrayList<ActorRef>(); //List of nodes in DKVS
    private final Map<Integer, ActorRef> rout = new HashMap<Integer, ActorRef>(); //Map between key and their nodes in DKVS
    private final Map<Integer, Pair<String, Integer>> element = new HashMap<Integer, Pair<String, Integer>>();  //Object mantained by the node
    private final Map<Integer, Req> waitC = new HashMap<Integer, WaitC>(); //Map between the key and the waiting request of a client

    //Waiting request of a client
    public class Req  {

        int count; //number of response to this request
        ActorRef a; //Client associated to this request
        boolean success; //Outcome of the request (successful or unsuccessful)
        boolean timeout; //Variable that indicates if a timeout can raised against this request
        String value; //Value associated to the request (write operation case)

        List<Pair<String, Integer>> respo; //List of object received in the request's answers (read operation case)

        List<ActorRef> repl; //List of replica nodes where this request is sent (write operation case)

        List<Integer> version; //List of object's version in request's answers (write operation case)

        public Req(ActorRef a) {
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
            this.group = Collections.unmodifiableList(new TreeMap<Integer, ActorRef>(group));
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


    private void onJoinGroupMsg(JoinGroupMsg msg) {

        for (Map.Entry<Integer, ActorRef> entry : msg.group.entrySet()) {
            Integer key = entry.getKey();
            ActorRef value = entry.getValue();
            this.rout.put(key, value);
        }

    }

    private void onchange(change msg) {
        waitC.put(count, Req(getSender()));
        waitC.get(count).value = msg.value;
        count++;
        ActorRef va = null;
        Integer key;
        int i = 0;
        boolean first = true;
        for (Map.Entry<Integer, ActorRef> entry : this.rout.entrySet()) {

            key = entry.getKey();
            ActorRef value = entry.getValue();
            if (key > msg.key) {
                if (first) {
                    va.tell(new readforwrite(msg.key, count), getSelf());
                    waitC.get(count).repl.add(va);
                    i++;
                    first = false;
                }
                if (i < main.N) {
                    value.tell(new readforwrite(msg.key, count), getSelf());
                    waitC.get(count).repl.add(va);
                    i++;
                } else {
                    break;
                }
            }
            va = value;
        }
        if (msg.key < main.RANGE && i < main.N) {
            for (Map.Entry<Integer, ActorRef> entry : this.rout.entrySet()) {
                if (i >= main.N) {
                    break;
                }
                key = entry.getKey();
                ActorRef value = entry.getValue();
                value.tell(new readforwrite(msg.key, count), getSelf());
                waitC.get(count).repl.add(va);
                i++;


            }
        }
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutW(count, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );

    }

    private void onretrive(retrive msg) {

        waitC.put(count, Req(getSender()));
        count++;
        ActorRef va = null;
        Integer key;
        int i = 0;
        boolean first = true;
        for (Map.Entry<Integer, ActorRef> entry : this.rout.entrySet()) {

            key = entry.getKey();
            ActorRef value = entry.getValue();
            if (key > msg.key) {
                if (first) {
                    va.tell(new read(msg.key, count), getSelf());
                    i++;
                    first = false;
                }
                if (i < main.N) {
                    value.tell(new read(msg.key, count), getSelf());
                    i++;
                } else {
                    break;
                }
            }
            va = value;
        }
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
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutR(count, msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );


    }

    private void onreadforwrite(readforwrite msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if (e != null) {

            getSender().tell(new responseRFW(e.getValue(), msg.count, msg.key), getSelf());
        }
    }

    private void onread(read msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if (e != null) {

            getSender().tell(new responseRead(e, msg.count, msg.key), getSelf());
        }
    }

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

    private void onresponseRead(read msg) {
        if (waitC.get(msg.count) != null && waitC.get(msg.key).success) {
            if (waitC.get(msg.count).count >= main.R) {
                waitC.get(msg.key).timeout = false;

                waitC.get(msg.count).a.tell(new response(max(waitC.get(msg.count).respo), true, msg.key, "read"), getSelf());
            }
            waitC.get(msg.count).respo.add(msg.e);
            waitC.get(msg.count).count++;

        }


    }

    private Integer maxI(List<Integer> l) {
        int max = -1;

        for (Integer i : l) {
            if (i > max) {
                max = i;
            }
        }

        return max;

    }

    private void onresponseRFW(responseRFW msg) {
        if (waitC.get(msg.count) != null && waitC.get(msg.count).success) {
            if (waitC.get(msg.count).count >= main.W) {
                waitC.get(msg.key).timeout = false;
                int maxV = maxI(waitC.get(msg.count).version);
                waitC.get(msg.count).a.tell(new response(new Pair("", maxV), true, msg.key, "write"), getSelf());
                for (ActorRef r : waitC.get(msg.count).repl) {
                    r.tell(new write(msg.key, waitC.get(msg.count).value, maxV++), getSelf());
                }
            }
            waitC.get(msg.count).version.add(msg.ver);
            waitC.get(msg.count).count++;

        }


    }

    private void onwrite(write msg) {
        this.element.put(msg.key, new Pair(msg.value, msg.ver));
    }

    private void onTimeoutR(TimeoutR msg) {
        if (waitC.get(msg.count).timeout) {
            waitC.get(msg.count).a.tell(new response(null, false, msg.key, "read"), getSelf());
            waitC.get(msg.count).success = false;

        }
    }

    private void onTimeoutW(TimeoutW msg) {
        if (waitC.get(msg.count).timeout) {
            waitC.get(msg.count).a.tell(new response(null, false, msg.key, "write"), getSelf());
            waitC.get(msg.count).success = false;

        }
    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(retrive.class, this::onretrive)
                .match(change.class, this::onchange)
                .match(read.class, this::onread)
                .match(responseRead.class, this::onresponseRead)
                .match(TimeoutR.class, this::onTimeoutR)
                .match(readforwrite.class, this::onreadforwrite)
                .match(responseRFW.class, this::onresponseRFW)
                .match(write.class, this::onwrite)
                .build();
    }





}

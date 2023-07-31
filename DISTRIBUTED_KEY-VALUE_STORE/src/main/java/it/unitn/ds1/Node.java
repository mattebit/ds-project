package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.*;
import javafx.util.Pair;
import it.unitn.ds1.main;
import java.lang.Math;


public class Node extends AbstractActor {
    int key;
    int value;
    private List<ActorRef> peers = new ArrayList<ActorRef>();
    private Map<Integer,ActorRef> rout = new HashMap<Integer,ActorRef>();
    private Map<Integer,Pair<String, Integer>> element = new HashMap<Integer, Pair<String, Integer>>();

    public Node(int id) {
        this.key = id;

    }

    static public Props props(int id, boolean snapshotInitiator) {
        return Props.create(Node.class, () -> new Node(id));
    }

    public static class JoinGroupMsg implements Serializable {
        public final Map<Integer,ActorRef> group;   // an array of group members
        public JoinGroupMsg(Map<Integer,ActorRef> group) {
            this.group = Collections.unmodifiableList(new TreeMap<Integer,ActorRef>(group));
        }
    }

    public static class retrive implements Serializable {
        public final int key;   // an array of group members
        public retrive(int key) {
            this.key = key;
        }
    }

    public static class change implements Serializable {
        public final int key;
        public final String value;
        public change(int key, String val) {
            this.key = key;
            this.value = val;
        }
    }

    public static class read implements Serializable {
        public final int key;
        public read(int key) {
            this.key = key;
        }
    }

    public static class responseRead implements Serializable {
        public final Pair<String, Integer> e;
        public responseRead(Pair<String, Integer> pair) {
            String key = pair.getKey();
            Integer value = pair.getValue();
            e = new Pair<String, Integer>(key,value);

        }
    }

    private void onJoinGroupMsg(JoinGroupMsg msg) {

        for(Map.Entry<Integer,ActorRef> entry : msg.group.entrySet()) {
            Integer key = entry.getKey();
            ActorRef value = entry.getValue();
            this.rout.put(key,value);
        }

    }

    private void onchange(JoinGroupMsg msg) {
        for (ActorRef b: msg.group) {
            this.peers.add(b);
        }
        System.out.println("" + id + ": starting with " +
                msg.group.size() + " peer(s)");
        getSelf().tell(new NextTransfer(), getSelf());  // schedule 1st transaction
    }

    private void onretrive(retrive msg) {
        ActorRef va = null;
        Integer key;
        int i = 0;
        boolean first = true;
        for(Map.Entry<Integer,ActorRef> entry : this.rout.entrySet()) {

            key = entry.getKey();
            ActorRef value = entry.getValue();
            if(key > msg.key){
                if(first) {
                    va.tell(new read(msg.key), getSelf());
                    i++;
                    first = false;
                }
                if(i<main.N){
                    value.tell(new read(msg.key), getSelf());
                    i++;
                }else{
                    break;
                }
            }
            va = value;
        }
        if(msg.key < main.RANGE && i<main.N){
            for(Map.Entry<Integer,ActorRef> entry : this.rout.entrySet()) {
                if(i>=main.N){
                    break;
                }
                key = entry.getKey();
                ActorRef value = entry.getValue();
                value.tell(new read(msg.key), getSelf());
                i++;


            }
        }


    }

    private void onread(read msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if(e != null){

            getSender().tell(new responseRead(e));
        }
    }

    private void onresponseRead(read msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if(e != null){

            getSender().tell(new responseRead(e));
        }
    }




    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
                .match(retrive.class,  this::onretrive)
                .match(change.class,  this::onchange)
                .match(read.class, this::onread)
                .match(responseRead.class, this::onresponseRead)
                .build();
    }


}

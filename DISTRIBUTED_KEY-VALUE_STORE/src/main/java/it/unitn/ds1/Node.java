package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.*;
import javafx.util.Pair;
import it.unitn.ds1.main;
import java.lang.Math;
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;

import it.unitn.ds1.Client.response;

public class Node extends AbstractActor {
    int key;

    int count;
    private List<ActorRef> peers = new ArrayList<ActorRef>();
    private Map<Integer,ActorRef> rout = new HashMap<Integer,ActorRef>();
    private Map<Integer,Pair<String, Integer>> element = new HashMap<Integer, Pair<String, Integer>>();

    public class Req implements Serializable {

        int count;

        ActorRef a;

        boolean success;

        List<Pair<String, Integer>> respo;

        public Req(ActorRef a) {
            this.count = 0;
            this.a = a;
            this.success = false;
            respo = new ArrayList<Pair<String, Integer>>;

        }
    }
    private Map<Integer,Req> waitC = new HashMap<Integer,WaitC>();


    public Node(int id) {
        this.key = id;
        this.count = 0;

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
        public final int count;
        public read(int key, int count) {
            this.key = key;
            this.count = count;
        }
    }

    public static class responseRead implements Serializable {
        public final Pair<String, Integer> e;
        public final int count;

        public final int key;
        public responseRead(Pair<String, Integer> pair, int count, int key) {
            String ind = pair.getKey();
            Integer value = pair.getValue();
            this.e = new Pair<String, Integer>(ind,value);
            this.count = count;
            this.key = key;


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

        waitC.put(count,Req(getSender()));
        count++;
        ActorRef va = null;
        Integer key;
        int i = 0;
        boolean first = true;
        for(Map.Entry<Integer,ActorRef> entry : this.rout.entrySet()) {

            key = entry.getKey();
            ActorRef value = entry.getValue();
            if(key > msg.key){
                if(first) {
                    va.tell(new read(msg.key, count), getSelf());
                    i++;
                    first = false;
                }
                if(i<main.N){
                    value.tell(new read(msg.key, count), getSelf());
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
                value.tell(new read(msg.key, count), getSelf());
                i++;


            }
        }
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(),
                new Timeout(count), // the message to send
                getContext().system().dispatcher(), getSelf()
        );


    }

    private void onread(read msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if(e != null){

            getSender().tell(new responseRead(e,msg.count,msg.key));
        }
    }

    private Pair<String,Integer> max(List<Pair<String,Integer>> l){
        int max = -1;
        Pair<String,Integer> pa = new Pair("0",0);
        for(Pair<String,Integer> p: l){
            if(p.getValue()>max){
                pa=p;
            }
        }

        return pa;

    }
    private void onresponseRead(read msg) {
        if(waitC.get(msg.count) != null){
            if(waitC.get(msg.count).count >= main.R){
                waitC.get(msg.count).success = true;

                waitC.get(msg.count).a.tell(new response(max(waitC.get(msg.count).respo),true,msg.key));
            }
            waitC.get(msg.count).respo.add(msg.e);
            waitC.get(msg.count).count++;

        }


    }

    private void onTimeout(read msg) {
        if(waitC.get(msg.key).success){
            waitC.get(msg.count).a.tell(new response(msg.e,false,msg.key));
        }
    }





    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
                .match(retrive.class,  this::onretrive)
                .match(change.class,  this::onchange)
                .match(read.class, this::onread)
                .match(responseRead.class, this::onresponseRead)
                .match(Timeout.class, this::onTimeout)
                .build();
    }


}

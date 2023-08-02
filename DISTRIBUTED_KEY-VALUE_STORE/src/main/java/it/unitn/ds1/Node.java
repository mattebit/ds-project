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
        boolean timeout;

        String value;

        List<Pair<String, Integer>> respo;

        List<ActorRef> repl;

        List<Integer> version;

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

    public static class readforwrite implements Serializable {
        public final int key;
        public final int count;
        public readforwrite(int key, int count) {
            this.key = key;
            this.count = count;
        }
    }

    public static class TimeoutR implements Serializable {

        public final int count;

        public final int key;
        public TimeoutR( int count, int key) {

            this.count = count;
            this.key = key;


        }
    }

    public static class TimeoutW implements Serializable {

        public final int count;

        public final int key;
        public TimeoutW( int count, int key) {

            this.count = count;
            this.key = key;


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

    public static class responseRFW implements Serializable {
        public Integer ver;
        public final int count;

        public final int key;
        public responseRFW(Integer ver, int count, int key) {

            this.ver = ver;
            this.count = count;
            this.key = key;


        }
    }

    public static class write implements Serializable {
        public Integer ver;
        public final String value;

        public final int key;
        public write(Integer ver, String value, int key) {

            this.ver = ver;
            this.value = value;
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

    private void onchange(change msg) {
        waitC.put(count,Req(getSender()));
        waitC.get(count).value = msg.value;
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
                    va.tell(new readforwrite(msg.key, count), getSelf());
                    waitC.get(count).repl.add(va);
                    i++;
                    first = false;
                }
                if(i<main.N){
                    value.tell(new readforwrite(msg.key, count), getSelf());
                    waitC.get(count).repl.add(va);
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
                value.tell(new readforwrite(msg.key, count), getSelf());
                waitC.get(count).repl.add(va);
                i++;


            }
        }
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutW(count,msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );

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
                new TimeoutR(count,msg.key), // the message to send
                getContext().system().dispatcher(), getSelf()
        );


    }

    private void onreadforwrite(readforwrite msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if(e != null){

            getSender().tell(new responseRFW(e.getValue(),msg.count,msg.key),getSelf());
        }
    }

    private void onread(read msg) {
        Pair<String, Integer> e = element.get(msg.key);
        if(e != null){

            getSender().tell(new responseRead(e,msg.count,msg.key),getSelf());
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
        if(waitC.get(msg.count) != null && waitC.get(msg.key).success){
            if(waitC.get(msg.count).count >= main.R){
                waitC.get(msg.key).timeout=false;

                waitC.get(msg.count).a.tell(new response(max(waitC.get(msg.count).respo),true,msg.key,"read"),getSelf());
            }
            waitC.get(msg.count).respo.add(msg.e);
            waitC.get(msg.count).count++;

        }


    }

    private Integer maxI(List<Integer> l){
        int max = -1;

        for(Integer i: l){
            if(i>max){
                max=i;
            }
        }

        return max;

    }

    private void onresponseRFW(responseRFW msg) {
        if(waitC.get(msg.count) != null && waitC.get(msg.count).success){
            if(waitC.get(msg.count).count >= main.W){
                waitC.get(msg.key).timeout=false;
                int maxV = maxI(waitC.get(msg.count).version);
                waitC.get(msg.count).a.tell(new response(new Pair("",maxV),true,msg.key,"write"),getSelf());
                for(ActorRef r: waitC.get(msg.count).repl){
                    r.tell(new write(msg.key,waitC.get(msg.count).value,maxV++),getSelf());
                }
            }
            waitC.get(msg.count).version.add(msg.ver);
            waitC.get(msg.count).count++;

        }


    }

    private void onwrite(write msg) {
        this.element.put(msg.key,new Pair(msg.value,msg.ver));
    }
    private void onTimeoutR(TimeoutR msg) {
        if(waitC.get(msg.count).timeout){
            waitC.get(msg.count).a.tell(new response(null,false,msg.key,"read"),getSelf());
            waitC.get(msg.count).success = false;

        }
    }

    private void onTimeoutW(TimeoutW msg) {
        if(waitC.get(msg.count).timeout){
            waitC.get(msg.count).a.tell(new response(null,false,msg.key,"write"),getSelf());
            waitC.get(msg.count).success = false;

        }
    }





    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
                .match(retrive.class,  this::onretrive)
                .match(change.class,  this::onchange)
                .match(read.class, this::onread)
                .match(responseRead.class, this::onresponseRead)
                .match(TimeoutR.class, this::onTimeoutR)
                .match(readforwrite.class, this::onreadforwrite)
                .match(responseRFW.class, this::onresponseRFW)
                .match(write.class, this::onwrite)
                .build();
    }


}

package it.unitn.ds1;

import java.io.Serializable;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.Node.change;
import it.unitn.ds1.Node.retrive;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

//Clients of the system
public class Client extends AbstractActor {

    //private List<ActorRef> peers = new ArrayList<>();

    private final Random rnd = new Random();

    public class result {

        Pair<String, Integer> p;

        int key;

        boolean success;

        String op;

        result(Pair<String, Integer> p, int key, boolean success, String op) {
            this.key = key;
            this.success = success;
            this.p = p;
            this.op = op;


        }
    }
    private final List<result> responseList = new ArrayList<result>(); //List of the answer from the DKVS
    int id; //Id of the client

    //Start message
    public static class JoinGroupMsgC implements Serializable {
        public final List<ActorRef> group;   // an array of nodes

        public JoinGroupMsgC(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    //Answer message
    public static class response implements Serializable {
        public final Pair<String, Integer> p; //Result object (value,version)
        public final int key; //Key of the object

        public final Boolean success; //Outcome of the operation

        public final String op; //Type of the operation (write or read)


        public response(Pair<String, Integer> pair, Boolean success, int key, String op) {
            this.success = success;
            this.key = key;
            if(pair != null){
                String ind = pair.getKey();
                Integer value = pair.getValue();
            }else{
                p = mull;
            }
            this.p = new Pair(ind, value);
            this.op = op;
        }
    }

    //Read messsage
    public static class get implements Serializable {
    }
    //Write message
    public static class update implements Serializable {
    }



    public Client(int id) {
        this.id = id;
    }

    static public Props props(int id, boolean snapshotInitiator) {
        return Props.create(Client.class, () -> new Client(id));
    }

    //Method that starts the operation bt the client
    private void onJoinGroupMsgC(JoinGroupMsgC msg) {
        /*for (ActorRef b: msg.group) {
            this.peers.add(b);
        }*/

        //Start of the occurrences of read
        Cancellable timer1 = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(4, TimeUnit.SECONDS),        // when to start generating messages
                Duration.create(2, TimeUnit.SECONDS),        // how frequently generate them
                getSelf(),                                          // destination actor reference
                new get(),                                // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
        //Start of the occurrences of write
        Cancellable timer2 = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(4, TimeUnit.SECONDS),        // when to start generating messages
                Duration.create(1, TimeUnit.SECONDS),        // how frequently generate them
                getSelf(),                                          // destination actor reference
                new update(),                                // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );

    }
    //Read method
    private void onget(get msg) {
        int to = rnd.nextInt(main.groupn.size()); //Choice a random target node
        int key = rnd.nextInt(main.RANGE); //Choice a random target object key


        // model a random network/processing delay
        try {
            Thread.sleep(rnd.nextInt(10));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        main.groupn.get(to).tell(new retrive(key), getSelf());


    }
    //Write method
    private void onupdate(update msg) {
        int to = rnd.nextInt(main.groupn.size()); //Choice a random target node
        int key = rnd.nextInt(main.RANGE); //Choice a random target object key
        String val = Integer.toString(this.id); //Value to write

        // model a random network/processing delay
        try {
            Thread.sleep(rnd.nextInt(10));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        main.groupn.get(to).tell(new change(key, val), getSelf());
    }

    //Method to handle the answer from nodes
    private void onresponse(response msg) {

        responseList.add(new result(msg.p, msg.key, msg.success, msg.op));


    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsgC.class, this::onJoinGroupMsgC)
                .match(get.class, this::onget)
                .match(update.class, this::onupdate)
                .match(response.class, this::onresponse)
                .build();
    }




}

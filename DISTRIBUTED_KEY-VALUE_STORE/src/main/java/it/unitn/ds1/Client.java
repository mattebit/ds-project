package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.Node.change;
import it.unitn.ds1.Node.retrive;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import java.sql.Timestamp;
import java.util.Date;

//Clients of the system
public class Client extends AbstractActor {
    //private List<ActorRef> peers = new ArrayList<>();

    private final Random rnd = new Random();
    private final List<result> responseList = new ArrayList<result>(); //List of the answer from the DKVS
    int id; //Id of the client

    Date date = new Date();

    public Client(int id) {
        this.id = id;

    }

    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    Cancellable timer1; //Read timer

    Cancellable timer2; //Write timer

    /**
     * Method to block the timers
     *
     * @param msg
     */
    private void onBlockTimer(BlockTimer msg) {
        timer1.cancel();
        timer2.cancel();
    }

    /**
     * Method that starts the operation bt the client
     *
     * @param msg
     */
    private void onJoinGroupMsgC(JoinGroupMsgC msg) {
        /*for (ActorRef b: msg.group) {
            this.peers.add(b);
        }*/




        //Start of the occurrences of write
        timer2 = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(3, TimeUnit.SECONDS),        // when to start generating messages
                Duration.create(9, TimeUnit.SECONDS),        // how frequently generate them
                getSelf(),                                          // destination actor reference
                new update(),                                // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
        /*for(int i=0;i<4;i++){
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(2+i, TimeUnit.SECONDS),
                    getSelf(),
                    new update(), // the message to send
                    getContext().system().dispatcher(), getSelf()
            );

        }*/

        //Start of the occurrences of read
        timer1 = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(4, TimeUnit.SECONDS),        // when to start generating messages
                Duration.create(8, TimeUnit.SECONDS),        // how frequently generate them
                getSelf(),                                          // destination actor reference
                new get(),                                // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );

        /*for(int i=0;i<4;i++){
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(3+i, TimeUnit.SECONDS),
                    getSelf(),
                    new get(), // the message to send
                    getContext().system().dispatcher(), getSelf()
            );

        }*/




    }
    /**
     * Read method
     *
     * @param msg
     */
    private void onget(get msg) {
        int to = rnd.nextInt(main.mapgroupn.size()); //Choice a random target node
        int key = rnd.nextInt(main.RANGE); //Choice a random target object key

        // model a random network/processing delay
        try {
            Thread.sleep(rnd.nextInt(5));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        main.get_random_node().tell(new retrive(key), getSelf());

    }
    /**
     * Write method
     *
     * @param msg
     */
    private void onupdate(update msg) {
        int key = rnd.nextInt(main.RANGE); //Choice a random target object key
        String val = Integer.toString(this.id); //Value to write

        // model a random network/processing delay
        try {
            Thread.sleep(rnd.nextInt(4));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        main.get_random_node().tell(new change(key, val), getSelf());

    }
    /**
     * Method to handle the answer from nodes
     *
     * @param msg
     */
    private void onresponse(response msg) {
        responseList.add(new result(msg.p, msg.key, msg.success, msg.op, new Timestamp(date.getTime())));
    }
    /**
     * Method to handle the print of answers to the client
     *
     * @param msg
     */
    private void onprintAnswer(printAnswer msg) {
        for (result r : responseList) {
            if (r != null && r.success) {
                System.out.println("ID:" + this.id + " version:" + r.p.getValue() + " key:" + r.key + " value:" + r.p.getKey() + " op:" + r.op + " success:" + r.success + " Timestamp" + r.t);
            }
            if (r != null && !r.success) {
                System.out.println("ID:" + this.id + " key:" + r.key + " op:" + r.op + " success:" + r.success + " Timestamp" + r.t );
            }
        }
    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsgC.class, this::onJoinGroupMsgC)
                .match(get.class, this::onget)
                .match(update.class, this::onupdate)
                .match(response.class, this::onresponse)
                .match(printAnswer.class, this::onprintAnswer)
                .match(BlockTimer.class, this::onBlockTimer)
                .build();
    }


    //Start message
    public static class JoinGroupMsgC implements Serializable {
    }

    //Message to block the timers
    public static class BlockTimer implements Serializable {
    }

    //Answer message
    public static class response implements Serializable {
        public final Pair<String, Integer> p; //Result object (value,version)
        public final int key; //Key of the object

        public final Boolean success; //Outcome of the operation

        public final String op; //Type of the operation (write or read) // TODO change to enum


        public response(Pair<String, Integer> pair, Boolean success, int key, String op) {
            this.success = success;
            this.key = key;
            if (pair != null) {

                String ind = pair.getKey();
                Integer value = pair.getValue();
                this.p = new Pair(ind, value);
            } else {
                this.p = null;
            }

            this.op = op;
        }
    }

    //Read messsage
    public static class get implements Serializable {
    }

    //Write message
    public static class update implements Serializable {
    }

    //Print message
    public static class printAnswer implements Serializable {
    }

    //Class of the answers from nodes
    public class result {

        Pair<String, Integer> p; //Object value and version

        int key; //Object key

        boolean success; //It indicates if the operation was a success or not

        String op; //Write or read

        //int count; //operatuo

        Timestamp t; //Time when the answer arrive to the client

        result(Pair<String, Integer> p, int key, boolean success, String op, Timestamp t) {
            this.key = key;
            this.success = success;
            this.p = p;
            this.op = op;
            this.t = t;
        }
    }
}

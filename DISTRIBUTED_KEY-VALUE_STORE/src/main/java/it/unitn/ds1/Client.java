package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.Node.Change;
import it.unitn.ds1.Node.Retrive;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

//Clients of the system
public class Client extends AbstractActor {
    //private List<ActorRef> peers = new ArrayList<>();
    private final Random rnd = new Random();
    private final List<Result> responseList = new ArrayList<Result>(); //List of the answer from the DKVS
    int id; //Id of the client
    Date date = new Date();
    Cancellable timer1; //Read timer
    Cancellable timer2; //Write timer
    boolean stop; //variable that decide if the client can accept another read and message

    public Client(int id) {
        this.id = id;
        stop = false;
    }

    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }

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
        if (msg.auto) {
            //Start of the occurrences of write
            timer2 = getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.create(3, TimeUnit.SECONDS),        // when to start generating messages
                    Duration.create(9, TimeUnit.SECONDS),        // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new Update(0, "0", msg.auto),                                // the message to send
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
                    new Get(0, msg.auto),                                // the message to send
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
    }

    /**
     * Read method
     *
     * @param msg
     */
    private void onget(Get msg) {
        if (!stop) {
            stop = true;
        } else {
            return;
        }
        if (msg.auto) {
            int to = rnd.nextInt(main.mapgroupn.size()); //Choice a random target node
            int key = rnd.nextInt(main.RANGE); //Choice a random target object key
            // model a random network/processing delay
            try {
                Thread.sleep(rnd.nextInt(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            main.get_random_node().tell(new Retrive(key), getSelf());
        } else {
            main.get_random_node().tell(new Retrive(msg.key), getSelf());
        }
    }

    /**
     * Write method
     *
     * @param msg
     */
    private void onupdate(Update msg) {
        if (!stop) {
            stop = true;
        } else {
            return;
        }
        if (msg.auto) {
            int key = rnd.nextInt(main.RANGE); //Choice a random target object key
            String val = Integer.toString(this.id); //Value to write
            // model a random network/processing delay
            try {
                Thread.sleep(rnd.nextInt(4));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            main.get_random_node().tell(new Change(key, val), getSelf());
        } else {
            main.get_random_node().tell(new Change(msg.key, msg.value), getSelf());
        }
    }

    /**
     * Method to handle the answer from nodes
     *
     * @param msg
     */
    private void onresponse(Response msg) {
        System.out.println("ID:" + this.id + " version:" + msg.p.getValue() + " key:" + msg.key + " value:" + msg.p.getKey() + " op:" + msg.op + " success:" + msg.success + " Timestamp" + new Timestamp(date.getTime()));
        responseList.add(new Result(msg.p, msg.key, msg.success, msg.op, new Timestamp(date.getTime())));
        stop = false;
    }

    /**
     * Method to handle the print of answers to the client
     *
     * @param msg
     */
    private void onprintAnswer(PrintAnswer msg) {
        for (Result r : responseList) {
            if (r != null && r.success) {
                System.out.println("ID:" + this.id + " version:" + r.p.getValue() + " key:" + r.key + " value:" + r.p.getKey() + " op:" + r.op + " success:" + r.success + " Timestamp" + r.t);
            }
            if (r != null && !r.success) {
                System.out.println("ID:" + this.id + " key:" + r.key + " op:" + r.op + " success:" + r.success + " Timestamp" + r.t);
            }
        }
    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsgC.class, this::onJoinGroupMsgC)
                .match(Get.class, this::onget)
                .match(Update.class, this::onupdate)
                .match(Response.class, this::onresponse)
                .match(PrintAnswer.class, this::onprintAnswer)
                .match(BlockTimer.class, this::onBlockTimer)
                .build();
    }

    //Start message
    public static class JoinGroupMsgC implements Serializable {
        public final boolean auto; //variable that decides if the read and write are automatic

        public JoinGroupMsgC(boolean auto) {
            this.auto = auto;
        }
    }

    //Message to block the timers
    public static class BlockTimer implements Serializable {
    }

    //Answer message
    public static class Response implements Serializable {
        public final Pair<String, Integer> p; //Result object (value,version)
        public final int key; //Key of the object
        public final Boolean success; //Outcome of the operation
        public final String op; //Type of the operation (write or read) // TODO change to enum

        public Response(Pair<String, Integer> pair, Boolean success, int key, String op) {
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

    //Read message
    public static class Get implements Serializable {
        public final int key; //Key of the object to retrive
        public final boolean auto; //variable that decides if the read and write are automatic

        Get(int key, boolean auto) {
            this.key = key;
            this.auto = auto;
        }
    }

    //Write message
    public static class Update implements Serializable {
        public final String value; //Value of the object to update
        public final int key; //Key of the object to update
        public final boolean auto; //variable that decides if the read and write are automatic

        Update(int key, String value, boolean auto) {
            this.key = key;
            this.value = value;
            this.auto = auto;
        }
    }

    //Print message
    public static class PrintAnswer implements Serializable {
    }

    //Class of the answers from nodes
    public class Result {
        Pair<String, Integer> p; //Object value and version
        int key; //Object key
        boolean success; //It indicates if the operation was a success or not
        String op; //Write or read
        //int count; //operatuo
        Timestamp t; //Time when the answer arrive to the client

        Result(Pair<String, Integer> p, int key, boolean success, String op, Timestamp t) {
            this.key = key;
            this.success = success;
            this.p = p;
            this.op = op;
            this.t = t;
        }
    }
}

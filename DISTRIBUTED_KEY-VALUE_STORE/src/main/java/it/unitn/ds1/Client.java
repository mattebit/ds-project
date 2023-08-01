package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;
import java.util.*;
import it.unitn.ds1.Node.retrive;
import it.unitn.ds1.Node.change;


public class Client {

    //private List<ActorRef> peers = new ArrayList<>();

    private Random rnd = new Random();

    int id;

    public Client(int id) {
        this.id = id;
    }

    public class result {

        Pair<String,Integer>> p;

        int key;

        boolean success;

        result(Pair<String,Integer> p,int key,boolean success){
            this.key = key;
            this.success = success;
            this.p = p;

        }
    }

    private List<result> responseList = new ArrayList<result>();

    static public Props props(int id, boolean snapshotInitiator) {
        return Props.create(Client.class, () -> new Node(id));
    }
    public static class JoinGroupMsgC implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsgC(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    public static class response implements Serializable {
        public final Pair<String,Integer> p;
        public final int key;

        public final Boolean success;


        public response(Pair<String,Integer> p,Boolean success,int key) {
            this.success = success;
            this.key = key;
            String ind = pair.getKey();
            Integer value = pair.getValue();
            this.p = new Pair(ind,value);
        }
    }

    public static class get implements Serializable{}

    public static class update implements Serializable{}

    private void onJoinGroupMsg(Node.JoinGroupMsg msg) {
        /*for (ActorRef b: msg.group) {
            this.peers.add(b);
        }*/


        Cancellable timer1 = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(4, TimeUnit.SECONDS),        // when to start generating messages
                Duration.create(2, TimeUnit.SECONDS),        // how frequently generate them
                getSelf(),                                          // destination actor reference
                new get(),                                // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
        Cancellable timer2 = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(4, TimeUnit.SECONDS),        // when to start generating messages
                Duration.create(1, TimeUnit.SECONDS),        // how frequently generate them
                getSelf(),                                          // destination actor reference
                new update(),                                // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );

    }

    private void onget(JoinGroupMsgC msg) {
        int to = rnd.nextInt(main.groupn.size());
        int key = rnd.nextInt(main.RANGE);


        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
        main.groupn.get(to).tell(new retrive(key), getSelf());


    }

    private void onupdate(JoinGroupMsgC msg) {
        int to = rnd.nextInt(main.groupn.size());
        int key = rnd.nextInt(main.RANGE);
        String val = Integer.toString(this.id);

        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
        main.groupn.get(to).tell(new change(key,val), getSelf());
    }

    private void onresponse(response msg) {

        responseList.add(new result(msg.p,msg.key,msg.success));


    }



    public Receive createReceive() {
        return receiveBuilder()
                .match(Client.JoinGroupMsgC.class,  this::onJoinGroupMsg)
                .match(Client.get.class,  this::onget)
                .match(Client.update.class,  this::onupdate)
                .match(Client.response.class, this::onresponse)
                .build();
    }


}

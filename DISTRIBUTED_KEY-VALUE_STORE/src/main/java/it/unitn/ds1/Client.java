package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;
import java.util.Random;

public class Client {

    private List<ActorRef> peers = new ArrayList<>();

    private Random rnd = new Random();

    int id;

    public Client(int id) {
        this.id = id;
    }

    static public Props props(int id, boolean snapshotInitiator) {
        return Props.create(Client.class, () -> new Node(id));
    }
    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    public static class get implements Serializable{}

    public static class update implements Serializable{}

    private void onJoinGroupMsg(Node.JoinGroupMsg msg) {
        for (ActorRef b: msg.group) {
            this.peers.add(b);
        }
        System.out.println("" + id + ": starting with " +
                msg.group.size() + " peer(s)");

        Cancellable timer1 = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(4, TimeUnit.SECONDS),        // when to start generating messages
                Duration.create(1, TimeUnit.SECONDS),        // how frequently generate them
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

    private void onget(Node.JoinGroupMsg msg) {
        int to = rnd.nextInt(this.peers.size());
        int key = rnd.nextInt(main.RANGE);


        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
        peers.get(to).tell(new retrive(key), getSelf());


    }

    private void onupdate(Node.JoinGroupMsg msg) {
        int to = rnd.nextInt(this.peers.size());
        int val = this.id;

        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
        peers.get(to).tell(new change(val), getSelf());
    }





    public Receive createReceive() {
        return receiveBuilder()
                .match(Client.JoinGroupMsg.class,  this::onJoinGroupMsg)
                .match(Client.get.class,  this::onget)
                .match(Client.update.class,  this::onupdate)
                .build();
    }


}

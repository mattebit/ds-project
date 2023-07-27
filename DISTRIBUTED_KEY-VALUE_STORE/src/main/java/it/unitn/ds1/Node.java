package it.unitn.ds1;
package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;

public class Node extends AbstractActor {
    int key;
    int value;
    private List<ActorRef> peers = new ArrayList<>();

    public Node(int id) {
        this.key = id;
    }

    static public Props props(int id, boolean snapshotInitiator) {
        return Props.create(Node.class, () -> new Node(id));
    }

    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    public static class retrive implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    public static class change implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    private void onJoinGroupMsg(JoinGroupMsg msg) {
        for (ActorRef b: msg.group) {
            this.peers.add(b);
        }
        System.out.println("" + id + ": starting with " +
                msg.group.size() + " peer(s)");
        getSelf().tell(new NextTransfer(), getSelf());  // schedule 1st transaction
    }





    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
                .match(retrive.class,  this::onJoinGroupMsg)
                .match(change.class,  this::onJoinGroupMsg)
                .build();
    }


}

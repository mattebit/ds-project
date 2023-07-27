package it.unitn.ds1;
import java.io.IOException;
import java.util.List;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.Node.JoinGroupMsg;




public class main {
    static int N_NODES = 5;
    static int N_CLIENTS = 5;
    final static int RANGE = 50;
    static int N = 2;
    static int W = 5;
    static int R = 1;


    public static void main(String[] args) {

        final ActorSystem system = ActorSystem.create("DHT");

        List<ActorRef> groupn = new ArrayList<>();
        for (int i = 0; i < N_NODES; i++) {
            group.add(system.actorOf(Node.props(i), "node" + i));
        }

        List<ActorRef> groupc = new ArrayList<>();
        for (int i = 0; i < N_CLIENTS*10; i = i + 10) {
            groupc.add(system.actorOf(Client.props(i), "client" + i));
        }

        // Send join messages to the banks to inform them of the whole group
        JoinGroupMsg start = new JoinGroupMsg(groupn);
        for (ActorRef peer: groupn) {
            peer.tell(start, ActorRef.noSender());
        }

        for (ActorRef peer: groupc) {
            peer.tell(start, ActorRef.noSender());
        }

        System.out.println(">>> Press ENTER to exit <<<");
        try {
            System.in.read();
        } catch (IOException ioe) {
        } finally {
            system.terminate();
        }

    }

}

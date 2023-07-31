package it.unitn.ds1;
import java.io.IOException;
import java.util.List;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.util.*;

import it.unitn.ds1.Node.JoinGroupMsg;
import it.unitn.ds1.Client.JoinGroupMsgC;




public class main {
    static int N_NODES = 5;
    static int N_CLIENTS = 5;
    final static int RANGE = 50;
    final static int N = 5;
    final static int W = 5;
    final static int R = 1;

    static List<ActorRef> groupn;


    public static void main(String[] args) {

        final ActorSystem system = ActorSystem.create("DHT");

        Map<Integer,ActorRef> Mapgroupn = new TreeMap<Integer,ActorRef>();
        groupn = new ArrayList<ActorRef>();
        for (int i = 0; i < N_NODES; i++) {
            ActorRef a = system.actorOf(Node.props(i), "node" + i);
            Mapgroupn.put(i,a);
            groupn.add(a);
        }

        List<ActorRef> groupc = new ArrayList<ActorRef>();
        for (int i = 0; i < N_CLIENTS*10; i = i + 10) {
            groupc.add(system.actorOf(Client.props(i), "client" + i));
        }

        // Send join messages to the banks to inform them of the whole group
        JoinGroupMsg start = new JoinGroupMsg(Mapgroupn);
        JoinGroupMsgC start2 = new JoinGroupMsgC(groupn);
        for (ActorRef peer: groupn) {
            peer.tell(start, ActorRef.noSender());
        }

        for (ActorRef peer: groupc) {
            peer.tell(start2, ActorRef.noSender());
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

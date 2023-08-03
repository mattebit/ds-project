package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.Client.JoinGroupMsgC;
import it.unitn.ds1.Node.JoinGroupMsg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class main {
    final static int RANGE = 50; //Range of key in DKVS
    final static int N = 5; //Number of replicas
    final static int W = 5; //Dimension of read quorum
    final static int R = 1; //Dimension of write quorum
    static int N_NODES = 5; //Number of the initial nodes
    static int N_CLIENTS = 5; //Number of the initial clients
    static List<ActorRef> groupn; //List of nodes in DKVS


    public static void main(String[] args) {

        final ActorSystem system = ActorSystem.create("DKVS");


        Map<Integer, ActorRef> Mapgroupn = new TreeMap<Integer, ActorRef>(); //Map between the nodes and their key
        groupn = new ArrayList<ActorRef>();
        for (int i = 0; i < N_NODES; i++) {
            ActorRef a = system.actorOf(Node.props(i), "node" + i);
            Mapgroupn.put(i, a);
            groupn.add(a);
        }

        List<ActorRef> groupc = new ArrayList<ActorRef>(); //List of clients
        for (int i = 0; i < N_CLIENTS * 10; i = i + 10) {
            groupc.add(system.actorOf(Client.props(i), "client" + i));
        }

        // Join messages to the nodes to inform them about the map
        JoinGroupMsg start = new JoinGroupMsg(Mapgroupn);
        // Join messages to the clients to inform them about the nodes
        JoinGroupMsgC start2 = new JoinGroupMsgC(groupn);
        for (ActorRef peer : groupn) {
            peer.tell(start, ActorRef.noSender());
        }

        for (ActorRef peer : groupc) {
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

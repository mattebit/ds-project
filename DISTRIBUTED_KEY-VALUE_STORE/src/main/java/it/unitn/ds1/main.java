package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.Client.BlockTimer;
import it.unitn.ds1.Client.JoinGroupMsgC;
import it.unitn.ds1.Client.PrintAnswer;
import it.unitn.ds1.Node.JoinGroupMsg;
import it.unitn.ds1.Node.PrintElem;

import java.io.IOException;
import java.util.*;

public class main {
    final static int RANGE = 50; //Range of key in DKVS
    final static int N = 4; //Number of replicas
    final static int W = 4; //Dimension of read quorum
    final static int R = 1; //Dimension of write quorum
    final static ActorSystem system = ActorSystem.create("DKVS");
    static int N_NODES = 5; //Number of the initial nodes
    static int N_CLIENTS = 5; //Number of the initial clients
    static Map<Integer, ActorRef> mapgroupn;

    public static void main(String[] args) {
        mapgroupn = new TreeMap<Integer, ActorRef>(); //Map between the nodes and t33heir key
        for (int i = 0; i < N_NODES * 10; i = i + 10) {
            ActorRef a = system.actorOf(Node.props(i), "node" + i);
            mapgroupn.put(i, a);
        }
        List<ActorRef> groupc = new ArrayList<ActorRef>(); //List of clients
        for (int i = 0; i < N_CLIENTS; i++) {
            groupc.add(system.actorOf(Client.props(i), "client" + i));
        }
        Map<Integer, Map<Integer, Integer>> repl_indexes = Utils.get_replicas_indexes(mapgroupn, N);
        // Join messages to the clients to inform them about the nodes
        JoinGroupMsgC start2 = new JoinGroupMsgC();
        // Start all the nodes
        for (Map.Entry<Integer, ActorRef> entry : mapgroupn.entrySet()) {
            // Join messages to the nodes to inform them about the map
            JoinGroupMsg start = new JoinGroupMsg(mapgroupn, repl_indexes.get(entry.getKey()));
            entry.getValue().tell(start, ActorRef.noSender());
        }
        try {
            System.out.println(">>> GO <<<");
            System.in.read();
        } catch (IOException e) {
        }
        // start all the clients
        for (ActorRef client : groupc) {
            client.tell(start2, ActorRef.noSender());
        }
        try {
            System.out.println(">>> Block read and write<<<");
            System.in.read();
        } catch (IOException e) {
        }
        BlockTimer block = new BlockTimer();
        for (ActorRef client : groupc) {
            client.tell(block, ActorRef.noSender());
        }
        boolean done = false;
        try {
            while (!done) {
                System.out.println(">>> Press ENTER to print answer<<<");
                System.in.read();
                //create_new_node(25);
                mapgroupn.get(20).tell(new Node.LeaveRequest(), ActorRef.noSender());
                PrintAnswer printa = new PrintAnswer();
                for (ActorRef n : groupc) {
                    n.tell(printa, ActorRef.noSender());
                    System.out.println(">>> continue <<<");
                    System.in.read();
                }
                System.out.println(">>> Press ENTER to exit <<<");
                System.in.read();
                done = true;
            }
        } catch (IOException e) {
        }
        done = false;
        try {
            while (!done) {
                System.out.println(">>> Press ENTER to print Elements<<<");
                System.in.read();
                //TODO: list of change message to random nodes before add
                //create_new_node(25);
                PrintElem printa = new PrintElem();
                for (ActorRef n : mapgroupn.values()) {
                    n.tell(printa, ActorRef.noSender());
                    System.out.println(">>> continue <<<");
                    System.in.read();
                }
                System.out.println(">>> Press ENTER to exit <<<");
                System.in.read();
                done = true;
            }
        } catch (IOException e) {
        } finally {
            system.terminate();
        }
    }

    public static ActorRef get_random_node() {
        Random r = new Random();
        Set<Integer> s = mapgroupn.keySet();
        Integer ran = r.nextInt(s.size());
        int count = 0;
        int res = 0;
        for (Integer k : s) {
            if (ran == count) {
                res = k;
                break;
            }
        }
        return mapgroupn.get(res);
    }

    public static void create_new_node(Integer id) {
        if (mapgroupn.containsKey(id)) {
            throw new RuntimeException("a node with id " + " is already present");
        }
        // create new node
        ActorRef a = system.actorOf(Node.props(id), "node" + id);
        mapgroupn.put(id, a);
        // choose a random bootstrapper node
        ActorRef bootstrapper = get_random_node();
        // get updated replication indexes for new node
        Map<Integer, Map<Integer, Integer>> repl_indexes = Utils.get_replicas_indexes(mapgroupn, N); // recalculate repl indexes for new node
        // tell the new node about the bootstrapper
        a.tell(new Node.JoinNode(bootstrapper, repl_indexes.get(id)), ActorRef.noSender());
    }
}

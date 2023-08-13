package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.Client.JoinGroupMsgC;
import it.unitn.ds1.Client.printAnswer;
import it.unitn.ds1.Node.printElem;
import it.unitn.ds1.Node.JoinGroupMsg;
//import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;

import java.io.IOException;
import java.util.*;


public class main {
    final static int RANGE = 50; //Range of key in DKVS
    final static int N = 4; //Number of replicas
    final static int W = 4; //Dimension of read quorum
    final static int R = 1; //Dimension of write quorum
    static int N_NODES = 5; //Number of the initial nodes
    static int N_CLIENTS = 5; //Number of the initial clients
    static List<ActorRef> groupn; //List of nodes in DKVS



    final static ActorSystem system = ActorSystem.create("DKVS");

    public static void main(String[] args) {


        Map<Integer, ActorRef> mapgroupn = new TreeMap<Integer, ActorRef>(); //Map between the nodes and their key

        groupn = new ArrayList<ActorRef>();
        for (int i = 0; i < N_NODES * 10; i = i + 10) {
            ActorRef a = system.actorOf(Node.props(i), "node" + i);
            mapgroupn.put(i, a);
            groupn.add(a);
        }

        List<ActorRef> groupc = new ArrayList<ActorRef>(); //List of clients
        for (int i = 0; i < N_CLIENTS; i++) {
            groupc.add(system.actorOf(Client.props(i), "client" + i));
        }

        // Join messages to the nodes to inform them about the map
        JoinGroupMsg start = new JoinGroupMsg(mapgroupn);
        // Join messages to the clients to inform them about the nodes
        JoinGroupMsgC start2 = new JoinGroupMsgC();

        // Start all the nodes
        for (ActorRef peer : groupn) {
            peer.tell(start, ActorRef.noSender());
        }

        try {
            System.out.println(">>> GO <<<");
            System.in.read();
        }catch (IOException e) {}

        // start all the clients
        for (ActorRef client : groupc) {
            client.tell(start2, ActorRef.noSender());
        }

        boolean done = false;

        try {
            //while(!done) {
            System.out.println(">>> Press ENTER to print <<<");
            System.in.read();

            printAnswer printa = new printAnswer();

            for (ActorRef client : groupc) {
                client.tell(printa, ActorRef.noSender());
                System.out.println(">>> continue <<<");
                System.in.read();
            }


            //}
        } catch (IOException e) {

        }
        try {
            //while(!done) {
                System.out.println(">>> Press ENTER to print Elemnts<<<");
                System.in.read();

                printElem printa = new printElem();

                for (ActorRef node : groupn) {
                    node.tell(printa, ActorRef.noSender());
                    System.out.println(">>> continue <<<");
                    System.in.read();
                }

                System.out.println(">>> Press ENTER to exit <<<");
                System.in.read();
            //}
        } catch (IOException e) {

        } finally {
            system.terminate();
        }
    }

    /*public void create_new_node(Integer id) {
        if (mapgroupn.containsKey(id)) {
            throw new ValueException("a node with id " + " is already present");
        }

        // create new node
        ActorRef a = system.actorOf(Node.props(id), "node" + id);

        // choose a random bootstrapper node
        Random r = new Random();
        ActorRef bootstrapper = groupn.get(r.nextInt(groupn.size()));

        // tell the new node about the bootstrapper
        a.tell(new Node.JoinNode(bootstrapper), ActorRef.noSender());
    }*/
}

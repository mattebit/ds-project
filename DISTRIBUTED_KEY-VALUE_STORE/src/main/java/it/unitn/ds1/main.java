package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.Client.*;
import it.unitn.ds1.Node.JoinGroupMsg;
import it.unitn.ds1.Node.PrintElem;

import java.io.IOException;
import java.util.*;

public class main {
    final static int N = 4; //Number of replicas
    final static int W = 4; //Dimension of read quorum
    final static int R = 1; //Dimension of write quorum
    final static ActorSystem system = ActorSystem.create("DKVS");
    static int N_NODES = 5; //Number of the initial nodes
    final static int RANGE = 10 * N_NODES; //Range of key in DKVS
    static int N_CLIENTS = 5; //Number of the initial clients
    static Map<Integer, ActorRef> mapgroupn;
    static List<ActorRef> groupc = new ArrayList<>();

    /**
     * Generates test data for a node, given a range (from, to]
     */
    public static MapElements generate_test_elements(int from, int to) {
        MapElements m = new MapElements();

        for (int i = from + 1; i <= to; i++) {
            m.put(i, new Pair<>("0", 0));
        }

        return m;
    }


    public static void init(boolean gen_test_elements) {
        mapgroupn = new TreeMap<Integer, ActorRef>(); //Map between the nodes and t33heir key
        for (int i = 0; i < N_NODES * 10; i = i + 10) {
            ActorRef a = system.actorOf(Node.props(i), "node" + i);
            mapgroupn.put(i, a);
        }

        for (int i = 0; i < N_CLIENTS; i++) {
            groupc.add(system.actorOf(Client.props(i), "client" + i));
        }

        // Start all the nodes
        for (Map.Entry<Integer, ActorRef> entry : mapgroupn.entrySet()) {
            // Join messages to the nodes to inform them about the map

            JoinGroupMsg start = null;
            if (gen_test_elements) {
                if (entry.getKey() != 0) {
                    start = new JoinGroupMsg(
                            mapgroupn, generate_test_elements(entry.getKey() - 10, entry.getKey()));
                } else {
                    start = new JoinGroupMsg(
                            mapgroupn, generate_test_elements((N_NODES - 1) * 10, N_NODES * 10));
                }
            } else {
                start = new JoinGroupMsg(mapgroupn, null);
            }
            entry.getValue().tell(start, ActorRef.noSender());
        }

        System.out.println("[DEBUG] init done");
    }

    public static void demo_fixed_elements_standard() {
        init(true);

        try {
            System.out.println(">>> Start with the testing <<<"); //wait that all the nodes are initiated
            System.in.read();
        } catch (IOException e) {
        }

        printnodes();
    }

    /**
     * Demo that inits the nodes with fixed elements and creates three new nodes 15,35,25
     *
     * @throws IOException
     */
    public static void demo_fixed_elements_create_node() throws IOException {
        init(true);

        try {
            System.out.println(">>> Create new node 15 <<<"); //wait that all the nodes are initiated
            System.in.read();
        } catch (IOException e) {
        }

        create_new_node(15);
        System.out.println(">>> Create new node 35 <<<");
        System.in.read();
        create_new_node(35);
        System.out.println(">>> Create new node 25 <<<");
        System.in.read();
        create_new_node(25);

        printnodes();
    }

    /**
     * Demo that inits the nodes with random elements and creates three new nodes 15
     *
     * @throws IOException
     */
    public static void demo_random_elements_create_node() throws IOException {
        init(false);

        try {
            System.out.println(">>> Initiate nodes <<<");
            System.in.read();
        } catch (IOException e) {
        }

        automaticop(groupc); // generates random data in nodes

        try {
            System.out.println(">>> Create new node 15 <<<"); //wait that all the nodes are initiated
            System.in.read();
        } catch (IOException e) {
        }

        create_new_node(15);

        printnodes();
    }

    public static void demo_fixed_elements_remove_node() throws IOException {
        init(true);

        try {
            System.out.println(">>> Remove node 10 <<<"); //wait that all the nodes are initiated
            System.in.read();
        } catch (IOException e) {
        }

        // remove node 10
        mapgroupn.get(10).tell(new Node.LeaveRequest(), ActorRef.noSender());
        mapgroupn.remove(10);

        printnodes();
    }

    public static void main(String[] args) throws IOException {
        System.out.println(">>> Start with the testing <<<"); //wait that all the nodes are initiated
        System.in.read();

        // select just one of the one below:
        //test_w_rep();
        test_r();
        //test_se_co();
        //demo_fixed_elements_standard();
        //demo_fixed_elements_create_node();
        //demo_random_elements_create_node();
        //demo_fixed_elements_remove_node();

        try {
            System.out.println(">>> Press ENTER to terminate program <<<");
            System.in.read();
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

    /**
     * Print every element of every node and the history of the version
     */
    public static void printnodes() {
        boolean done = false;
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
                System.out.println(">>> Press ENTER to exit from print <<<");
                System.in.read();
                done = true;
            }
        } catch (IOException e) {
        }
    }

    /**
     * Print the log of the different operation did by every client
     *
     * @param groupc
     */
    public static void printclients(List<ActorRef> groupc) {
        boolean done = false;
        try {
            while (!done) {
                System.out.println(">>> Press ENTER to print answer of the clients <<<");
                System.in.read();
                //create_new_node(25);
                //mapgroupn.get(20).tell(new Node.LeaveRequest(), ActorRef.noSender());
                PrintAnswer printa = new PrintAnswer();
                for (ActorRef n : groupc) {
                    n.tell(printa, ActorRef.noSender());
                    System.out.println(">>> continue <<<");
                    System.in.read();
                }
                System.out.println(">>> Press ENTER to exit from print <<<");
                System.in.read();
                done = true;
            }
        } catch (IOException e) {
        }
    }

    /**
     * Start automatic operation from clients
     *
     * @param groupc
     */
    public static void automaticop(List<ActorRef> groupc) {
        Automate join = new Automate();// Message to start the automatic read and write by clients
        // All clients start
        for (ActorRef clienta : groupc) {
            clienta.tell(join, ActorRef.noSender());
        }

        try {
            System.out.println(">>> Block read and write<<<");
            System.in.read();
        } catch (IOException e) {
        }
        BlockTimer block = new BlockTimer(); //Block the automatic read and write
        for (ActorRef clienta : groupc) {
            clienta.tell(block, ActorRef.noSender());
        }
    }

    /**
     * Test replication and write
     *
     */
    public static void test_w_rep() {
        init(false);

        try {
            System.out.println(">>> Test replication and write <<<");
            System.in.read();
        } catch (IOException e) {
        }
        //ask the user a key and value to write
        int key;
        String value;
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter key: ");
        key = sc.nextInt();
        System.out.println("value: Pino");
        value = "Pino";
        Update write = new Update(key, value, false);
        ActorRef client = groupc.get(0); //select first client
        client.tell(write, ActorRef.noSender()); //tell to the client to write the object (key,value)

        printnodes();
    }

    /**
     * Test read
     *
     */
    public static void test_r() {
        init(false);

        try {
            System.out.println(">>> Initiate nodes <<<");
            System.in.read();
        } catch (IOException e) {
        }

        automaticop(groupc); // generates random data in nodes

        printnodes();

        try {
            System.out.println(">>> Test read element <<<"); //wait that all the nodes are initiated
            System.in.read();
        } catch (IOException e) {
        }

        int key;
        String value;
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter key: ");
        key = sc.nextInt();
        Get read = new Get(key, false);
        groupc.get(0).tell(read, ActorRef.noSender()); //tell to the client to read the object with the indicated key
    }

    /**
     * Test sequential consistency
     *
     */
    public static void test_se_co() {
        init(false);

        try {
            System.out.println(">>> Initiate nodes <<<");
            System.in.read();
        } catch (IOException e) {
        }

        automaticop(groupc); // generates random data in nodes

        try {
            System.out.println(">>> Test sequential consistency and multiple client working <<<"); //wait that all the nodes are initiated
            System.in.read();
        } catch (IOException e) {
        }

        printnodes();
        printclients(groupc);
    }


}

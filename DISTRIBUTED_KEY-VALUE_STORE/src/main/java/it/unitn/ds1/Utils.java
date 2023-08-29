package it.unitn.ds1;

import akka.actor.ActorRef;

import java.util.*;

public class Utils {
    /**
     * Update a given Map with respect a new one
     *
     * @param to_be_updated the map to be updated
     * @param new_map       the new map containing new or removed elements
     * @param <T>           the type of the values in the map
     */
    public static <T> void update_remove(Map<Integer, T> to_be_updated,
                                         Map<Integer, T> new_map) {
        // add new elements
        update(to_be_updated, new_map);
        // remove entry not present in new map that are present in to_be_updated
        to_be_updated.entrySet().removeIf(entry -> !new_map.containsKey(entry.getKey()));
    }

    /**
     * Update a given Map with respect a new one
     *
     * @param to_be_updated the map to be updated
     * @param new_map       the new map containing new elements
     * @param <T>           the type of the values in the map
     */
    public static <T> void update(Map<Integer, T> to_be_updated,
                                  Map<Integer, T> new_map) {
        // add new elements
        to_be_updated.putAll(new_map);
    }

    public static <T> void update_remove(List<T> to_be_updated,
                                         List<T> new_list) {
        // add new items
        for (T item : new_list) {
            if (!to_be_updated.contains(item)) {
                to_be_updated.add(item);
            }
        }
        // remove items not present
        to_be_updated.removeIf(item -> !new_list.contains(item));
    }

    /**
     * Given the node map, and the integer N, returns all the replication indexes for all the nodes
     *
     * @param nodes the node map
     * @param N     the constant N of replication
     * @return the replication indexes for each node
     */
    public static Map<Integer, Map<Integer, Integer>> get_replicas_indexes(
            Map<Integer, ActorRef> nodes, Integer N) {
        List<Integer> indexes = new ArrayList<>(nodes.keySet());
        Collections.sort(indexes);
        Map<Integer, Map<Integer, Integer>> replication_indexes_nodes = new HashMap<>();
        for (Integer i : indexes) {
            replication_indexes_nodes.put(i, new HashMap<>());
        }
        int act = 0;
        while (act < indexes.size()) {
            //Map<Integer, Integer> act_repl_indexes = new HashMap<>();
            // basically, for the next N-1 nodes, set current node ID in the replication indexes
            for (int i = 1; i < N - 1; i++) {
                int to_fill_indx = (act + i) % indexes.size();
                replication_indexes_nodes.get(indexes.get(to_fill_indx)).put(indexes.get(act), N - 2 - i);
                //act_repl_indexes.put(indexes.get(to_fill_indx), N-2-i);
            }
            //replication_indexes_nodes.put(indexes.get(act), act_repl_indexes);
            act++;
        }
        return replication_indexes_nodes;
    }
}

package it.unitn.ds1;

import java.util.List;
import java.util.Map;

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
        update(to_be_updated,new_map);

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
}

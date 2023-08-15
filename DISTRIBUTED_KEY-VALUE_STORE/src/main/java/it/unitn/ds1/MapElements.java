package it.unitn.ds1;

import java.util.HashMap;
import java.util.Map;

public class MapElements extends HashMap<Integer, Pair<String, Integer>> {
    /**
     * Update the elements in this map without removing them if not present in new map
     *
     * @param new_map
     */
    public void update(MapElements new_map) {
        for (Entry<Integer, Pair<String, Integer>> el : new_map.entrySet()) {
            Integer el_key = el.getKey();
            Pair<String, Integer> el_value = el.getValue();

            if (this.get(el.getKey()) == null) {
                // element not present, add
                this.put(el_key, el_value);
            } else {
                // element present, check version
                Integer stored_version = this.get(el_key).getValue();
                Integer new_version = el_value.getValue();

                if (stored_version < new_version) {
                    this.put(el_key, el_value);
                }
            }
        }
    }

    /**
     * Update this map to match the new map, also remove elements not present in new map
     *
     * @param new_map
     */
    public void update_remove(MapElements new_map) {
        this.update(new_map); // update present values

        this.entrySet().removeIf(entry -> !new_map.containsKey(entry.getKey())); // remove elements not present
    }

    /**
     * Get all the elements in this map that are smaller than the given key, also removes them from this map.
     *
     * @param key
     * @return
     */
    public MapElements get_remove_less_than_key(Integer key) {
        MapElements selected = new MapElements();

        // select keys
        for (Map.Entry<Integer, Pair<String, Integer>> el : this.entrySet()) {
            if (el.getKey() <= key) {
                selected.put(el.getKey(), el.getValue());
            }
        }

        // Remove no-more responsible keys
        for (Integer k : selected.keySet()) {
            this.remove(k);
        }

        return selected;
    }

    /**
     * Given a key and an element, put it in the Map if newer (if not present it will not be added)
     *
     * @param key
     * @param el
     */
    public void put_if_newer(Integer key, Pair<String, Integer> el) {
        Pair<String, Integer> old_el = this.get(key);

        if (old_el.getValue() < el.getValue()) {
            this.put(key, el);
        }
    }
}

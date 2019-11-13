package my.artfultom.zallak.dto;

import java.util.HashMap;
import java.util.List;

public class DataMap<K, V> extends HashMap<SortedTuple<K>, List<V>> {

    public static <K, V> DataMap<K, V> of(SortedTuple<K> key, List<V> value) {
        DataMap<K, V> dataMap = new DataMap<>();
        dataMap.put(key, value);

        return dataMap;
    }
}

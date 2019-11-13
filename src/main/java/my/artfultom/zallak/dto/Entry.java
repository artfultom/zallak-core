package my.artfultom.zallak.dto;

import java.io.Serializable;

public class Entry<K, V> implements Serializable {

    private final String nodeName;
    private final DataMap<K, V> data;

    private Entry(String nodeName, DataMap<K, V> data) {
        this.nodeName = nodeName;
        this.data = data;
    }

    public static <K, V> Entry<K, V> of(String nodeName, DataMap<K, V> data) {
        return new Entry<>(nodeName, data);
    }

    @Override
    public String toString() {
        return "NodeEntry{" +
                "nodeName='" + nodeName + '\'' +
                ", data=" + data +
                '}';
    }

    public String getNodeName() {
        return nodeName;
    }

    public DataMap<K, V> getData() {
        return data;
    }
}

package my.artfultom.zallak.node;

import my.artfultom.zallak.dto.SortedTuple;

import java.util.List;
import java.util.Map;

public abstract class ReduceNode<K, V> extends Node {

    public ReduceNode(String name) {
        this.name = name;
    }

    protected abstract Map<SortedTuple<K>, List<V>> process(SortedTuple<K> key, List<List<V>> input);

    public Map<SortedTuple<K>, List<V>> execute(SortedTuple<K> key, List<List<V>> input) {
        return process(key, input);
    }
}

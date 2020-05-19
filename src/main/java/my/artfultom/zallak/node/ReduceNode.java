package my.artfultom.zallak.node;

import my.artfultom.zallak.dto.SortedTuple;

import java.util.List;
import java.util.Map;

public abstract class ReduceNode<KIn, VIn> extends Node {

    public ReduceNode(String name) {
        this.name = name;
    }

    protected abstract Map<SortedTuple<KIn>, List<VIn>> process(SortedTuple<KIn> key, List<List<VIn>> input);

    public Map<SortedTuple<KIn>, List<VIn>> execute(SortedTuple<KIn> key, List<List<VIn>> input) {
        return process(key, input);
    }
}

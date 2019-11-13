package my.artfultom.zallak.node;

import my.artfultom.zallak.dto.SortedTuple;

import java.util.List;

public abstract class ReduceNode<KIn, VIn> extends Node {

    public ReduceNode(String name) {
        this.name = name;
    }

    protected abstract void process(SortedTuple<KIn> key, List<List<VIn>> input);

    public void execute(SortedTuple<KIn> key, List<List<VIn>> input) {
        process(key, input);
    }
}

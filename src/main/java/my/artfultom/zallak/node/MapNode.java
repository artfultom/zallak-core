package my.artfultom.zallak.node;

import my.artfultom.zallak.dto.DataMap;
import my.artfultom.zallak.dto.ResultList;

public abstract class MapNode<KIn, VIn, KOut, VOut> extends Node {

    public MapNode(String name) {
        this.name = name;
    }

    protected abstract ResultList<KOut, VOut> process(DataMap<KIn, VIn> input);

    public ResultList<KOut, VOut> execute(DataMap<KIn, VIn> input) {
        return process(input);
    }
}

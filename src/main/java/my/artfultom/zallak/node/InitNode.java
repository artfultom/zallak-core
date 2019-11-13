package my.artfultom.zallak.node;

import my.artfultom.zallak.dto.ResultList;

public abstract class InitNode<KOut, VOut> extends Node {

    public InitNode() {
        this.name = "INIT_NODE";
    }

    protected abstract ResultList<KOut, VOut> process();

    public ResultList<KOut, VOut> execute() {
        return process();
    }
}

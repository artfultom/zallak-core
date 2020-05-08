package my.artfultom.zallak;

import my.artfultom.zallak.config.Timeout;
import my.artfultom.zallak.dto.Entry;
import my.artfultom.zallak.dto.ResultList;
import my.artfultom.zallak.dto.SortedTuple;
import my.artfultom.zallak.node.InitNode;
import my.artfultom.zallak.node.MapNode;
import my.artfultom.zallak.node.ReduceNode;

import java.util.*;
import java.util.concurrent.*;

public class NodeStarter {

    private int poolSize;
    private Timeout timeout = Timeout.of(60, TimeUnit.SECONDS);

    private InitNode<?, ?> initNode;

    private final Map<String, MapNode> mapNodes = new ConcurrentHashMap<>();
    private final Map<String, ReduceNode> reduceNodes = new ConcurrentHashMap<>();

    public NodeStarter(int poolSize) {
        this.poolSize = poolSize;
    }

    public void add(InitNode node) {
        this.initNode = node;
    }

    public void add(MapNode node) {
        if (mapNodes.containsKey(node.getName())) {
            System.out.println("Node '" + node.getName() + "' is already exist.");
        } else {
            mapNodes.put(node.getName(), node);
        }
    }

    public void add(ReduceNode node) {
        if (reduceNodes.containsKey(node.getName())) {
            System.out.println("Node '" + node.getName() + "' is already exist.");
        } else {
            reduceNodes.put(node.getName(), node);
        }
    }

    public void start() {
        final ThreadPoolExecutor pool = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()
        );

        final Queue<Entry<?, ?>> mapTaskQueue = new ConcurrentLinkedQueue<>();
        final Queue<Entry<?, ?>> reduceTaskQueue = new ConcurrentLinkedQueue<>();

        if (this.initNode == null) {
            System.out.println("Cannot find init node.");
        } else {
            Future<ResultList<?, ?>> initResultFuture = pool.submit(() -> initNode.execute());

            try {
                ResultList<?, ?> initResults = initResultFuture.get();
                mapTaskQueue.addAll(initResults);

                while (pool.getActiveCount() > 0 || pool.getQueue().size() > 0 || mapTaskQueue.size() > 0) {
                    Entry<?, ?> mapResult = mapTaskQueue.poll();

                    if (mapResult != null) {
                        MapNode node = mapNodes.get(mapResult.getNodeName());

                        if (node == null) {
                            System.out.println("Cannot find map node by name: " + mapResult.getNodeName());
                        } else {
                            CompletableFuture
                                    .supplyAsync(() -> (ResultList<?, ?>) node.execute(mapResult.getData()), pool)
                                    .whenComplete((future, error) -> {
                                        if (error != null) {
                                            System.out.println(error.getMessage()); // TODO
                                        }
                                    })
                                    .thenAccept(results -> {
                                        for (Entry<?, ?> result : results) {
                                            boolean error = true;

                                            if (mapNodes.containsKey(result.getNodeName())) {
                                                mapTaskQueue.add(result);
                                                error = false;
                                            }

                                            if (reduceNodes.containsKey(result.getNodeName())) {
                                                reduceTaskQueue.add(result);
                                                error = false;
                                            }

                                            if (error) {
                                                System.out.println("Unknown node: " + result.getNodeName());
                                            }
                                        }
                                    });
                        }
                    }
                }

                Map<String, Map<SortedTuple<?>, List<List<?>>>> reduceMap = new HashMap<>();

                for (Entry<?, ?> task : reduceTaskQueue) {
                    for (SortedTuple key : task.getData().keySet()) {
                        Map<SortedTuple<?>, List<List<?>>> map;

                        if (reduceMap.containsKey(task.getNodeName())) {
                            map = reduceMap.get(task.getNodeName());
                        } else {
                            map = new HashMap<>();
                            reduceMap.put(task.getNodeName(), map);
                        }

                        map.computeIfPresent(key, (k, v) -> {
                            v.add(task.getData().get(key));
                            return v;
                        });

                        map.computeIfAbsent(key, (k) -> {
                            List<List<?>> list = new ArrayList<>();
                            list.add(task.getData().get(key));
                            return list;
                        });
                    }
                }

                for (String nodeName : reduceMap.keySet()) {
                    ReduceNode node = reduceNodes.get(nodeName);

                    if (node == null) {
                        System.out.println("Cannot find reduce node by name: " + nodeName);
                    } else {
                        for (SortedTuple<?> id : reduceMap.get(nodeName).keySet()) {
                            CompletableFuture
                                    .runAsync(() -> node.execute(id, reduceMap.get(nodeName).get(id)), pool)
                                    .whenComplete((future, error) -> {
                                        if (error != null) {
                                            System.out.println(error.getMessage()); // TODO
                                        }
                                    });
                        }
                    }
                }

                pool.shutdown();
                pool.awaitTermination(timeout.getTimeout(), timeout.getUnit());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace(); // TODO
            }
        }
    }
}

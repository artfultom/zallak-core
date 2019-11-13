package my.artfultom.zallak;

import my.artfultom.zallak.dto.DataMap;
import my.artfultom.zallak.dto.Entry;
import my.artfultom.zallak.dto.ResultList;
import my.artfultom.zallak.dto.SortedTuple;
import my.artfultom.zallak.node.InitNode;
import my.artfultom.zallak.node.MapNode;
import my.artfultom.zallak.node.ReduceNode;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodeStarterTest {

    @Test
    void commonFriendsCaseTest() {
        NodeStarter starter = new NodeStarter(3);

        starter.add(new InitNode<String, String>() {
            @Override
            protected ResultList<String, String> process() {
                ResultList<String, String> result = new ResultList<>();

                try (BufferedReader br = Files.newBufferedReader(Paths.get("src", "test", "resources", "best_friends.input"))) {
                    List<String> lines = br.lines().collect(Collectors.toList());

                    for (String line : lines) {
                        SortedTuple<String> key = SortedTuple.of(line.split(":")[0].split(","));
                        List<String> value = Arrays.asList(line.split(":")[1].split(","));

                        result.add(Entry.of("SECOND", DataMap.of(key, value)));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return result;
            }
        });

        starter.add(new MapNode<String, String, String, String>("SECOND") {
            @Override
            protected ResultList<String, String> process(DataMap<String, String> input) {
                ResultList<String, String> result = new ResultList<>();

                for (SortedTuple<String> key : input.keySet()) {
                    List<String> value = input.get(key);

                    DataMap<String, String> data = new DataMap<>();
                    for (String v : value) {
                        List<String> newKeyList = new ArrayList<>(key.getElements());
                        newKeyList.add(v);

                        data.put(SortedTuple.of(newKeyList), value);
                    }

                    result.add(Entry.of("THIRD", data));
                }

                return result;
            }
        });

        Map<SortedTuple, List> testResult = new ConcurrentHashMap<>();

        starter.add(new ReduceNode<String, String>("THIRD") {
            @Override
            protected void process(SortedTuple<String> key, List<List<String>> input) {
                Set<String> letters = new HashSet<>();

                for (List<String> list : input) {
                    letters.addAll(list);
                }

                Map<String, Integer> counter = new HashMap<>();

                for (String letter : letters) {
                    counter.put(letter, 0);
                }

                for (List<String> list : input) {
                    for (String letter : list) {
                        counter.computeIfPresent(letter, (k, v) -> ++v);
                    }
                }

                List<String> result = counter.entrySet().stream()
                        .filter(entry -> entry.getValue() == input.size())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                testResult.put(key, result);
            }
        });

        starter.start();

        assertEquals(testResult.size(), 9);
        assertEquals(testResult.get(SortedTuple.of("A", "B")).size(), 2);
        assertEquals(testResult.get(SortedTuple.of("B", "C")).size(), 3);
        assertEquals(testResult.get(SortedTuple.of("D", "E")).size(), 2);
        assertEquals(testResult.get(SortedTuple.of("A", "C")).size(), 2);
        assertEquals(testResult.get(SortedTuple.of("C", "D")).size(), 3);
        assertEquals(testResult.get(SortedTuple.of("B", "D")).size(), 3);
        assertEquals(testResult.get(SortedTuple.of("A", "D")).size(), 2);
        assertEquals(testResult.get(SortedTuple.of("B", "E")).size(), 2);
        assertEquals(testResult.get(SortedTuple.of("C", "E")).size(), 2);
    }

    @Test
    void commonFriendsNumsCaseTest() {
        NodeStarter starter = new NodeStarter(3);

        starter.add(new InitNode<Integer, Integer>() {
            @Override
            protected ResultList<Integer, Integer> process() {
                ResultList<Integer, Integer> result = new ResultList<>();

                try (BufferedReader br = Files.newBufferedReader(Paths.get("src", "test", "resources", "best_friends_nums.input"))) {
                    List<String> lines = br.lines().collect(Collectors.toList());

                    for (String line : lines) {
                        Integer[] keyArray = Arrays
                                .stream(line.split(":")[0].split(","))
                                .mapToInt(Integer::parseInt)
                                .boxed().toArray(Integer[]::new);

                        SortedTuple<Integer> key = SortedTuple.of(keyArray);

                        Integer[] valueArray = Arrays
                                .stream(line.split(":")[1].split(","))
                                .mapToInt(Integer::parseInt)
                                .boxed().toArray(Integer[]::new);

                        List<Integer> value = Arrays.asList(valueArray);

                        result.add(Entry.of("SECOND", DataMap.of(key, value)));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return result;
            }
        });

        starter.add(new MapNode<Integer, Integer, Integer, Integer>("SECOND") {
            @Override
            protected ResultList<Integer, Integer> process(DataMap<Integer, Integer> input) {
                ResultList<Integer, Integer> result = new ResultList<>();

                for (SortedTuple<Integer> key : input.keySet()) {
                    List<Integer> value = input.get(key);

                    DataMap<Integer, Integer> data = new DataMap<>();
                    for (Integer v : value) {
                        List<Integer> newKeyList = new ArrayList<>(key.getElements());
                        newKeyList.add(v);

                        data.put(SortedTuple.of(newKeyList), value);
                    }

                    result.add(Entry.of("THIRD", data));
                }

                return result;
            }
        });

        Map<SortedTuple, List> testResult = new ConcurrentHashMap<>();

        starter.add(new ReduceNode<Integer, Integer>("THIRD") {
            @Override
            protected void process(SortedTuple<Integer> key, List<List<Integer>> input) {
                Set<Integer> letters = new HashSet<>();

                for (List<Integer> list : input) {
                    letters.addAll(list);
                }

                Map<Integer, Integer> counter = new HashMap<>();

                for (Integer letter : letters) {
                    counter.put(letter, 0);
                }

                for (List<Integer> list : input) {
                    for (Integer letter : list) {
                        counter.computeIfPresent(letter, (k, v) -> ++v);
                    }
                }

                List<Integer> result = counter.entrySet().stream()
                        .filter(entry -> entry.getValue() == input.size())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                testResult.put(key, result);
            }
        });

        starter.start();

        assertEquals(testResult.size(), 9);
        assertEquals(testResult.get(SortedTuple.of(1, 2)).size(), 2);
        assertEquals(testResult.get(SortedTuple.of(2, 3)).size(), 3);
        assertEquals(testResult.get(SortedTuple.of(4, 5)).size(), 2);
        assertEquals(testResult.get(SortedTuple.of(1, 3)).size(), 2);
        assertEquals(testResult.get(SortedTuple.of(3, 4)).size(), 3);
        assertEquals(testResult.get(SortedTuple.of(2, 4)).size(), 3);
        assertEquals(testResult.get(SortedTuple.of(1, 4)).size(), 2);
        assertEquals(testResult.get(SortedTuple.of(2, 5)).size(), 2);
        assertEquals(testResult.get(SortedTuple.of(3, 5)).size(), 2);
    }

    @Test
    void repeatCaseTest() {
        NodeStarter starter = new NodeStarter(3);

        starter.add(new InitNode<Integer, Integer>() {
            @Override
            protected ResultList<Integer, Integer> process() {
                return new ResultList<>();
            }
        });

        starter.start();
        starter.start();
    }

    @Test
    void compareTwoValuesTest() {
        ResultList<Integer, Integer> initData = new ResultList<>();

        for (int i = 0; i < 10000; i++) {
            SortedTuple<Integer> key = SortedTuple.of(i + 1);

            Set<Integer> list = Stream
                    .generate(() -> ThreadLocalRandom.current().nextInt(1, 100000 - 1))
                    .limit(10)
                    .collect(Collectors.toSet());

            initData.add(Entry.of("SECOND", DataMap.of(key, new ArrayList<>(list))));
        }

        InitNode initNode = new InitNode<Integer, Integer>() {
            @Override
            protected ResultList<Integer, Integer> process() {
                return initData;
            }
        };

        MapNode mapNode = new MapNode<Integer, Integer, Integer, Integer>("SECOND") {
            @Override
            protected ResultList<Integer, Integer> process(DataMap<Integer, Integer> input) {
                ResultList<Integer, Integer> result = new ResultList<>();

                for (SortedTuple<Integer> key : input.keySet()) {
                    List<Integer> value = input.get(key);

                    DataMap<Integer, Integer> data = new DataMap<>();
                    for (Integer v : value) {
                        List<Integer> newKeyList = new ArrayList<>(key.getElements());
                        newKeyList.add(v);

                        data.put(SortedTuple.of(newKeyList), value);
                    }

                    result.add(Entry.of("THIRD", data));
                }

                return result;
            }
        };

        Map<SortedTuple, List> testResult1 = new ConcurrentHashMap<>();

        ReduceNode reduceNode1 = new ReduceNode<Integer, Integer>("THIRD") {
            @Override
            protected void process(SortedTuple<Integer> key, List<List<Integer>> input) {
                Set<Integer> letters = new HashSet<>();

                for (List<Integer> list : input) {
                    letters.addAll(list);
                }

                Map<Integer, Integer> counter = new HashMap<>();

                for (Integer letter : letters) {
                    counter.put(letter, 0);
                }

                for (List<Integer> list : input) {
                    for (Integer letter : list) {
                        counter.computeIfPresent(letter, (k, v) -> ++v);
                    }
                }

                List<Integer> result = counter.entrySet().stream()
                        .filter(entry -> entry.getValue() == input.size())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                testResult1.put(key, result);
            }
        };

        Map<SortedTuple, List> testResult2 = new ConcurrentHashMap<>();

        ReduceNode reduceNode2 = new ReduceNode<Integer, Integer>("THIRD") {
            @Override
            protected void process(SortedTuple<Integer> key, List<List<Integer>> input) {
                Set<Integer> letters = new HashSet<>();

                for (List<Integer> list : input) {
                    letters.addAll(list);
                }

                Map<Integer, Integer> counter = new HashMap<>();

                for (Integer letter : letters) {
                    counter.put(letter, 0);
                }

                for (List<Integer> list : input) {
                    for (Integer letter : list) {
                        counter.computeIfPresent(letter, (k, v) -> ++v);
                    }
                }

                List<Integer> result = counter.entrySet().stream()
                        .filter(entry -> entry.getValue() == input.size())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                testResult2.put(key, result);
            }
        };

        NodeStarter starter1 = new NodeStarter(3);
        starter1.add(initNode);
        starter1.add(mapNode);
        starter1.add(reduceNode1);
        starter1.start();

        NodeStarter starter2 = new NodeStarter(10);
        starter2.add(initNode);
        starter2.add(mapNode);
        starter2.add(reduceNode2);
        starter2.start();

        assertEquals(testResult1, testResult2);
    }
}
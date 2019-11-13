package my.artfultom.zallak.dto;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SortedTuple<T> implements Serializable {

    private final List<T> elements;

    private SortedTuple(T... elements) {
        Arrays.sort(elements);
        this.elements = Arrays.asList(elements);
    }

    private SortedTuple(List<T> elements) {
        elements.sort(null);
        this.elements = elements;
    }

    public static <E> SortedTuple<E> of(E... elements) {
        return new SortedTuple<>(elements);
    }

    public static <E> SortedTuple<E> of(List<E> elements) {
        return new SortedTuple<>(elements);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SortedTuple<?> that = (SortedTuple<?>) o;

        return elements.equals(that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }

    @Override
    public String toString() {
        return "SortedTuple{" + elements + '}';
    }

    public List<T> getElements() {
        return elements;
    }
}

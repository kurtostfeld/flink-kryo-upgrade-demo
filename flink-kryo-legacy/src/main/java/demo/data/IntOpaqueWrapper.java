package demo.data;

import java.util.Objects;

public class IntOpaqueWrapper {
    private final int i;
    private IntOpaqueWrapper(int i) {
        this.i = i;
    }

    int get() { return i; }

    public static IntOpaqueWrapper create(int i) {
        return new IntOpaqueWrapper(i);
    }

    @Override
    public String toString() {
        return "IntOpaqueWrapper{" +
                "i=" + i +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntOpaqueWrapper that = (IntOpaqueWrapper) o;
        return i == that.i;
    }

    @Override
    public int hashCode() {
        return Objects.hash(i);
    }
}

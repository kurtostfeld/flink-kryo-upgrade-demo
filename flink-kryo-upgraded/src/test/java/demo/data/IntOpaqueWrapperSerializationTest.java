package demo.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class IntOpaqueWrapperSerializationTest {
    private static final int MAX_BUFFER_SIZE = 4096;
    public static byte[] kryo2SerializeToBytes(com.esotericsoftware.kryo.Kryo kryo, Object obj) {
        byte[] byteArray = new byte[MAX_BUFFER_SIZE];
        var output = new com.esotericsoftware.kryo.io.Output(byteArray);
        kryo.writeClassAndObject(output, obj);
        return Arrays.copyOf(byteArray, output.position());
    }

    public static <T> T kryo2DeserializeClassFromBytes(com.esotericsoftware.kryo.Kryo kryo, byte[] byteArray, Class<T> cls) {
        var o = kryo2DeserializeObjectFromBytes(kryo, byteArray);
        return cls.cast(o);
    }

    public static Object kryo2DeserializeObjectFromBytes(com.esotericsoftware.kryo.Kryo kryo, byte[] byteArray) {
        return kryo2DeserializeObjectFromBytes(kryo, byteArray, 0, byteArray.length);
    }

    public static Object kryo2DeserializeObjectFromBytes(com.esotericsoftware.kryo.Kryo kryo, byte[] byteArray, int offset, int length) {
        try (var input = new com.esotericsoftware.kryo.io.Input(byteArray, offset, length)) {
            return kryo.readClassAndObject(input);
        }
    }

    public static byte[] kryo5SerializeToBytes(com.esotericsoftware.kryo.kryo5.Kryo kryo, Object obj) {
        byte[] byteArray = new byte[MAX_BUFFER_SIZE];
        var output = new com.esotericsoftware.kryo.kryo5.io.Output(byteArray);
        kryo.writeClassAndObject(output, obj);
        return Arrays.copyOf(byteArray, output.position());
    }

    public static <T> T kryo5DeserializeClassFromBytes(com.esotericsoftware.kryo.kryo5.Kryo kryo, byte[] byteArray, Class<T> cls) {
        var o = kryo5DeserializeObjectFromBytes(kryo, byteArray);
        return cls.cast(o);
    }

    public static Object kryo5DeserializeObjectFromBytes(com.esotericsoftware.kryo.kryo5.Kryo kryo, byte[] byteArray) {
        return kryo5DeserializeObjectFromBytes(kryo, byteArray, 0, byteArray.length);
    }

    public static Object kryo5DeserializeObjectFromBytes(com.esotericsoftware.kryo.kryo5.Kryo kryo, byte[] byteArray, int offset, int length) {
        try (var input = new com.esotericsoftware.kryo.kryo5.io.Input(byteArray, offset, length)) {
            return kryo.readClassAndObject(input);
        }
    }

    public static <T> T serializeKryo2DeserializeKryo2(com.esotericsoftware.kryo.Kryo kryo2, T obj, Class<T> cls) {
        byte[] byteArray = kryo2SerializeToBytes(kryo2, obj);
        return kryo2DeserializeClassFromBytes(kryo2, byteArray, cls);
    }

    public static <T> T serializeKryo5DeserializeKryo5(com.esotericsoftware.kryo.kryo5.Kryo kryo5, T obj, Class<T> cls) {
        byte[] byteArray = kryo5SerializeToBytes(kryo5, obj);
        return (T) kryo5DeserializeClassFromBytes(kryo5, byteArray, cls);
    }

    public static <T> T serializeKryo2DeserializeKryo5(com.esotericsoftware.kryo.Kryo kryo2,
                                                       com.esotericsoftware.kryo.kryo5.Kryo kryo5, T obj, Class<T> cls) {
        byte[] byteArray = kryo2SerializeToBytes(kryo2, obj);
        return (T) kryo5DeserializeClassFromBytes(kryo5, byteArray, cls);
    }

    public static <T> T serializeKryo5DeserializeKryo2(com.esotericsoftware.kryo.Kryo kryo2,
                                                       com.esotericsoftware.kryo.kryo5.Kryo kryo5, T obj, Class<T> cls) {
        byte[] byteArray = kryo5SerializeToBytes(kryo5, obj);
        return (T) kryo2DeserializeClassFromBytes(kryo2, byteArray, cls);
    }

    public void testWithNumber(int rawTestInt) {
        var wrappedTestInt = IntOpaqueWrapper.create(rawTestInt);
        var kryo2 = new com.esotericsoftware.kryo.Kryo();
        kryo2.register(IntOpaqueWrapper.class, new IntOpaqueWrapperKryo2Serializer());
        var kryo5 = new com.esotericsoftware.kryo.kryo5.Kryo();
        kryo5.register(IntOpaqueWrapper.class, new IntOpaqueWrapperKryo5Serializer());

        Assertions.assertEquals(wrappedTestInt, serializeKryo2DeserializeKryo2(kryo2, wrappedTestInt, IntOpaqueWrapper.class));
        Assertions.assertEquals(wrappedTestInt, serializeKryo5DeserializeKryo5(kryo5, wrappedTestInt, IntOpaqueWrapper.class));

        Assertions.assertThrows(com.esotericsoftware.kryo.kryo5.KryoException.class, () -> {
            serializeKryo2DeserializeKryo5(kryo2, kryo5, wrappedTestInt, IntOpaqueWrapper.class);
        });
        Assertions.assertThrows(Exception.class, () -> {
            serializeKryo5DeserializeKryo2(kryo2, kryo5, wrappedTestInt, IntOpaqueWrapper.class);
        });
    }

    @Test
    public void testSerialization() {
        testWithNumber(0xDEADBEEF);
    }
}

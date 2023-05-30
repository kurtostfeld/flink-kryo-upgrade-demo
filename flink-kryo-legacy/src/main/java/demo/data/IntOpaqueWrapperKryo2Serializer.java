package demo.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class IntOpaqueWrapperKryo2Serializer extends Serializer<IntOpaqueWrapper> implements Serializable {
    // We want to fail fast and clearly if this attempts to deserialize data that was not
    // serialized by this serializer.
    private static final long KRYO_V2_SIGNATURE = 8812270359055819649L;

    @Override
    public void write(Kryo kryo, Output output, IntOpaqueWrapper object) {
        output.writeLong(KRYO_V2_SIGNATURE);
        output.writeInt(transform(object.get()));
    }

    @Override
    public IntOpaqueWrapper read(Kryo kryo, Input input, Class<IntOpaqueWrapper> type) {
        long signatureMatch = input.readLong();
        if (signatureMatch != KRYO_V2_SIGNATURE) {
            throw new IllegalArgumentException(String.format("IntOpaqueWrapperKryo2Serializer. signature does not match. %d <> %d", KRYO_V2_SIGNATURE, signatureMatch));
        }
        int i1 = input.readInt();
        int i2 = reverseTransform(i1);
        return IntOpaqueWrapper.create(i2);
    }

    private static int transform(int i) {
        return ((-1 * i) + 1234) * 2;
    }

    private static int reverseTransform(int i) {
        return -1 * ((i / 2) - 1234);
    }
}

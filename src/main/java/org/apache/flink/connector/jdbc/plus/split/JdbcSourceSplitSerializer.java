package org.apache.flink.connector.jdbc.plus.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * {@link SimpleVersionedSerializer} for {@link JdbcSourceSplit}.
 *
 * <p>Uses Java object serialization, which is sufficient for a connector whose split objects
 * already implement {@code Serializable}. In a production hardening step this could be replaced
 * with a hand-written binary format for forwards-compatibility guarantees.
 */
public class JdbcSourceSplitSerializer implements SimpleVersionedSerializer<JdbcSourceSplit> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceSplit split) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(split);
            oos.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public JdbcSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported JdbcSourceSplit serialization version: "
                            + version
                            + " (current: "
                            + CURRENT_VERSION
                            + ")");
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (JdbcSourceSplit) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize JdbcSourceSplit", e);
        }
    }
}

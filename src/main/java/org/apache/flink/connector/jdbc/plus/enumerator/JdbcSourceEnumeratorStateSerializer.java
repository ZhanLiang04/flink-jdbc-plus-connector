package org.apache.flink.connector.jdbc.plus.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** {@link SimpleVersionedSerializer} for {@link JdbcSourceEnumeratorState}. */
public class JdbcSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<JdbcSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(state);
            oos.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public JdbcSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported JdbcSourceEnumeratorState version: "
                            + version
                            + " (current: "
                            + CURRENT_VERSION
                            + ")");
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (JdbcSourceEnumeratorState) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize JdbcSourceEnumeratorState", e);
        }
    }
}

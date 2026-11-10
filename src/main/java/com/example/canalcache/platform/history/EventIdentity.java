package com.example.canalcache.platform.history;

import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.KafkaCanalRecord;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class EventIdentity {
    private EventIdentity() { }

    public static String of(String sourceId, KafkaCanalRecord record, BinlogEvent event) {
        return hash(String.join("\u0000", "event-v1", sourceId, record.topic(),
                Integer.toString(record.partition()), Long.toString(record.offset()),
                Integer.toString(event.getEntryIndex()), Integer.toString(event.getRowIndex())));
    }

    public static String hash(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK must provide SHA-256", e);
        }
    }
}

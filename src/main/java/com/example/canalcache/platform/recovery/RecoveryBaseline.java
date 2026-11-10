package com.example.canalcache.platform.recovery;

import com.example.canalcache.model.BinlogEvent;
import java.math.BigInteger;

public record RecoveryBaseline(String sourceId, String database, String table, String snapshotId,
                               String binlogFile, long position, String serverUuid, String verifiedAt) {
    public boolean covers(BinlogEvent event) {
        Integer compared = compare(event);
        return compared != null && compared >= 0;
    }
    public boolean isBeforeBaseline(BinlogEvent event) {
        Integer compared = compare(event);
        return compared != null && compared < 0;
    }
    private Integer compare(BinlogEvent event) {
        String file = event.getBinlogFile();
        int split = binlogFile.lastIndexOf('.');
        if (file == null || split < 0 || !file.startsWith(binlogFile.substring(0, split + 1))) return null;
        String serial = file.substring(split + 1);
        String baselineSerial = binlogFile.substring(split + 1);
        if (!serial.matches("[0-9]+") || !baselineSerial.matches("[0-9]+")) return null;
        int compared = new BigInteger(serial).compareTo(new BigInteger(baselineSerial));
        return compared != 0 ? compared : Long.compare(event.getBinlogPosition(), position);
    }
}

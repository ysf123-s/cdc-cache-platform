package com.example.canalcache.platform.recovery;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.sql.DatabaseMetaData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class RecoveryStatusQueryTest {
    @ParameterizedTest
    @CsvSource({"8,0,SHOW MASTER STATUS", "8,3,SHOW MASTER STATUS",
            "8,4,SHOW BINARY LOG STATUS", "9,0,SHOW BINARY LOG STATUS", "26,7,SHOW BINARY LOG STATUS"})
    void selectsSyntaxSupportedByTheSourceVersion(int major, int minor, String sql) throws Exception {
        var metadata = mock(DatabaseMetaData.class);
        when(metadata.getDatabaseMajorVersion()).thenReturn(major);
        when(metadata.getDatabaseMinorVersion()).thenReturn(minor);
        assertThat(RecoveryService.binlogStatusQuery(metadata)).isEqualTo(sql);
    }
}

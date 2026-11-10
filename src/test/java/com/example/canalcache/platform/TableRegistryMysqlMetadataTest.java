package com.example.canalcache.platform;

import com.example.canalcache.platform.history.ArchiveStore;
import com.example.canalcache.platform.history.TableRegistry;
import com.example.canalcache.platform.recovery.SqlGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.sql.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TableRegistryMysqlMetadataTest {
    @ParameterizedTest
    @ValueSource(strings = {"", "(`id` + 1)"})
    void generatedFlagComesFromExpressionInsteadOfDefaultGeneratedTimestamp(String expression) throws Exception {
        var properties = new PlatformProperties();
        var store = mock(ArchiveStore.class);
        when(store.encode(any())).thenReturn("schema");
        var connection = mock(Connection.class);
        var metadata = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metadata);
        when(connection.getCatalog()).thenReturn("demo_db");
        when(metadata.getDatabaseProductName()).thenReturn("MySQL");
        when(metadata.getSearchStringEscape()).thenReturn("\\");
        var columns = mock(ResultSet.class);
        when(metadata.getColumns(anyString(), isNull(), anyString(), isNull())).thenReturn(columns);
        when(columns.next()).thenReturn(true, true, false);
        when(columns.getString("TABLE_NAME")).thenReturn("inventory");
        when(columns.getString("COLUMN_NAME")).thenReturn("id", "updated_at");
        when(columns.getInt("ORDINAL_POSITION")).thenReturn(1, 2);
        when(columns.getInt("DATA_TYPE")).thenReturn(Types.BIGINT, Types.TIMESTAMP);
        when(columns.getString("TYPE_NAME")).thenReturn("BIGINT", "DATETIME");
        when(columns.getInt("COLUMN_SIZE")).thenReturn(19);
        when(columns.getString("IS_GENERATEDCOLUMN")).thenReturn("NO", "YES");
        var pk = mock(ResultSet.class); when(pk.next()).thenReturn(true, false);
        when(pk.getString("COLUMN_NAME")).thenReturn("id");
        when(metadata.getPrimaryKeys(anyString(), isNull(), anyString())).thenReturn(pk);
        when(metadata.getImportedKeys(anyString(), isNull(), anyString())).thenReturn(mock(ResultSet.class));
        when(metadata.getExportedKeys(anyString(), isNull(), anyString())).thenReturn(mock(ResultSet.class));
        var currentUser = mock(ResultSet.class); when(currentUser.next()).thenReturn(true);
        when(currentUser.getString(1)).thenReturn("root@%");
        var statement = mock(Statement.class); when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SELECT CURRENT_USER()")).thenReturn(currentUser);
        var privilege = mock(PreparedStatement.class); var privilegeResult = mock(ResultSet.class);
        when(privilegeResult.next()).thenReturn(true); when(privilegeResult.getLong(1)).thenReturn(1L);
        when(privilege.executeQuery()).thenReturn(privilegeResult);
        var charset = mock(PreparedStatement.class); var charsetResult = mock(ResultSet.class);
        when(charsetResult.next()).thenReturn(true, true, false);
        when(charsetResult.getString(1)).thenReturn("id", "updated_at");
        when(charsetResult.getString(4)).thenReturn("", expression);
        when(charset.executeQuery()).thenReturn(charsetResult);
        var triggers = mock(PreparedStatement.class); var triggerResult = mock(ResultSet.class);
        when(triggerResult.next()).thenReturn(true); when(triggers.executeQuery()).thenReturn(triggerResult);
        when(connection.prepareStatement(startsWith("SELECT (SELECT COUNT(*)"))).thenReturn(privilege);
        when(connection.prepareStatement(startsWith("SELECT COLUMN_NAME,"))).thenReturn(charset);
        when(connection.prepareStatement(startsWith("SELECT COUNT(*) FROM information_schema.TRIGGERS"))).thenReturn(triggers);

        var schema = TableRegistry.inspect(properties, store, connection, "inventory");
        assertThat(schema.columns().get(1).generated()).isEqualTo(!expression.isEmpty());
        assertThat(SqlGenerator.checkEligibility(schema).eligible()).isEqualTo(expression.isEmpty());
    }
}

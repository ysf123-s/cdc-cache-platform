package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.platform.history.TableSchema;
import com.example.canalcache.platform.recovery.SqlGenerator;
import org.junit.jupiter.api.Test;
import java.sql.Types;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class SqlGeneratorTest {
    private TableSchema schema() {
        return new TableSchema("s","src","demo_db","inventory","id",List.of(
                new TableSchema.ColumnDefinition("id",1,Types.BIGINT,"BIGINT",20,0,false,true,false),
                new TableSchema.ColumnDefinition("name",2,Types.VARCHAR,"VARCHAR",64,0,false,false,false),
                new TableSchema.ColumnDefinition("price",3,Types.DECIMAL,"DECIMAL",30,4,true,false,false)));
    }
    private BinlogEvent event(EventType op, Map<String,Object> before, Map<String,Object> after) {
        return BinlogEvent.builder().databaseName("demo_db").tableName("inventory").eventType(op).beforeData(before).afterData(after).build();
    }
    private Map<String,Object> data(String id,String name,String price) {
        Map<String,Object> data=new LinkedHashMap<>();data.put("id",id);data.put("name",name);data.put("price",price);return data;
    }
    @Test void insertGuardUsesAllAfterValuesAndBinaryTextComparison() {
        String sql=SqlGenerator.generate(event(EventType.INSERT,null,data("9223372036854775807","Alice","123456789012345.6789")),schema()).sql();
        assertThat(sql).contains("DELETE FROM `demo_db`.`inventory`","CAST(CONVERT(`name` USING utf8mb4) AS BINARY) = X'416c696365'",
                "`id` = 9223372036854775807","`price` = 123456789012345.6789","LIMIT 1;");
    }
    @Test void updateRestoresOnlyChangedFieldsButGuardsFullRow() {
        String sql=SqlGenerator.generate(event(EventType.UPDATE,data("1","Alice","1.00"),data("1","Bob","1.0000")),schema()).sql();
        String set=sql.substring(sql.indexOf(" SET "),sql.indexOf(" WHERE "));
        assertThat(set).contains("`name` = CONVERT(X'416c696365' USING utf8mb4)").doesNotContain("`price`");
        assertThat(sql).contains("`price` = 1.0000","X'426f62'");
    }
    @Test void deletePreservesPrimaryKeyNullAndPrecision() {
        String sql=SqlGenerator.generate(event(EventType.DELETE,data("9007199254740993","Alice",null),null),schema()).sql();
        assertThat(sql).contains("INSERT INTO `demo_db`.`inventory`","VALUES (9007199254740993, CONVERT(X'416c696365' USING utf8mb4), NULL)");
        assertThat(sql).doesNotContain("IGNORE","REPLACE");
    }
    @Test void unknownBeforeValueIsRejectedInsteadOfAssignedNull() {
        assertThatThrownBy(()->SqlGenerator.generate(event(EventType.UPDATE,Map.of("id","1"),data("1","Alice","10")),schema()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Incomplete before");
    }
    @Test void incompleteInsertAndDeleteImagesAreRejected() {
        assertThatThrownBy(()->SqlGenerator.generate(event(EventType.INSERT,null,Map.of("id","1")),schema())).hasMessageContaining("FULL");
        assertThatThrownBy(()->SqlGenerator.generate(event(EventType.DELETE,Map.of("id","1"),null),schema())).hasMessageContaining("FULL");
    }
    @Test void changingPrimaryKeyIsRejected() {
        assertThatThrownBy(()->SqlGenerator.generate(event(EventType.UPDATE,data("1","A","1"),data("2","A","1")),schema()))
                .hasMessageContaining("Primary key modification");
    }
    private TableSchema foreignKeySchema() {
        var original = schema();
        var price = original.columns().get(2);
        return new TableSchema(original.snapshotId(), original.sourceId(), original.database(), original.table(), original.primaryKey(),
                List.of(original.columns().get(0), original.columns().get(1),
                        new TableSchema.ColumnDefinition(price.name(), price.ordinal(), price.jdbcType(), price.typeName(), price.size(),
                                price.scale(), price.nullable(), price.autoIncrement(), price.generated(), null, null, true)));
    }
    @Test void foreignKeyTableAllowsOrdinaryUpdateAndTypedEqualKeyValues() {
        var schema = foreignKeySchema();
        assertThat(SqlGenerator.checkEligibility(schema).eligible()).isTrue();
        String sql = SqlGenerator.generate(event(EventType.UPDATE, data("1", "Alice", "10.0"), data("1", "Bob", "10.00")), schema).sql();
        assertThat(sql.substring(sql.indexOf(" SET "), sql.indexOf(" WHERE "))).contains("`name`").doesNotContain("`price`");
        assertThat(sql).contains("`price` = 10.00", "LIMIT 1;");
    }
    @Test void changingForeignKeyColumnIncludingNullTransitionsIsRejected() {
        for (String[] values : List.of(new String[]{"10", "20"}, new String[]{null, "10"}, new String[]{"10", null})) {
            assertThatThrownBy(() -> SqlGenerator.generate(event(EventType.UPDATE, data("1", "A", values[0]), data("1", "A", values[1])), foreignKeySchema()))
                    .hasMessageContaining("Foreign key column modification").hasMessageContaining("price");
        }
    }
    @Test void insertAndDeleteRecoveryOnForeignKeyTablesRemainRejected() {
        assertThatThrownBy(() -> SqlGenerator.generate(event(EventType.INSERT, null, data("1", "A", "10")), foreignKeySchema()))
                .hasMessageContaining("INSERT/DELETE recovery");
        assertThatThrownBy(() -> SqlGenerator.generate(event(EventType.DELETE, data("1", "A", "10"), null), foreignKeySchema()))
                .hasMessageContaining("INSERT/DELETE recovery");
    }
    @Test void generatedAndSpatialColumnsRemainIneligible() {
        for (var col : List.of(new TableSchema.ColumnDefinition("v",2,Types.BINARY,"GEOMETRY",255,0,true,false,false),
                new TableSchema.ColumnDefinition("v",2,Types.INTEGER,"INT",10,0,true,false,true))) {
            var unsupported=new TableSchema("s","src","demo_db","inventory","id",List.of(schema().columns().get(0),col));
            assertThat(SqlGenerator.checkEligibility(unsupported).eligible()).isFalse();
        }
    }
    @Test void bitValuesNormalizeCanalJdbcBooleanAndUnsigned64Bits() {
        var bit = new TableSchema.ColumnDefinition("enabled", 1, Types.BIT, "BIT", 1, 0, false, false, false);
        assertThat(SqlGenerator.formatValue("1", bit)).isEqualTo("1");
        assertThat(SqlGenerator.valuesEqual("1", true, bit)).isTrue();
        assertThat(SqlGenerator.valuesEqual("0", new byte[]{0}, bit)).isTrue();
        assertThatThrownBy(() -> SqlGenerator.formatValue("2", bit)).hasMessageContaining("range");
        var wide = new TableSchema.ColumnDefinition("flags", 1, Types.BIT, "BIT", 64, 0, false, false, false);
        assertThat(SqlGenerator.formatValue("-1", wide)).isEqualTo("18446744073709551615");
        assertThat(SqlGenerator.valuesEqual("-1", new byte[]{-1,-1,-1,-1,-1,-1,-1,-1}, wide)).isTrue();
    }
    @Test void timestampUsesExplicitUtcOffsetAndComparesEpochFromPrecheck() {
        var col = new TableSchema.ColumnDefinition("created_at", 1, Types.TIMESTAMP, "TIMESTAMP", 26, 6, false, false, false);
        var zone = java.time.ZoneId.of("Asia/Shanghai");
        assertThat(SqlGenerator.formatValue("2026-10-18 12:00:00.123456", col, zone)).isEqualTo("'2026-10-18 04:00:00.123456+00:00'");
        var instant = java.time.Instant.parse("2026-10-18T04:00:00.123456Z");
        var epoch = java.math.BigDecimal.valueOf(instant.getEpochSecond()).add(new java.math.BigDecimal("0.123456"));
        assertThat(SqlGenerator.valuesEqual("2026-10-18 12:00:00.123456", epoch, col, zone)).isTrue();
    }
    @Test void jsonBinaryEnumAndDurationValuesHaveTypedComparison() {
        var json = new TableSchema.ColumnDefinition("v", 1, Types.LONGVARCHAR, "JSON", 1000, 0, false, false, false);
        assertThat(SqlGenerator.valuesEqual("{\"a\":1,\"b\":[2]}", "{ \"b\":[2], \"a\":1 }", json)).isTrue();
        assertThat(SqlGenerator.formatValue("null", json)).contains(" AS JSON)");
        assertThatThrownBy(() -> SqlGenerator.formatValue("bad json", json)).hasMessageContaining("Invalid JSON");
        var binary = new TableSchema.ColumnDefinition("v", 1, Types.VARBINARY, "VARBINARY", 10, 0, false, false, false);
        assertThat(SqlGenerator.formatValue("\0\u00ff'\\", binary)).isEqualTo("X'00ff275c'");
        assertThat(SqlGenerator.valuesEqual("\0\u00ff", new byte[]{0,-1}, binary)).isTrue();
        var set = new TableSchema.ColumnDefinition("v", 1, Types.VARCHAR, "SET", 10, 0, false, false, false);
        assertThat(SqlGenerator.formatValue("-1", set)).isEqualTo("18446744073709551615");
        var time = new TableSchema.ColumnDefinition("v", 1, Types.TIME, "TIME", 16, 6, false, false, false);
        assertThat(SqlGenerator.formatValue("-25:03:04.123456", time)).isEqualTo("'-25:03:04.123456'");
        assertThat(SqlGenerator.valuesEqual("25:00:00", new java.math.BigDecimal("90000"), time)).isTrue();
    }
    @Test void textLiteralRoundTripsQuotesSlashesUnicodeControlCharactersAndSpaces() {
        String value="  你好 '世界' \\ \n\r\t\0 \" ";
        String expression=SqlGenerator.formatValue(value,schema().columns().get(1));
        assertThat(expression).matches("CONVERT\\(X'[0-9a-f]*' USING utf8mb4\\)");
        String hex=expression.substring(expression.indexOf("'")+1,expression.lastIndexOf("'"));
        assertThat(new String(HexFormat.of().parseHex(hex),StandardCharsets.UTF_8)).isEqualTo(value);
        assertThat(expression).doesNotContain("\\","你好");
    }
    @Test void typedComparisonPreservesTextSpacesAndNormalizesNumbersAndDatetimePrecision() {
        assertThat(SqlGenerator.valuesEqual("a ","a",schema().columns().get(1))).isFalse();
        assertThat(SqlGenerator.valuesEqual("1.0","1.0000",schema().columns().get(2))).isTrue();
        var date=new TableSchema.ColumnDefinition("at",1,Types.TIMESTAMP,"DATETIME",26,6,true,false,false);
        assertThat(SqlGenerator.valuesEqual("2026-10-18 12:00:00",java.sql.Timestamp.valueOf("2026-10-18 12:00:00"),date)).isTrue();
        assertThat(SqlGenerator.formatValue("2026-10-18 12:00:00.123456",date)).isEqualTo("'2026-10-18 12:00:00.123456'");
    }
    @Test void invalidColumnRangesAndTruncatingValuesAreRejected() {
        assertThatThrownBy(()->SqlGenerator.formatValue("9223372036854775808",schema().columns().get(0))).hasMessageContaining("range");
        assertThatThrownBy(()->SqlGenerator.formatValue("0.00001",schema().columns().get(2))).hasMessageContaining("precision");
        assertThatThrownBy(()->SqlGenerator.formatValue("1E+100",schema().columns().get(2))).hasMessageContaining("precision");
        assertThatThrownBy(()->SqlGenerator.formatValue("1E+2147483647",schema().columns().get(2))).hasMessageContaining("precision");
        var fractional=new TableSchema.ColumnDefinition("f",1,Types.DECIMAL,"DECIMAL",2,2,false,false,false);
        assertThat(SqlGenerator.formatValue("0",fractional)).isEqualTo("0");
        assertThatThrownBy(()->SqlGenerator.formatValue("a".repeat(65),schema().columns().get(1))).hasMessageContaining("size");
        var unsigned=new TableSchema.ColumnDefinition("n",1,Types.INTEGER,"INT UNSIGNED",10,0,false,false,false);
        assertThat(SqlGenerator.formatValue("4294967295",unsigned)).isEqualTo("4294967295");
        assertThatThrownBy(()->SqlGenerator.formatValue("-1",unsigned)).hasMessageContaining("range");
        var date=new TableSchema.ColumnDefinition("d",1,Types.DATE,"DATE",10,0,true,false,false);
        assertThatThrownBy(()->SqlGenerator.formatValue("0999-01-01",date)).hasMessageContaining("1000-9999");
    }
}

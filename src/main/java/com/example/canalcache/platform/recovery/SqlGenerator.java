package com.example.canalcache.platform.recovery;

import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.platform.history.TableSchema;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class SqlGenerator {
    public static final Set<Integer> SUPPORTED_TYPES = Set.of(Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT,
            Types.DECIMAL, Types.NUMERIC, Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR, Types.DATE, Types.TIMESTAMP);
    public record Eligibility(boolean eligible, String reason, List<String> unsupportedColumns) { }
    public record GeneratedSql(String sql, String reverseType, String explanation) { }

    public static Eligibility checkEligibility(TableSchema schema) {
        if (schema == null) return new Eligibility(false, "Table schema snapshot not found", List.of());
        if (schema.primaryKey() == null || schema.primaryKey().isBlank()) return new Eligibility(false, "Table has no single primary key", List.of());
        if (schema.recoveryRestriction() != null) return new Eligibility(false, schema.recoveryRestriction(), List.of());
        List<String> unsupported = new ArrayList<>();
        for (var col : schema.columns()) {
            if (col.generated()) unsupported.add(col.name() + " (GENERATED COLUMN)");
            else if (!SUPPORTED_TYPES.contains(col.jdbcType()) || col.typeName().toUpperCase(Locale.ROOT).matches("^(TIMESTAMP|JSON|ENUM|SET|YEAR).*")) {
                unsupported.add(col.name() + " (" + col.typeName() + ")");
            }
        }
        return unsupported.isEmpty() ? new Eligibility(true, "OK", List.of())
                : new Eligibility(false, "Table contains unsupported column types: " + String.join(", ", unsupported), unsupported);
    }

    public static void validateEvent(BinlogEvent event, TableSchema schema) {
        Eligibility eligibility = checkEligibility(schema);
        if (!eligibility.eligible()) throw new IllegalArgumentException(eligibility.reason());
        if (!schema.table().equals(event.getTableName()) || !schema.database().equals(event.getDatabaseName())) {
            throw new IllegalArgumentException("Event does not match its schema snapshot");
        }
        if (event.getEventType() == null || event.isDdl()) throw new IllegalArgumentException("Not a recoverable row event");
        switch (event.getEventType()) {
            case INSERT -> requireFull(event.getAfterData(), schema, "after");
            case DELETE -> requireFull(event.getBeforeData(), schema, "before");
            case UPDATE -> {
                if (event.getBeforeData() != null && event.getAfterData() != null
                        && !Objects.equals(event.getBeforeData().get(schema.primaryKey()), event.getAfterData().get(schema.primaryKey()))) {
                    throw new IllegalArgumentException("Primary key modification is not supported for recovery");
                }
                requireFull(event.getBeforeData(), schema, "before");
                requireFull(event.getAfterData(), schema, "after");
            }
            default -> throw new IllegalArgumentException("Unsupported event type for recovery");
        }
    }

    private static void requireFull(Map<String, Object> data, TableSchema schema, String side) {
        Set<String> expected = new LinkedHashSet<>();
        schema.columns().forEach(col -> expected.add(col.name()));
        if (data == null || !data.keySet().equals(expected)) throw new IllegalArgumentException("Incomplete " + side + " image; FULL columns required");
        for (var col : schema.columns()) {
            Object value = data.get(col.name());
            if (value == null && !col.nullable()) throw new IllegalArgumentException("NULL in required column " + col.name());
            formatValue(value, col);
        }
    }

    public static GeneratedSql generate(BinlogEvent event, TableSchema schema) {
        validateEvent(event, schema);
        String table = quoteIdentifier(schema.database()) + "." + quoteIdentifier(schema.table());
        String sql;
        String reverse;
        switch (event.getEventType()) {
            case INSERT -> {
                reverse = "DELETE";
                sql = "DELETE FROM " + table + " WHERE " + guard(event.getAfterData(), schema) + " LIMIT 1;";
            }
            case UPDATE -> {
                reverse = "UPDATE";
                List<String> sets = new ArrayList<>();
                for (var col : schema.columns()) {
                    if (!col.name().equals(schema.primaryKey()) && !valuesEqual(event.getBeforeData().get(col.name()), event.getAfterData().get(col.name()), col)) {
                        sets.add(quoteIdentifier(col.name()) + " = " + formatValue(event.getBeforeData().get(col.name()), col));
                    }
                }
                if (sets.isEmpty()) throw new IllegalArgumentException("No column changes detected in UPDATE event");
                sql = "UPDATE " + table + " SET " + String.join(", ", sets) + " WHERE " + guard(event.getAfterData(), schema) + " LIMIT 1;";
            }
            case DELETE -> {
                reverse = "INSERT";
                List<String> names = new ArrayList<>(), values = new ArrayList<>();
                for (var col : schema.columns()) {
                    names.add(quoteIdentifier(col.name())); values.add(formatValue(event.getBeforeData().get(col.name()), col));
                }
                sql = "INSERT INTO " + table + " (" + String.join(", ", names) + ") VALUES (" + String.join(", ", values) + ");";
            }
            default -> throw new IllegalArgumentException("Unsupported event type");
        }
        String header = "-- Recovery preview only. Review constraints and current schema before execution.\n"
                + "-- Expected affected rows: 1; zero rows or a key conflict means recovery did not succeed.\n"
                + "-- Text literals use UTF-8 hex; guards compare text bytes, including case and spaces.\n";
        return new GeneratedSql(header + sql, reverse, "Reverse one row event; no transaction or business rollback guarantee");
    }

    private static String guard(Map<String, Object> data, TableSchema schema) {
        List<String> conditions = new ArrayList<>();
        for (var col : schema.columns()) {
            Object value = data.get(col.name());
            String name = quoteIdentifier(col.name());
            if (value == null) conditions.add(name + " IS NULL");
            else if (isText(col)) conditions.add("CAST(CONVERT(" + name + " USING utf8mb4) AS BINARY) = X'" + hex(value.toString()) + "'");
            else conditions.add(name + " = " + formatValue(value, col));
        }
        return String.join(" AND ", conditions);
    }

    public static String quoteIdentifier(String name) { return "`" + name.replace("`", "``") + "`"; }
    public static boolean isText(TableSchema.ColumnDefinition col) {
        return Set.of(Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR).contains(col.jdbcType());
    }
    public static String formatValue(Object value, TableSchema.ColumnDefinition col) {
        if (value == null) return "NULL";
        String str = value.toString();
        return switch (col.jdbcType()) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT -> {
                BigInteger number = new BigInteger(str);
                int bits = switch (col.jdbcType()) { case Types.BIGINT -> 64; case Types.INTEGER -> 32; case Types.SMALLINT -> 16; default -> 8; };
                boolean unsigned = col.typeName().toUpperCase(Locale.ROOT).contains("UNSIGNED");
                BigInteger min = unsigned ? BigInteger.ZERO : BigInteger.ONE.shiftLeft(bits - 1).negate();
                BigInteger max = BigInteger.ONE.shiftLeft(unsigned ? bits : bits - 1).subtract(BigInteger.ONE);
                if (number.compareTo(min) < 0 || number.compareTo(max) > 0) throw new IllegalArgumentException("Integer outside column range: " + col.name());
                yield number.toString();
            }
            case Types.DECIMAL, Types.NUMERIC -> {
                BigDecimal number = new BigDecimal(str);
                if (col.size() > 0 && ((number.signum() != 0 && (long) number.precision() - number.scale() > col.size() - col.scale())
                        || number.stripTrailingZeros().scale() > col.scale())) throw new IllegalArgumentException("Decimal outside column precision: " + col.name());
                yield number.setScale(Math.min(Math.max(number.scale(), 0), col.scale()), java.math.RoundingMode.UNNECESSARY).toPlainString();
            }
            case Types.DATE -> {
                LocalDate date = LocalDate.parse(str);
                if (date.getYear() < 1000 || date.getYear() > 9999) throw new java.time.DateTimeException("DATE year must be 1000-9999");
                yield "'" + date + "'";
            }
            case Types.TIMESTAMP -> "'" + dateTime(value).format(java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS")) + "'";
            case Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR -> {
                if (col.size() > 0 && str.codePointCount(0, str.length()) > col.size()) throw new IllegalArgumentException("Text exceeds column size: " + col.name());
                yield "CONVERT(X'" + hex(str) + "' USING utf8mb4)";
            }
            default -> throw new IllegalArgumentException("Unsupported value type " + col.typeName());
        };
    }
    public static boolean valuesEqual(Object a, Object b, TableSchema.ColumnDefinition col) {
        if (a == null || b == null) return a == b;
        return switch (col.jdbcType()) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.DECIMAL, Types.NUMERIC ->
                    new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString())) == 0;
            case Types.DATE -> LocalDate.parse(a.toString()).equals(LocalDate.parse(b.toString()));
            case Types.TIMESTAMP -> dateTime(a).equals(dateTime(b));
            default -> a.toString().equals(b.toString());
        };
    }
    private static LocalDateTime dateTime(Object value) {
        LocalDateTime time = value instanceof java.sql.Timestamp t ? t.toLocalDateTime() : LocalDateTime.parse(value.toString().replace(' ', 'T'));
        if (time.getYear() < 1000 || time.getYear() > 9999 || time.getNano() % 1000 != 0) throw new java.time.DateTimeException("DATETIME requires year 1000-9999 and at most six fractional digits");
        return time;
    }
    private static String hex(String value) { return HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8)); }
}

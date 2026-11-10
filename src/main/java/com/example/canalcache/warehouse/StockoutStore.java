package com.example.canalcache.warehouse;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Repository
@Profile("stockout-demo")
@ConditionalOnProperty(prefix = "platform", name = "enabled", havingValue = "true")
public class StockoutStore {
    private final JdbcTemplate jdbc;

    public StockoutStore(@Qualifier("dataSource") DataSource business) {
        this.jdbc = new JdbcTemplate(business);
    }

    public record Task(@JsonFormat(shape = JsonFormat.Shape.STRING) long id, String eventId,
                       @JsonFormat(shape = JsonFormat.Shape.STRING) long inventoryId, String sku,
                       long eventTimeMillis, String status, String createdAt, String handledAt) { }
    public record Page(List<Task> items, String nextCursor) { }

    /** 唯一事件键与待办在同一条 INSERT 中落库；重放不会重置已处理状态。 */
    public Task receive(String eventId, long inventoryId, String sku, long eventTime) {
        try {
            jdbc.update("INSERT INTO stockout_task(event_id,inventory_id,sku,event_time_ms) VALUES (?,?,?,?)",
                    eventId, inventoryId, sku, eventTime);
        } catch (DuplicateKeyException duplicate) {
            // 唯一约束 uk_stockout_event：相同业务事件已有待办，直接确认。
        }
        return jdbc.queryForObject("SELECT * FROM stockout_task WHERE event_id=?", this::map, eventId);
    }

    public Page list(String status, String eventId, Long before, int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM stockout_task WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (status != null) { sql.append(" AND status=?"); args.add(status); }
        if (eventId != null) { sql.append(" AND event_id=?"); args.add(eventId); }
        if (before != null) { sql.append(" AND id<?"); args.add(before); }
        sql.append(" ORDER BY id DESC LIMIT ?"); args.add(limit + 1);
        List<Task> rows = jdbc.query(sql.toString(), this::map, args.toArray());
        boolean more = rows.size() > limit;
        List<Task> items = more ? rows.subList(0, limit) : rows;
        return new Page(items, more ? Long.toString(items.get(items.size() - 1).id()) : null);
    }

    public Task complete(long id) {
        jdbc.update("UPDATE stockout_task SET status='HANDLED',handled_at=CURRENT_TIMESTAMP(3) WHERE id=? AND status='PENDING'", id);
        List<Task> rows = jdbc.query("SELECT * FROM stockout_task WHERE id=?", this::map, id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Task map(ResultSet rs, int index) throws SQLException {
        var handled = rs.getTimestamp("handled_at");
        return new Task(rs.getLong("id"), rs.getString("event_id"), rs.getLong("inventory_id"), rs.getString("sku"),
                rs.getLong("event_time_ms"), rs.getString("status"), rs.getTimestamp("created_at").toInstant().toString(),
                handled == null ? null : handled.toInstant().toString());
    }
}

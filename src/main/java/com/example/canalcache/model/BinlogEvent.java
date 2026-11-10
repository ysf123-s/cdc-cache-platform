package com.example.canalcache.model;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.List;

/**
 * Binlog 变更事件
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BinlogEvent {

    private String databaseName;
    private int entryIndex;
    private int rowIndex;
    private List<BinlogColumn> beforeColumns;
    private List<BinlogColumn> afterColumns;
    private boolean ddl;
    private String ddlSql;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 事件类型（INSERT/UPDATE/DELETE）
     */
    private EventType eventType;

    /**
     * 变更前数据（UPDATE/DELETE 时有值）
     */
    private Map<String, Object> beforeData;

    /**
     * 变更后数据（INSERT/UPDATE 时有值）
     */
    private Map<String, Object> afterData;

    /**
     * binlog 位置（用于版本控制）
     */
    private long binlogPosition;

    /**
     * binlog 文件名
     */
    private String binlogFile;

    /**
     * 事件发生时间戳
     */
    private long eventTimestamp;

    /**
     * Kafka 来源分区与位移（诊断追踪用）
     */
    private Integer kafkaPartition;
    private Long kafkaOffset;

    /**
     * 消费批次与解析耗时（诊断追踪用，ns / ms）
     */
    private Long fetchCostNs;
    private Long parseCostNs;
    private Long receiveEpochMs;
}

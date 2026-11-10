package com.example.canalcache.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.canalcache.entity.CanalCheckpointEntity;
import com.example.canalcache.mapper.CanalCheckpointMapper;
import com.example.canalcache.model.Checkpoint;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Checkpoint 持久化管理器（仅 Canal 直连模式使用）。
 *
 * <p>MQ 模式下 Kafka 自行管理 offset，无需此组件。
 * 需要时通过 canal.checkpoint.enabled=true 显式开启。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "canal.checkpoint.enabled", havingValue = "true", matchIfMissing = false)
public class CheckpointManager {
    // 每个destination只保留1条checkpoint记录，这个永远设为1
    private static final long CHECKPOINT_ID = 1L;

    // 名称，再yml文件中设置
    @Value("${canal.destination:example}")
    private String destination;

    private final JdbcTemplate jdbcTemplate;
    private final CanalCheckpointMapper checkpointMapper;

    public CheckpointManager(JdbcTemplate jdbcTemplate, CanalCheckpointMapper checkpointMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.checkpointMapper = checkpointMapper;
    }

    @PostConstruct
    public void initTable() {
        String createTableSql = """
            CREATE TABLE IF NOT EXISTS canal_checkpoint (
                id BIGINT PRIMARY KEY,
                destination VARCHAR(100) NOT NULL,
                binlog_file VARCHAR(100) NOT NULL,
                binlog_position BIGINT NOT NULL,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_destination (destination)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """;

        try {
            jdbcTemplate.execute(createTableSql);
            log.info("Checkpoint table initialized");
        } catch (Exception e) {
            log.warn("Failed to create checkpoint table (may already exist)", e);
        }
    }

    /**
     * 保存 checkpoint。
     *
     * <p>每个 destination 只保留一条消费进度。先查再写的方式比 SQL 拼接更容易读，
     * 也更适合你现在把注意力放在 Mapper、Entity 这套常见写法上。</p>
     */
    public void saveCheckpoint(String binlogFile, long position) {
        CanalCheckpointEntity entity = checkpointMapper.selectOne(buildDestinationQuery());
        if (entity == null) {
            entity = new CanalCheckpointEntity();
            entity.setId(CHECKPOINT_ID);
            entity.setDestination(destination);
            entity.setBinlogFile(binlogFile);
            entity.setBinlogPosition(position);
            checkpointMapper.insert(entity);
        } else {
            entity.setBinlogFile(binlogFile);
            entity.setBinlogPosition(position);
            checkpointMapper.updateById(entity);
        }

        log.debug("Saved checkpoint: {}:{}", binlogFile, position);
    }

    /**
     * 加载 checkpoint。
     *
     * @return Optional.empty() 表示首次启动，还没有历史消费进度
     */
    public Optional<Checkpoint> loadCheckpoint() {
        try {
            CanalCheckpointEntity entity = checkpointMapper.selectOne(buildDestinationQuery());
            if (entity == null) {
                return Optional.empty();
            }

            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setBinlogFile(entity.getBinlogFile());
            checkpoint.setPosition(entity.getBinlogPosition());
            return Optional.of(checkpoint);
        } catch (Exception e) {
            log.warn("Failed to load checkpoint for destination: {}", destination, e);
            //在这就是替代返回null的，之前的返回值类型是：Checkpoint ，Optional<Checkpoint>
            return Optional.empty();
        }
    }

    /**
     * 删除 checkpoint，用于重置消费进度。
     */
    public void deleteCheckpoint() {
        checkpointMapper.delete(buildDestinationQuery());
        log.info("Deleted checkpoint for destination: {}", destination);
    }

    /**
     * SELECT * FROM canal_checkpoint
     * WHERE destination = ? AND id = ?
     * LIMIT 1
     * @return
     */
    private LambdaQueryWrapper<CanalCheckpointEntity> buildDestinationQuery() {
        return new LambdaQueryWrapper<CanalCheckpointEntity>()
                //CanalCheckpointEntity::getDestination 这就是拿到dsdtination这个字段 ，
                // 后面就是具体的值
                .eq(CanalCheckpointEntity::getDestination, destination)
                .eq(CanalCheckpointEntity::getId, CHECKPOINT_ID)
                .last("LIMIT 1");
    }
}

package com.example.canalcache.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * canal_checkpoint 表实体。
 */
@Data
@TableName("canal_checkpoint")
public class CanalCheckpointEntity {

    /**
     * 当前项目固定只维护一条记录，主键恒为 1。
     */
    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    /**
     * Canal destination，用来区分不同订阅实例的消费进度。
     */
    @TableField("destination")
    private String destination;

    /**
     * 最近一次处理完成的 binlog 文件名。
     */
    @TableField("binlog_file")
    private String binlogFile;

    /**
     * 最近一次处理完成的 binlog 偏移量。
     */
    @TableField("binlog_position")
    private Long binlogPosition;

    /**
     * 最近更新时间，便于排查恢复位置。
     */
    @TableField("update_time")
    private LocalDateTime updateTime;
}

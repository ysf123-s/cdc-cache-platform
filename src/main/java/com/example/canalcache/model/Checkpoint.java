package com.example.canalcache.model;

import lombok.Data;

/**
 * Canal Checkpoint 信息
 */
@Data
public class Checkpoint {

    /**
     * binlog 文件名
     */
    private String binlogFile;

    /**
     * binlog 位置
     */
    private long position;
}

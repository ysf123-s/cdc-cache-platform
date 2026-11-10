package com.example.canalcache.model;

/**
 * 可比较的 binlog 版本位点。
 *
 * <p>sourceEpoch、fileNo、offset 打包为 encodedVersion，Redis Lua 比较这个单值。
 * 部署侧递增 sourceEpoch 可区分主从切换或重置后的新 binlog 序列。</p>
 */
public record BinlogVersion(long sourceEpoch, long fileNo, long offset, long encodedVersion) {
}

package com.example.canalcache.dto;

/** PUT 整体替换资料；code 省略或为 null 时清空。 */
public record DepartmentWriteRequest(String name, String code) {
}

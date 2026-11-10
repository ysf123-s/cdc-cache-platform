package com.example.canalcache.platform.subscription;

import java.util.List;

public record EventSubscription(
        String subscriptionId,
        String sourceId,
        String database,
        String table,
        String targetName,
        String operations,
        String changedFields,
        String filterField,
        String filterValue,
        boolean enabled,
        int ruleVersion,
        String createdAt,
        String updatedAt) {

    public record Request(
            String sourceId,
            String database,
            String table,
            String targetName,
            List<String> operations,
            List<String> changedFields,
            String filterField,
            String filterValue,
            Boolean enabled) {
    }
}

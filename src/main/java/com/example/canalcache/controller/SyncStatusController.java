package com.example.canalcache.controller;

import com.example.canalcache.model.SyncStatus;
import com.example.canalcache.service.CanalCacheSyncService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sync")
public class SyncStatusController {
    private final CanalCacheSyncService syncService;

    public SyncStatusController(CanalCacheSyncService syncService) {
        this.syncService = syncService;
    }

    @GetMapping("/status")
    public SyncStatus getStatus() {
        return syncService.getStatus();
    }
}

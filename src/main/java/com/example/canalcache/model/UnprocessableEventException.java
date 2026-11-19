package com.example.canalcache.model;

/**
 * 事件内容本身无法处理：同样的消息、同样的代码，重试多少次结果都一样。
 *
 * <p>缓存消费者遇到它时把事件写入死信并继续处理后续消息；Redis、Kafka 等外部故障
 * 不使用这个异常，仍然整批回滚重试。继承 IllegalArgumentException 以保持原有调用方语义。</p>
 */
public class UnprocessableEventException extends IllegalArgumentException {

    public enum Reason {
        MALFORMED_ENTRY,
        UNKNOWN_EVENT_TYPE,
        MISSING_PRIMARY_KEY
    }

    private final Reason reason;

    public UnprocessableEventException(Reason reason, String message) {
        this(reason, message, null);
    }

    public UnprocessableEventException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}

package com.payflow.saga.command;

import java.time.LocalDateTime;
import java.util.UUID;

public class OrderCommand {

    private String commandType; // "CONFIRM" or "CANCEL"
    private String commandId;
    private String sagaId;
    private Long orderId;
    private String reason;
    private LocalDateTime timestamp;

    public OrderCommand() {
    }

    public static OrderCommand confirm(String sagaId, Long orderId) {
        OrderCommand cmd = new OrderCommand();
        cmd.setCommandType("CONFIRM");
        cmd.setCommandId(UUID.randomUUID().toString());
        cmd.setSagaId(sagaId);
        cmd.setOrderId(orderId);
        cmd.setTimestamp(LocalDateTime.now());
        return cmd;
    }

    public static OrderCommand cancel(String sagaId, Long orderId, String reason) {
        OrderCommand cmd = new OrderCommand();
        cmd.setCommandType("CANCEL");
        cmd.setCommandId(UUID.randomUUID().toString());
        cmd.setSagaId(sagaId);
        cmd.setOrderId(orderId);
        cmd.setReason(reason);
        cmd.setTimestamp(LocalDateTime.now());
        return cmd;
    }

    public String getCommandType() {
        return commandType;
    }

    public void setCommandType(String commandType) {
        this.commandType = commandType;
    }

    public String getCommandId() {
        return commandId;
    }

    public void setCommandId(String commandId) {
        this.commandId = commandId;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}

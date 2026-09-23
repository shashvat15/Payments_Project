package com.payflow.saga.command;

import java.time.LocalDateTime;
import java.util.UUID;

public class InventoryCommand {

    private String commandType; // "RESERVE" or "RELEASE"
    private String commandId;
    private String sagaId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private Boolean simulateInventoryFailure;
    private LocalDateTime timestamp;

    public InventoryCommand() {
    }

    public static InventoryCommand reserve(String sagaId, Long orderId, Long productId, Integer quantity, Boolean simulateInventoryFailure) {
        InventoryCommand cmd = new InventoryCommand();
        cmd.setCommandType("RESERVE");
        cmd.setCommandId(UUID.randomUUID().toString());
        cmd.setSagaId(sagaId);
        cmd.setOrderId(orderId);
        cmd.setProductId(productId);
        cmd.setQuantity(quantity);
        cmd.setSimulateInventoryFailure(simulateInventoryFailure != null && simulateInventoryFailure);
        cmd.setTimestamp(LocalDateTime.now());
        return cmd;
    }

    public static InventoryCommand release(String sagaId, Long orderId, Long productId, Integer quantity) {
        InventoryCommand cmd = new InventoryCommand();
        cmd.setCommandType("RELEASE");
        cmd.setCommandId(UUID.randomUUID().toString());
        cmd.setSagaId(sagaId);
        cmd.setOrderId(orderId);
        cmd.setProductId(productId);
        cmd.setQuantity(quantity);
        cmd.setSimulateInventoryFailure(false);
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

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Boolean getSimulateInventoryFailure() {
        return simulateInventoryFailure;
    }

    public void setSimulateInventoryFailure(Boolean simulateInventoryFailure) {
        this.simulateInventoryFailure = simulateInventoryFailure;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}

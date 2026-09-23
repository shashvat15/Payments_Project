package com.payflow.inventory.command;

import java.time.LocalDateTime;
import java.util.UUID;

public class ReserveInventoryCommand {

    private String commandId;
    private String sagaId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private Boolean simulateInventoryFailure;
    private LocalDateTime timestamp;

    public ReserveInventoryCommand() {
    }

    public ReserveInventoryCommand(String sagaId, Long orderId, Long productId, Integer quantity, Boolean simulateInventoryFailure) {
        this.commandId = UUID.randomUUID().toString();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.simulateInventoryFailure = simulateInventoryFailure != null && simulateInventoryFailure;
        this.timestamp = LocalDateTime.now();
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

package com.payflow.inventory.command;

import java.time.LocalDateTime;
import java.util.UUID;

public class ReleaseInventoryCommand {

    private String commandId;
    private String sagaId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private LocalDateTime timestamp;

    public ReleaseInventoryCommand() {
    }

    public ReleaseInventoryCommand(String sagaId, Long orderId, Long productId, Integer quantity) {
        this.commandId = UUID.randomUUID().toString();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}

package com.payflow.payment.command;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class ProcessPaymentCommand {

    private String commandId;
    private String sagaId;
    private Long orderId;
    private Long customerId;
    private BigDecimal amount;
    private Boolean simulatePaymentFailure;
    private LocalDateTime timestamp;

    public ProcessPaymentCommand() {
    }

    public ProcessPaymentCommand(String commandId, String sagaId, Long orderId, Long customerId, BigDecimal amount, Boolean simulatePaymentFailure, LocalDateTime timestamp) {
        this.commandId = commandId;
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure;
        this.timestamp = timestamp;
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

    public Long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Boolean getSimulatePaymentFailure() {
        return simulatePaymentFailure;
    }

    public void setSimulatePaymentFailure(Boolean simulatePaymentFailure) {
        this.simulatePaymentFailure = simulatePaymentFailure;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}

package com.payflow.payment.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public class ProcessPaymentRequest {

    @NotNull(message = "Order ID must not be null")
    private Long orderId;

    @NotNull(message = "Amount must not be null")
    @DecimalMin(value = "0.01", message = "Amount must be greater than zero")
    private BigDecimal amount;

    private Boolean simulateFailure = false;

    public ProcessPaymentRequest() {
    }

    public ProcessPaymentRequest(Long orderId, BigDecimal amount) {
        this.orderId = orderId;
        this.amount = amount;
        this.simulateFailure = false;
    }

    public ProcessPaymentRequest(Long orderId, BigDecimal amount, Boolean simulateFailure) {
        this.orderId = orderId;
        this.amount = amount;
        this.simulateFailure = simulateFailure != null && simulateFailure;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Boolean getSimulateFailure() {
        return simulateFailure;
    }

    public void setSimulateFailure(Boolean simulateFailure) {
        this.simulateFailure = simulateFailure;
    }
}

package com.payflow.order.dto;

import java.math.BigDecimal;

public class PaymentRequestDto {

    private Long orderId;
    private BigDecimal amount;
    private Boolean simulateFailure;

    public PaymentRequestDto() {
    }

    public PaymentRequestDto(Long orderId, BigDecimal amount, Boolean simulateFailure) {
        this.orderId = orderId;
        this.amount = amount;
        this.simulateFailure = simulateFailure;
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

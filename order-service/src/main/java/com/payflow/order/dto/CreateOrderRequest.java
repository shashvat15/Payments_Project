package com.payflow.order.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public class CreateOrderRequest {

    @NotNull(message = "Customer ID must not be null")
    private Long customerId;

    @NotNull(message = "Amount must not be null")
    @DecimalMin(value = "0.01", message = "Amount must be greater than zero")
    private BigDecimal amount;

    // Simulation flags for demonstrating distributed failure modes
    private Boolean simulatePaymentFailure = false;
    private Boolean simulateOrderUpdateFailure = false;
    private Boolean simulateKafkaPublishFailure = false;

    public CreateOrderRequest() {
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount) {
        this.customerId = customerId;
        this.amount = amount;
        this.simulatePaymentFailure = false;
        this.simulateOrderUpdateFailure = false;
        this.simulateKafkaPublishFailure = false;
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount, Boolean simulatePaymentFailure) {
        this.customerId = customerId;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateOrderUpdateFailure = false;
        this.simulateKafkaPublishFailure = false;
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount, Boolean simulatePaymentFailure, Boolean simulateOrderUpdateFailure) {
        this.customerId = customerId;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateOrderUpdateFailure = simulateOrderUpdateFailure != null && simulateOrderUpdateFailure;
        this.simulateKafkaPublishFailure = false;
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount, Boolean simulatePaymentFailure, Boolean simulateOrderUpdateFailure, Boolean simulateKafkaPublishFailure) {
        this.customerId = customerId;
        this.amount = amount;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateOrderUpdateFailure = simulateOrderUpdateFailure != null && simulateOrderUpdateFailure;
        this.simulateKafkaPublishFailure = simulateKafkaPublishFailure != null && simulateKafkaPublishFailure;
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

    public Boolean getSimulateOrderUpdateFailure() {
        return simulateOrderUpdateFailure;
    }

    public void setSimulateOrderUpdateFailure(Boolean simulateOrderUpdateFailure) {
        this.simulateOrderUpdateFailure = simulateOrderUpdateFailure;
    }

    public Boolean getSimulateKafkaPublishFailure() {
        return simulateKafkaPublishFailure;
    }

    public void setSimulateKafkaPublishFailure(Boolean simulateKafkaPublishFailure) {
        this.simulateKafkaPublishFailure = simulateKafkaPublishFailure;
    }
}

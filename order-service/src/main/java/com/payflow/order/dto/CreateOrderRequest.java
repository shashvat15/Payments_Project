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

    // Phase 2B Inventory Fields
    private Long productId = 1001L;
    private Integer quantity = 1;

    // Simulation flags for demonstrating distributed failure modes
    private Boolean simulatePaymentFailure = false;
    private Boolean simulateInventoryFailure = false;
    private Boolean simulateOrderUpdateFailure = false;
    private Boolean simulateKafkaPublishFailure = false;

    public CreateOrderRequest() {
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount) {
        this.customerId = customerId;
        this.amount = amount;
        this.productId = 1001L;
        this.quantity = 1;
        this.simulatePaymentFailure = false;
        this.simulateInventoryFailure = false;
        this.simulateOrderUpdateFailure = false;
        this.simulateKafkaPublishFailure = false;
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount, Boolean simulatePaymentFailure) {
        this.customerId = customerId;
        this.amount = amount;
        this.productId = 1001L;
        this.quantity = 1;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateInventoryFailure = false;
        this.simulateOrderUpdateFailure = false;
        this.simulateKafkaPublishFailure = false;
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount, Boolean simulatePaymentFailure, Boolean simulateOrderUpdateFailure) {
        this.customerId = customerId;
        this.amount = amount;
        this.productId = 1001L;
        this.quantity = 1;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateInventoryFailure = false;
        this.simulateOrderUpdateFailure = simulateOrderUpdateFailure != null && simulateOrderUpdateFailure;
        this.simulateKafkaPublishFailure = false;
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount, Boolean simulatePaymentFailure, Boolean simulateOrderUpdateFailure, Boolean simulateKafkaPublishFailure) {
        this.customerId = customerId;
        this.amount = amount;
        this.productId = 1001L;
        this.quantity = 1;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateInventoryFailure = false;
        this.simulateOrderUpdateFailure = simulateOrderUpdateFailure != null && simulateOrderUpdateFailure;
        this.simulateKafkaPublishFailure = simulateKafkaPublishFailure != null && simulateKafkaPublishFailure;
    }

    public CreateOrderRequest(Long customerId, BigDecimal amount, Long productId, Integer quantity, Boolean simulatePaymentFailure, Boolean simulateInventoryFailure) {
        this.customerId = customerId;
        this.amount = amount;
        this.productId = productId != null ? productId : 1001L;
        this.quantity = quantity != null ? quantity : 1;
        this.simulatePaymentFailure = simulatePaymentFailure != null && simulatePaymentFailure;
        this.simulateInventoryFailure = simulateInventoryFailure != null && simulateInventoryFailure;
        this.simulateOrderUpdateFailure = false;
        this.simulateKafkaPublishFailure = false;
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

    public Boolean getSimulatePaymentFailure() {
        return simulatePaymentFailure;
    }

    public void setSimulatePaymentFailure(Boolean simulatePaymentFailure) {
        this.simulatePaymentFailure = simulatePaymentFailure;
    }

    public Boolean getSimulateInventoryFailure() {
        return simulateInventoryFailure;
    }

    public void setSimulateInventoryFailure(Boolean simulateInventoryFailure) {
        this.simulateInventoryFailure = simulateInventoryFailure;
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

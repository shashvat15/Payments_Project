package com.payflow.saga.dto;

import com.payflow.saga.entity.SagaState;
import com.payflow.saga.entity.SagaStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class SagaResponse {

    private Long id;
    private String sagaId;
    private Long orderId;
    private Long customerId;
    private Long productId;
    private Integer quantity;
    private BigDecimal amount;
    private SagaState currentState;
    private SagaStatus status;
    private String failureReason;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public SagaResponse() {
    }

    public SagaResponse(Long id, String sagaId, Long orderId, Long customerId, Long productId, Integer quantity,
                        BigDecimal amount, SagaState currentState, SagaStatus status, String failureReason,
                        LocalDateTime createdAt, LocalDateTime updatedAt) {
        this.id = id;
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.currentState = currentState;
        this.status = status;
        this.failureReason = failureReason;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public SagaState getCurrentState() {
        return currentState;
    }

    public void setCurrentState(SagaState currentState) {
        this.currentState = currentState;
    }

    public SagaStatus getStatus() {
        return status;
    }

    public void setStatus(SagaStatus status) {
        this.status = status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}

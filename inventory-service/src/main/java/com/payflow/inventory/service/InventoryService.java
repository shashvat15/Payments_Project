package com.payflow.inventory.service;

import com.payflow.inventory.dto.InventoryResponse;
import com.payflow.inventory.entity.InventoryItem;
import com.payflow.inventory.event.InventoryResultEvent;
import com.payflow.inventory.repository.InventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryRepository inventoryRepository;

    public InventoryService(InventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
    }

    /**
     * Local Transaction: Reserve Inventory for an order in the Saga workflow.
     */
    @Transactional
    public InventoryResultEvent reserveInventory(String sagaId, Long orderId, Long productId, Integer quantity, Boolean simulateFailure) {
        log.info("[Saga: {}] Attempting to reserve inventory: orderId={}, productId={}, quantity={}, simulateFailure={}",
                sagaId, orderId, productId, quantity, simulateFailure);

        if (Boolean.TRUE.equals(simulateFailure)) {
            log.warn("[Saga: {}] Simulated inventory failure triggered for orderId={}, productId={}",
                    sagaId, orderId, productId);
            return InventoryResultEvent.reservationFailed(sagaId, orderId, productId, quantity, "SIMULATED_INVENTORY_FAILURE");
        }

        Optional<InventoryItem> itemOpt = inventoryRepository.findByProductId(productId);
        if (itemOpt.isEmpty()) {
            log.error("[Saga: {}] Product not found in inventory: productId={}", sagaId, productId);
            return InventoryResultEvent.reservationFailed(sagaId, orderId, productId, quantity, "PRODUCT_NOT_FOUND");
        }

        InventoryItem item = itemOpt.get();
        if (item.getAvailableQuantity() < quantity) {
            log.warn("[Saga: {}] Insufficient inventory: available={}, requested={}",
                    sagaId, item.getAvailableQuantity(), quantity);
            return InventoryResultEvent.reservationFailed(sagaId, orderId, productId, quantity, "INSUFFICIENT_STOCK");
        }

        // Deduct available, increment reserved
        item.setAvailableQuantity(item.getAvailableQuantity() - quantity);
        item.setReservedQuantity(item.getReservedQuantity() + quantity);
        inventoryRepository.save(item);

        log.info("[Saga: {}] Successfully reserved inventory: productId={}, remainingAvailable={}, reservedTotal={}",
                sagaId, productId, item.getAvailableQuantity(), item.getReservedQuantity());

        return InventoryResultEvent.reserved(sagaId, orderId, productId, quantity);
    }

    /**
     * Compensating Action: Release previously reserved inventory when a subsequent Saga step fails.
     * Note: This is an independent local transaction, NOT a database rollback.
     */
    @Transactional
    public InventoryResultEvent releaseInventory(String sagaId, Long orderId, Long productId, Integer quantity) {
        log.info("[Saga: {}] Executing COMPENSATION: Releasing inventory for orderId={}, productId={}, quantity={}",
                sagaId, orderId, productId, quantity);

        Optional<InventoryItem> itemOpt = inventoryRepository.findByProductId(productId);
        if (itemOpt.isPresent()) {
            InventoryItem item = itemOpt.get();
            int releaseQty = (quantity != null && quantity > 0) ? quantity : 1;
            int actualRelease = Math.min(item.getReservedQuantity(), releaseQty);

            item.setReservedQuantity(item.getReservedQuantity() - actualRelease);
            item.setAvailableQuantity(item.getAvailableQuantity() + actualRelease);
            inventoryRepository.save(item);

            log.info("[Saga: {}] Compensation complete: Released {} units for productId={}. Available now: {}, Reserved now: {}",
                    sagaId, actualRelease, productId, item.getAvailableQuantity(), item.getReservedQuantity());
        } else {
            log.warn("[Saga: {}] Compensation: Product {} not found in inventory during release attempt", sagaId, productId);
        }

        return InventoryResultEvent.released(sagaId, orderId, productId, quantity);
    }

    @Transactional(readOnly = true)
    public Optional<InventoryResponse> getInventoryByProductId(Long productId) {
        return inventoryRepository.findByProductId(productId)
                .map(this::mapToResponse);
    }

    @Transactional(readOnly = true)
    public List<InventoryResponse> getAllInventory() {
        return inventoryRepository.findAll().stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private InventoryResponse mapToResponse(InventoryItem item) {
        return new InventoryResponse(
                item.getId(),
                item.getProductId(),
                item.getAvailableQuantity(),
                item.getReservedQuantity(),
                item.getCreatedAt(),
                item.getUpdatedAt()
        );
    }
}

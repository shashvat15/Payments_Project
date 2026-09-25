package com.payflow.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.payflow.inventory.dto.InventoryResponse;
import com.payflow.inventory.entity.InventoryItem;
import com.payflow.inventory.entity.OutboxEvent;
import com.payflow.inventory.entity.OutboxStatus;
import com.payflow.inventory.event.InventoryResultEvent;
import com.payflow.inventory.repository.InventoryRepository;
import com.payflow.inventory.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private InventoryRepository inventoryRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private ObjectMapper objectMapper;
    private InventoryService inventoryService;

    private InventoryItem sampleItem;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        inventoryService = new InventoryService(inventoryRepository, outboxEventRepository, objectMapper);

        sampleItem = new InventoryItem(1001L, 50, 0);
        sampleItem.setId(1L);
    }

    @Test
    @DisplayName("Should successfully reserve inventory and save OutboxEvent atomically (Saga Phase 2C)")
    void testReserveInventory_Success() {
        when(inventoryRepository.findByProductId(1001L)).thenReturn(Optional.of(sampleItem));
        when(inventoryRepository.save(any(InventoryItem.class))).thenReturn(sampleItem);

        InventoryResultEvent result = inventoryService.reserveInventory("saga-1", 101L, 1001L, 5, false);

        assertNotNull(result);
        assertEquals("RESERVED", result.getStatus());
        assertEquals(45, sampleItem.getAvailableQuantity());
        assertEquals(5, sampleItem.getReservedQuantity());
        verify(inventoryRepository).save(sampleItem);

        // Verify OutboxEvent persistence in the same local transaction
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        OutboxEvent outboxEvent = outboxCaptor.getValue();
        assertEquals("InventoryResult", outboxEvent.getEventType());
        assertEquals("Inventory", outboxEvent.getAggregateType());
        assertEquals("1001", outboxEvent.getAggregateId());
        assertEquals("inventory-result", outboxEvent.getTopic());
        assertEquals("101", outboxEvent.getMessageKey());
        assertEquals(OutboxStatus.NEW, outboxEvent.getStatus());
        assertTrue(outboxEvent.getPayload().contains("RESERVED"));
    }

    @Test
    @DisplayName("Should fail reservation when requested quantity exceeds available stock and save OutboxEvent")
    void testReserveInventory_InsufficientStock() {
        when(inventoryRepository.findByProductId(1001L)).thenReturn(Optional.of(sampleItem));

        InventoryResultEvent result = inventoryService.reserveInventory("saga-2", 102L, 1001L, 100, false);

        assertNotNull(result);
        assertEquals("RESERVATION_FAILED", result.getStatus());
        assertEquals("INSUFFICIENT_STOCK", result.getReason());
        verify(inventoryRepository, never()).save(any());

        // Even on failure, OutboxEvent must be saved to notify Saga Orchestrator
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertEquals(OutboxStatus.NEW, outboxCaptor.getValue().getStatus());
        assertTrue(outboxCaptor.getValue().getPayload().contains("INSUFFICIENT_STOCK"));
    }

    @Test
    @DisplayName("Should fail reservation when product is not found and save OutboxEvent")
    void testReserveInventory_ProductNotFound() {
        when(inventoryRepository.findByProductId(9999L)).thenReturn(Optional.empty());

        InventoryResultEvent result = inventoryService.reserveInventory("saga-3", 103L, 9999L, 1, false);

        assertNotNull(result);
        assertEquals("RESERVATION_FAILED", result.getStatus());
        assertEquals("PRODUCT_NOT_FOUND", result.getReason());
        verify(outboxEventRepository).save(any(OutboxEvent.class));
    }

    @Test
    @DisplayName("Should fail reservation when simulated inventory failure is set to true and save OutboxEvent")
    void testReserveInventory_SimulatedFailure() {
        InventoryResultEvent result = inventoryService.reserveInventory("saga-4", 104L, 1001L, 1, true);

        assertNotNull(result);
        assertEquals("RESERVATION_FAILED", result.getStatus());
        assertEquals("SIMULATED_INVENTORY_FAILURE", result.getReason());
        verify(inventoryRepository, never()).findByProductId(any());
        verify(outboxEventRepository).save(any(OutboxEvent.class));
    }

    @Test
    @DisplayName("Should successfully release inventory as a compensating action and save OutboxEvent")
    void testReleaseInventory_Compensation() {
        sampleItem.setAvailableQuantity(45);
        sampleItem.setReservedQuantity(5);
        when(inventoryRepository.findByProductId(1001L)).thenReturn(Optional.of(sampleItem));
        when(inventoryRepository.save(any(InventoryItem.class))).thenReturn(sampleItem);

        InventoryResultEvent result = inventoryService.releaseInventory("saga-5", 105L, 1001L, 5);

        assertNotNull(result);
        assertEquals("RELEASED", result.getStatus());
        assertEquals(50, sampleItem.getAvailableQuantity());
        assertEquals(0, sampleItem.getReservedQuantity());
        verify(inventoryRepository).save(sampleItem);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertEquals(OutboxStatus.NEW, outboxCaptor.getValue().getStatus());
        assertTrue(outboxCaptor.getValue().getPayload().contains("RELEASED"));
    }

    @Test
    @DisplayName("Should fetch all inventory items")
    void testGetAllInventory() {
        when(inventoryRepository.findAll()).thenReturn(List.of(sampleItem));

        List<InventoryResponse> responses = inventoryService.getAllInventory();

        assertEquals(1, responses.size());
        assertEquals(1001L, responses.get(0).getProductId());
    }

    @Test
    @DisplayName("Should fetch outbox events")
    void testGetOutboxEvents() {
        OutboxEvent event = new OutboxEvent("InventoryResult", "Inventory", "1001", "inventory-result", "101", "{}");
        when(outboxEventRepository.findAll()).thenReturn(List.of(event));

        List<OutboxEvent> events = inventoryService.getOutboxEvents();

        assertEquals(1, events.size());
        assertEquals("InventoryResult", events.get(0).getEventType());
    }
}

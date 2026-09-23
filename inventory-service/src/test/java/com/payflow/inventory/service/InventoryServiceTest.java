package com.payflow.inventory.service;

import com.payflow.inventory.entity.InventoryItem;
import com.payflow.inventory.event.InventoryResultEvent;
import com.payflow.inventory.repository.InventoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private InventoryRepository inventoryRepository;

    @InjectMocks
    private InventoryService inventoryService;

    private InventoryItem sampleItem;

    @BeforeEach
    void setUp() {
        sampleItem = new InventoryItem(1001L, 50, 0);
        sampleItem.setId(1L);
    }

    @Test
    @DisplayName("Should successfully reserve inventory when sufficient stock is available")
    void testReserveInventory_Success() {
        when(inventoryRepository.findByProductId(1001L)).thenReturn(Optional.of(sampleItem));
        when(inventoryRepository.save(any(InventoryItem.class))).thenReturn(sampleItem);

        InventoryResultEvent result = inventoryService.reserveInventory("saga-1", 101L, 1001L, 5, false);

        assertNotNull(result);
        assertEquals("RESERVED", result.getStatus());
        assertEquals(45, sampleItem.getAvailableQuantity());
        assertEquals(5, sampleItem.getReservedQuantity());
        verify(inventoryRepository).save(sampleItem);
    }

    @Test
    @DisplayName("Should fail reservation when requested quantity exceeds available stock")
    void testReserveInventory_InsufficientStock() {
        when(inventoryRepository.findByProductId(1001L)).thenReturn(Optional.of(sampleItem));

        InventoryResultEvent result = inventoryService.reserveInventory("saga-2", 102L, 1001L, 100, false);

        assertNotNull(result);
        assertEquals("RESERVATION_FAILED", result.getStatus());
        assertEquals("INSUFFICIENT_STOCK", result.getReason());
        verify(inventoryRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should fail reservation when product is not found")
    void testReserveInventory_ProductNotFound() {
        when(inventoryRepository.findByProductId(9999L)).thenReturn(Optional.empty());

        InventoryResultEvent result = inventoryService.reserveInventory("saga-3", 103L, 9999L, 1, false);

        assertNotNull(result);
        assertEquals("RESERVATION_FAILED", result.getStatus());
        assertEquals("PRODUCT_NOT_FOUND", result.getReason());
    }

    @Test
    @DisplayName("Should fail reservation when simulated inventory failure is set to true")
    void testReserveInventory_SimulatedFailure() {
        InventoryResultEvent result = inventoryService.reserveInventory("saga-4", 104L, 1001L, 1, true);

        assertNotNull(result);
        assertEquals("RESERVATION_FAILED", result.getStatus());
        assertEquals("SIMULATED_INVENTORY_FAILURE", result.getReason());
        verify(inventoryRepository, never()).findByProductId(any());
    }

    @Test
    @DisplayName("Should successfully release inventory as a compensating action")
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
    }
}

package com.payflow.inventory.controller;

import com.payflow.inventory.dto.InventoryResponse;
import com.payflow.inventory.entity.OutboxEvent;
import com.payflow.inventory.service.InventoryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(InventoryController.class)
class InventoryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private InventoryService inventoryService;

    @Test
    @DisplayName("GET /api/inventory/{productId} should return 200 when item exists")
    void testGetInventoryByProductId_Found() throws Exception {
        InventoryResponse response = new InventoryResponse(
                1L, 1001L, 50, 0, LocalDateTime.now(), LocalDateTime.now()
        );
        when(inventoryService.getInventoryByProductId(1001L)).thenReturn(Optional.of(response));

        mockMvc.perform(get("/api/inventory/1001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.productId").value(1001))
                .andExpect(jsonPath("$.availableQuantity").value(50));
    }

    @Test
    @DisplayName("GET /api/inventory/{productId} should return 404 when item does not exist")
    void testGetInventoryByProductId_NotFound() throws Exception {
        when(inventoryService.getInventoryByProductId(9999L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/inventory/9999"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/inventory should return 200 with list of inventory items")
    void testGetAllInventory() throws Exception {
        InventoryResponse response = new InventoryResponse(
                1L, 1001L, 50, 0, LocalDateTime.now(), LocalDateTime.now()
        );
        when(inventoryService.getAllInventory()).thenReturn(List.of(response));

        mockMvc.perform(get("/api/inventory"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].productId").value(1001));
    }

    @Test
    @DisplayName("GET /api/inventory/outbox should return 200 with list of outbox events")
    void testGetOutboxEvents() throws Exception {
        OutboxEvent event = new OutboxEvent("InventoryResult", "Inventory", "1001", "inventory-result", "101", "{}");
        when(inventoryService.getOutboxEvents()).thenReturn(List.of(event));

        mockMvc.perform(get("/api/inventory/outbox"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].eventType").value("InventoryResult"))
                .andExpect(jsonPath("$[0].topic").value("inventory-result"));
    }
}

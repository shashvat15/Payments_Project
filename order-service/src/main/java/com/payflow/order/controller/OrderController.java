package com.payflow.order.controller;

import com.payflow.order.dto.CreateOrderRequest;
import com.payflow.order.dto.OrderResponse;
import com.payflow.order.service.OrderService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        log.info("Received request to create order for customerId: {}, amount: {}",
                request.getCustomerId(), request.getAmount());
        OrderResponse response = orderService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> getOrderById(@PathVariable Long id) {
        log.info("Fetching order with id: {}", id);
        OrderResponse response = orderService.getOrderById(id);
        return ResponseEntity.ok(response);
    }

    @GetMapping
    public ResponseEntity<List<OrderResponse>> getAllOrders(
            @RequestParam(value = "customerId", required = false) Long customerId) {
        if (customerId != null) {
            log.info("Fetching orders for customerId: {}", customerId);
            return ResponseEntity.ok(orderService.getOrdersByCustomerId(customerId));
        }
        log.info("Fetching all orders");
        return ResponseEntity.ok(orderService.getAllOrders());
    }

    @GetMapping("/outbox")
    public ResponseEntity<List<com.payflow.order.entity.OutboxEvent>> getOutboxEvents(
            @RequestParam(value = "status", required = false) com.payflow.order.entity.OutboxStatus status) {
        log.info("Fetching outbox events, status filter: {}", status);
        return ResponseEntity.ok(orderService.getOutboxEvents(status));
    }
}

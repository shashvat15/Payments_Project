package com.payflow.saga.controller;

import com.payflow.saga.dto.SagaResponse;
import com.payflow.saga.service.SagaOrchestratorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaOrchestratorService sagaOrchestratorService;

    public SagaController(SagaOrchestratorService sagaOrchestratorService) {
        this.sagaOrchestratorService = sagaOrchestratorService;
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaResponse> getSagaById(@PathVariable String sagaId) {
        return sagaOrchestratorService.getSagaById(sagaId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<List<SagaResponse>> getSagasByOrderId(@PathVariable Long orderId) {
        return ResponseEntity.ok(sagaOrchestratorService.getSagasByOrderId(orderId));
    }

    @GetMapping
    public ResponseEntity<List<SagaResponse>> getAllSagas() {
        return ResponseEntity.ok(sagaOrchestratorService.getAllSagas());
    }
}

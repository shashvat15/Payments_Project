package com.payflow.order.client;

import com.payflow.order.dto.PaymentRequestDto;
import com.payflow.order.dto.PaymentResponseDto;
import com.payflow.order.exception.ServiceCommunicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
public class PaymentClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentClient.class);

    private final RestClient restClient;

    public PaymentClient(
            RestClient.Builder restClientBuilder,
            @Value("${payment-service.url:http://localhost:8082}") String paymentServiceUrl) {
        log.info("Initializing PaymentClient with base URL: {}", paymentServiceUrl);
        this.restClient = restClientBuilder
                .baseUrl(paymentServiceUrl)
                .build();
    }

    public PaymentResponseDto processPayment(PaymentRequestDto paymentRequest) {
        log.info("Calling Payment Service synchronously for orderId: {}, amount: {}",
                paymentRequest.getOrderId(), paymentRequest.getAmount());

        try {
            return restClient.post()
                    .uri("/api/payments")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(paymentRequest)
                    .retrieve()
                    .body(PaymentResponseDto.class);
        } catch (RestClientException ex) {
            log.error("Failed to communicate with Payment Service for orderId {}: {}",
                    paymentRequest.getOrderId(), ex.getMessage());
            throw new ServiceCommunicationException(
                    "Unable to communicate with Payment Service at this time: " + ex.getMessage(), ex);
        }
    }
}

package com.shopmart.order.service.impl;

import com.shopmart.order.client.InventoryClient;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.OrderService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    @Transactional
    @CircuitBreaker(name = "inventoryService", fallbackMethod = "createOrderFallback")
    public OrderResponse createOrder(OrderRequest request) {
        ProductDto product = inventoryClient.getProductById(request.getProductId());
        if (product == null) {
            throw new ResourceNotFoundException("Product not found");
        }
        
        BigDecimal totalAmount = product.getPrice().multiply(BigDecimal.valueOf(request.getQuantity()));

        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Created order id={} with status PENDING", saved.getId());

        OrderEvent event = new OrderEvent(
                saved.getId(),
                saved.getProductId(),
                saved.getQuantity(),
                saved.getTotalAmount(),
                SagaEventType.ORDER_CREATED,
                null
        );
        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(saved.getId()), event);
        log.info("Published ORDER_CREATED event for order id={}", saved.getId());

        return OrderResponse.from(saved);
    }
    
    public OrderResponse createOrderFallback(OrderRequest request, Throwable t) {
        log.error("Fallback activated for createOrder due to: {}", t.getMessage());
        throw new RuntimeException("Inventory service is currently unavailable. Please try again later.");
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("Order id={} COMPLETED", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("Order id={} CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("KhA'ng tAm thy `n hAng id=" + id));
    }
}

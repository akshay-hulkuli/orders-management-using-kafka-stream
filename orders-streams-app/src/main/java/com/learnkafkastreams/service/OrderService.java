package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrderService {

    private OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStoreDTO> getOrderCount(String orderType) {
        ReadOnlyKeyValueStore<String, Long> materializedStore = getOrderStore(orderType);
    }

    private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {
        switch (orderType) {
            case GENERAL_ORDERS -> {
                return orderStoreService.orderCountStore(GENERAL_ORDERS_COUNT);
            }
            case RESTAURANT_ORDERS -> {
                return orderStoreService.orderCountStore(RESTAURANT_ORDERS_COUNT);
            }
            default -> throw new IllegalStateException("Not a valid order type");
        }
    }
}

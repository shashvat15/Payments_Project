package com.payflow.inventory.config;

import com.payflow.inventory.entity.InventoryItem;
import com.payflow.inventory.repository.InventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataInitializer {

    private static final Logger log = LoggerFactory.getLogger(DataInitializer.class);

    @Bean
    public CommandLineRunner initInventory(InventoryRepository inventoryRepository) {
        return args -> {
            if (inventoryRepository.findByProductId(1001L).isEmpty()) {
                InventoryItem item1 = new InventoryItem(1001L, 50, 0);
                inventoryRepository.save(item1);
                log.info("Initialized inventory for Product 1001 with 50 available units");
            }

            if (inventoryRepository.findByProductId(1002L).isEmpty()) {
                InventoryItem item2 = new InventoryItem(1002L, 0, 0);
                inventoryRepository.save(item2);
                log.info("Initialized inventory for Product 1002 with 0 units (Out-of-Stock for failure testing)");
            }
        };
    }
}

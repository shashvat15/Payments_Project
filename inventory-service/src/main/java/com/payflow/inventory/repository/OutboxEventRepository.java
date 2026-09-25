package com.payflow.inventory.repository;

import com.payflow.inventory.entity.OutboxEvent;
import com.payflow.inventory.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxStatus status);

    List<OutboxEvent> findByAggregateId(String aggregateId);
}

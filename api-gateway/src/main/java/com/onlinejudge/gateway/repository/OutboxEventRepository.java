package com.onlinejudge.gateway.repository;

import com.onlinejudge.gateway.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query("SELECT e FROM OutboxEvent e WHERE e.published = false ORDER BY e.createdAt ASC LIMIT :limit")
    List<OutboxEvent> findUnpublished(@Param("limit") int limit);
}

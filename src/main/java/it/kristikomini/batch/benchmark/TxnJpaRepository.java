package it.kristikomini.batch.benchmark;

import org.springframework.data.jpa.repository.JpaRepository;

/** Benchmark-only repository for the naive {@code saveAll} approach. */
public interface TxnJpaRepository extends JpaRepository<TxnEntity, String> {
}

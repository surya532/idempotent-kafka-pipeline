package com.suryadevathi.pipeline;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcEventStore implements EventStore {

    private final JdbcTemplate jdbc;

    public JdbcEventStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean markProcessed(PaymentEvent e) {
        // ON CONFLICT DO NOTHING: no exception on duplicates, so the surrounding transaction stays healthy.
        int inserted = jdbc.update(
                "INSERT INTO processed_events (event_id, account_id, amount_cents) VALUES (?, ?, ?) "
                        + "ON CONFLICT (event_id) DO NOTHING",
                e.eventId(), e.accountId(), e.amountCents());
        return inserted == 1;
    }

    @Override
    public void applyBalance(String accountId, long deltaCents) {
        jdbc.update(
                "INSERT INTO account_balances (account_id, balance_cents) VALUES (?, ?) "
                        + "ON CONFLICT (account_id) DO UPDATE "
                        + "SET balance_cents = account_balances.balance_cents + EXCLUDED.balance_cents",
                accountId, deltaCents);
    }

    @Override
    public long processedCount() {
        Long n = jdbc.queryForObject("SELECT count(*) FROM processed_events", Long.class);
        return n == null ? 0 : n;
    }

    @Override
    public long balanceOf(String accountId) {
        try {
            Long b = jdbc.queryForObject(
                    "SELECT balance_cents FROM account_balances WHERE account_id = ?", Long.class, accountId);
            return b == null ? 0 : b;
        } catch (EmptyResultDataAccessException e) {
            return 0;
        }
    }
}

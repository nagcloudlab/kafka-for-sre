package com.example;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Transaction {

    @Builder.Default
    private String transactionId = UUID.randomUUID().toString();

    private String fromAccount;
    private String toAccount;
    private BigDecimal amount;

    @Builder.Default
    private String currency = "INR";

    private String type;

    @Builder.Default
    private String status = "INITIATED";

    @Builder.Default
    private Instant timestamp = Instant.now();
}
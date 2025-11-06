package com.bitorgroup.via.smbs;

import org.springframework.data.annotation.Id;

import jakarta.persistence.Entity;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@Entity
public class SmbsSequence {
    @Id
    private final String name;
    @Builder.Default
    private final Integer sequence = 1;
}

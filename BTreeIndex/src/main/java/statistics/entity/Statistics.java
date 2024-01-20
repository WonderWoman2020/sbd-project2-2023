package statistics.entity;

import lombok.*;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class Statistics {

    /**
     * Operation number. It is counted for each operation type separately.
     */
    private long operation;

    /**
     * Database operation types. Possible types: CREATE, READ, UPDATE, DELETE.
     */
    private String type;

    // Counters of things that happened during operation

    private int merges;

    private int splits;

    private int compensations;

    private int tapeWrites;

    private int tapeReads;


}

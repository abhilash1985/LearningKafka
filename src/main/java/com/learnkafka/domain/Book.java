package com.learnkafka.domain;

import lombok.*;
import org.jetbrains.annotations.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {
    @NotNull
    private Integer bookId;
    @NotNull
    private String bookName;
    @NotNull
    private String bookAuthor;
}

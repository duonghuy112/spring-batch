package com.huynguyen.springbatch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StudentJson {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
}

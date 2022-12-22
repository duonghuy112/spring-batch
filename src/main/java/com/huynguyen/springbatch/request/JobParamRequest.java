package com.huynguyen.springbatch.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobParamRequest {
    private String paramKey;
    private String paramValue;
}

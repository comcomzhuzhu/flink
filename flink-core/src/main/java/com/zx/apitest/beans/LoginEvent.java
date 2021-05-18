package com.zx.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private String id;
    private String ip;
    private String state;
    private Long ts;
}

package com.atguigu.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName MarketingUserBehavior
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/13 15:09
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}

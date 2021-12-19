package com.zds.kafka;

import lombok.*;

/**
 * KafkaEntity
 *
 * @author zhongdongsheng
 * @since 2021/12/19
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class KafkaEntity {
    private Long id;

    private String name;
}

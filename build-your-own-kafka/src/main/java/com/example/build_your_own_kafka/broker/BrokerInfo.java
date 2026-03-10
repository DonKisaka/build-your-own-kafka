package com.example.build_your_own_kafka.broker;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;



@Getter
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class BrokerInfo {

    @EqualsAndHashCode.Include
    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }
}

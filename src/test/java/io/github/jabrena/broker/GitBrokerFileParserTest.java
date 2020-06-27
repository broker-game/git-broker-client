package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

class GitBrokerFileParserTest {

    @Test
    public void given_file_when_parse_then_Ok() {

        String sample = "1589496032977_PING_PING.json";
        GitBrokerFileParser brokerFileParser =  new GitBrokerFileParser(sample);

        then(brokerFileParser.getEpoch()).isInstanceOf(Long.class);
    }

}

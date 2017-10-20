package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

public class KafkaTimestampedMessageParser extends TimestampedMessageParser {


    public KafkaTimestampedMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        return toMillis(message.getTimestamp());
    }


}

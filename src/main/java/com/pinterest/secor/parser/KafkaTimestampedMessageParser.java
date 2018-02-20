package com.pinterest.secor.parser;

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class KafkaTimestampedMessageParser extends DailyOffsetMessageParser {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTimestampedMessageParser.class);

    public KafkaTimestampedMessageParser(SecorConfig config) {

        super(config);

    }


    @Override
    public String[] extractPartitions(Message message) throws Exception {

        long millis = toMillis(message.getTimestamp());
        long offset = message.getOffset();
        long offsetsPerPartition = mConfig.getOffsetsPerPartition();
        long partition = (offset / offsetsPerPartition) * offsetsPerPartition;
        String[] dailyPartition = generatePartitions(millis, mUsingHourly, mUsingMinutely);
        String dailyPartitionPath = StringUtils.join(dailyPartition, '/');
        //LOG.warn("parition path: {}", dailyPartitionPath);
        String[] result = {dailyPartitionPath, offsetPrefix + partition};
        return result;
    }


    @Override
    public long extractTimestampMillis(final Message message) {

        long millis = toMillis(message.getTimestamp());
        LOG.error("date milli: {}", millis);
        System.err.println("Prntln date milli: {}" + String.valueOf(millis));
        try {
        FileWriter fstream = new FileWriter("/tmp/as_secorout.txt", true);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("Prntln date milli: {}\n" + String.valueOf(millis));
        }
        catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        return millis;

    }


}

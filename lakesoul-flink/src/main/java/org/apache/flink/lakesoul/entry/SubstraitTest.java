package org.apache.flink.lakesoul.entry;

import io.substrait.extension.SimpleExtension;

import java.io.IOException;

public class SubstraitTest {
    public static void main(String[] args) throws IOException {
        SimpleExtension.loadDefaults();
    }
}

package com.datastax;

/**
 * Created by Chun Gao on 17/8/21
 */

public class SparkDemo {
    public static void main(String args[]) {
        SparkConnector sparkConnector = new SparkConnector();
        sparkConnector.runTestQuery();
    }
}

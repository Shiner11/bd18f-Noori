package edu.snu.bd.examples;

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;

public class DoubleInteger implements BeamSqlUdf {
    public static Integer eval(String input) {
        int value = Integer.parseInt(input);
        net.objecthunter.exp4j.Expression e = new net.objecthunter.exp4j.ExpressionBuilder("2 * x")
                .variable("x")
                .build()
                .setVariable("x", value);
        return (int) e.evaluate();
    }
}
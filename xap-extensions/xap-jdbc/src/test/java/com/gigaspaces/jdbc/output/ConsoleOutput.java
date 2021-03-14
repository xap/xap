package com.gigaspaces.jdbc.output;

/**
 * Created by moran on 6/8/18.
 */
public class ConsoleOutput {

    public static void newline() {
        System.out.println();
    }

    public static void println(Output output) {
        System.out.println( prepareOutput( output ) );
    }

    public static void print(Output output) {
        System.out.print( prepareOutput( output ) );
    }

    private static String prepareOutput(Output output) {
        final PlainTextOutput plainTextOutput = new PlainTextOutput(output);
        return plainTextOutput.getOutput();
    }

    public static void println(String line){
        System.out.println(line);
    }
}

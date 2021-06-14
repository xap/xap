/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

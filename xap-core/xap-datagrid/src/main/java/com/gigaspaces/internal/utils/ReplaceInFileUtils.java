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

package com.gigaspaces.internal.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.StringTokenizer;

/**
 * Using replaceInFile function you need make instance of <code>ReplaceInFileUtils</code> class.
 * Replacing example: ReplaceInFileUtils cli = new ReplaceInFileUtils( fileName );
 * cli.replaceInFile( "oldStr", "newStr" ); cli.replaceInFile( "oldStr1", "newStr1" );
 * cli.replaceInFile( "oldStr2", "newStr2" ); cli.close();
 *
 * You can replace many string in file, at the end you need to close instace of
 * <code>ReplaceInFileUtils</code> object using close() function, it's mean that you don't
 * intresting to replace anymore in this file.
 **/
public class ReplaceInFileUtils {
    private final String fileName;
    private final StringBuffer sb;

    /**
     * Constructor of ReplaceInFileUtils.
     *
     * @param fileName The file name to replace strings.
     **/
    public ReplaceInFileUtils(String fileName)
            throws IOException {
        int ch;
        this.fileName = fileName;
        sb = new StringBuffer();

        // open input stream from file
        try (InputStreamReader isr = new InputStreamReader(new FileInputStream(fileName))) {

            // read file to string buffer
            while ((ch = isr.read()) != -1)
                sb.append((char) ch);
        }
    }


    /**
     * Coping certain files to destination directory. Example: copy c:/myDir/file.txt
     * c:/myDir/file1.txt c:/myDir/file2.txt c:/destDir
     **/
    static public void copyFiles(String[] files, String destPath)
            throws IOException {
        int ch;

        // start to copy files destination directory
        for (String file : files) {
            String fileName = file.substring(file.lastIndexOf("/"));
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destPath + fileName))) {
                    while ((ch = bis.read()) != -1) {
                        bos.write(ch);
                    }
                }
            }
        } /* for */
    }


    /**
     * Coping certain file to destination file. Example: copy c:/test/oldFile.txt
     * c:/destDir/newFile.txt
     **/
    static public void copyFile(String fileName, String destFileName)
            throws IOException {
        int ch;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName))) {
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destFileName))) {
                while ((ch = bis.read()) != -1) {
                    bos.write(ch);
                }
            }
        }
    }


    /**
     * Find and replace specific characters or words in specic file.
     *
     * @param oldStr String to find and replace.
     * @param newStr New string to replace.
     **/
    public void replaceInFile(String oldStr, String newStr) {
        int next = 0;
        String line;

        line = sb.toString();

        // replace old string with new string
        while ((next = line.indexOf(oldStr, next)) != -1)
            line = sb.replace(next, next + oldStr.length(), newStr).toString();
    }


    /**
     * Find and replace specific characters or words in specific file.
     *
     * @param oldStr         String to find and replace.
     * @param newStr         New string to replace.
     * @param fromIndex      Started to replace from set index. Start to replace from beginning of
     *                       file set fromIndex = 1. For example str="igor igor igor best"
     *                       replaceInFile( "igor", "'replacedStr'", 2, false ); str="igor
     *                       'replacedStr' 'replacedStr' best"
     * @param isOneIteration <code>true<code/> replacing will be execute only one time depends
     *                       <code>fromIndex<code/> variable For example str="igor igor igor igor
     *                       igor best" replaceInFile( "igor", "'replacedStr'", 3, true ); The
     *                       result is str="igor igor 'replacedStr' igor igor best"
     **/
    public void replaceInFile(String oldStr, String newStr, int fromIndex, boolean isOneIteration) {
        int next = 0;
        int indexCounter = 1;
        String line;

        // check fromIndex != 0
        if (fromIndex <= 0)
            fromIndex = 0;

        line = sb.toString();

        // replace old string with new string
        while ((next = line.indexOf(oldStr, next)) != -1) {
            if (indexCounter >= fromIndex) {
                line = sb.replace(next, next + oldStr.length(), newStr).toString();

                if (isOneIteration)
                    return;
            }

            indexCounter++;
        }
    }


    /**
     * Find and replace specific XML tag value using property value. For example we want to replace
     * specific tag value <enabled>true</enabled> to "false" in the following XML example: <com>
     * <j_spaces> <enabled>true</enabled> </j_spaces> </com>
     *
     * Property value for find XML tag will be "com.j_spaces.enabled", <code>replacedValue</code>
     * param will be "false". After replacing, the XML example will be looking: <com> <j_spaces>
     * <enabled>false</enabled> </j_spaces> </com>
     *
     * NOTE: If XML tag not found this method will do nothing!
     *
     * @param propValue     Property value for find XML tag. For example: com.j_space.enabled
     * @param replacedValue XML tag value to replace.
     **/
    public void xmlReplace(String propValue, String replacedValue) {
        StringTokenizer st = new StringTokenizer(propValue, ".");
        String str = sb.toString();
        int inx = 0;
        String nextToken;
        String prevToken = null;

        while ((inx = str.indexOf((nextToken = st.nextToken()) + ">", inx)) != -1) {
            //be sure that we are looking for such tag name:
            //<nextToken> by this checking the following bug is avoiding:
            //<XXXx...nextToken>
            while (!str.substring(inx - 1, inx).equals("<")) {
                inx = str.indexOf(nextToken + ">", inx + 1);
                if (inx == -1)
                    break;
            }
            // store previous token
            if (st.hasMoreTokens())
                prevToken = nextToken;

            if (!st.hasMoreTokens()) {
                // check if index of replacable XML tag not bigger of closed Parent XML tag
                if (str.indexOf("</" + nextToken, inx) > str.indexOf("</" + prevToken, inx))
                    return;

                int start = inx + nextToken.length() + 1;
                int end = str.indexOf("</" + nextToken, inx);

                if (start < 0 || end < 0 || start > end) {
                    throw new StringIndexOutOfBoundsException("Can't find property \"" + propValue +
                            "\" with start index " + start + " and end index " + end);
                }

                // replace value of XML tag
                sb.replace(start, end, replacedValue);
                return;
            }
        }
    }


    // close replacing stream in file
    public void close()
            throws FileNotFoundException {
        // open file and flush all string-buffer
        try (PrintStream ps = new PrintStream(new FileOutputStream(fileName))) {
            ps.println(sb.toString());
            ps.flush();
        }
    }


    /**
     * Recursive utility function that delete entire directory.
     *
     * @param dirName         Directory to delete.
     * @param excludedDirName Directory name that will not deleted. Can be <code>null</code>.
     **/
    static public void delTree(String dirName, String excludedDirName) {
        File dir = new File(dirName);
        File[] files = dir.listFiles();

        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                if (files[i].isFile())
                    files[i].delete();
                else {
                    if (excludedDirName != null && files[i].getPath().equalsIgnoreCase(dir + File.separator + excludedDirName))
                        continue;

                    delTree(files[i].getPath(), excludedDirName);
                }
            } /* for */
            // remove current dir
            dir.delete();
        }

    } /* end delTree */
} /* end class */
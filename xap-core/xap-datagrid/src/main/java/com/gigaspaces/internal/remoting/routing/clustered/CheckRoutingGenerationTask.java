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
package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.start.SystemInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

public class CheckRoutingGenerationTask implements com.gigaspaces.internal.utils.concurrent.CompetitiveTask {

    private boolean isLatestGeneration;
    private PartitionToChunksMap newMap;
    private Exception exception;
    private String urlSuffix;

    CheckRoutingGenerationTask(String puName, int clientCurrentGeneration) {
        urlSuffix = "/v2/pus/" + puName + "/routing?generation=" + clientCurrentGeneration;
    }


    public boolean isLatest() {
        return isLatestGeneration;
    }

    public PartitionToChunksMap getNewMap() {
        return newMap;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public boolean execute(boolean isLastIteration) {
        try {
            String adminRestUrl = SystemInfo.singleton().getManagerClusterInfo().getCurrServer().getAdminRestUrl();
            URL url = new URL(adminRestUrl + urlSuffix);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            String requestMethod = "GET";
            con.setRequestMethod(requestMethod);
            int ret = con.getResponseCode();
            if (ret != HttpURLConnection.HTTP_OK && ret != HttpURLConnection.HTTP_NOT_MODIFIED) {
                System.out.println(requestMethod + " url:\n" + url + " return HTTP CODE " + ret);
                InputStream err = con.getErrorStream();
                if (err != null) {
                    String wholeErrorMessage = readFromInputStream(err);
                    System.out.println(wholeErrorMessage);
                }
            } else {
                if (ret == HttpURLConnection.HTTP_NOT_MODIFIED) {
                    this.isLatestGeneration = true;
                    return false;
                }

                InputStream response = con.getInputStream();
                if (response != null) {
                    HashMap hashMap = (HashMap) parseJSON(readFromInputStream(response));
                    ArrayList<HashMap> partitionsChunks = (ArrayList<HashMap>) hashMap.get("partitionChunks");
                    int generation = ((Long) hashMap.get("generation")).intValue();
                    PartitionToChunksMap chunksMap = new PartitionToChunksMap(partitionsChunks.size(), generation);
                    for (HashMap partitionChunks : partitionsChunks) {
                        int partitionId = ((Long) partitionChunks.get("partitionId")).intValue();
                        Set<Integer> mapChunks = chunksMap.getPartitionsToChunksMap().get(partitionId);
                        ArrayList<Long> chunks = (ArrayList<Long>) partitionChunks.get("chunks");
                        for (Long chunk : chunks) {
                            mapChunks.add(chunk.intValue());
                            chunksMap.getChunksToPartitionMap().put(chunk.intValue(), partitionId);
                        }
                    }

                    this.newMap = chunksMap;
                    return true;

                }

            }
        } catch (Exception e) {
            this.exception = e;
            return false;
        }
        return false;
    }

    private String readFromInputStream(InputStream is) throws IOException {
        StringBuilder response = new StringBuilder();
        if (is != null) {
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
                response.append("\n");
            }
            br.close();
        }

        return response.toString();
    }

    public Object parseJSON(String text) throws Exception {
        return parseJSON(new Scanner(text));
    }

    public Object parseJSON(Scanner s) throws Exception {
        Object ret = null;
        skipWhitespace(s);
        if (s.findWithinHorizon("\\{", 1) != null) {
            HashMap<Object, Object> retMap = new HashMap<>();
            ret = retMap;
            skipWhitespace(s);
            if (s.findWithinHorizon("\\}", 1) == null) {
                while (s.hasNext()) {
                    Object key = parseJSON(s);
                    skipWhitespace(s);
                    if (s.findWithinHorizon(":", 1) == null) {
                        fail(s, ":");
                    }
                    Object value = parseJSON(s);
                    retMap.put(key, value);
                    skipWhitespace(s);
                    if (s.findWithinHorizon(",", 1) == null) {
                        break;
                    }
                }
                if (s.findWithinHorizon("\\}", 1) == null) {
                    fail(s, "}");
                }
            }
        } else if (s.findWithinHorizon("\"", 1) != null) {
            ret = s.findWithinHorizon("(\\\\\\\\|\\\\\"|[^\"])*", 0)
                    .replace("\\\\", "\\")
                    .replace("\\\"", "\"");
            if (s.findWithinHorizon("\"", 1) == null) {
                fail(s, "quote");
            }
        } else if (s.findWithinHorizon("'", 1) != null) {
            ret = s.findWithinHorizon("(\\\\\\\\|\\\\'|[^'])*", 0);
            if (s.findWithinHorizon("'", 1) == null) {
                fail(s, "quote");
            }
        } else if (s.findWithinHorizon("\\[", 1) != null) {
            ArrayList<Object> retList = new ArrayList<>();
            ret = retList;
            skipWhitespace(s);
            if (s.findWithinHorizon("\\]", 1) == null) {
                while (s.hasNext()) {
                    retList.add(parseJSON(s));
                    skipWhitespace(s);
                    if (s.findWithinHorizon(",", 1) == null) {
                        break;
                    }
                }
                if (s.findWithinHorizon("\\]", 1) == null) {
                    fail(s, ", or ]");
                }
            }
        } else if (s.findWithinHorizon("true", 4) != null) {
            ret = true;
        } else if (s.findWithinHorizon("false", 5) != null) {
            ret = false;
        } else if (s.findWithinHorizon("null", 4) != null) {
            ret = null;
        } else {
            String numberStart = s.findWithinHorizon("[-0-9+eE]", 1);
            if (numberStart != null) {
                String numStr = numberStart + s.findWithinHorizon("[-0-9+eE.]*", 0);
                if (numStr.contains(".") | numStr.contains("e")) {
                    ret = Double.valueOf(numStr);
                } else {
                    ret = Long.valueOf(numStr);
                }
            } else {
                throw new Exception("No JSON value found. Found: " + s.findWithinHorizon(".{0,5}", 5));
            }
        }
        return ret;
    }

    private void fail(Scanner scanner, String expected) throws Exception {
        throw new Exception("Expected " + expected + " but found:" + scanner.findWithinHorizon(".{0,5}", 5));
    }

    private void skipWhitespace(Scanner s) {
        s.findWithinHorizon("\\s*", 0);
    }
}

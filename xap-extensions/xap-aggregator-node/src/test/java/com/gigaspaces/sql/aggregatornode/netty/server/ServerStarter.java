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
package com.gigaspaces.sql.aggregatornode.netty.server;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.AbstractSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ServerStarter {
    public static void main(String[] args) throws Exception {
//        GigaSpace space = createAndFillSpace(true, true);
        try (ServerBean server = new ServerBean()) {
            server.init();
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private static GigaSpace createAndFillSpace(boolean newDriver, boolean embedded) throws ParseException {
        String spaceName = "demo" + (newDriver ? "new" : "old");
        AbstractSpaceConfigurer configurer = embedded ? new EmbeddedSpaceConfigurer(spaceName)
                .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
//                .tieredStorage(new TieredStorageConfigurer().addTable(new TieredStorageTableConfig().setName(MyPojo.class.getName()).setCriteria("age > 20")))
                : new SpaceProxyConfigurer(spaceName);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
        GigaSpace gigaSpace = new GigaSpaceConfigurer(configurer).gigaSpace();
        if (embedded || gigaSpace.count(null) == 0) {
            java.util.Date date1 = simpleDateFormat.parse("10/09/2001 05:20:00.231");
            java.util.Date date2 = simpleDateFormat.parse("11/09/2001 10:20:00.250");
            java.util.Date date3 = simpleDateFormat.parse("12/09/2001 15:20:00.100");
            java.util.Date date4 = simpleDateFormat.parse("13/09/2001 20:20:00.300");
            gigaSpace.write(new MyPojo("Adler Aa", 20, "Israel", date1, new Time(date1.getTime()), new Timestamp(date1.getTime())));
            gigaSpace.write(new MyPojo("Adam Bb", 30, "Israel", date2, new Time(date2.getTime()), new Timestamp(date2.getTime())));
            gigaSpace.write(new MyPojo("Eve Cc", 35, "UK", date3, new Time(date3.getTime()), new Timestamp(date3.getTime())));
            gigaSpace.write(new MyPojo("NoCountry Dd", 40, null, date4, new Time(date4.getTime()), new Timestamp(date4.getTime())));
        }
        return gigaSpace;
    }
}

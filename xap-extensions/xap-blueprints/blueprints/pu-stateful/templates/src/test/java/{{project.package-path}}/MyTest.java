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
package {{project.groupId}};

import org.junit.*;
import org.openspaces.core.GigaSpace;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class MyTest {
    private AnnotationConfigApplicationContext appContext;

    @Before
    public void before() {
        appContext = new AnnotationConfigApplicationContext(ServiceConfig.class);
    }

    @After
    public void after() {
        appContext.close();
    }

    @Test
    public void testService() {
        GigaSpace gigaSpace = appContext.getBean(GigaSpace.class);
        Assert.assertEquals("{{space.name}}", gigaSpace.getSpaceName());
        Assert.assertTrue(gigaSpace.getSpace().isEmbedded());
    }
}

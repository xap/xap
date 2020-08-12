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

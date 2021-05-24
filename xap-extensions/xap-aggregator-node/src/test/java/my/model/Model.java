package my.model;

import com.gigaspaces.annotation.pojo.SpaceId;

public class Model {
    private String id;
    private String name;
    private int age;

    public Model() {
    }

    public Model(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public Model setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Model setName(String name) {
        this.name = name;
        return this;
    }

    public int getAge() {
        return age;
    }

    public Model setAge(int age) {
        this.age = age;
        return this;
    }
}

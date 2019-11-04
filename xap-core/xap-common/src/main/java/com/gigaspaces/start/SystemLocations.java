package com.gigaspaces.start;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.utils.GsEnv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@InternalApi
public class SystemLocations {

    private static final SystemLocations instance = new SystemLocations();

    public static SystemLocations singleton() {
        return instance;
    }

    private final Path home;
    private final String homeFwdSlash;
    private final Path xapNetHome;
    private final Path bin;
    private final Path config;
    private final Path lib;
    private final Path libRequired;
    private final Path libOptional;
    private final Path libOptionalSecurity;
    private final Path libPlatform;
    private final Path libPlatformExt;
    private final Path work;
    private final Path deploy;
    private final Path sparkHome;
    private final Path userProductHome;

    private SystemLocations() {
        this.home = initHome();
        this.homeFwdSlash = initHomeFwdSlash(home);
        this.xapNetHome = fromSystemProperty("com.gs.xapnet.home", null);
        this.bin = xapNetHome != null ? xapNetHome : home.resolve("bin");
        this.config = home.resolve("config");
        this.lib = fromSystemProperty("com.gigaspaces.lib", home.resolve("lib"));
        this.libRequired = fromSystemProperty("com.gigaspaces.lib.required", lib.resolve("required"));
        this.libOptional = fromSystemProperty("com.gigaspaces.lib.opt", lib.resolve("optional"));
        this.libOptionalSecurity = fromSystemProperty("com.gigaspaces.lib.opt.security", libOptional.resolve("security"));
        this.libPlatform = fromSystemProperty("com.gigaspaces.lib.platform", lib.resolve("platform"));
        this.libPlatformExt = fromSystemProperty("com.gigaspaces.lib.platform.ext", libPlatform.resolve("ext"));
        this.work = fromSystemProperty("com.gs.work", home.resolve("work"), true);
        this.deploy = fromSystemProperty("com.gs.deploy", home.resolve("deploy"), true);
        this.userProductHome = Paths.get(System.getProperty("user.home"), ".gigaspaces");
        this.sparkHome = fromEnvVar("SPARK_HOME", home.resolve("insightedge").resolve("spark"));
        System.setProperty("spark.home", sparkHome.toString());
    }

    private static Path initHome() {
        String result = System.getProperty(CommonSystemProperties.GS_HOME);
        if (result == null) {
            result = GsEnv.get("HOME", System.getProperty("user.dir"));
            System.setProperty(CommonSystemProperties.GS_HOME, result);
        }

        return Paths.get(result).toAbsolutePath();
    }

    private static String initHomeFwdSlash(Path home) {
        String result = home.toFile().toString().replace("\\", "/");
        System.setProperty(CommonSystemProperties.GS_HOME + ".fwd-slash", result);
        return result;
    }

    private static Path fromEnvVar(String key, Path defaultValue) {
        final String result = System.getenv(key);
        return result != null ? Paths.get(result) : defaultValue;
    }

    private static Path fromSystemProperty(String key, Path defaultValue) {
        return fromSystemProperty(key, defaultValue, false);
    }

    private static Path fromSystemProperty(String key, Path defaultValue, boolean initSystemProperty) {
        String result = System.getProperty(key);
        if (result != null)
            return Paths.get(result);
        if (initSystemProperty)
            System.setProperty(key, defaultValue.toString());
        return defaultValue;
    }

    public Path home() {
        return home;
    }

    public Path home(String subpath) {
        return home.resolve(subpath);
    }

    public Path home(String subpath, String ... more) {
        return home.resolve(Paths.get(subpath, more));
    }

    public String homeFwdSlash() {
        return homeFwdSlash;
    }

    public Path config() {
        return config;
    }

    public Path config(String subdir) {
        return config.resolve(subdir);
    }

    public Path work() {
        return work;
    }

    public Path work(String subpath) {
        return work.resolve(subpath);
    }

    public Path deploy(){
        return deploy;
    }

    public Path deploy(String subpath) {
        return deploy.resolve(subpath);
    }

    public Path sparkHome() {
        return sparkHome;
    }

    public Path xapNetHome() {
        return xapNetHome;
    }

    public Path bin() {
        return bin;
    }

    public Path bin(String scriptName) {
        String suffix;
        if (xapNetHome != null)
            suffix = ".exe";
        else
            suffix = (JavaUtils.isWindows() ? ".bat" : ".sh");
        return bin.resolve(scriptName + suffix);
    }

    public Path lib() {
        return lib;
    }

    public Path lib(String subpath) {
        return lib.resolve(subpath);
    }

    public Path lib(XapModules module) {
        return lib.resolve(module.getJarFilePath());
    }

    public Path libRequired() {
        return libRequired;
    }

    public Collection<Path> libRequired(Predicate<Path> filter) {
        try (Stream<Path> stream = Files.list(libRequired)) {
            return stream.filter(filter).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path libOptional() {
        return libOptional;
    }

    public Path libOptional(String subpath) {
        return libOptional.resolve(subpath);
    }

    public Path libOptionalSecurity() {
        return libOptionalSecurity;
    }

    public Path libPlatform() {
        return libPlatform;
    }

    public Path libPlatform(String subpath) {
        return libPlatform.resolve(subpath);
    }

    public Path libPlatformExt() {
        return libPlatformExt;
    }

    public Path userProductHome() {
        return userProductHome;
    }
}

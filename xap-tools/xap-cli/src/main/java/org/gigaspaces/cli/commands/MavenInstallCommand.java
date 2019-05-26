package org.gigaspaces.cli.commands;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.start.SystemInfo;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliCommandException;
import picocli.CommandLine;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
@CommandLine.Command(name="install", header = "Installs GigaSpaces artifacts from this package to maven repository")
public class MavenInstallCommand extends CliCommand {

    @CommandLine.Option(names = {"--artifacts-path" }, description = "Override default artifacts path", split = ",")
    List<Path> artifactsPath;

    @CommandLine.Option(names = {"--generate-only" }, description = "Generate the installer but don't run it")
    boolean generateOnly;

    @CommandLine.Option(names = {"--create-checksum" }, description = "Determines if checksum should be created", defaultValue = "true")
    boolean createChecksum;

    @CommandLine.Option(names = {"--maven-args" }, description = "Additional maven arguments for install command")
    String mavenArguments = "";

    @Override
    protected void execute() throws Exception {
        long startTime = System.currentTimeMillis();
        Path gsMaven = Paths.get(SystemInfo.singleton().getXapHome(), "tools", "maven");

        if (artifactsPath == null) {
            artifactsPath = new ArrayList<>();
            artifactsPath.add(gsMaven.resolve("xap-artifacts.txt"));
            // Temp hack pending merge of artifacts files.
            Path i9eArtifacts = Paths.get(SystemInfo.singleton().getXapHome(), "insightedge", "tools", "maven", "insightedge-artifacts.txt");
            if (Files.exists(i9eArtifacts))
                artifactsPath.add(i9eArtifacts);
        }

        Path target = gsMaven.resolve("installer");
        System.out.println("Generating GigaSpaces Maven artifacts installer at " + target.toAbsolutePath() + "...");
        if (Files.exists(target))
            throw new IOException("Path already exists: " + target.toAbsolutePath());
        Files.createDirectory(target);

        List<String> artifacts = new ArrayList<>();
        for (Path path : artifactsPath) {
            artifacts.addAll(load(path));
        }
        generatePom(artifacts, target);

        if (generateOnly) {
            System.out.printf("Generated installer for %s artifacts - Run 'mvn install' from generated folder to install all artifacts.%n",
                    artifacts.size());
        } else {
            try {
                String mavenPath = JavaUtils.isWindows() ? "mvn.cmd" : "mvn";
                String mavenGoal = "install";
                System.out.println("Executing install command: " + mavenPath + " " + mavenGoal + " " + mavenArguments + "...");
                int exitCode = new ProcessBuilder(mavenPath, mavenGoal, mavenArguments)
                        .directory(target.toAbsolutePath().toFile())
                        .inheritIO()
                        .start()
                        .waitFor();
                if (exitCode != 0)
                    throw CliCommandException.userError("Maven command exited with code " + exitCode);

                float duration = (System.currentTimeMillis() - startTime) / 1000f;
                System.out.printf("Installation of GigaSpaces Maven artifacts completed (artifacts=%s, duration=%.2fs)%n",
                        artifacts.size(), duration);
            } finally {
                deleteRecursive(target);
            }
        }
    }

    private void generatePom(List<String> artifacts, Path target) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("<project>");
        lines.add("  <modelVersion>4.0.0</modelVersion>");
        lines.add("  <groupId>org.gigaspaces</groupId>");
        lines.add("  <artifactId>gs-maven-installer</artifactId>");
        lines.add("  <version>1.0</version>");
        lines.add("  <properties>");
        lines.add("    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>");
        lines.add("    <gs.home>" + SystemInfo.singleton().getXapHome() + "</gs.home>");
        lines.add("    <createChecksum>" + createChecksum + "</createChecksum>");
        lines.add("  </properties>");
        lines.add("  <build>");
        lines.add("    <plugins>");
        lines.add("      <plugin>");
        lines.add("        <groupId>org.apache.maven.plugins</groupId>");
        lines.add("        <artifactId>maven-install-plugin</artifactId>");
        lines.add("        <version>2.5.2</version>");
        lines.add("        <executions>");

        List<String> template = createXmlTemplate();
        artifacts.forEach(artifact -> lines.addAll(processArtifact(template, target, artifact)));

        lines.add("        </executions>");
        lines.add("      </plugin>");
        lines.add("    </plugins>");
        lines.add("  </build>");
        lines.add("</project>");

        Files.write(target.resolve("pom.xml"), lines);
    }

    private List<String> processArtifact(List<String> template, Path target, String artifact) {
        Path path = Paths.get(artifact.replace("${gs.home}", SystemInfo.singleton().getXapHome()));
        String fileName = path.getFileName().toString();
        String name;
        try {
            if (fileName.endsWith(".jar")) {
                name = fileName.substring(0, fileName.length() - ".jar".length());
                extractPom(path, target.resolve("pom-" + name + ".xml"));
            } else if (fileName.equals("pom.xml")) {
                name = "parent-" + path.getParent().getFileName().toString();
                artifact = "pom-" + name + ".xml";
                Files.copy(path, target.resolve(artifact));
            } else {
                throw new IllegalArgumentException("Unsupported artifact: " + path);
            }

            Map<String, String> placeholders = new HashMap<>();
            placeholders.put("{{name}}", name);
            placeholders.put("{{artifact}}", artifact);
            return template.stream().map(l -> replaceAll(l, placeholders)).collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> createXmlTemplate() {
        List<String> template = new ArrayList<>();
        template.add("          <execution>");
        template.add("            <id>install-{{name}}</id>");
        template.add("            <phase>package</phase>");
        template.add("            <goals>");
        template.add("              <goal>install-file</goal>");
        template.add("            </goals>");
        template.add("            <configuration>");
        template.add("              <file>{{artifact}}</file>");
        template.add("              <pomFile>pom-{{name}}.xml</pomFile>");
        template.add("              <createChecksum>${createChecksum}</createChecksum>");
        template.add("            </configuration>");
        template.add("          </execution>");
        return template;
    }

    private static List<String> load(Path path) throws IOException {
        try (Stream<String> stream = Files.lines(path)) {
            return stream.filter(s -> !s.startsWith("#"))
                    .map(l -> l.replace("${XAP_HOME}", "${gs.home}")) // Temp, replace when xap-artifacts is fixed.
                    .collect(Collectors.toList());
        }
    }

    private static String replaceAll(String s, Map<String, String> placeholders) {
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            s = s.replace(entry.getKey(), entry.getValue());
        }
        return s;
    }

    private static void extractPom(Path artifact, Path target) throws IOException {
        JarFile jar = new JarFile(artifact.toFile());
        JarEntry pomEntry = jar.stream()
                .filter(e -> e.getName().startsWith("META-INF/maven") && e.getName().endsWith("pom.xml"))
                .findFirst().orElseThrow(() -> new IOException("failed to find pom.xml for " + jar.getName()));
        extract(jar, pomEntry, target);
    }

    private static void extract(JarFile jar, JarEntry entry, Path target) throws IOException {
        try (InputStream in = new BufferedInputStream(jar.getInputStream(entry))) {
            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(target.toFile()))) {
                byte[] buffer = new byte[2048];
                for (; ; ) {
                    int nBytes = in.read(buffer);
                    if (nBytes <= 0)
                        break;
                    out.write(buffer, 0, nBytes);
                }
                out.flush();
            }
        }
    }

    private static void deleteRecursive(Path path) throws IOException {
        Files.walk(path).sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to delete " + p, e);
                    }
                });
    }
}

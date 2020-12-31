package org.gigaspaces.cli.commands;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.start.SystemLocations;
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
@CommandLine.Command(name="install", header = "Installs GigaSpaces artifacts from this package to the Maven repository")
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
        Path gsMaven = SystemLocations.singleton().config("maven");

        if (artifactsPath == null) {
            artifactsPath = Collections.singletonList(gsMaven.resolve("gs-artifacts.txt"));
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
                List<String> commands = new ArrayList<>();
                commands.add(JavaUtils.isWindows() ? "mvn.cmd" : "mvn");
                commands.add("install");
                if(!mavenArguments.isEmpty())
                    commands.add(mavenArguments);
                System.out.println("Executing install command: " + String.join(" ",commands));
                int exitCode = new ProcessBuilder(commands)
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
                BootIOUtils.deleteRecursive(target);
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
        lines.add("    <gs.home>" + SystemLocations.singleton().home() + "</gs.home>");
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
        final Path path = Paths.get(artifact.replace("${gs.home}", SystemLocations.singleton().home().toString()));
        final String fileName = path.getFileName().toString();
        final String extension = getExtension(fileName);
        final String name = fileName.substring(0, fileName.length() - extension.length());

        try {
            switch (extension) {
                case ".jar":
                    extractPom(path, target.resolve("pom-" + name + ".xml"));
                    break;
                case ".xml":
                    artifact = "pom-" + name + ".xml";
                    Files.copy(path, target.resolve(artifact));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported extension: " + extension);

            }

            Map<String, String> placeholders = new HashMap<>();
            placeholders.put("{{name}}", name);
            placeholders.put("{{artifact}}", artifact);
            return template.stream().map(l -> replaceAll(l, placeholders)).collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getExtension(String s) {
        return s.substring(s.lastIndexOf('.'));
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
            return stream
                    .filter(s -> !s.isEmpty() && !s.startsWith("#"))
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

}

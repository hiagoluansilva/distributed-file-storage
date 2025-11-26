import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class Client {

    private static final int TIMEOUT = 5000;

    private static final String DEFAULT_HOST = "localhost";

    private static class RemoteFile {
        String fileId;
        String name;

        RemoteFile(String fileId, String name) {
            this.fileId = fileId;
            this.name = name;
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("====================================");
            System.out.println(" Distributed Storage Client");
            System.out.println("====================================");
            System.out.println("0 - Exit");
            System.out.println("1 - Test server");
            System.out.println("2 - List files on node");
            System.out.println("3 - Upload file to node");
            System.out.println("4 - Download file from node");
            System.out.print("Choose an option: ");

            String line = scanner.nextLine().trim();
            int option;
            try {
                option = Integer.parseInt(line);
            } catch (NumberFormatException e) {
                System.out.println("Invalid option.");
                continue;
            }

            if (option == 0) {
                System.out.println("Bye!");
                break;
            }

            try {
                switch (option) {
                    case 1:
                        testServer(scanner);
                        break;
                    case 2:
                        listFilesMenu(scanner);
                        break;
                    case 3:
                        uploadMenu(scanner);
                        break;
                    case 4:
                        downloadMenu(scanner);
                        break;
                    default:
                        System.out.println("Invalid option.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }

            System.out.println();
        }

        scanner.close();
    }

    // ===========================
    //  MENU OPTIONS
    // ===========================

    private static void testServer(Scanner scanner) throws IOException {
        int port = askPort(scanner);

        String path = "/status";
        String response = httpGetString(DEFAULT_HOST, port, path);

        System.out.println("Server " + DEFAULT_HOST + ":" + port + " responded:");
        System.out.println(response.trim());
    }

    private static void listFilesMenu(Scanner scanner) throws IOException {
        int port = askPort(scanner);

        List<RemoteFile> files = listRemoteFiles(DEFAULT_HOST, port);

        if (files.isEmpty()) {
            System.out.println("No files available on node " + port + ".");
            return;
        }

        System.out.println("Files on node " + port + ":");
        for (int i = 0; i < files.size(); i++) {
            RemoteFile f = files.get(i);
            System.out.printf("%d) %s (fileId=%s)%n", i + 1, f.name, f.fileId);
        }
    }

    private static void uploadMenu(Scanner scanner) throws IOException {
        int port = askPort(scanner);

        System.out.print("Enter local directory path (ENTER for current directory): ");
        String dirInput = scanner.nextLine().trim();
        Path dir = dirInput.isEmpty() ? Paths.get(".") : Paths.get(dirInput);

        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            System.out.println("Directory does not exist: " + dir.toAbsolutePath());
            return;
        }

        List<Path> localFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                if (Files.isRegularFile(p)) {
                    localFiles.add(p);
                }
            }
        }

        if (localFiles.isEmpty()) {
            System.out.println("No files found in directory " + dir.toAbsolutePath());
            return;
        }

        System.out.println("Available local files:");
        for (int i = 0; i < localFiles.size(); i++) {
            System.out.printf("%d) %s%n", i + 1, localFiles.get(i).getFileName());
        }

        System.out.print("Choose file number to upload: ");
        String line = scanner.nextLine().trim();
        int idx;
        try {
            idx = Integer.parseInt(line) - 1;
        } catch (NumberFormatException e) {
            System.out.println("Invalid number.");
            return;
        }

        if (idx < 0 || idx >= localFiles.size()) {
            System.out.println("Invalid file selection.");
            return;
        }

        Path file = localFiles.get(idx);
        byte[] content = Files.readAllBytes(file);
        String filename = file.getFileName().toString();

        System.out.println("Uploading " + filename + " to " + DEFAULT_HOST + ":" + port + " ...");

        String path = "/upload?name=" + urlEncode(filename);
        String response = httpPostString(DEFAULT_HOST, port, path, content);

        System.out.println("Server response:");
        System.out.println(response.trim());
    }

    private static void downloadMenu(Scanner scanner) throws IOException {
        int port = askPort(scanner);

        // 1) lista arquivos no nó
        List<RemoteFile> files = listRemoteFiles(DEFAULT_HOST, port);
        if (files.isEmpty()) {
            System.out.println("No files available on node " + port + ".");
            return;
        }

        System.out.println("Files on node " + port + ":");
        for (int i = 0; i < files.size(); i++) {
            RemoteFile f = files.get(i);
            System.out.printf("%d) %s (fileId=%s)%n", i + 1, f.name, f.fileId);
        }

        System.out.print("Choose file number to download: ");
        String line = scanner.nextLine().trim();
        int idx;
        try {
            idx = Integer.parseInt(line) - 1;
        } catch (NumberFormatException e) {
            System.out.println("Invalid number.");
            return;
        }

        if (idx < 0 || idx >= files.size()) {
            System.out.println("Invalid selection.");
            return;
        }

        RemoteFile chosen = files.get(idx);

        // 2) baixa arquivo
        String path = "/download?fileId=" + chosen.fileId;
        System.out.println("Downloading " + chosen.name + " from " + DEFAULT_HOST + ":" + port + " ...");

        byte[] data = httpGetBytes(DEFAULT_HOST, port, path);

        // 3) salva em downloads/<nome>
        Path downloadsDir = Paths.get("downloads");
        Files.createDirectories(downloadsDir);
        Path outPath = downloadsDir.resolve(chosen.name);

        Files.write(outPath, data);
        System.out.println("File saved to: " + outPath.toAbsolutePath());
    }

    // ===========================
    //  COMMON HELPERS
    // ===========================

    private static int askPort(Scanner scanner) {
        System.out.print("Enter node port (e.g. 5001..5005): ");
        String line = scanner.nextLine().trim();
        int port;
        try {
            port = Integer.parseInt(line);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port, using 5001.");
            port = 5001;
        }
        return port;
    }

    private static List<RemoteFile> listRemoteFiles(String host, int port) throws IOException {
        String json = httpGetString(host, port, "/files").trim();

        List<RemoteFile> files = new ArrayList<>();
        if (json.equals("[]") || json.isEmpty()) return files;

        // formato: [{"fileId":"...","name":"..."},{"fileId":"...","name":"..."}]
        // parsing simples na mão
        String content = json.substring(1, json.length() - 1).trim(); // remove [ ]
        if (content.isEmpty()) return files;

        String[] items = content.split("\\},\\{");
        for (String item : items) {
            String s = item.replace("{", "").replace("}", "").replace("\"", "");
            // s = fileId:xxx,name:yyy
            String[] fields = s.split(",");
            String fileId = null;
            String name = null;
            for (String f : fields) {
                String[] kv = f.split(":", 2);
                if (kv.length != 2) continue;
                if (kv[0].trim().equals("fileId")) {
                    fileId = kv[1].trim();
                } else if (kv[0].trim().equals("name")) {
                    name = kv[1].trim();
                }
            }
            if (fileId != null && name != null) {
                files.add(new RemoteFile(fileId, name));
            }
        }

        return files;
    }

    // ===========================
    //  HTTP (via HttpURLConnection)
    // ===========================

    private static String httpGetString(String host, int port, String path) throws IOException {
        byte[] data = httpGetBytes(host, port, path);
        return new String(data, StandardCharsets.UTF_8);
    }

    private static byte[] httpGetBytes(String host, int port, String path) throws IOException {
        URL url = new URL("http://" + host + ":" + port + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(TIMEOUT);
        conn.setReadTimeout(TIMEOUT);
        conn.setRequestMethod("GET");

        int status = conn.getResponseCode();
        InputStream is = (status >= 200 && status < 300)
                ? conn.getInputStream()
                : conn.getErrorStream();

        if (is == null) {
            throw new IOException("No response body, HTTP status " + status);
        }

        byte[] body = is.readAllBytes();
        is.close();
        conn.disconnect();
        return body;
    }

    private static String httpPostString(String host, int port, String path, byte[] body) throws IOException {
        URL url = new URL("http://" + host + ":" + port + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(TIMEOUT);
        conn.setReadTimeout(TIMEOUT);
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);

        conn.setRequestProperty("Content-Length", String.valueOf(body.length));

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body);
        }

        int status = conn.getResponseCode();
        InputStream is = (status >= 200 && status < 300)
                ? conn.getInputStream()
                : conn.getErrorStream();

        if (is == null) {
            throw new IOException("No response body, HTTP status " + status);
        }

        byte[] resp = is.readAllBytes();
        is.close();
        conn.disconnect();
        return new String(resp, StandardCharsets.UTF_8);
    }

    private static String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            return s;
        }
    }
}

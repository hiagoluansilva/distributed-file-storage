import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.Base64;

public class StorageNode {

    private final String nodeId;
    private final int port;
    private final Path dataRoot;

    private final int TOTAL_NODES = 5;

    public StorageNode(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
        this.dataRoot = Paths.get("data", "node-" + nodeId);
    }

    public void start() throws IOException {
        Files.createDirectories(dataRoot);
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.printf("Node %s listening on port %d%n", nodeId, port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(() -> handleClient(clientSocket)).start();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (Socket socket = clientSocket;
             InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream();
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), true)) {

            String requestLine = readLine(in);
            if (requestLine == null || requestLine.isEmpty()) return;

            System.out.println("[" + nodeId + "] Request: " + requestLine);

            String[] parts = requestLine.split(" ");
            String method = parts.length > 0 ? parts[0] : "";
            String rawPath = parts.length > 1 ? parts[1] : "";

            // separa path e query (?fileId=...&name=...)
            String path = rawPath;
            String query = null;
            int qPos = rawPath.indexOf('?');
            if (qPos != -1) {
                path = rawPath.substring(0, qPos);
                query = rawPath.substring(qPos + 1);
            }

            int contentLength = -1;
            while (true) {
                String headerLine = readLine(in);
                if (headerLine == null || headerLine.isEmpty()) break;
                String lower = headerLine.toLowerCase();
                if (lower.startsWith("content-length:")) {
                    try {
                        contentLength = Integer.parseInt(headerLine.split(":", 2)[1].trim());
                    } catch (NumberFormatException ignored) { }
                }
            }

            // ======== ROTAS EXTERNAS ========
            if ("GET".equalsIgnoreCase(method) && "/status".equals(path)) {
                sendPlain(writer, out, 200, "OK");
                return;
            }

            if ("GET".equalsIgnoreCase(method) && "/files".equals(path)) {
                handleListFiles(writer, out);
                return;
            }

            if ("GET".equalsIgnoreCase(method) && "/download".equals(path)) {
                handleDownload(writer, out, query);
                return;
            }

            if ("POST".equalsIgnoreCase(method) && "/upload".equals(path)) {
                handleUpload(writer, out, in, contentLength, query);
                return;
            }

            // ======== ROTAS INTERNAS ========
            if ("POST".equalsIgnoreCase(method) && "/internal/storeFragments".equals(path)) {
                handleInternalStoreFragments(writer, out, in, contentLength);
                return;
            }

            if ("POST".equalsIgnoreCase(method) && "/internal/announceFile".equals(path)) {
                handleInternalAnnounceFile(writer, out, in, contentLength);
                return;
            }

            if ("GET".equalsIgnoreCase(method) && "/internal/getFragment".equals(path)) {
                handleInternalGetFragment(writer, out, query);
                return;
            }

            sendPlain(writer, out, 404, "Not Found");

        } catch (IOException e) {
            System.err.println("[" + nodeId + "] Error: " + e.getMessage());
        }
    }

    // ================================
    //  UPLOAD
    // ================================

    private void handleUpload(PrintWriter writer, OutputStream out, InputStream in, int contentLength, String query) throws IOException {
        if (contentLength < 0) {
            sendPlain(writer, out, 411, "Content-Length required");
            return;
        }

        byte[] fileBytes = readFixed(in, contentLength);
        System.out.printf("[%s] Received upload: %d bytes%n", nodeId, fileBytes.length);

        String fileId = sha256Hex(fileBytes);
        System.out.printf("[%s] FileId = %s%n", nodeId, fileId);

        // pega nome amigável da query (?name=...)
        Map<String, String> params = parseQuery(query);
        String originalName = params.get("name");
        if (originalName == null || originalName.isEmpty()) {
            originalName = "file-" + fileId.substring(0, 8);
        }
        System.out.printf("[%s] Original name = %s%n", nodeId, originalName);

        int total = fileBytes.length;
        int parts = TOTAL_NODES;
        int baseSize = total / parts;
        int remainder = total % parts;

        int nodeIndex = Integer.parseInt(nodeId) - 1;
        int myFrag1 = nodeIndex;
        int myFrag2 = (nodeIndex + 1) % parts;

        Path fileDir = dataRoot.resolve(fileId);
        Path fragmentsDir = fileDir.resolve("fragments");
        Files.createDirectories(fragmentsDir);

        List<Fragment> fragments = new ArrayList<>();
        int offset = 0;

        for (int i = 0; i < parts; i++) {
            int partSize = baseSize + (i < remainder ? 1 : 0);
            byte[] fragment = new byte[partSize];
            if (partSize > 0) System.arraycopy(fileBytes, offset, fragment, 0, partSize);

            String hash = sha256Hex(fragment);
            System.out.printf("[%s] Fragment %d: %d bytes, hash=%s%n", nodeId, i, partSize, hash);

            fragments.add(new Fragment(i, fragment, hash));

            if (i == myFrag1 || i == myFrag2) {
                Path fragPath = fragmentsDir.resolve(i + ".frag");
                Files.write(fragPath, fragment);
                System.out.printf("[%s] Saved fragment %d locally%n", nodeId, i);
            }

            offset += partSize;
        }

        // 1) Replicação dos fragments para os outros nós
        if (!sendFragmentsToPeers(fileId, fragments, myFrag1, myFrag2)) {
            sendPlain(writer, out, 500, "Replication failed");
            return;
        }

        // 2) Geração do manifest (só com fileId + originalName + totalFragments)
        String manifestJson = buildManifestJson(fileId, originalName);

        // 3) Salva manifest localmente
        saveManifestLocal(fileId, manifestJson);

        // 4) Anuncia o arquivo para os outros nós
        announceManifestToPeers(fileId, manifestJson);

        sendPlain(writer, out, 201, "Uploaded");
    }

    // ================================
    //  SEND FRAGMENTS TO PEERS
    // ================================

    private boolean sendFragmentsToPeers(String fileId, List<Fragment> fragments, int myFrag1, int myFrag2) {
        for (int node = 0; node < TOTAL_NODES; node++) {
            if (node == (Integer.parseInt(nodeId) - 1)) continue;

            int frag1 = node;
            int frag2 = (node + 1) % TOTAL_NODES;

            List<Fragment> sendList = new ArrayList<>();
            sendList.add(fragments.get(frag1));
            sendList.add(fragments.get(frag2));

            boolean ok = false;

            for (int attempt = 1; attempt <= 3; attempt++) {
                System.out.printf("[%s] Sending fragments %d and %d to node %d (attempt %d)%n",
                        nodeId, frag1, frag2, (node+1), attempt);

                try {
                    ok = sendFragmentsToNode(node + 1, fileId, sendList);
                    if (ok) break;
                } catch (Exception ignored) { }
            }

            if (!ok) {
                System.out.printf("[%s] FAILED sending to node %d%n", nodeId, node+1);
                return false;
            }
        }
        return true;
    }

    private boolean sendFragmentsToNode(int targetNodeId, String fileId, List<Fragment> frags) throws IOException {
        URL url = new URL("http://localhost:500" + targetNodeId + "/internal/storeFragments");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");

        String json = buildFragmentsJson(fileId, frags);

        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        conn.setRequestProperty("Content-Length", String.valueOf(jsonBytes.length));
        try (OutputStream os = conn.getOutputStream()) {
            os.write(jsonBytes);
        }

        int status = conn.getResponseCode();
        if (status != 200) return false;

        String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        Map<Integer, String> remoteHashes = parseResponseHashes(response);

        for (Fragment f : frags) {
            String hashRemote = remoteHashes.get(f.index);
            if (!f.hash.equals(hashRemote)) {
                System.out.printf("[%s] Hash mismatch on fragment %d (local=%s, remote=%s)%n",
                        nodeId, f.index, f.hash, hashRemote);
                return false;
            }
        }
        return true;
    }

    // ================================
    //  INTERNAL: STORE FRAGMENTS
    // ================================

    private void handleInternalStoreFragments(PrintWriter writer, OutputStream out, InputStream in, int contentLength) throws IOException {
        String body = new String(readFixed(in, contentLength), StandardCharsets.UTF_8);

        Map<String, Object> parsed = parseJson(body);

        String fileId = (String) parsed.get("fileId");
        List<Map<String, String>> fragList = (List<Map<String, String>>) parsed.get("fragments");

        Path fileDir = dataRoot.resolve(fileId);
        Path fragmentsDir = fileDir.resolve("fragments");
        Files.createDirectories(fragmentsDir);

        Map<Integer, String> response = new HashMap<>();

        for (Map<String, String> fragInfo : fragList) {
            int index = Integer.parseInt(fragInfo.get("index"));
            byte[] data = Base64.getDecoder().decode(fragInfo.get("data"));
            String hash = sha256Hex(data);

            Path fragPath = fragmentsDir.resolve(index + ".frag");
            Files.write(fragPath, data);

            response.put(index, hash);
        }

        String jsonResponse = buildHashResponse(fileId, response);

        sendJson(writer, out, 200, jsonResponse);
    }

    // ================================
    //  INTERNAL: ANNOUNCE FILE (MANIFEST)
    // ================================

    private void handleInternalAnnounceFile(PrintWriter writer, OutputStream out, InputStream in, int contentLength) throws IOException {
        String body = new String(readFixed(in, contentLength), StandardCharsets.UTF_8);

        String fileId = extractFileIdFromManifest(body);
        if (fileId == null) {
            sendPlain(writer, out, 400, "Invalid manifest");
            return;
        }

        saveManifestLocal(fileId, body);

        sendJson(writer, out, 200, "{\"status\":\"OK\"}");
    }

    private void announceManifestToPeers(String fileId, String manifestJson) {
        int myIndex = Integer.parseInt(nodeId) - 1;

        for (int node = 0; node < TOTAL_NODES; node++) {
            if (node == myIndex) continue; // já salvei localmente

            int targetNodeId = node + 1;
            for (int attempt = 1; attempt <= 3; attempt++) {
                try {
                    URL url = new URL("http://localhost:500" + targetNodeId + "/internal/announceFile");
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setConnectTimeout(2000);
                    conn.setReadTimeout(2000);
                    conn.setRequestMethod("POST");
                    conn.setDoOutput(true);
                    conn.setRequestProperty("Content-Type", "application/json");

                    byte[] bytes = manifestJson.getBytes(StandardCharsets.UTF_8);
                    conn.setRequestProperty("Content-Length", String.valueOf(bytes.length));
                    try (OutputStream os = conn.getOutputStream()) {
                        os.write(bytes);
                    }

                    int status = conn.getResponseCode();
                    if (status == 200) {
                        System.out.printf("[%s] Manifest announced to node %d%n", nodeId, targetNodeId);
                        break;
                    } else {
                        System.out.printf("[%s] Manifest announce to node %d failed (status=%d, attempt=%d)%n",
                                nodeId, targetNodeId, status, attempt);
                    }
                } catch (IOException e) {
                    System.out.printf("[%s] Manifest announce to node %d failed: %s (attempt=%d)%n",
                            nodeId, targetNodeId, e.getMessage(), attempt);
                }
            }
        }
    }

    private void saveManifestLocal(String fileId, String manifestJson) throws IOException {
        Path fileDir = dataRoot.resolve(fileId);
        Files.createDirectories(fileDir);
        Path manifestPath = fileDir.resolve("manifest.json");
        Files.write(manifestPath, manifestJson.getBytes(StandardCharsets.UTF_8));
        System.out.printf("[%s] Saved manifest at %s%n", nodeId, manifestPath.toAbsolutePath());
    }

    // ================================
    //  LIST FILES
    // ================================

    private void handleListFiles(PrintWriter writer, OutputStream out) throws IOException {
        List<String> entries = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataRoot)) {
            for (Path p : stream) {
                if (Files.isDirectory(p)) {
                    Path manifestPath = p.resolve("manifest.json");
                    if (Files.exists(manifestPath)) {
                        String manifestJson = Files.readString(manifestPath, StandardCharsets.UTF_8);
                        String fileId = p.getFileName().toString();
                        String originalName = extractOriginalNameFromManifest(manifestJson);
                        if (originalName == null || originalName.isEmpty()) {
                            originalName = fileId;
                        }
                        entries.add("{\"fileId\":\"" + fileId + "\",\"name\":\"" + originalName + "\"}");
                    }
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < entries.size(); i++) {
            sb.append(entries.get(i));
            if (i < entries.size() - 1) sb.append(",");
        }
        sb.append("]");

        sendJson(writer, out, 200, sb.toString());
    }

    // ================================
    //  DOWNLOAD (sem parse complexo de manifest)
    // ================================

    private void handleDownload(PrintWriter writer, OutputStream out, String query) throws IOException {
        Map<String, String> params = parseQuery(query);
        String fileId = params.get("fileId");
        if (fileId == null || fileId.isEmpty()) {
            sendPlain(writer, out, 400, "Missing fileId");
            return;
        }

        Path manifestPath = dataRoot.resolve(fileId).resolve("manifest.json");
        if (!Files.exists(manifestPath)) {
            sendPlain(writer, out, 404, "File not found");
            return;
        }

        String manifestJson = Files.readString(manifestPath, StandardCharsets.UTF_8);
        String originalName = extractOriginalNameFromManifest(manifestJson);
        if (originalName == null || originalName.isEmpty()) {
            originalName = "file-" + fileId.substring(0, 8);
        }

        ByteArrayOutputStream full = new ByteArrayOutputStream();
        int myId = Integer.parseInt(nodeId);

        for (int i = 0; i < TOTAL_NODES; i++) {
            byte[] fragData = tryLoadFragmentLocal(fileId, i);

            if (fragData == null) {
                // nós que devem ter esse fragmento: i+1 e (i-1+N)%N+1
                int nodeA = i + 1;
                int nodeB = ((i - 1 + TOTAL_NODES) % TOTAL_NODES) + 1;

                int[] candidates = new int[] { nodeA, nodeB };

                for (int node : candidates) {
                    if (node == myId) continue;
                    try {
                        fragData = fetchFragmentFromNode(node, fileId, i);
                        if (fragData != null) {
                            break;
                        }
                    } catch (IOException ignored) { }
                }
            }

            if (fragData == null) {
                sendPlain(writer, out, 500, "Could not retrieve fragment " + i);
                return;
            }

            full.write(fragData);
        }

        byte[] fileBytes = full.toByteArray();

        // checa integridade usando o próprio fileId (sha256 do conteúdo)
        String checkId = sha256Hex(fileBytes);
        if (!checkId.equals(fileId)) {
            sendPlain(writer, out, 500, "File corrupted");
            return;
        }

        sendBinaryWithFilename(writer, out, 200, "application/octet-stream", fileBytes, originalName);
    }

    private byte[] tryLoadFragmentLocal(String fileId, int index) throws IOException {
        Path fragPath = dataRoot.resolve(fileId).resolve("fragments").resolve(index + ".frag");
        if (Files.exists(fragPath)) {
            return Files.readAllBytes(fragPath);
        }
        return null;
    }

    private byte[] fetchFragmentFromNode(int nodeId, String fileId, int index) throws IOException {
        URL url = new URL("http://localhost:500" + nodeId +
                "/internal/getFragment?fileId=" + fileId + "&index=" + index);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        conn.setRequestMethod("GET");

        int status = conn.getResponseCode();
        if (status != 200) return null;

        return conn.getInputStream().readAllBytes();
    }

    // ================================
    //  INTERNAL: GET FRAGMENT
    // ================================

    private void handleInternalGetFragment(PrintWriter writer, OutputStream out, String query) throws IOException {
        Map<String, String> params = parseQuery(query);
        String fileId = params.get("fileId");
        String indexStr = params.get("index");

        if (fileId == null || indexStr == null) {
            sendPlain(writer, out, 400, "Missing params");
            return;
        }

        int index;
        try {
            index = Integer.parseInt(indexStr);
        } catch (NumberFormatException e) {
            sendPlain(writer, out, 400, "Invalid index");
            return;
        }

        Path fragPath = dataRoot.resolve(fileId).resolve("fragments").resolve(index + ".frag");
        if (!Files.exists(fragPath)) {
            sendPlain(writer, out, 404, "Fragment not found");
            return;
        }

        byte[] data = Files.readAllBytes(fragPath);
        sendBinary(writer, out, 200, "application/octet-stream", data);
    }

    // ================================
    //  HELPERS
    // ================================

    private Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null || query.isEmpty()) return map;

        String[] pairs = query.split("&");
        for (String p : pairs) {
            String[] kv = p.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
            }
        }
        return map;
    }

    private byte[] readFixed(InputStream in, int length) throws IOException {
        byte[] data = new byte[length];
        int read = 0;
        while (read < length) {
            int r = in.read(data, read, length - read);
            if (r == -1) throw new IOException("Unexpected end of stream");
            read += r;
        }
        return data;
    }

    private String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        boolean gotCR = false;
        while ((b = in.read()) != -1) {
            if (b == '\r') { gotCR = true; continue; }
            if (b == '\n') break;
            if (gotCR) { buffer.write('\r'); gotCR = false; }
            buffer.write(b);
        }
        if (b == -1 && buffer.size() == 0) return null;
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private void sendPlain(PrintWriter writer, OutputStream out, int code, String body) throws IOException {
        byte[] bodyBytes = (body + "\n").getBytes(StandardCharsets.UTF_8);
        writer.print("HTTP/1.1 " + code + " OK\r\n");
        writer.print("Content-Type: text/plain; charset=utf-8\r\n");
        writer.print("Content-Length: " + bodyBytes.length + "\r\n");
        writer.print("\r\n");
        writer.flush();
        out.write(bodyBytes);
        out.flush();
    }

    private void sendJson(PrintWriter writer, OutputStream out, int code, String body) throws IOException {
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        writer.print("HTTP/1.1 " + code + " OK\r\n");
        writer.print("Content-Type: application/json; charset=utf-8\r\n");
        writer.print("Content-Length: " + bodyBytes.length + "\r\n");
        writer.print("\r\n");
        writer.flush();
        out.write(bodyBytes);
        out.flush();
    }

    private void sendBinary(PrintWriter writer, OutputStream out, int code, String contentType, byte[] data) throws IOException {
        writer.print("HTTP/1.1 " + code + " OK\r\n");
        writer.print("Content-Type: " + contentType + "\r\n");
        writer.print("Content-Length: " + data.length + "\r\n");
        writer.print("\r\n");
        writer.flush();
        out.write(data);
        out.flush();
    }

    private void sendBinaryWithFilename(PrintWriter writer, OutputStream out, int code, String contentType, byte[] data, String filename) throws IOException {
        writer.print("HTTP/1.1 " + code + " OK\r\n");
        writer.print("Content-Type: " + contentType + "\r\n");
        writer.print("Content-Length: " + data.length + "\r\n");
        writer.print("Content-Disposition: attachment; filename=\"" + filename + "\"\r\n");
        writer.print("\r\n");
        writer.flush();
        out.write(data);
        out.flush();
    }

    private String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ================================
    //  JSON BUILDING / PARSING
    // ================================

    // Manifest JSON (simples: fileId + originalName + totalFragments)
    private String buildManifestJson(String fileId, String originalName) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"fileId\":\"").append(fileId).append("\",");
        sb.append("\"originalName\":\"").append(originalName).append("\",");
        sb.append("\"totalFragments\":").append(TOTAL_NODES).append("}");
        return sb.toString();
    }

    // Payload p/ /internal/storeFragments
    private String buildFragmentsJson(String fileId, List<Fragment> frags) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"fileId\":\"").append(fileId).append("\",\"fragments\":[");
        for (int i = 0; i < frags.size(); i++) {
            Fragment f = frags.get(i);
            sb.append("{\"index\":\"").append(f.index)
              .append("\",\"data\":\"")
              .append(Base64.getEncoder().encodeToString(f.data))
              .append("\"}");
            if (i < frags.size() - 1) sb.append(",");
        }
        sb.append("]}");
        return sb.toString();
    }

    private String buildHashResponse(String fileId, Map<Integer, String> hashes) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"fileId\":\"").append(fileId).append("\",\"received\":[");
        int count = 0;
        for (Map.Entry<Integer, String> e : hashes.entrySet()) {
            sb.append("{\"index\":\"").append(e.getKey())
              .append("\",\"hash\":\"").append(e.getValue()).append("\"}");
            if (++count < hashes.size()) sb.append(",");
        }
        sb.append("]}");
        return sb.toString();
    }

    private Map<Integer, String> parseResponseHashes(String json) {
        Map<Integer, String> map = new HashMap<>();

        json = json.trim();

        int start = json.indexOf("\"received\"");
        if (start == -1) return map;

        start = json.indexOf("[", start);
        int end = json.indexOf("]", start);
        if (start == -1 || end == -1) return map;

        String arrayContent = json.substring(start + 1, end).trim();
        if (arrayContent.isEmpty()) return map;

        String[] items = arrayContent.split("\\},\\{");

        for (String item : items) {
            item = item.replace("{", "").replace("}", "").replace("\"", "").trim();

            String[] fields = item.split(",");
            Integer index = null;
            String hash = null;

            for (String field : fields) {
                String[] kv = field.split(":", 2);
                if (kv.length != 2) continue;

                String key = kv[0].trim();
                String value = kv[1].trim();

                if (key.equals("index")) {
                    index = Integer.parseInt(value);
                } else if (key.equals("hash")) {
                    hash = value;
                }
            }

            if (index != null && hash != null) {
                map.put(index, hash);
            }
        }

        return map;
    }

    // Parser simples p/ /internal/storeFragments
    private Map<String, Object> parseJson(String json) {
        Map<String, Object> result = new HashMap<>();
        String fileId = null;
        List<Map<String, String>> fragments = new ArrayList<>();

        int idxFileId = json.indexOf("\"fileId\"");
        if (idxFileId != -1) {
            int colon = json.indexOf(":", idxFileId);
            int q1 = json.indexOf("\"", colon + 1);
            int q2 = json.indexOf("\"", q1 + 1);
            if (q1 != -1 && q2 != -1) {
                fileId = json.substring(q1 + 1, q2);
            }
        }

        int idxFragsKey = json.indexOf("\"fragments\"");
        if (idxFragsKey != -1) {
            int startArr = json.indexOf("[", idxFragsKey);
            int endArr = json.indexOf("]", startArr);
            if (startArr != -1 && endArr != -1) {
                String arr = json.substring(startArr + 1, endArr).trim();
                if (!arr.isEmpty()) {
                    String[] items = arr.split("\\},\\{");
                    for (String item : items) {
                        item = item.replace("{", "").replace("}", "").trim();

                        String[] fields = item.split(",");
                        Map<String, String> frag = new HashMap<>();
                        for (String field : fields) {
                            String[] kv = field.split(":", 2);
                            if (kv.length != 2) continue;
                            String key = kv[0].trim().replace("\"", "");
                            String val = kv[1].trim().replace("\"", "");

                            if (key.equals("index") || key.equals("data")) {
                                frag.put(key, val);
                            }
                        }
                        if (frag.containsKey("index") && frag.containsKey("data")) {
                            fragments.add(frag);
                        }
                    }
                }
            }
        }

        result.put("fileId", fileId);
        result.put("fragments", fragments);
        return result;
    }

    private String extractFileIdFromManifest(String manifestJson) {
        int idx = manifestJson.indexOf("\"fileId\"");
        if (idx == -1) return null;
        int colon = manifestJson.indexOf(":", idx);
        int q1 = manifestJson.indexOf("\"", colon + 1);
        int q2 = manifestJson.indexOf("\"", q1 + 1);
        if (q1 == -1 || q2 == -1) return null;
        return manifestJson.substring(q1 + 1, q2);
    }

    private String extractOriginalNameFromManifest(String manifestJson) {
        int idx = manifestJson.indexOf("\"originalName\"");
        if (idx == -1) return null;
        int colon = manifestJson.indexOf(":", idx);
        int q1 = manifestJson.indexOf("\"", colon + 1);
        int q2 = manifestJson.indexOf("\"", q1 + 1);
        if (q1 == -1 || q2 == -1) return null;
        return manifestJson.substring(q1 + 1, q2);
    }

    // ================================
    //  Fragment struct
    // ================================

    private static class Fragment {
        int index;
        byte[] data;
        String hash;

        Fragment(int index, byte[] data, String hash) {
            this.index = index;
            this.data = data;
            this.hash = hash;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java StorageNode <nodeId> <port>");
            return;
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);

        StorageNode node = new StorageNode(nodeId, port);
        node.start();
    }
}

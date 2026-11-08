import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import io.minio.*;
import io.minio.messages.Item;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DataHandler {

    private static final Set<String> ValidEvents = Set.of("play", "like", "search", "listen");
    record SeperatedData(LocalDateTime localDateTime, UUID uuid, String eventType, int episodeNumber, String fileKey) {}
    record FaultyLines (String fileKey, String line, String reason) {}
    private static final Pattern EP_NUMBER = Pattern.compile("#\\s*(\\d+)");
    static int numOfEpisodes = 0;
    private static final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(20))
            .build();

    public static void main (String[] args) throws Exception {
        String endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");

        MinioClient minio = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("minioadmin", "minioadmin")
                .build();



        createBucket(minio, "bronze");
        createBucket(minio, "silver");
        //putData(minio, "bronze");
        cleanData(minio, "silver", "bronze");
    }

    static void createBucket (MinioClient minio, String bucket) throws Exception {
        boolean exists = minio.bucketExists(
                BucketExistsArgs.builder().bucket(bucket).build());

        if (exists) {
            System.out.println("Bucket '" + bucket + "' already exists");
            return;
        }
            minio.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
            System.out.println("Created bucket '" + bucket + "'");
    }

    static void putData (MinioClient minio, String bucket) throws Exception {
        boolean exists = minio.bucketExists(
                BucketExistsArgs.builder().bucket(bucket).build());

        if (!exists || !bucket.equals("bronze")) return;
        Path folder = Paths.get("C:\\Users\\kfirt\\projects\\project spotify\\data\\raw\\interactions_30");
        LocalTime timeStartUp = LocalTime.now();
        System.out.println("Transferring raw files to bronze");

        List<Path> files;
        try (Stream<Path> stream = Files.list(folder)) {files = stream.toList();}

        for (Path file : files) {
            String fileName = file.getFileName().toString();
            minio.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucket)
                            .object("raw/interactions/" + fileName)
                            .filename(file.toString())
                            .build());
            }

        minio.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucket)
                        .object("raw/feed.xml")
                        .filename("C:\\Users\\kfirt\\projects\\project spotify\\data\\raw\\feed.xml")
                        .build());

        LocalTime timeFinishing = LocalTime.now();
        Duration duration = Duration.between(timeStartUp, timeFinishing);
        long hours = duration.toHoursPart();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        System.out.println("Finished transferring in " + hours + "h " + minutes + "m " + seconds + "s");
            return;
    }

    static void cleanData (MinioClient minio, String silverBucket, String bronzeBucket) throws Exception {
        boolean exists = minio.bucketExists(
                BucketExistsArgs.builder().bucket(silverBucket).build());

        if (!exists || !silverBucket.equals("silver")) return;

        List<SeperatedData> seperatedData = new ArrayList<>();
        List<FaultyLines> faultyLines = new ArrayList<>();


        Iterable<Result<Item>> results = minio.listObjects(
                ListObjectsArgs.builder()
                        .bucket("bronze")
                        .prefix("raw/interactions/")
                        .recursive(true)
                        .build());

        for (Result<Item> result : results) {
            Item item = result.get();
            if (item.isDir() || !item.objectName().endsWith("txt")) continue;
            String fileKey = item.objectName();

            try (InputStream stream = minio.getObject(
                    GetObjectArgs.builder().bucket(bronzeBucket).object(item.objectName()).build())) {
                String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);

                content = content.replace("\uFEFF", "");
                content = content.replace("\u200B", "");
                content = content.replaceAll("\\h+", "");

                String[] lines = content.split("\\R");
                int i = 0;

                for (String line : lines) { if (line.isEmpty()) continue;
                    try {
                        seperatedData.add(parseLine(line, fileKey));

                    } catch (BadLineException e) {
                        faultyLines.add(new FaultyLines(fileKey, line, e.getMessage()));
                    }

                }
            }
            System.out.println("Clean data " + seperatedData.size() + " Faulty data " + faultyLines.size());
        }

        try (InputStream stream = minio.getObject(
                GetObjectArgs.builder()
                        .bucket(bronzeBucket)
                        .object("raw/feed.xml")
                        .build())) {

            SyndFeed feed = new SyndFeedInput().build(new XmlReader(stream));
            List<SyndEntry> entries = feed.getEntries();

            numOfEpisodes = entries.size();
            for (SyndEntry entry : entries) {
                String title = entry.getTitle().strip();

                Matcher m = EP_NUMBER.matcher(title);
                if (!m.find()) throw new IllegalArgumentException("No episode number in title: " + title);
                int episodeNumber = Integer.parseInt(m.group(1));

                String audioUrl = entry.getEnclosures().get(0).getUrl();
                System.out.println(entry.getTitle());

                String cleanTitle = title
                        .replaceFirst("(?i)^MamraMic\\s*#\\s*\\d+\\s*-?\\s*", "")
                        .replaceAll("[\\\\/:*?\"<>|]", "")
                        .replaceAll("\\s+", " ")
                        .strip();

                String fileTitle = "MamraMic#" + episodeNumber + " - " + cleanTitle;
                String mp3Key = "mp3/" + fileTitle + ".mp3";

                Set<String> existingMp3s = new HashSet<>();

                for (Result<Item> result : minio.listObjects(
                        ListObjectsArgs.builder()
                                .bucket("silver")
                                .prefix("mp3/")
                                .recursive(true)
                                .build())) {
                    existingMp3s.add(result.get().objectName());
                }

                if (!existingMp3s.contains(mp3Key)) {


                    HttpRequest request = HttpRequest.newBuilder(URI.create(audioUrl)).GET().build();
                    HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());

                    try (InputStream body = response.body()) {
                        if (response.statusCode() != 200) throw new IllegalArgumentException("File network error");

                        minio.putObject(PutObjectArgs.builder()
                                .bucket("silver")
                                .object(mp3Key)
                                .stream(body, -1, 10 * 1024 * 1024)   // size unknown → upload in 10 MB parts
                                .contentType("audio/mpeg")
                                .userMetadata(Map.of("episode-number", String.valueOf(episodeNumber)))
                                .build());
                    }
                }
            }
        }



    }
    static SeperatedData parseLine (String line, String fileKey) throws BadLineException {

        String[] separateData = line.split("\\|", -1);

        if (separateData.length != 4) throw new BadLineException("Wrong amount of lines " + separateData.length);
        for (String s : separateData)
            if (s.isEmpty()) throw new BadLineException("empty line");

        LocalDateTime localDateTime;
        try {
            localDateTime = LocalDateTime.parse(separateData[0]);
        } catch (DateTimeParseException e) {
            throw new BadLineException("Bad timestamp");
        }

        UUID uuid;
        try {
            uuid = UUID.fromString(separateData[1]);
        } catch (IllegalArgumentException e) {
            throw new BadLineException("Bad user id");
        }

        String event = separateData[2].toLowerCase();
        if (!ValidEvents.contains(event)) throw new BadLineException("Unkown event " + event);

        String[] episodeInfo = separateData[3].split("-");
        if (episodeInfo.length != 2 || !episodeInfo[0].equalsIgnoreCase("episode")) throw new BadLineException("Bad episode format");
        int episodeNumber;
        try {
            episodeNumber = Integer.parseInt(episodeInfo[1]);
        } catch (IllegalArgumentException e){
            throw new BadLineException("Bad episode number");
        }

        if (episodeNumber > numOfEpisodes || episodeNumber < 1) throw new BadLineException("Episode does not exist");

        return new SeperatedData(localDateTime, uuid, event, episodeNumber, fileKey);
    }


}
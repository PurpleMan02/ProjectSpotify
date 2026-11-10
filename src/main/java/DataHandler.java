import com.rometools.modules.itunes.EntryInformation;
import com.rometools.modules.itunes.ITunes;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import io.minio.*;
import io.minio.messages.Item;
import org.jsoup.Jsoup;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DataHandler {

    private static final Set<String> ValidEvents = Set.of("like", "search", "listen");
    record SeparatedData (LocalDateTime localDateTime, String event_id, String eventType, int episodeNumber, String fileKey) {}
    record FaultyLines (String fileKey, String line, String reason) {}
    record Episode (int episodeNumber, String title, LocalDateTime pubDate, int duration,
                   String description, String audioUrl, String mp3Key) {}

    private static final List<Episode> episodes = new ArrayList<>();
    private static final List<SeparatedData> separatedData = new ArrayList<>();
    private static final List<FaultyLines> faultyLines = new ArrayList<>();

    private static final Pattern EP_NUMBER = Pattern.compile("#\\s*(\\d+)");
    static int numOfEpisodes = 112;
    private static final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(20))
            .build();

    private static final String DATA_DIR = System.getenv().getOrDefault("DATA_DIR", "../data");
    private static final String MINIO_ENDPOINT = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
    private static final String MINIO_HOST = MINIO_ENDPOINT.replace("http://", "");

    private static final String PG_HOST = System.getenv().getOrDefault("PG_HOST", "localhost");
    private static final String PG_PORT = System.getenv().getOrDefault("PG_PORT", "5433");

    public static void main (String[] args) throws Exception {
        String endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");

        MinioClient minio = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("minioadmin", "minioadmin")
                .build();

        switch (args[0]) {
            case "bronze":
                createBucket(minio, "bronze");
                putData(minio, "bronze");
                break;

            case "silver":
                createBucket(minio, "silver");
                cleanData(minio, "silver", "bronze");
                createPar();
                break;

            case "gold":
                createDB();
                break;

            default:
                System.out.println("Unknown command: " + args[0]);
                System.exit(1);
        }
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
        Path folder = Paths.get(DATA_DIR, "raw", "interactions");
        LocalTime timeStartUp = LocalTime.now();
        System.out.println("Transferring raw files to bronze");

        Set<String> existingData = new HashSet<>();

        for (Result<Item> result : minio.listObjects(
                ListObjectsArgs.builder()
                        .bucket("bronze")
                        .prefix("raw/interactions/")
                        .recursive(true)
                        .build())) {
            existingData.add(result.get().objectName());
        }

        List<Path> files;
        try (Stream<Path> stream = Files.list(folder)) {files = stream.toList();}

        for (Path file : files) {
            String fileName = file.getFileName().toString();
            String key = "raw/interactions/" + fileName;
            if (!existingData.contains(key)) {
                minio.uploadObject(
                        UploadObjectArgs.builder()
                                .bucket(bucket)
                                .object("raw/interactions/" + fileName)
                                .filename(file.toString())
                                .build());
                System.out.println("Printed " + fileName);
                }
            }

        minio.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucket)
                        .object("raw/feed.xml")
                        .filename(Paths.get(DATA_DIR, "raw", "feed.xml").toString())
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

        //Downloading the episodes
        try (InputStream stream = minio.getObject(
                GetObjectArgs.builder()
                        .bucket(bronzeBucket)
                        .object("raw/feed.xml")
                        .build())) {

            SyndFeed feed = new SyndFeedInput().build(new XmlReader(stream));
            List<SyndEntry> entries = feed.getEntries();

            numOfEpisodes = entries.size();

            Set<String> existingMp3s = new HashSet<>();

            for (Result<Item> result : minio.listObjects(
                    ListObjectsArgs.builder()
                            .bucket("silver")
                            .prefix("mp3/")
                            .recursive(true)
                            .build())) {
                existingMp3s.add(result.get().objectName());
            }

            for (SyndEntry entry : entries) {
                String title = entry.getTitle().strip();
                LocalDateTime pbDate = entry.getPublishedDate().toInstant().atZone(ZoneId.of("Asia/Jerusalem")).toLocalDateTime();

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

                if (!existingMp3s.contains(mp3Key)) {

                    HttpRequest request = HttpRequest.newBuilder(URI.create(audioUrl)).GET().build();
                    HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());

                    try (InputStream body = response.body()) {
                        if (response.statusCode() != 200) throw new IllegalArgumentException("File network error");

                        minio.putObject(PutObjectArgs.builder()
                                .bucket("silver")
                                .object(mp3Key)
                                .stream(body, -1, 10 * 1024 * 1024)
                                .contentType("audio/mpeg")
                                .build());
                    }
                }
                EntryInformation itunes = (EntryInformation) entry.getModule(ITunes.URI);
                int duration = (int) (itunes.getDuration().getMilliseconds() / 1000);

                String description = Jsoup.parse(entry.getDescription().getValue()).text();

                episodes.add(new Episode(episodeNumber, fileTitle, pbDate, duration,
                        description, audioUrl, mp3Key));
            }
        }

        //Clean data and sort it into the record and the list
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
                        separatedData.add(parseLine(line, fileKey));

                    } catch (BadLineException e) {
                        faultyLines.add(new FaultyLines(fileKey, line, e.getMessage()));
                    }

                }
            }
        }
        System.out.println("Clean data " + separatedData.size() + " Faulty data " + faultyLines.size());
        for (FaultyLines f : faultyLines) System.out.println(f.reason + " " + f.line + " " + "File key - " + f.fileKey);





    }
    static SeparatedData parseLine (String line, String fileKey) throws BadLineException {

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

        String event_id;
        try {
            event_id = separateData[1];
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

        return new SeparatedData(localDateTime, event_id, event, episodeNumber, fileKey);
    }

    static void createPar () {
        try (Connection db = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = db.createStatement()) {

            connectDuckToMinio(st);

            st.execute("""
        CREATE TABLE episodes (
            episode_number  INTEGER,
            title           VARCHAR,
            pub_date        TIMESTAMP,
            duration        INTEGER,
            description     VARCHAR,
            audio_url       VARCHAR,
            mp3_key         VARCHAR
        )""");
            st.execute("""
        CREATE TABLE events (
            event_time      TIMESTAMP,
            event_id        VARCHAR,
            event_type      VARCHAR,
            episode_num     INTEGER,
            file_key        VARCHAR
        )""");

            try (PreparedStatement insert = db.prepareStatement(
                    "INSERT INTO episodes VALUES (?, ?, ?, ?, ?, ?, ?)")) {

                for (Episode e : episodes) {
                    insert.setInt(1, e.episodeNumber());
                    insert.setString(2, e.title());
                    insert.setObject(3, e.pubDate());
                    insert.setInt(4, e.duration());
                    insert.setString(5, e.description());
                    insert.setString(6, e.audioUrl());
                    insert.setString(7, e.mp3Key());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = db.prepareStatement(
                    "INSERT INTO events VALUES (?, ?, ?, ?, ?)")) {

                for (SeparatedData s : separatedData) {
                    insert.setObject(1, s.localDateTime());
                    insert.setString(2, s.event_id);
                    insert.setString(3, s.eventType());
                    insert.setInt(4, s.episodeNumber());
                    insert.setString(5, s.fileKey());
                    insert.addBatch();
                }
                insert.executeBatch();
            }

            st.execute("COPY episodes TO 's3://silver/episodes/episodes.parquet' (FORMAT parquet)");
            st.execute("COPY events TO 's3://silver/events/events.parquet' (FORMAT parquet)");

            System.out.println("Data copied into parquets");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static void createDB () {
        String pgUrl = System.getenv().getOrDefault("PG_URL", "jdbc:postgresql://localhost:5433/postgres");

        try (Connection pg = DriverManager.getConnection(pgUrl, "postgres", "postgres");
             Statement st = pg.createStatement()) {

            st.execute("DROP TABLE IF EXISTS events");
            st.execute("DROP TABLE IF EXISTS episodes");

            st.execute("""
                    CREATE TABLE episodes (
                        episode_number  INTEGER PRIMARY KEY,
                        title           VARCHAR NOT NULL,
                        pub_date        TIMESTAMP NOT NULL,
                        duration        INTEGER NOT NULL,
                        description     VARCHAR NOT NULL,
                        audio_url       VARCHAR NOT NULL,
                        mp3_key         VARCHAR NOT NULL
                    )""");
            st.execute("""
                    CREATE TABLE events (
                        event_time      TIMESTAMP NOT NULL,
                        event_id        VARCHAR NOT NULL,
                        event_type      VARCHAR NOT NULL,
                        episode_num     INTEGER NOT NULL REFERENCES episodes (episode_number),
                        file_key        VARCHAR NOT NULL
                    )""");

            System.out.println("Gold tables created");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection db = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = db.createStatement()) {

            connectDuckToMinio(st);

            st.execute("INSTALL postgres");
            st.execute("LOAD postgres");
            st.execute("ATTACH 'host=" + PG_HOST + " port=" + PG_PORT + " dbname=postgres user=postgres password=postgres' AS pg (TYPE postgres)");
            st.execute("""
            INSERT INTO pg.public.episodes
            SELECT episode_number, title, pub_date, duration, description, audio_url, mp3_key
            FROM read_parquet('s3://silver/episodes/episodes.parquet')
            """);

                st.execute("""
            INSERT INTO pg.public.events (event_time, event_id, event_type, episode_num, file_key)
            SELECT event_time, event_id, event_type, episode_num, file_key
            FROM read_parquet('s3://silver/events/events.parquet')
            """);

                System.out.println("Data copied to Gold");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static void connectDuckToMinio(Statement st) throws SQLException {
        st.execute("INSTALL httpfs");
        st.execute("LOAD httpfs");
        st.execute("CREATE SECRET minio (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', "
                + "ENDPOINT '" + MINIO_HOST + "', URL_STYLE 'path', USE_SSL false)");
    }

    }
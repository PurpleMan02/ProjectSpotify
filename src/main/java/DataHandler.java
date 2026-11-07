import io.minio.*;
import io.minio.messages.Item;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class DataHandler {

    public static void main (String[] args) throws Exception {
        String endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");

        MinioClient minio = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("minioadmin", "minioadmin")
                .build();

        createBucket(minio, "bronze");
        createBucket(minio, "silver");
        putData(minio, "bronze");
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
        LocalTime timeFinishing = LocalTime.now();
        Duration duration = Duration.between(timeStartUp, timeFinishing);
        long hours = duration.toHoursPart();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        System.out.println("Finished transferring in " + hours + "h " + minutes + "m " + seconds + "s");
            return;
    }

    static void cleanData (MinioClient minio, String bucketInput, String bucketOutput) throws Exception {
        boolean exists = minio.bucketExists(
                BucketExistsArgs.builder().bucket(bucketInput).build());

        if (!exists || !bucketInput.equals("silver")) return;

        Iterable<Result<Item>> results = minio.listObjects(
                ListObjectsArgs.builder()
                        .bucket("bronze")
                        .prefix("raw/interactions/")   // only keys starting with this
                        .recursive(true)               // go into "sub-folders" too
                        .build());

        List<String> keys = new ArrayList<>();
        for (Result<Item> result : results) {
            Item item = result.get();                  // may throw if a page failed to load
            if (item.isDir()) continue;                // skip "folder" entries
            String key = item.objectName();            // e.g. raw/interactions/1794082582317_file_84540.txt
            if (key.endsWith(".txt")) {
                keys.add(key);
            }
        }
        System.out.println("Found " + keys.size() + " files");
    }
}
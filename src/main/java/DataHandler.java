import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalTime;
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
        Path folder = Paths.get("C:\\Users\\kfirt\\OneDrive\\Desktop\\project spotify\\data\\raw\\interactions");
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
}
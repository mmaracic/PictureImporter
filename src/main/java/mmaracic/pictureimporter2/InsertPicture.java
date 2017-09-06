package mmaracic.pictureimporter2;


import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;

public class InsertPicture {

    public static String host = "localhost";

    public static String database = "face_req";

    public static String imagePath = "E:\\Bases Large\\FERET\\smaller_uncompressed";

    public static String imageDatabase = "FERET";

    public static void main(String[] argv) throws SQLException, IOException {

        System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");

        try {

            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
            e.printStackTrace();
            return;

        }

        System.out.println("PostgreSQL JDBC Driver Registered!");

        final Connection conn;

        try {

            conn = DriverManager.getConnection("jdbc:postgresql://" + host + "/" + database, database, database);

        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;

        }

        if (conn != null) {
            System.out.println("Connection to the database established");
        } else {
            System.out.println("Failed to make connection!");
        }

        File folder = new File(imagePath);// your picture path
        if (!folder.isDirectory()) {
            System.out.println("Path is not a folder!");
            return;
        }
        
        final int databaseId;
        PreparedStatement ps = conn.prepareStatement("SELECT id_base FROM face_req.bases WHERE description = ?;");
        ps.setString(1, imageDatabase);
        ResultSet rs = ps.executeQuery();
        if (rs.next()){
            databaseId = rs.getInt(1);
        } else {
            PreparedStatement ps1 = conn.prepareStatement("INSERT INTO face_req.bases(description, name) VALUES (?, ?) RETURNING id_base;");
            ps1.setString(1, imageDatabase);
            ps1.setString(2, imageDatabase);
            ResultSet rs1 = ps1.executeQuery();
            rs1.next();
            databaseId = rs1.getInt(1);
        }

        Map<String, Integer> personImages = new HashMap<>();
        Map<String, Integer> personIds = new HashMap<>();

        Files.walkFileTree(folder.toPath(), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                File f = path.toFile();// your picture path
                String fileName = f.getName();
                String[] fileNameParts = fileName.split("\\.");
                String extension = fileNameParts[1];
                String[] nameParts = fileNameParts[0].split("_");
                String person = nameParts[0];
                String pose = nameParts[2];

                if (personImages.containsKey(person)) {
                    personImages.put(person, personImages.get(person) + 1);
                } else {
                    personImages.put(person, 1);
                    try {
                        PreparedStatement ps = conn.prepareStatement("INSERT INTO face_req.person_on_photo(name)VALUES (?) RETURNING id;");
                        ps.setString(1, person);
                        ResultSet rs = ps.executeQuery();
                        rs.next();
                        int personId = rs.getInt(1);
                        personIds.put(person, personId);
                    } catch (SQLException ex) {
                        Logger.getLogger(InsertPicture.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                int pic_order = personImages.get(person) - 1;
                int ownerId = personIds.get(person);

                BufferedImage im = ImageIO.read(f);
                ByteArrayOutputStream baos = null;
                if (im == null){
                    System.out.println("-- ERROR -- Can not read image " + f.getAbsolutePath());
                    return FileVisitResult.CONTINUE;
                }
                try {
                    baos = new ByteArrayOutputStream();
                    ImageIO.write(im, "png", baos);
                    PreparedStatement ps = conn.prepareStatement("INSERT INTO face_req.picture(num_of_pic, height, width, format, id_owner, id_base, picture, description)VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

                    ps.setInt(1, pic_order);
                    ps.setInt(2, im.getHeight());
                    ps.setInt(3, im.getWidth());
                    ps.setString(4, extension);
                    ps.setInt(5, ownerId);
                    ps.setInt(6, databaseId);
                    ps.setBytes(7, baos.toByteArray());
                    ps.setString(8, fileName);
                    ps.executeUpdate();
                    ps.close();

                    System.out.println("Insert successfull for " + fileName);
                } catch (SQLException ex) {

                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    if (baos != null){
                        baos.close();
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
        
        System.out.println("All done!");
    }
}

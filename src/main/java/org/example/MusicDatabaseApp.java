package org.example;

import java.sql.*;

public class MusicDatabaseApp {

    private static final String MUSIC_URL = "jdbc:postgresql://localhost:5432/music_db";
    private static final String BOOKS_URL = "jdbc:postgresql://localhost:5432/books_db";// Specify database name
    private static final String USER = "postgres"; // Specify user
    private static final String PASSWORD = "postgres"; // Specify password

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(MUSIC_URL, USER, PASSWORD)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS study;");
                System.out.println("Schema 'study' created.");
                statement.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS study.music (" +
                                "id INT PRIMARY KEY, " +
                                "name TEXT NOT NULL);"
                );
                System.out.println("Table 'study.music' created.");
                statement.executeUpdate(
                        "INSERT INTO study.music (id, name) " +
                                "SELECT * FROM (VALUES " +
                                "(1, 'Bohemian Rhapsody'), " +
                                "(2, 'Stairway to Heaven'), " +
                                "(3, 'Imagine'), " +
                                "(4, 'Sweet Child O Mine'), " +
                                "(5, 'Hey Jude'), " +
                                "(6, 'Hotel California'), " +
                                "(7, 'Billie Jean'), " +
                                "(8, 'Wonderwall'), " +
                                "(9, 'Smells Like Teen Spirit'), " +
                                "(10, 'Let It Be'), " +
                                "(11, 'I Want It All'), " +
                                "(12, 'November Rain'), " +
                                "(13, 'Losing My Religion'), " +
                                "(14, 'One'), " +
                                "(15, 'With or Without You'), " +
                                "(16, 'Sweet Caroline'), " +
                                "(17, 'Yesterday'), " +
                                "(18, 'Dont Stop Believin'), " +
                                "(19, 'Crazy Train'), " +
                                "(20, 'Always') " +
                                ") AS new_data " +
                                "WHERE NOT EXISTS (SELECT 1 FROM study.music WHERE music.id = new_data.column1);"
                );
            }
            System.out.println("\nAll musics");
            getAllMusic(connection);
            System.out.println("\nFiltred musics");
            getFilteredMusic(connection);

            addMusic(connection);

            System.out.println("\nList of music compositions after adding:");
            getAllMusic(connection);

            dropStudyTable(connection, "music");

        } catch (SQLException e) {
            System.err.println("Error connecting to or working with the database.");
            e.printStackTrace();
        }

        try (Connection connection = DriverManager.getConnection(BOOKS_URL, USER, PASSWORD)) {

            String createVisitorsTable = "CREATE TABLE IF NOT EXISTS visitor ("
                    + "id SERIAL PRIMARY KEY, "
                    + "name TEXT, "
                    + "surname TEXT, "
                    + "phone TEXT, "
                    + "subscribed BOOLEAN)";

            String createBooksTable = "CREATE TABLE IF NOT EXISTS book ("
                    + "name TEXT, "
                    + "author TEXT, "
                    + "publishing_year INTEGER, "
                    + "isbn INTEGER UNIQUE, "
                    + "publisher TEXT)";

            // Создание таблицы FavoriteBooks для связи "один ко многим" между Visitors и Books
            String createFavoriteBooksTable = "CREATE TABLE IF NOT EXISTS favorite_book ("
                    + "visitor_id INTEGER REFERENCES visitor(id) ON DELETE CASCADE, "
                    + "book_id INTEGER REFERENCES book(isbn) ON DELETE CASCADE, "
                    + "PRIMARY KEY (visitor_id, book_id))";

            try (Statement statement = connection.createStatement()) {
                statement.execute(createVisitorsTable);
                statement.execute(createBooksTable);
                statement.execute(createFavoriteBooksTable);
                statement.executeUpdate(
                    "INSERT INTO visitor (name, surname, phone, subscribed) " +
                        "SELECT * FROM (VALUES " +
                            "('John', 'Doe', '123-456-7890', TRUE), " +
                            "('Jane', 'Smith', '987-654-3210', FALSE), " +
                            "('Michael', 'Johnson', '555-123-4567', TRUE), " +
                            "('Emily', 'Brown', '555-987-6543', TRUE), " +
                            "('David', 'Wilson', '555-111-2222', FALSE), " +
                            "('Olivia', 'Miller', '555-333-4444', TRUE), " +
                            "('William', 'Davis', '555-555-5555', TRUE), " +
                            "('Sophia', 'Garcia', '555-666-7777', FALSE), " +
                            "('James', 'Martinez', '555-888-9999', TRUE), " +
                            "('Isabella', 'Anderson', '555-000-1111', FALSE), " +
                            "('Ethan', 'Taylor', '555-222-3333', TRUE), " +
                            "('Ava', 'Thomas', '555-444-5555', TRUE), " +
                            "('Jack', 'Hill', '555-666-7777', TRUE), " +
                            "('Lily', 'Jones', '555-777-8888', TRUE), " +
                            "('Oliver', 'Baker', '555-888-9999', TRUE) " +
                        ") AS new_data " +
                        "WHERE NOT EXISTS (SELECT 1 FROM visitor " +
                        "WHERE visitor.name = new_data.column1 AND visitor.surname = new_data.column2);"
                );
                statement.executeUpdate(
                        "INSERT INTO book (name, author, publishing_year, isbn, publisher) " +
                                "SELECT * FROM (VALUES " +
                                "('The Lord of the Rings', 'J.R.R. Tolkien', 1954, 0395026468, 'Allen & Unwin'), " +
                                "('To Kill a Mockingbird', 'Harper Lee', 1960, 0446310759, 'HarperPerennial'), " +
                                "('The Fault in Our Stars', 'John Green', 2012, 0316038746, 'Dutton'), " +
                                "('The Book Thief', 'Markus Zusak', 2005, 0375831004, 'Knopf'), " +
                                "('The Shack', 'William P. Young', 2007, 0316067860, 'Windblown Media'), " +
                                "('The Kite Runner', 'Khaled Hosseini', 2003, 0385506982, 'Riverhead Books'), " +
                                "('The Nightingale', 'Kristin Hannah', 2015, 0385387035, 'St. Martin''s Press'), " +
                                "('Pride and Prejudice', 'Jane Austen', 1813, 0525472125, 'Penguin Classics'), " +
                                "('1984', 'George Orwell', 1949, 0451534852, 'Signet Classics'), " +
                                "('The Hitchhiker''s Guide to the Galaxy', 'Douglas Adams', 1979, 0345390829, 'Del Rey'), " +
                                "('The Great Gatsby', 'F. Scott Fitzgerald', 1925, 0743273567, 'Scribner'), " +
                                "('Harry Potter and the Philosopher''s Stone', 'J.K. Rowling', 1997, 0747532735, 'Bloomsbury'), " +
                                "('Brave New World', 'Aldous Huxley', 1932, 0060860495, 'Harper Perennial'), " +
                                "('The Hunger Games', 'Suzanne Collins', 2008, 0439023483, 'Scholastic'), " +
                                "('The Catcher in the Rye', 'J.D. Salinger', 1951, 0316769487, 'Little, Brown'), " +
                                "('The Alchemist', 'Paulo Coelho', 1988, 0060920508, 'HarperOne'), " +
                                "('The Da Vinci Code', 'Dan Brown', 2003, 0385504209, 'Doubleday'), " +
                                "('Gone Girl', 'Gillian Flynn', 2012, 0316205775, 'Crown'), " +
                                "('The Girl on the Train', 'Paula Hawkins', 2015, 0007555445, 'Riverhead Books'), " +
                                "('The Martian', 'Andy Weir', 2011, 0553418438, 'Crown') " +
                                ") AS new_data " +
                                "WHERE NOT EXISTS (SELECT 1 FROM book " +
                                "WHERE book.isbn = new_data.column4);"
                );

                statement.executeUpdate(
                        "INSERT INTO favorite_book (visitor_id, book_id) VALUES " +
                                "(1, 0395026468), " +
                                "(1, 0446310759), " +
                                "(1, 0316038746), " +
                                "(1, 0375831004), " +
                                "(1, 0316067860), " +
                                "(1, 0385506982), " +
                                "(1, 0385387035), " +
                                "(2, 0525472125), " +
                                "(2, 0451534852), " +
                                "(3, 0345390829), " +
                                "(3, 0743273567), " +
                                "(4, 0747532735), " +
                                "(4, 0395026468), " +
                                "(5, 0451534852), " +
                                "(5, 0060860495), " +
                                "(6, 0525472125), " +
                                "(6, 0439023483), " +
                                "(7, 0316769487), " +
                                "(7, 0743273567), " +
                                "(8, 0446310759), " +
                                "(8, 0439023483), " +
                                "(9, 0345390829), " +
                                "(9, 0395026468), " +
                                "(10, 0525472125), " +
                                "(10, 0743273567), " +
                                "(10, 0060920508), " +
                                "(10, 0385504209), " +
                                "(11, 0316769487), " +
                                "(11, 0439023483), " +
                                "(11, 0316205775), " +
                                "(12, 0446310759), " +
                                "(12, 0439023483), " +
                                "(13, 0345390829), " +
                                "(13, 0743273567), " +
                                "(14, 0525472125), " +
                                "(14, 0439023483), " +
                                "(14, 0316205775), " +
                                "(14, 0007555445), " +
                                "(14, 0553418438), " +
                                "(15, 0316769487), " +
                                "(15, 0743273567), " +
                                "(15, 0385387035);"
                );

            } catch (SQLException e) {
                System.out.println("Error creating tables: " + e.getMessage());
            }

            printBooksByYear(connection);
            printBooksAfter2000(connection);


            addMyData(connection);

            dropTable(connection, "visitor");
            dropTable(connection, "book");
            dropTable(connection, "favorite_book");


        } catch (SQLException e) {
            System.err.println("Error connecting to or working with the database.");
            e.printStackTrace();
        }
    }

    private static void getAllMusic(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT id, name " +
                         "FROM study.music"
             )) {

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                System.out.println(id + "  " + name);
            }
        }
    }

    private static void getFilteredMusic(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT id, name " +
                         "FROM study.music " +
                         "WHERE LOWER(name) NOT LIKE '%m%' AND LOWER(name) NOT LIKE '%t%'; "
             )) {


            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                System.out.println(id + ": " + name);
            }
        }
    }

    private static void addMusic(Connection connection) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO study.music (id, name) " +
                        "VALUES (?, ?)"
        )) {
            preparedStatement.setInt(1, 52);
            preparedStatement.setString(2, "Only you");
            preparedStatement.executeUpdate();
            System.out.println("\nComposition added: " + 52 + "  " + "Only you");
        }
    }
    public static void dropStudyTable(Connection connection, String table_name) {
        try (PreparedStatement pstatement = connection.prepareStatement(
                "DROP TABLE IF EXISTS study." + table_name + " CASCADE"
        )) {
            pstatement.executeUpdate();
            System.out.println("\nTable " + table_name + " successfully dropped.");
        } catch (SQLException e) {
            System.out.println("Error dropping table: " + e.getMessage());
        }
    }
    private static void printBooksByYear(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT name, author, publishing_year, isbn, publisher " +
                             "FROM book " +
                             "ORDER BY publishing_year"
             )) {

            System.out.println("\nSorted books");
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String author = resultSet.getString("author");
                int year = resultSet.getInt("publishing_year");
                int isbn = resultSet.getInt("isbn");
                String publisher = resultSet.getString("publisher");

                System.out.println("Name: " + name + ", Author: " + author +
                        ", Year: " + year + ", ISBN: " + isbn + ", Publisher: " + publisher);
            }
        }
    }

    private static void printBooksAfter2000(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT name, author, publishing_year, isbn, publisher " +
                             "FROM book " +
                             "WHERE publishing_year > 2000"
             )) {

            System.out.println("\nFiltred books");
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String author = resultSet.getString("author");
                int year = resultSet.getInt("publishing_year");
                int isbn = resultSet.getInt("isbn");
                String publisher = resultSet.getString("publisher");

                System.out.println("Name: " + name + ", Author: " + author +
                        ", Year: " + year + ", ISBN: " + isbn + ", Publisher: " + publisher);
            }
        }
    }

    private static void addMyData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {

            // Добавляем данные в таблицу visitor
            statement.executeUpdate(
                    "INSERT INTO visitor (name, surname, phone, subscribed) " +
                            "VALUES ('Arseniy', 'Rumuantsev', '896-172-719', true);"
            );

            // Добавляем данные в таблицу book
            statement.executeUpdate(
                    "INSERT INTO book (name, author, publishing_year, isbn, publisher) " +
                            "VALUES ('Metro 2033', 'Dmitry Glukhovsky', 2002, 0395026461, 'Eksmo')," +
                            "('A dream to defeat', 'Alexey Kalugin', 2008, 0395026463, 'Eksmo');"
            );

            // Добавляем данные в таблицу favorite_book
            statement.executeUpdate(
                    "INSERT INTO favorite_book (visitor_id, book_id) " +
                            "VALUES (16, 0395026461)," +
                            "(16, 0395026463);"
            );

            String query = """
            SELECT v.id, v.name, v.surname, v.phone, v.subscribed,\s
                   b.name AS book_name, b.author, b.publishing_year, b.isbn, b.publisher
            FROM visitor v
            LEFT JOIN favorite_book fb ON v.id = fb.visitor_id
            LEFT JOIN book b ON fb.book_id = b.isbn
            WHERE v.name = 'Arseniy'\s
           \s""";

            try (ResultSet resultSet = statement.executeQuery(query)) {
                System.out.println("\nAdded data:");
                boolean visitorFound = false;
                while (resultSet.next()) {
                    if (!visitorFound) {
                        int id = resultSet.getInt("id");
                        String name = resultSet.getString("name");
                        String surname = resultSet.getString("surname");
                        String phone = resultSet.getString("phone");
                        boolean subscribed = resultSet.getBoolean("subscribed");

                        System.out.printf("Visitor ID: %d, Name: %s, Surname: %s, Phone: %s, Subscribed: %b\n",
                                id, name, surname, phone, subscribed);
                        System.out.println("Favorite Books:");
                        visitorFound = true;
                    }

                    String bookName = resultSet.getString("book_name");
                    if (bookName != null) {
                        String author = resultSet.getString("author");
                        int publishingYear = resultSet.getInt("publishing_year");
                        int isbn = resultSet.getInt("isbn");
                        String publisher = resultSet.getString("publisher");

                        System.out.printf("- Name: %s, Author: %s, Year: %d, ISBN: %d, Publisher: %s\n",
                                bookName, author, publishingYear, isbn, publisher);
                    }
                }
            }
        }
    }

    public static void dropTable(Connection connection, String table_name) {
        try (PreparedStatement pstatement = connection.prepareStatement(
                "DROP TABLE IF EXISTS " + table_name + " CASCADE"
        )) {
            pstatement.executeUpdate();
            System.out.println("\nTable " + table_name + " successfully dropped.");
        } catch (SQLException e) {
            System.out.println("Error dropping table: " + e.getMessage());
        }
    }
}

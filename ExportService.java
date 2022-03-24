package com.example.services;

import com.example.models.*;
import com.example.models.enums.Entity;
import com.example.services.metadata.MetadataService;
import com.example.services.metadata.MetadataServiceImpl;
import com.example.util.Constants;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExportService {

    public static void createSqlDump(String databaseName) {
        File[] databaseFiles = new File(Constants.DB_DIRECTORY).listFiles();
        boolean isGenerated = false;
        for (int i = 0; i < databaseFiles.length; i++) {
            String fileName = databaseFiles[i].toString();
            if ((databaseName.concat(Constants.TXT_FILE_EXTENSION).equals(fileName.split("_")[1].concat("_").concat(fileName.split("_")[2])))){
                BufferedReader bufferReader = null;
                try {
                    String dbFilePath = Constants.DB_DIRECTORY.concat("MD_").concat(databaseName).concat(Constants.TXT_FILE_EXTENSION);
                    writeDbInFile(databaseName);
                    bufferReader = new BufferedReader(new FileReader(dbFilePath));
                    parse(bufferReader, databaseName);
                    isGenerated = true;
                    break;
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        bufferReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        if (isGenerated){
            System.out.println("Dump file created successfully");
        }else {
            System.out.println("Something is not right with export export database.. ");
        }

    }

    public static void writeDbInFile(String DB_NAME) {
        String DB_FORMAT = "`".concat(DB_NAME).concat("`");
        try {
            PrintWriter writer = new PrintWriter(DB_NAME.concat(Constants.SQL_FILE_EXTENSION), "UTF-8");
            writer.write("CREATE DATABASE IF NOT EXISTS " + DB_FORMAT + "\n");
            writer.write("USE " + DB_FORMAT + ";\n");
            writer.close();
        }catch (UnsupportedEncodingException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void parse(BufferedReader bufferedReader, String database) {
        String line;
        try {
            File file = new File(database.concat(Constants.SQL_FILE_EXTENSION));
            FileWriter fr = new FileWriter(file, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fr);
            Set<String> tableNames = new HashSet<String>();
            while ((line = bufferedReader.readLine()) != null) {
                switch (line.split("\\|")[0]) {
                    case "TB":
                        String tableName = line.split("\\|")[1];
                        bufferedWriter.write("\n");
                        bufferedWriter.write("\n");
                        bufferedWriter.write("--\n");
                        bufferedWriter.write("-- Table structure for table " + "`" + tableName + "`" + "\n");
                        bufferedWriter.write("--\n");
                        bufferedWriter.write("\n");
                        bufferedWriter.write("\n");
                        bufferedWriter.write("DROP TABLE IF EXISTS " + "`" + tableName + "`" + "\n");
                        bufferedWriter.write("CREATE TABLE " + "`" + tableName + "`" + " ("+"\n");
                        tableNames.add(tableName);
                        break;
                    case "CL":
                        bufferedWriter.write("`" + line.split("\\|")[1] + "` " + line.split("\\|")[2] + ",\n");
                        break;
                    case "PK":
                        bufferedWriter.write("PRIMARY KEY " + "(`" + line.split("\\|")[1] + "`)\n");
                        bufferedWriter.write(");\n");
                        break;
                }
            }
            bufferedWriter.close();

            for(String table: tableNames) {
                insert(database, table);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void insert(String DB_NAME, String table){
        String tableDataLocation = Constants.TB_DIRECTORY.concat(DB_NAME).concat("/").concat("TB_").concat(table).concat(Constants.TXT_FILE_EXTENSION);
        BufferedReader bufferReader = null;
        BufferedWriter bufferedWriter = null;
        try {
            String line;
            bufferReader = new BufferedReader(new FileReader(tableDataLocation));
            File file = new File(DB_NAME.concat(Constants.SQL_FILE_EXTENSION));
            FileWriter fr = new FileWriter(file, true);
            bufferedWriter = new BufferedWriter(fr);
            bufferedWriter.write("\n");
            bufferedWriter.write("\n");
            bufferedWriter.write("--\n");
            bufferedWriter.write("-- Dumping data for table " + "`" + table + "`\n");
            bufferedWriter.write("--\n");
            bufferedWriter.write("\n");
            bufferedWriter.write("LOCK TABLES " + "`" + table + "`" + " WRITE;\n");
            bufferedWriter.write("INSERT INTO " + "`" + table + "`" + " VALUES ");
            while ((line = bufferReader.readLine()) != null) {
                String[] splitStrings = line.split("\\|");
                bufferedWriter.write("(");
                for (int i = 1; i < splitStrings.length; i++) {
                    if (i != splitStrings.length - 1) {
                        bufferedWriter.write("'" + splitStrings[i] + "'" + ",");
                    }else {
                        bufferedWriter.write("'" + splitStrings[i] + "'");
                    }
                }
                if (bufferReader.read() != -1) {
                    bufferedWriter.write("),");
                }else {
                    bufferedWriter.write(");");
                }
            }
            bufferedWriter.write("\n");
            bufferedWriter.write("UNLOCK TABLES;");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try{
            bufferedWriter.close();
            bufferReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void linePrinter(FileWriter fileWriter) throws IOException {
        fileWriter.write(String.format("\n-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"));
    }
    public static void valuePrinter(FileWriter fileWriter, String value) throws IOException {
        fileWriter.write(String.format("%20s |", value));

    }
    public static void createErd(String databaseName) {
        try {
            File file = new File("storage/erd/" + databaseName + "_ERD.txt");
            file.createNewFile();
            FileWriter fileWriter = new FileWriter(file, true);
            ;

            MetadataService metadataService = new MetadataServiceImpl();
            Metadata metadata = metadataService.read(Entity.DATABASE, databaseName);
            for (Table table : metadata.getAllTablesFromDatabase()) {
                try {
                    linePrinter(fileWriter);
                    fileWriter.write(String.format("\n%80s", "Table Name:" + table.getName()));
                    linePrinter(fileWriter);

                    fileWriter.write(String.format("\n%-25s |", "Column Names"));
                    for (Column column : table.getColumns()) {
                        valuePrinter(fileWriter, column.getName());
                    }
                    linePrinter(fileWriter);

                    fileWriter.write(String.format("\n%-25s |", "Column Type"));
                    for (Column column : table.getColumns()) {
                        valuePrinter(fileWriter, column.getDataType());
                    }
                    linePrinter(fileWriter);

                    fileWriter.write(String.format("\n%-25s |", "Primary Key"));
                    for (Column column : table.getColumns()) {
                        try {
                            if (table.getPrimaryKey().getName().equals(column.getName())) {
                                valuePrinter(fileWriter, "PK");
                            } else {
                                valuePrinter(fileWriter, "");
                            }
                        } catch (Exception e) {
                            valuePrinter(fileWriter, "");
                        }
                    }

                    if (table.getForeignKeys() != null) {
                        System.out.println(table.getForeignKeys().size());
                        linePrinter(fileWriter);

                        fileWriter.write(String.format("\n%-25s |", "Foreign Key"));
                        for (Column column : table.getColumns()) {
                            try {
                                for (ForeignKey fKey : table.getForeignKeys()) {
                                    if (column.getName().equals(fKey.getForeignKeyColumn())) {
                                        valuePrinter(fileWriter, "FK");
                                    } else {
                                        valuePrinter(fileWriter, "");
                                    }
                                }

                            } catch (Exception e) {
                                valuePrinter(fileWriter, "");
                            }
                        }
                        linePrinter(fileWriter);

                        fileWriter.write(String.format("\n%-25s |", "Reference Table Name"));
                        for (Column column : table.getColumns()) {
                            try {
                                for (ForeignKey fKey : table.getForeignKeys()) {
                                    if (column.getName().equals(fKey.getForeignKeyColumn())) {
                                        valuePrinter(fileWriter, fKey.getReferenceTableName());

                                    } else {
                                        valuePrinter(fileWriter, "");
                                    }
                                }

                            } catch (Exception e) {
                                valuePrinter(fileWriter, "");

                            }
                        }
                        linePrinter(fileWriter);

                        fileWriter.write(String.format("\n%-25s |", "Reference Column Name"));
                        for (Column col : table.getColumns()) {
                            try {
                                for (ForeignKey fKey : table.getForeignKeys()) {
                                    if (col.getName().equals(fKey.getForeignKeyColumn())) {
                                        valuePrinter(fileWriter, fKey.getReferenceColumnName());
                                    } else {
                                        valuePrinter(fileWriter, "");

                                    }
                                }

                            } catch (Exception e) {
                                valuePrinter(fileWriter, "");
                            }
                        }
                    }
                    linePrinter(fileWriter);
                    fileWriter.write(String.format("\n\n"));
                } catch (Exception e) {
                    valuePrinter(fileWriter, "");
                }

            }
            fileWriter.close();
            System.out.println("ERD is created for database " + databaseName + " with the name of " + databaseName + "_ERD.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

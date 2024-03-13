#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <zlib.h>
#include <thread>
#include <atomic>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <unordered_map>
#include <chrono>
#include <algorithm>

#include <archive.h>
#include <archive_entry.h>
#include <cstring>
#include <regex>
#include <unordered_set>


// MySQL C API headers
#include <mysql/mysql.h>


using namespace std;
namespace fs = std::filesystem;

const int BUFFER_SIZE = 1024 * 1024; // 1 MB buffer size
const int MAX_THREADS = 4;             // Maximum number of threads

// Data structure to store table creation statements
struct TableDefinition {
    std::string tableName;
    std::string createStatement;
};


atomic<bool> searchComplete(false); // Atomic flag to indicate search completion

// this replaces the site name spaces with underscores
std::string replaceSpacesWithUnderscores(std::string& str) {
    std::string modifiedStr = str;
    size_t spaceCount = 0;
    for (char c : modifiedStr) {
        if (c == ' ') {
            spaceCount++;
        }
    }

    if (spaceCount > 0) {
        size_t newLength = modifiedStr.length() + spaceCount;
        modifiedStr.resize(newLength);

        for (size_t i = 0; i < modifiedStr.length(); ++i) {
            if (modifiedStr[i] == ' ') {
                modifiedStr[i] = '_';
            }
        }
    }

    return modifiedStr;
}

void removeSubstring(std::string& word, const std::string substring) {
    // Find the position of the substring in the main string
    size_t pos = word.find(substring);
    if (pos != std::string::npos) {
        // Erase the substring from the main string
        word.erase(pos, substring.length());
    }
}

std::vector<std::string> splitString(const std::string& input, char delimiter) {
    std::istringstream ss(input);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }

    // If there are at least two tokens
    if (tokens.size() >= 2) {
        // Remove single quotes from the second token
        size_t pos = tokens[1].find('\'');
        while (pos != std::string::npos) {
            tokens[1].erase(pos, 1);
            pos = tokens[1].find('\'', pos);
        }
    }

    return tokens;
}

// Function to load environment variables from a file
void loadEnvironmentFromFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open environment file: " << filename << std::endl;
        return;
    }

    std::unordered_map<std::string, std::string> envVariables;
    std::string line;
    while (std::getline(file, line)) {
        size_t pos = line.find('=');
        if (pos != std::string::npos) {
            std::string key = line.substr(0, pos);
            std::string value = line.substr(pos + 1);
            envVariables[key] = value;
        }
    }

    file.close();

    // Set environment variables
    for (const auto& [key, value] : envVariables) {
        setenv(key.c_str(), value.c_str(), 1); // Overwrite existing variable if it exists
    }
}

// Function to check if a table exists
bool tableExists(MYSQL* conn, const std::string& tableName) {
    std::string query = "SHOW TABLES LIKE '" + tableName + "'";
    if (mysql_query(conn, query.c_str()) != 0) {
        std::cerr << "Error checking table existence: " << mysql_error(conn) << std::endl;
        return false;
    }

    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) {
        std::cerr << "Error storing result: " << mysql_error(conn) << std::endl;
        return false;
    }

    bool exists = mysql_num_rows(result) > 0;

    mysql_free_result(result);

    return exists;
}

// Function to check if a column exists in a table
bool columnExists(MYSQL* conn, const std::string& tableName, const std::string& columnName) {
    std::string query = "SHOW COLUMNS FROM " + tableName + " LIKE '" + columnName + "'";
    if (mysql_query(conn, query.c_str()) != 0) {
        std::cerr << "Error checking column existence: " << mysql_error(conn) << std::endl;
        return false;
    }

    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) {
        std::cerr << "Error storing result: " << mysql_error(conn) << std::endl;
        return false;
    }

    bool exists = mysql_num_rows(result) > 0;

    mysql_free_result(result);

    return exists;
}

// Helper function to extract the column name from the CREATE TABLE statement
std::string extractColumnName(const std::string& createStatement) {
    // Assuming the column name is enclosed within backticks (`column_name`)
    size_t startPos = createStatement.find("`") + 1; // Find the start position of the column name
    size_t endPos = createStatement.find("`", startPos); // Find the end position of the column name
    if (startPos != std::string::npos && endPos != std::string::npos) {
        return createStatement.substr(startPos, endPos - startPos);
    }
    return ""; // Return an empty string if the column name couldn't be extracted
}

// Function to check if a table references another table
bool tableReferences(const TableDefinition& table, const TableDefinition& referencedTable) {
    // Extract the table names from the CREATE TABLE statements
    std::string tableName = table.tableName;
    std::string referencedTableName = referencedTable.tableName;

    // Extract foreign key constraints from the CREATE TABLE statement
    std::string createStatement = table.createStatement;

    // Check if the create statement contains a foreign key constraint referencing the referenced table
    std::string searchString = "FOREIGN KEY";
    size_t pos = createStatement.find(searchString);
    while (pos != std::string::npos) {
        // Look for the referenced table name within the foreign key constraint
        size_t referencedTablePos = createStatement.find(referencedTableName, pos);
        if (referencedTablePos != std::string::npos) {
            // Found a foreign key constraint referencing the referenced table
            return true;
        }
        // Continue searching for more foreign key constraints
        pos = createStatement.find(searchString, pos + 1);
    }

    // No foreign key constraints found referencing the referenced table
    return false;
}



// Function to create a table in the database
void createTable(MYSQL* conn, const std::string& createStatement) {
    // Execute the CREATE TABLE statement
    if (mysql_query(conn, createStatement.c_str()) != 0) {
        std::cerr << "Failed to create table: " << mysql_error(conn) << std::endl;
        return;
    }
    std::cout << "Table created successfully." << std::endl;
}


std::string getSubstringBetween(const std::string& text, const std::string& start, const std::string& end) {
    size_t startPos = text.find(start);
    if (startPos == std::string::npos) {
        // Start word not found
        return "";
    }

    startPos += start.length(); // Move startPos to end of start word

    size_t endPos = text.find(end, startPos);
    if (endPos == std::string::npos) {
        // End word not found after start word
        return "";
    }

    std::string result = text.substr(startPos, endPos - startPos);

    return result.substr(6);
}

bool closerToWord(const std::string& chaword, const std::string& reformat) {
    bool result = false;
    if(reformat.length() > 1 && reformat.length() < chaword.length()){
        bool closer = false;
        for(int i=0; i< reformat.length(); i++){

            if(reformat[i] == chaword[i] ){
                closer = true;

            }
            else{
                closer = false;
            }
        }
        if(closer == true){
            result = true;
            
        }
        else{
            result = false;
        }
    }
    

    return result;
}




// Function to restore MySQL dump from a gzipped file
bool restoreMySQLDumpC(const char* gzippedDumpFile, const char* mysqlHost, const char* mysqlUser, const char* mysqlPassword, const char* mysqlDatabase) {
    // Open MySQL connection
    unsigned int port = 3900;
    MYSQL *mysql = mysql_init(NULL);
    if (mysql == NULL) {
        std::cerr << "Error: Unable to initialize MySQL connection." << std::endl;
        return false;
    }

    if (!mysql_real_connect(mysql, mysqlHost, mysqlUser, mysqlPassword, NULL, port, NULL, 0)) {
        std::cerr << "Error: Unable to connect to MySQL database." << std::endl;
        mysql_close(mysql);
        return false;
    }

    // Select the database
    if (mysql_select_db(mysql, mysqlDatabase) != 0) {
        std::cerr << "Error: Unable to select MySQL database." << std::endl;
        mysql_close(mysql);
        return false;
    }

    // Set of version numbers to skip
    std::unordered_set<std::string> versionNumbers = {"40101", "50503","40103","40000","50001","40014","40111","50003"}; // Add more as needed

    // Open the gzipped file
    struct archive *a = archive_read_new();
    archive_read_support_filter_gzip(a);
    archive_read_support_format_raw(a);
    if (archive_read_open_filename(a, gzippedDumpFile, 10240) != ARCHIVE_OK) {
        std::cerr << "Error: Unable to open gzipped dump file." << std::endl;
        mysql_close(mysql);
        return false;
    }

    // Read SQL commands and execute them
    struct archive_entry *entry;
    std::string currentCommand;
    size_t currentCommandSize = 0;
    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        char buffer[4096];
        size_t size;

        while ((size = archive_read_data(a, buffer, sizeof(buffer))) > 0) {
            // Execute SQL commands
            std::string sql(buffer, size);
            size_t pos = 0;
            while ((pos = sql.find("/*!", pos)) != std::string::npos) {
                size_t end = sql.find("*/", pos + 3);
                if (end != std::string::npos) {
                    std::string command = sql.substr(pos + 3, end - pos - 3);
                    // Execute SET commands except version-specific ones
                    if (command.find("SET") != std::string::npos && command.find("/*!") == std::string::npos) {
                        // Check if any version number in the set is present in the command
                        bool skipCommand = false;
                        for (const auto& version : versionNumbers) {
                            if (command.find(version) != std::string::npos) {
                                skipCommand = true;
                                break;
                            }
                        }
                        if (skipCommand) {
                            std::cout << "Skipping version-specific command: " << command << std::endl;
                            skipCommand = false;
                        } else {
                            std::cout << "Executing SQL command: " << command << std::endl;
                            if (mysql_real_query(mysql, command.c_str(), command.length()) != 0) {
                                std::cerr << "Error: Failed to execute SQL command: " << mysql_error(mysql) << std::endl;
                                std::cerr << "Command: " << command << std::endl;
                                mysql_close(mysql);
                                archive_read_close(a);
                                return false;
                            }
                        }
                    }
                    pos = end + 2;
                } else {
                    //break; // Malformed /*! ... */ block, break and ignore it
                    // Concatenate the buffer content to the current command
                    currentCommand += std::string(buffer, size);
                    currentCommandSize += size;

                    // Check if the current command is complete
                    if (currentCommandSize > 0 && currentCommand[currentCommandSize - 1] == ';') {
                        std::cout << "query before it run: " << currentCommand.c_str() << std::endl;
                        // Execute the current command
                        if (mysql_real_query(mysql, currentCommand.c_str(), currentCommand.size()) != 0) {
                            std::cerr << "Error: Failed to execute SQL command: " << mysql_error(mysql) << std::endl;
                            mysql_close(mysql);
                            archive_read_close(a);
                            return false;
                        }

                        // Reset the current command
                        currentCommand.clear();
                        currentCommandSize = 0;
                    }
                }
            }
        }
    }

    // Clean up
    archive_read_close(a);
    mysql_close(mysql);

    return true;
}



bool restoreMySQLDump(const char* gzippedDumpFile, const char* mysqlHost, const char* mysqlUser, const char* mysqlPassword, const char* mysqlDatabase) {
    // Open MySQL connection
    unsigned int port = 3900; // Default MySQL port
    MYSQL *mysql = mysql_init(NULL);
    if (mysql == NULL) {
        std::cerr << "Error: Unable to initialize MySQL connection." << std::endl;
        return false;
    }

    if (!mysql_real_connect(mysql, mysqlHost, mysqlUser, mysqlPassword, NULL, port, NULL, 0)) {
        std::cerr << "Error: Unable to connect to MySQL database." << std::endl;
        mysql_close(mysql);
        return false;
    }

    // Select the database
    if (mysql_select_db(mysql, mysqlDatabase) != 0) {
        std::cerr << "Error: Unable to select MySQL database." << std::endl;
        mysql_close(mysql);
        return false;
    }

    // Open the gzipped file
    struct archive *a = archive_read_new();
    archive_read_support_filter_gzip(a);
    archive_read_support_format_raw(a);
    if (archive_read_open_filename(a, gzippedDumpFile, 10240) != ARCHIVE_OK) {
        std::cerr << "Error: Unable to open gzipped dump file." << std::endl;
        mysql_close(mysql);
        return false;
    }

    // Begin transaction
    if (mysql_query(mysql, "START TRANSACTION") != 0) {
        std::cerr << "Error: Failed to start transaction: " << mysql_error(mysql) << std::endl;
        mysql_close(mysql);
        archive_read_close(a);
        return false;
    }

    // Read SQL commands and execute them
    struct archive_entry *entry;
    std::ostringstream batchQueryStream;
    size_t batchSize = 0;
    // Set of version numbers to skip
    std::unordered_set<std::string> versionNumbers = {"40101", "50503","40103","40000","50001","40014","40111","50003"}; // Add more as needed

    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        char buffer[4096];
        size_t size;




        while ((size = archive_read_data(a, buffer, sizeof(buffer))) > 0) {
            // start here
            std::string sql(buffer, size);
            size_t pos = 0;
            while ((pos = sql.find("/*!", pos)) != std::string::npos) {
                size_t end = sql.find("*/", pos + 3);
                if (end != std::string::npos) {
                    std::string command = sql.substr(pos + 3, end - pos - 3);
                    // Execute SET commands except version-specific ones
                    if (command.find("SET") != std::string::npos && command.find("/*!") == std::string::npos) {
                        // Check if any version number in the set is present in the command
                        bool skipCommand = false;
                        for (const auto& version : versionNumbers) {
                            if (command.find(version) != std::string::npos) {
                                skipCommand = true;
                                break;
                            }
                        }
                        if (skipCommand) {
                            std::cout << "Skipping version-specific command: " << command << std::endl;
                            skipCommand = false;
                        } else {
                            std::cout << "Executing SQL command: " << command << std::endl;
                            if (mysql_real_query(mysql, command.c_str(), command.length()) != 0) {
                                std::cerr << "Error: Failed to execute SQL command: " << mysql_error(mysql) << std::endl;
                                std::cerr << "Command: " << command << std::endl;
                                mysql_close(mysql);
                                archive_read_close(a);
                                return false;
                            }
                        }
                    }
                    pos = end + 2;
                }
                else{
                    batchQueryStream.write(buffer, size);
                } 
            }

            // end here
            
        }

        // Execute batched commands
        std::string batchQuery = batchQueryStream.str();
        if (!batchQuery.empty()) {
            if (mysql_real_query(mysql, batchQuery.c_str(), batchQuery.length()) != 0) {
                std::cerr << "Error: Failed to execute SQL command: " << mysql_error(mysql) << std::endl;
                mysql_close(mysql);
                archive_read_close(a);
                return false;
            }
            batchQueryStream.str(""); // Clear the stringstream
        }
    }

    // Commit transaction
    if (mysql_query(mysql, "COMMIT") != 0) {
        std::cerr << "Error: Failed to commit transaction: " << mysql_error(mysql) << std::endl;
        mysql_close(mysql);
        archive_read_close(a);
        return false;
    }

    // Clean up
    archive_read_close(a);
    mysql_close(mysql);

    return true;
}


// Function to restore MySQL dump file
void restoreMySQLDumpB(const std::string& filename, const std::string& db_host, const std::string& db_user, const std::string& db_password, const std::string& db_name) {
    MYSQL *conn;
    conn = mysql_init(NULL);

    std::unordered_map<std::string, bool> createdTables;

    std::string db_hostb = "0.0.0.0";
    unsigned int port = 3900;
    if (!mysql_real_connect(conn, db_hostb.c_str(), db_user.c_str(), db_password.c_str(), NULL, port, NULL, 0)) {
        std::cerr << "Failed to connect to MySQL server: " << mysql_error(conn) << std::endl;
        return;
    }

    // Check if the database exists
    if (mysql_select_db(conn, db_name.c_str()) != 0) {
        // Database does not exist, create it
        if (mysql_query(conn, ("CREATE DATABASE " + db_name).c_str()) != 0) {
            std::cerr << "Failed to create database: " << mysql_error(conn) << std::endl;
            mysql_close(conn);
            return;
        } else {
            std::cout << "Database created: " << db_name << std::endl;
        }
    }

    // Close the connection
    mysql_close(conn);

    // Reconnect to the MySQL server and connect to the database
    conn = mysql_init(NULL);
    if (!mysql_real_connect(conn, db_host.c_str(), db_user.c_str(), db_password.c_str(), db_name.c_str(), port, NULL, 0)) {
        std::cerr << "Failed to connect to MySQL server: " << mysql_error(conn) << std::endl;
        return;
    }

    // Open the compressed SQL dump file
    gzFile file = gzopen(filename.c_str(), "rb");
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        mysql_close(conn);
        return;
    }

    std::string line;
    std::string query;
    std::string drop_query;
    std::string set_query;
    std::string delimeter_query;
    std::string asterik_query;
    std::string insert_query;
    char buffer[4096]; // Buffer to store decompressed data (4KB)
    int bytesRead;
    bool semicolonFound = false;
    bool inComment = false;
    std::vector<TableDefinition> tables;
    bool insideCreateTable = false;
    bool insideDropTable = false;
    bool insideSetTable = false;
    bool insideConstraintFound = false;
    bool insideDelimeterTable = false;
    bool insideInsertTable = false;

    // Read from the compressed file
    while ((bytesRead = gzread(file, buffer, sizeof(buffer))) > 0) {
        std::stringstream ss(std::string(buffer, buffer + bytesRead));
        while (std::getline(ss, line)) {
            // Skip empty lines and comments
            if (line.empty())
                continue;

            // Handle multiline comments
            if (!inComment && line.find("/*") != std::string::npos) {
                inComment = true;
            }
            else if (line.find("CREATE") != std::string::npos && line.find("CREATE") == 0 && insideDelimeterTable == false) {
                insideCreateTable = true;
            }
            else if (line.find("INSERT") != std::string::npos && line.find("INSERT") == 0 && insideDelimeterTable == false) {
                insideInsertTable = true;
            }
            else if (line.find("DROP") != std::string::npos && line.find("DROP") == 0 && insideDelimeterTable == false ) {
                insideDropTable = true;
            }
            else if (line.find("SET ") != std::string::npos && line.find("SET ") == 0 && insideDelimeterTable == false ) {
                insideSetTable = true;
            }
            else if (line.find("DELIMITER ;;") != std::string::npos && line.find("DELIMITER ;;") == 0) {
                insideDelimeterTable = true;
                //continue;
            }
            else if (line[0] == '#' || line.substr(0, 2) == "--") {
                continue; // Skip comment lines
            }
            else if(insideDelimeterTable == false){
                std::string reformat = line;
                // for INSERT
                std::string chaword1 = "INSERT";
                std::string chaword2 = "CREATE";
                std::string chaword3 = "DROP";
                std::string chaword4 = "SET";
                //std::string chaword2 = "CREATE";
                if(closerToWord(chaword1, reformat)){
                    insideInsertTable = true;

                }
                else if(closerToWord(chaword2, reformat)){
                    insideCreateTable = true;

                }
                else if(closerToWord(chaword3, reformat)){
                    insideDropTable = true;

                }
                else if(closerToWord(chaword4, reformat)){
                    insideSetTable = true;

                }
                
            }

//----------------------------------------------------------------------------------
            

            if (inComment) {
                asterik_query += line;
                //batchQueryStream.write(line, size); 
            } 
            else if (insideDropTable) {
                drop_query += line;
                //batchQueryStream.write(line, size); 
            }
            else if (insideInsertTable) {
                insert_query += line;
            }
            else if (insideSetTable) {
                set_query += line;
            }
            else if (insideDelimeterTable) {
                //std::cout<<"Before Assigned:::"<<line<<endl;
                if(line.find("DELIMITER ;") == std::string::npos){

                    std::istringstream iss(line);
                    std::string word;
                    std::string lastWord;
                    int count = 0;
                    while (iss >> word) {
                        lastWord = word; // Update the last word
                        count += 1;
                    }
                    
                    if(count == 1 && line.length()>1 ){
                        //delimeter_query += " "+line + " ";
                        delimeter_query += line + "\r";
                        std::cout<<"counted********"<<line<<endl;
                    }
                    else if (lastWord =="DETERMINISTIC" || lastWord =="BEGIN" || line.back() == ';' ) {
                        // Carriage return found
                        delimeter_query += line + "\r";
                        std::cout<<"at the end of the line********"<<line<<endl;
                    }
                    else{
                        delimeter_query += line +"\r";
                    } 

                    //std::cout<<"After Assigned::"<<line<<endl;

                }

            }
            else if (insideCreateTable) {
                query += line;
            }


//---------------------------------------------------------------------------------------  
            //std::cout<<"HEREWITH:: "<<line<<std::endl;          
            if (line.back() == ';' && insideDelimeterTable == false ) 
            {
                if(asterik_query.length()>1)
                {
                    // Check if the line contains the end of the comment
                    size_t endCommentPos = line.find("*/");
                    if (endCommentPos != std::string::npos) {
                        //line.erase(0, endCommentPos + 2); // Erase everything before the end of the comment
                        if(asterik_query.size() >= 3 && asterik_query[2] == ' '){
                            std::cout<<"COught In Action::"<<asterik_query<<endl;
                            asterik_query = "/*!" + asterik_query.substr(3);
                        }
                        std::string startWord = "/*!";
                        std::string endWord = "*/";

                        std::cout<<"see befor asterik_query run:"<<asterik_query<<endl;
                        std::string torun = getSubstringBetween(asterik_query, startWord, endWord);
                        std::cout<<"see befor asterik_query22 run:"<<torun<<endl;
                        if (mysql_query(conn, torun.c_str()) != 0) {
                            std::cerr << "Failed to execute asterik_query: " << mysql_error(conn) << std::endl;
                            mysql_close(conn);
                            gzclose(file);
                            return;
                        }
                        
                        //continue;

                    }
                    inComment = false;
                    asterik_query.clear(); 
                }
                else if(query.length() > 1)
                {
                    if(query.find("CREATE TABLE `concept_synonym` (")!= std::string::npos){
                       // std::cout<<"query::1 "<<query<<std::endl;
                        //return;
                    }
                    insideCreateTable = false;
                    semicolonFound = true;
                    std::cout<<"query::1 "<<query<<std::endl;
                    if (mysql_query(conn, query.c_str()) != 0) {
                        std::cerr << "Failed to execute query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }
                    query.clear();
                }
                else if(insert_query.length() > 1)
                {
                    insideInsertTable = false;
                    std::cout<<"insert_query"<<insert_query<<std::endl;
                    if (mysql_query(conn, insert_query.c_str()) != 0) {
                        std::cerr << "Failed to execute insert_query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }
                    insert_query.clear();

                }
                else if(drop_query.length() > 1)
                {
                    insideDropTable = false;
                    std::cout<<"check before drop_query run:"<<drop_query<<std::endl;
                    if (mysql_query(conn, drop_query.c_str()) != 0) {
                        std::cerr << "Failed to execute drop_query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }
                    drop_query.clear();
                }
                else if(set_query.length() > 1)
                {
                    insideSetTable = false;
                    std::cout<<"check before set_query run:"<<set_query<<std::endl;
                    if (mysql_query(conn, set_query.c_str()) != 0) {
                        std::cerr << "Failed to execute set_query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }
                    set_query.clear();
                }
                else{

                    std::cout<<"Check Delimeter state: "<<insideDelimeterTable<<std::endl;
                    std::cout<<"whats in the line bare_query "<<line<<std::endl;
                    std::cout<<"check before lineback run:"<<line.back()<<std::endl;
                    std::cout<<"check before query run:"<<query<<std::endl;
                    if (mysql_query(conn, line.c_str()) != 0) {
                        std::cerr << "Failed to execute bare_query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }

                }
            }
            else if(insideDelimeterTable == true && line.find("DELIMITER ;") != std::string::npos && line.find("DELIMITER ;") == 0 && line.length() == 11 ){

                insideDelimeterTable = false;
                if(delimeter_query.length() > 1)
                {
                    //std::cout<<"check before delimeter_query run last line:"<<line<<std::endl;
                    std::cout<<"check before delimeter_query run:"<<delimeter_query<<std::endl;

                    if (mysql_query(conn, delimeter_query.c_str()) != 0) {
                        std::cerr << "Failed to execute delimiter_query: " << mysql_error(conn) << std::endl;
                        if (std::strcmp(mysql_error(conn), "FUNCTION age already exists") != 0) {
                            mysql_close(conn);
                            gzclose(file);
                            return;
                        }
                        
                    }
                    delimeter_query.clear();
                }
            }

        }
    }


    // Add logic to check and create tables with constraints
    for (const auto& table : tables) {
        // Get the column name dynamically from the CREATE TABLE statement
        std::string columnName = extractColumnName(table.createStatement);

        // Check if the referenced table and column exist
        if (!tableExists(conn, table.tableName) || !columnExists(conn, table.tableName, columnName)) {
            // Search for the definition of the referenced table in the remaining SQL file
            bool found = false;
            for (const auto& otherTable : tables) {
                if (tableReferences(otherTable, table)) {
                    // Create the referenced table first
                    if (!createdTables[otherTable.tableName]) {
                        createTable(conn,otherTable.createStatement);
                        createdTables[otherTable.tableName] = true;
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                std::cerr << "Error: Referenced table not found: " << table.tableName << std::endl;
                continue;
            }
        }

        // Create the table with constraints
        createTable(conn,table.createStatement);
        createdTables[table.tableName] = true;
    }

    tables.clear(); // Clear tables vector after processing

    gzclose(file);

    if (!semicolonFound && !query.empty()) {
        std::cerr << "Error: Incomplete query found in the SQL file." << std::endl;
        mysql_close(conn);
        return;
    }

    // Close MySQL connection
    mysql_close(conn);
}



void searchInBuffer(const string &searchString1, const string &searchString2, const char *buffer, size_t bytesRead,const std::string &gzFileName)
{
    std::string removeText = "name\\n";
    // Search for the search strings in the buffer
    const char *pos = buffer;
    bool found1 = false, found2 = false;
    std::string new_sitename;
    char startChar = ',';
    char endChar = '\'';
    char startCombinator = '\'';
    char endCombinator = ',';
    // Retrieve database connection parameters from environment variables
    std::string db_host = std::getenv("DB_HOST");
    std::string db_user = std::getenv("DB_USER");
    std::string db_password = std::getenv("DB_PASSWORD");
    std::string db_port = std::getenv("DB_PORT");

    while ((pos = strstr(pos, searchString1.c_str())) != NULL)
    {
        found1 = true;
        std::string chaline = string(pos, min(static_cast<size_t>(60), bytesRead - (pos - buffer)));
        char delimiter = ',';
        std::vector<std::string> tokens = splitString(chaline, delimiter);
        std::string word = "";
        if (tokens.size() > 1) {
            word = tokens[1];

        }
        //std::string word = extractWord(chaline, startChar, endChar, startCombinator, endCombinator);
        removeSubstring(word, removeText);
        // Convert each character in the string to lowercase
        std::transform(word.begin(), word.end(), word.begin(),
                       [](unsigned char c){ return std::tolower(c); });
        new_sitename = replaceSpacesWithUnderscores(word);
        //std::cout<<"sitename:"<<chaline<<std::endl;
        //cout << "Found match for search string 1: " << string(pos, min(static_cast<size_t>(60), bytesRead - (pos - buffer))) << endl;
        pos += searchString1.size();
    }

    pos = buffer;
    while ((pos = strstr(pos, searchString2.c_str())) != NULL)
    {
        found2 = true;
        std::string chalineb = string(pos, min(static_cast<size_t>(60), bytesRead - (pos - buffer)));
        char delimiterb = ',';
        std::vector<std::string> tokensb = splitString(chalineb, delimiterb);
        std::string instance_id = "";
        if (tokensb.size() > 1) {
            instance_id = tokensb[1];
        }

        //std::cout<<"siteid:"<<chalineb<<std::endl;
        //std::string instance_id = extractWord(chalineb, startChar, endChar, startCombinator, endCombinator);
        if(new_sitename != "")
        {
            std::string db_name = new_sitename + "_" + instance_id;
            std::cout << "instance_name:" << db_name << std::endl;
            /*const char* gzippedDumpFile = gzFileName.c_str();
            const char* mysqlHost = db_host.c_str();
            const char* mysqlUser = db_user.c_str();
            const char* mysqlPassword = db_password.c_str();
            const char* mysqlDatabase = db_name.c_str();

            if (restoreMySQLDump(gzippedDumpFile, mysqlHost, mysqlUser, mysqlPassword, mysqlDatabase)) {
                std::cout << "MySQL dump restored successfully." << std::endl;
            } else {
                std::cerr << "Failed to restore MySQL dump." << std::endl;
            }*/

            //restoreMySQLDumpB(gzFileName, db_host, db_user, db_password, db_name);

            // Construct the command to restore the database from the SQL dump
            std::string restoreCommand = "gunzip < " + gzFileName + " | mysql -u " + db_user + " -p" + db_password  + " -P" + db_port + " " + db_name;

            // Execute the command
            int returnValue = system(restoreCommand.c_str());

            // Check if the command executed successfully
            if (returnValue == 0) {
                std::cout << "Database restore from " << gzFileName << " successful." << std::endl;
            } else {
                std::cerr << "Error: Database restore from " << gzFileName << " failed." << std::endl;
            }
        }
        //cout << "Found match for search string 2: " << string(pos, min(static_cast<size_t>(60), bytesRead - (pos - buffer))) << endl;
        pos += searchString2.size();
        chalineb = "";
    }

    // Set search complete flag to true if both match is found
    if (found1 && found2)
        searchComplete = true;
}

void searchInGzipFile(const string &gzFileName, const string &searchString1, const string &searchString2)
{
    gzFile file = gzopen(gzFileName.c_str(), "rb");
    if (!file)
    {
        cerr << "Error: Could not open file " << gzFileName << endl;
        return;
    }

    vector<thread> threads;

    char buffer[BUFFER_SIZE];
    while (!searchComplete)
    {
        int bytesRead = gzread(file, buffer, BUFFER_SIZE - 1);
        if (bytesRead <= 0)
        {
            break; // End of file or error
        }

        // Launch a thread to search in the current buffer
        threads.emplace_back(searchInBuffer, ref(searchString1), ref(searchString2), buffer, static_cast<size_t>(bytesRead),gzFileName);

        // Ensure we don't have too many threads active
        if (threads.size() >= MAX_THREADS)
        {
            for (auto &t : threads)
            {
                t.join();
            }
            threads.clear();
        }
    }

    // Join remaining threads
    for (auto &t : threads)
    {
        t.join();
    }

    gzclose(file);
}






void searchInFolder(const string &folderPath, const string &searchString1, const string &searchString2)
{
    for (const auto &entry : fs::directory_iterator(folderPath))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".gz")
        {
            cout << "Searching in file: " << entry.path() << endl;
            searchComplete = false;
            searchInGzipFile(entry.path(), searchString1, searchString2);
        }
    }
}

int main()
{
    loadEnvironmentFromFile("env.txt");
    // Path to the folder containing dump files
    const string folderPath = std::getenv("DUMP_FOLDER");
    const string searchString1 = std::getenv("SITENAME");
    const string searchString2 = std::getenv("SITEID");

    searchInFolder(folderPath, searchString1, searchString2);

    return 0;
}

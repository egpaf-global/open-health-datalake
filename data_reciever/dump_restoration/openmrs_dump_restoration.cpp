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


// Function to restore MySQL dump file
void restoreMySQLDump(const std::string& filename, const std::string& db_host, const std::string& db_user, const std::string& db_password, const std::string& db_name) {
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
    char buffer[4096]; // Buffer to store decompressed data (4KB)
    int bytesRead;
    bool semicolonFound = false;
    bool inComment = false;
    std::vector<TableDefinition> tables;
    bool insideCreateTable = false;
    bool insideDropTable = false;
    bool insideSetTable = false;
    bool insideConstraintFound = false;

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

            if (inComment) {
                // Check if the line contains the end of the comment
                size_t endCommentPos = line.find("*/");
                if (endCommentPos != std::string::npos) {
                    line.erase(0, endCommentPos + 2); // Erase everything before the end of the comment
                    inComment = false;
                } else {
                    continue; // Skip the entire line if still inside a comment
                }
            } else if (line[0] == '#' || line.substr(0, 2) == "--") {
                continue; // Skip comment lines
            }


            // adding logic to check multiline Queries

            // Check if the line contains a CREATE TABLE statement

            if (line.find("CREATE TABLE") != std::string::npos) {
                insideCreateTable = true;
                //query += line;
                //query = line; // Start capturing the CREATE TABLE statement
            }

            if (line.find("DROP TABLE") != std::string::npos) {
                insideDropTable = true;
                //drop_query += line;
                //query = line; // Start capturing the CREATE TABLE statement
            }

            if (line.find("SET ") != std::string::npos) {
                insideSetTable = true;
                //set_query += line;
                //query = line; // Start capturing the CREATE TABLE statement
            }

            if (insideDropTable) {
                drop_query += line;


            }
            if (insideSetTable) {
                set_query += line;

            }



            // Append the line to the query if inside a CREATE TABLE statement
            if (insideCreateTable) {
                
                query += line;
                // Check if the line contains a CONSTRAINT definition
                if (line.find("CONSTRAINT") != std::string::npos) {
                    // You can push this to the tables vector
                    //tables.push_back({line, query});

                    insideConstraintFound = true;
                }

                // Check if the line ends the CREATE TABLE statement
                if (line.find(")") != std::string::npos) {
                    if(insideConstraintFound){
                        tables.push_back({line, query});
                        insideCreateTable = false;
                        insideConstraintFound = false;
                        query.clear();
                        continue;
                    }
                    
                }
            }


            /*// Check if the line contains a CREATE TABLE statement with CONSTRAINT
            if (line.find("CREATE TABLE") != std::string::npos && line.find("CONSTRAINT") != std::string::npos) {
                // Store the table definition
                std::cout<<"see if it passes this stage:"<<line<<endl;
                tables.push_back({line, query});
                continue;
            }
            else{
                std::cout<<"Else::::"<<line<<endl;
            }*/


            // end

            //query += line;

            // Execute query if it ends with a semicolon
            if (line.back() == ';') {
                insideDropTable = false;
                insideSetTable = false;
                if(query.length() > 1)
                {
                    std::cout<<"check semicolonFound:"<<line.back()<<std::endl;
                    std::cout<<"check before run:"<<query<<std::endl;
                    semicolonFound = true;

                    
                    
                    //std::cout<<"query::"<<query<<std::endl;

                    if (mysql_query(conn, query.c_str()) != 0) {
                        std::cerr << "Failed to execute query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }
                }
                query.clear();

                //drop query
                if(drop_query.length() > 1)
                {
                    std::cout<<"check before drop_query run:"<<drop_query<<std::endl;
                    //semicolonFound = true;

                    
                    
                    //std::cout<<"query::"<<query<<std::endl;

                    if (mysql_query(conn, drop_query.c_str()) != 0) {
                        std::cerr << "Failed to execute drop_query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }
                }
                drop_query.clear();

                //set query
                if(set_query.length() > 1)
                {
                    std::cout<<"check before set_query run:"<<set_query<<std::endl;
                    //semicolonFound = true;

                    
                    
                    //std::cout<<"query::"<<query<<std::endl;

                    if (mysql_query(conn, set_query.c_str()) != 0) {
                        std::cerr << "Failed to execute set_query: " << mysql_error(conn) << std::endl;
                        mysql_close(conn);
                        gzclose(file);
                        return;
                    }
                }
                set_query.clear();

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
            restoreMySQLDump(gzFileName, db_host, db_user, db_password, db_name);
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

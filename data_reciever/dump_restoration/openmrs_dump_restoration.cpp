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

// Function to restore MySQL dump file
void restoreMySQLDump(const std::string& filename, const std::string& db_host, const std::string& db_user, const std::string& db_password, const std::string& db_name) {
    MYSQL *conn;
    conn = mysql_init(NULL);

    // Connect to MySQL
    if (!mysql_real_connect(conn, db_host.c_str(), db_user.c_str(), db_password.c_str(), NULL, 0, NULL, 0)) {
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
    if (!mysql_real_connect(conn, db_host.c_str(), db_user.c_str(), db_password.c_str(), db_name.c_str(), 0, NULL, 0)) {
        std::cerr << "Failed to connect to MySQL server: " << mysql_error(conn) << std::endl;
        return;
    }

    // Execute MySQL restore command
    std::string command = "mysql -h" + db_host + " -u" + db_user + " -p" + db_password + " " + db_name + " < " + filename;
    int status = std::system(command.c_str());

    if (status != 0) {
        std::cerr << "Failed to restore " << filename << std::endl;
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

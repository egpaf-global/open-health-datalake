#include <iostream>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <string>
#include <thread>
#include <regex>
#include <filesystem>
#include <zlib.h>
#include <cstdlib>
#include <unordered_map>

// MySQL C API headers
#include <mysql/mysql.h>

namespace fs = std::filesystem;


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
void restoreMySQLDump(const std::string& filename, const std::string& db_host, const std::string& db_user, const std::string& db_password, const std::string& db_name, const std::string& instance_name) {
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

// Function to process a dump file
void processDumpFile(const std::string& filename, const std::string& db_host, const std::string& db_user, const std::string& db_password, const std::string& db_name) {
    // Open the gzipped dump file
    gzFile file = gzopen(filename.c_str(), "rb");
    if (file == NULL) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }

    // Extract siteid and sitename from the dump file
    std::string line;
    std::string siteid;
    std::string sitename;
    std::smatch siteid_match;
    std::smatch sitename_match;
    std::regex siteid_regex("INSERT INTO global_property \\(property, property_value\\) VALUES \\('siteid', '(.+)'\\);");
    std::regex sitename_regex("INSERT INTO global_property \\(property, property_value\\) VALUES \\('sitename', '(.+)'\\);");

    while (gzgets(file, &line[0], line.size()) != Z_NULL) {
        if (std::regex_search(line, siteid_match, siteid_regex)) {
            siteid = siteid_match[1];
        } else if (std::regex_search(line, sitename_match, sitename_regex)) {
            sitename = sitename_match[1];
        }

        if (!siteid.empty() && !sitename.empty()) {
            break;
        }
    }

    gzclose(file);

    // If siteid and sitename are found, restore the dump file
    if (!siteid.empty() && !sitename.empty()) {
        std::string instance_name = sitename + "_" + siteid;
        restoreMySQLDump(filename, db_host, db_user, db_password, db_name, instance_name);
    } else {
        std::cerr << "Siteid and sitename not found in " << filename << std::endl;
    }
}

int main() {
    // Load environment variables from the file
    loadEnvironmentFromFile("env.txt");

    // Retrieve database connection parameters from environment variables
    std::string db_host = std::getenv("DB_HOST");
    std::string db_user = std::getenv("DB_USER");
    std::string db_password = std::getenv("DB_PASSWORD");
    std::string db_name = std::getenv("DB_NAME");

    // Path to the folder containing dump files
    std::string dump_folder = std::getenv("DUMP_FOLDER");

    // Vector to hold thread objects
    std::vector<std::thread> threads;

    // Iterate over files in the dump folder
    for (const auto& entry : fs::directory_iterator(dump_folder)) {
        std::string filename = entry.path().string();

        // Process only .gz files
        if (filename.find(".gz") != std::string::npos) {
            // Start a new thread to process the dump file
            threads.push_back(std::thread(processDumpFile, filename, db_host, db_user, db_password, db_name));
        }
    }

    // Join all threads
    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All dump files processed successfully!" << std::endl;

    return 0;
}

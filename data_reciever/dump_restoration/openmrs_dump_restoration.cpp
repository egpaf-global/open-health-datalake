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
#include <chrono>

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

// Function to extract a word between specified characters
std::string extractWord(const std::string& input, char startChar, char endChar, char startCombinator,  char endCombinator) {
    std::string word = "";
    bool insideWord = false;

    for (int i = 2; i < input.length(); ++i) {
        if (input[i] == startChar && input[i+1] == startCombinator) {
            insideWord = true;
        } else if (input[i] == endChar && input[i+1] == endCombinator) {
            insideWord = false;
            break; // Exit loop once endChar is found
        } else if (insideWord == true && input[i] != startChar && input[i] != endChar ) {
            word += input[i];
        }
    }

    return word;
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

// Function to search for text in a compressed file
void searchInCompressedFile(const std::string& filename, const std::string& searchText1, const std::string& searchText2, const std::string& removeText,const std::string& db_host, const std::string& db_user, const std::string& db_password, const std::string& db_name) {
    gzFile file = gzopen(filename.c_str(), "rb");
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

    const size_t bufferSize = 8192 * 4;
    char buffer[bufferSize];

    size_t searchText1Length = searchText1.length();
    size_t searchText2Length = searchText2.length();

    std::string partialMatchBuffer1;
    std::string partialMatchBuffer2;

    int bytesRead;
    while ((bytesRead = gzread(file, buffer, bufferSize)) > 0) {
        
        for (int i = 0; i < bytesRead; ++i) {
            partialMatchBuffer1 += buffer[i];
            partialMatchBuffer2 += buffer[i];

            if (partialMatchBuffer1.length() > searchText1Length) {
                partialMatchBuffer1.erase(0, partialMatchBuffer1.length() - searchText1Length);
            }

            if (partialMatchBuffer2.length() > searchText2Length) {
                partialMatchBuffer2.erase(0, partialMatchBuffer2.length() - searchText2Length);
            }


            std::string new_sitename;

            if (partialMatchBuffer1 == searchText1) {
                std::string line;
                int charsToShow = searchText1Length + 20;
                std::string chaline;
                for (int j = 0; j < charsToShow; ++j) {
                    if (i + j < bytesRead) {
                        chaline += buffer[i + j];
                    } else {
                        if ((bytesRead = gzread(file, buffer, bufferSize)) > 0) {
                            i = -1; // Reset index to begin from the start of the new buffer
                        } else {
                            break; // End of file
                        }
                    }
                }

                char startChar = ',';
                char endChar = '\'';
                char startCombinator = '\'';
                char endCombinator = ',';

                std::string word = extractWord(chaline, startChar, endChar, startCombinator, endCombinator);

                if (!word.empty()) {
                    removeSubstring(word, removeText);
                    new_sitename = replaceSpacesWithUnderscores(word);
                    //std::string instance_name = new_sitename + "_";
                    //std::cout << "instance_name:" << instance_name << std::endl;
                } else {
                    std::cout << "sitename not found:" << std::endl;
                }
            }

            if (partialMatchBuffer2 == searchText2) {
                std::string lineb;
                int charsToShowb = searchText2Length + 20;
                std::string chalineb;
                for (int j = 0; j < charsToShowb; ++j) {
                    if (i + j < bytesRead) {
                        chalineb += buffer[i + j];
                    } else {
                        if ((bytesRead = gzread(file, buffer, bufferSize)) > 0) {
                            i = -1; // Reset index to begin from the start of the new buffer
                        } else {
                            break; // End of file
                        }
                    }
                }

                char startChar = ',';
                char endChar = '\'';
                char startCombinator = '\'';
                char endCombinator = ',';

                std::string wordb = extractWord(chalineb, startChar, endChar, startCombinator, endCombinator);

                if (!wordb.empty()) {
                    std::string instance_id = wordb;
                    std::string instance_name = new_sitename + "_" + instance_id;
                    std::cout << "instance_name:" << instance_name << std::endl;
                    restoreMySQLDump(filename, db_host, db_user, db_password, db_name, instance_name);
                    return;
                } else {
                    std::cout << "Word not found:" << std::endl;
                }
            }
        }
        
    }

    //std::cout<<"check last line--"<<std::endl;

    gzclose(file);
}

// Function to process a dump file
void processDumpFile(const std::string& filename, const std::string& db_host, const std::string& db_user, const std::string& db_password, const std::string& db_name) {
    std::string searchText1 = "current_health_center_name";
    std::string searchText2 = "current_health_center_id";
    std::string removeText = "name\\n";
    std::cout << "filename:" << filename.c_str() << std::endl;
    auto start = std::chrono::steady_clock::now();
    searchInCompressedFile(filename, searchText1, searchText2, removeText,db_host, db_user, db_password, db_name);
    auto end = std::chrono::steady_clock::now();
    // Calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    // Display the duration in milliseconds
    std::cout << "Time taken: " << duration.count() << " milliseconds" << std::endl;
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


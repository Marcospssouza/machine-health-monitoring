#include <iostream>
#include <chrono>
#include <ctime>
#include <thread>
#include <unistd.h>
#include "json.hpp" // json handling
#include "mqtt/client.h" // paho mqtt
#include <iomanip>
#include <fstream>
#include <sys/sysinfo.h>
#include <random>
#include <cstring>

using namespace std;
#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"

float generate_value() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(50, 120);
    return dis(gen);
}

unsigned long long getMemoryUsage() {
    struct sysinfo memInfo;
    if (sysinfo(&memInfo) != 0) {
        return 0;
    }
    unsigned long long totalMemory = memInfo.totalram * memInfo.mem_unit;
    unsigned long long freeMemory = memInfo.freeram * memInfo.mem_unit;
    unsigned long long usedMemory = totalMemory - freeMemory;
    return usedMemory;
}

unsigned long long getTotalMemory() {
    struct sysinfo memInfo;
    if (sysinfo(&memInfo) != 0) {
        return 0;
    }
    unsigned long long totalMemory = memInfo.totalram * memInfo.mem_unit;
    return totalMemory;
}

std::string getCPUPercentage() {
    std::string cpuUsageCommand = "top -bn1 | grep \"%Cpu(s)\" | awk '{print $2}' | cut -d '.' -f 1";
    std::string cpuUsageOutput = "";
    char buffer[128];
    std::shared_ptr<FILE> pipe(popen(cpuUsageCommand.c_str(), "r"), pclose);
    if (!pipe) {
        return "Error: Failed to execute the CPU usage command.";
    }
    while (!feof(pipe.get())) {
        if (fgets(buffer, 128, pipe.get()) != nullptr) {
            cpuUsageOutput += buffer;
        }
    }

    // Remove whitespace and non-digit characters from the cpuUsageOutput string
    cpuUsageOutput.erase(std::remove_if(cpuUsageOutput.begin(), cpuUsageOutput.end(), [](char c) {
        return !std::isdigit(c);
    }), cpuUsageOutput.end());

    // Check if the cpuUsageOutput string is empty
    if (cpuUsageOutput.empty()) {
        return "0";
    }

    return cpuUsageOutput;
}

std::string getDiskUsage() {
    std::string usage;
    FILE* pipe = popen("df -h / | tail -n 1 | awk '{print $5}'", "r");
    if (pipe != nullptr) {
        char buffer[128];
        if (std::fgets(buffer, sizeof(buffer), pipe) != nullptr) {
            usage = buffer;
        }
        pclose(pipe);
    } else {
        std::cout << "Error opening pipe." << std::endl;
    }
    return usage;
}

std::string generate_machine_id() {
    // Get the unique machine identifier, in this case, the hostname.
    char hostname[1024];
    gethostname(hostname, 1024);
    std::string machineId(hostname);
    return machineId;
}

string generate_sensor_id() {
    string sensors[9] = {"Kok", "cpu", "disk", "mem", "yhask", "lij", "peq", "sdas", "bhr"};

    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> dis(0, 8);

    int random_index = dis(gen);
    return sensors[random_index];
}

string get_time_stamp() {
    // Get the current time in ISO 8601 format.
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm* now_tm = std::localtime(&now_c);
    std::stringstream ss;
    ss << std::put_time(now_tm, "%FT%TZ");
    std::string timestamp = ss.str();
    return timestamp;
}

// Generate a random value for non-disk and non-memory sensors

// Generate a value based on the sensor type
float generate_sensor_value(const std::string& sensor_id) {
    if (sensor_id == "disk") {
        std::string diskUsage = getDiskUsage();
        if (!diskUsage.empty()) {
            std::string diskPercentageString = diskUsage.substr(0, diskUsage.size() - 1); // Remove the percentage sign
            float diskPercentage = std::stof(diskPercentageString);
            return std::round(diskPercentage * 10.0f) / 10.0f; // Round to one decimal place
        } else {
            return 0;
        }
    } else if (sensor_id == "mem") {
        unsigned long long memoryUsage = getMemoryUsage();
        unsigned long long totalMemory = getTotalMemory();
        float memoryPercentage = ((static_cast<float>(memoryUsage) / totalMemory) * 100);
        return std::round(memoryPercentage * 1.0f) / 1.0f; // Round to one decimal place
    } else if (sensor_id == "cpu") {
        std::string cpuPercentage = getCPUPercentage();
        if (!cpuPercentage.empty()) {
            float cpuValue = stof(cpuPercentage);
            return std::round(cpuValue * 10.0f) / 10.0f; // Round to one decimal place
        }
         else {
            return    0; //std::round(generate_value() * 10.0f) / 10.0f; // Round to one decimal place;
        }
    } else {
        return std::round(generate_value() * 10.0f) / 10.0f;
    }
}

int main(int argc, char* argv[]) {
    std::string clientId = "sensor-monitor";
    mqtt::client client(BROKER_ADDRESS, clientId);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::clog << "Connected to the broker" << std::endl;

    // Get the unique machine identifier, in this case, the hostname.
    std::string machineId = generate_machine_id();

    // Set default frequency values
    int cpuFrequency = 1;
    int diskFrequency = 1;
    int memFrequency = 1;

    // Parse command-line arguments
    if (argc >= 4) {
        cpuFrequency = atoi(argv[1]);
        diskFrequency = atoi(argv[2]);
        memFrequency = atoi(argv[3]);
    }

    while (true) {
        string sensorId = generate_sensor_id();
        // Construct the JSON message.
        nlohmann::json j;
        j["timestamp"] = get_time_stamp();
        j["value"] = generate_sensor_value(sensorId);

        // Publish the JSON message to the appropriate topic for each sensor data entry.
        std::string topic = "/sensors/" + machineId + "/" + sensorId;
        mqtt::message msg(topic, j.dump(), QOS, false);
        std::clog << "topic: " << topic << " - message: " << j.dump() << std::endl;
        client.publish(msg);

        // Sleep for a specific frequency based on the sensor type
        if (sensorId == "cpu") {
            std::this_thread::sleep_for(std::chrono::seconds(cpuFrequency));
        } else if (sensorId == "disk") {
            std::this_thread::sleep_for(std::chrono::seconds(diskFrequency));
        } else if (sensorId == "mem") {
            std::this_thread::sleep_for(std::chrono::seconds(memFrequency));
        } else {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    return EXIT_SUCCESS;
}

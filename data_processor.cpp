///Data_processor
#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <unistd.h>
#include "json.hpp" 
#include "mqtt/client.h"
#include <vector>
#include <ctime>
#include <unordered_map>
#include <chrono>
#include <netinet/in.h>
#include <arpa/inet.h>

#define QOS 1
#define CARBON_PORT 2003
#define CARBON_SERVER "graphite"
#define BROKER_ADDRESS "tcp://localhost:1883"

using namespace std;
using json = nlohmann::json;

void send_metric_to_graphite (const std::string& metric) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        perror("Error creating socket");
        return;
    }
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(CARBON_PORT);
    inet_pton(AF_INET, CARBON_SERVER, &serverAddr.sin_addr);

    if (connect(sock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == -1) {
        perror("Error connecting to Carbon server");
        close(sock);
        return;
    }

    send(sock, metric.c_str(), metric.size(), 0);
    close(sock);
}

struct SensorData {
    string sensor__id;
    time_t timestamp;
};

std::vector<SensorData> sensorvec;

void insert_metric(const std::string& machine_id, const std::string& sensor_id,
                   const std::string& timestamp_str, const std::string& alarme,
                   int value) {
    std::tm tm{};
    std::istringstream ss{timestamp_str};
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    std::time_t time_t_timestamp = std::mktime(&tm);

    std::ostringstream metric;
    metric << "sensors." << machine_id << "." << sensor_id << " " << value << " " << time_t_timestamp << std::endl;

    send_metric_to_graphite (metric.str());
}

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}


// Classe de callback para MQTT
class Callback : public virtual mqtt::callback {
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> processlasttimestamp;

public:
    void message_arrived(mqtt::const_message_ptr msg) override {
        // Processar mensagem MQTT aqui
        std::tm tm{};
        std::time_t time_t_stamp;

        double value;
        string timestamp;
        auto j = nlohmann::json::parse(msg->get_payload());

        std::string topic = msg->get_topic();
        auto topic_parts = split(topic, '/');

        std::string machine_id = topic_parts[2];
        std::string sensor_id = topic_parts[3];
        value = j["value"];

        // Check if the "timestamp" field is null
        if (!j["timestamp"].is_null()) {
            timestamp = j["timestamp"];
            // Perform further processing using the timestamp value
            std::istringstream ss{timestamp};
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
            time_t_stamp = std::mktime(&tm);

        } else {
            timestamp = "Null";
        }

        cout << "Machine id: " << machine_id << " Sensor id: " << sensor_id << " Value: " << value << endl;
        SensorData cpu;
        if (strncmp(sensor_id.c_str(), "cpu", 3) == 0 || strncmp(sensor_id.c_str(), "mem", 3) == 0 || strncmp(sensor_id.c_str(), "disk", 3) == 0) {
            cpu.sensor__id = sensor_id;
            cpu.timestamp = time_t_stamp;
            auto currentTime = std::chrono::steady_clock::now();
            auto lastTimestamp = processlasttimestamp[cpu.sensor__id];
            std::chrono::duration<double> elapsedSeconds = currentTime - lastTimestamp;

            // Extract the elapsed time in seconds as a double value
            double elapsedSecondsValue = elapsedSeconds.count();
            int lastTwoDigits = static_cast<int>(elapsedSecondsValue) % 100;
            cout << "Time in seconds: " << lastTwoDigits << endl;

            if (lastTwoDigits > 10) {
                string alarme = "Sensor inativo por dez períodos de tempo previstos";
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                // Enviar métrica de alarme para o Graphite
                send_metric_to_graphite("alarms.inactive 1");
            } else {
                // Lógica para outros tipos de alarme e processamento personalizado
                // ...

                // Exemplo: Gerar um alarme de memória alta
                if (strncmp(sensor_id.c_str(), "mem", 3) == 0 && value > 93) {
                    string mem_err = "Memoria está > 90% utilizado";
                    // Enviar métrica de alarme para o Graphite
                    send_metric_to_graphite("alarms.mem_high 1");
                }

                // Exemplo: Enviar métrica normal para o Graphite
                send_metric_to_graphite("sensors." + machine_id + "." + sensor_id + " " + std::to_string(value) + " " + std::to_string(time_t_stamp));
            }

            // Update the lastTimestamp to restart the timer
            processlasttimestamp[cpu.sensor__id] = currentTime;
        }
    }
};

// Função para dividir uma string com base em um delimitador
std::vector<std::string> split(const std::string& str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}

int main() {
    // Configurar e conectar ao broker MQTT
    std::string clientId = "clientId";
    mqtt::client client(BROKER_ADDRESS, clientId);
    Callback cb;

    // Configurar opções de conexão
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    // Conectar ao broker e se inscrever no tópico
    try {
        client.connect(connOpts);
        client.subscribe("/sensors/#", QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Aguardar indefinidamente
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return EXIT_SUCCESS;
}

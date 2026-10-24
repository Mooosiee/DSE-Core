#pragma once
#include "../common/httplib.h"
#include "../common/crypto.hpp" // Required for sha256()
#include "IStorageBackend.hpp"
#include "../common/json.hpp"
#include <string>

using json = nlohmann::json;
class WorkerServer
{
private:
    httplib::Server svr;
    IStorageBackend *storage;

public:
    WorkerServer(IStorageBackend *database) : storage(database)
    {
        // VULNERABILITY FIX 1: Prevent OOM crashes.
        // Drops any incoming connection attempting to send a payload larger than 1MB.
        // 1048576 bytes (1MB) + 1024 bytes (Safety margin for headers/boundaries)
        svr.set_payload_max_length(1048576 + 1024);

        // 1. POST /chunk?hash=abc (Uploads and saves a chunk)
        svr.Post("/chunk", [this](const httplib::Request &req, httplib::Response &res)
                 {
            std::string expected_hash = req.get_param_value("hash");
            if (expected_hash.empty()) { res.status = 400; return; }

            // 1. Storage DoS Prevention (Verify Ticket)
            std::string ticket = req.get_header_value("X-Upload-Ticket");
            std::string expiry_str = req.get_header_value("X-Upload-Expiry");
            
            std::string expected_ticket = hmac_sha256(expected_hash + expiry_str, std::getenv("CLUSTER_SECRET"));
            if (ticket != expected_ticket) {
                res.status = 401; res.set_content("Unauthorized: Invalid Ticket", "text/plain"); return;
            }

            long long current_time = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            if (std::stoll(expiry_str) < current_time) {
                res.status = 401; res.set_content("Unauthorized: Ticket Expired", "text/plain"); return;
            }

            // 2. Byzantine Client Prevention (Verify Payload)
            std::string actual_hash = sha256(req.body);
            if (actual_hash != expected_hash) {
                res.status = 400; res.set_content("Hash mismatch. Payload corrupted.", "text/plain"); return;
            }

            std::vector<char> data(req.body.begin(), req.body.end());
            
            if (this->storage->SaveChunk(expected_hash, data)) {
                size_t actual_size = data.size();
                
                // 3. Generate Cryptographic Receipt
                std::string receipt = hmac_sha256(expected_hash + std::to_string(actual_size) + "SUCCESS", std::getenv("CLUSTER_SECRET"));
                
                nlohmann::json response_json = {
                    {"status", "OK"}, {"size", actual_size}, {"receipt", receipt}
                };
                res.status = 200;
                res.set_content(response_json.dump(), "application/json");
            } else {
                res.status = 500;
    } });

        // 2. GET /chunk?hash=abc (Reads and streams a chunk)
        svr.Get("/chunk", [this](const httplib::Request &req, httplib::Response &res)
                {
            std::string hash = req.get_param_value("hash");
            std::vector<char> data = this->storage->LoadChunk(hash);

            if (!data.empty()) {
                res.status = 200;
                res.set_content(std::string(data.begin(), data.end()), "application/octet-stream");
            } else {
                res.status = 404; 
            } });

        // 3. DELETE /chunk?hash=abc (Purges a physical chunk - Called by Master GC)
        svr.Delete("/chunk", [this](const httplib::Request &req, httplib::Response &res)
                   {
            std::string hash = req.get_param_value("hash");
            if (this->storage->DeleteChunk(hash)) {
                res.status = 200;
                res.set_content("DELETED", "text/plain");
            } else {
                res.status = 404;
            } });
    }

    void Listen(int port)
    {
        svr.listen("0.0.0.0", port);
    }

    void Stop()
    {
        svr.stop();
    }
};
#include "ParallelTransferManager.hpp"
#include "../../common/httplib.h"
#include "../../common/json.hpp"
#include "../../common/Semaphore.hpp"
#include <fstream>
#include <mutex>
#include <iostream>
#include <cstring>
#include <filesystem>
#include <deque>
#include <future>
#include <thread>
#include <chrono>
#include <vector>

using json = nlohmann::json;

ParallelTransferManager::ParallelTransferManager(std::shared_ptr<ICryptoEngine> crypto_engine)
    : crypto_engine_(std::move(crypto_engine)) {}

struct SlicerStreamGuard
{
  IFileSlicer *slicer_ptr;
  explicit SlicerStreamGuard(IFileSlicer *s) : slicer_ptr(s) {}
  ~SlicerStreamGuard() { slicer_ptr->CloseStream(); }
};

static bool UploadChunkToWorker(const std::string &host, int port, const std::string &hash, const std::vector<char> &data,
                                const std::string &ticket, long long expiry, ChunkReceipt &out_receipt)
{
  for (int attempt = 0; attempt < 3; ++attempt)
  {
    try
    {
      httplib::Client client(host, port);
      client.set_connection_timeout(2, 0);
      client.set_read_timeout(5, 0);
      client.set_write_timeout(5, 0);

      httplib::Headers headers = {
          {"X-Upload-Ticket", ticket},
          {"X-Upload-Expiry", std::to_string(expiry)}};

      auto res = client.Post("/chunk?hash=" + hash, headers, data.data(), data.size(), "application/octet-stream");

      if (res && res->status == 200)
      {
        auto json_res = nlohmann::json::parse(res->body);
        out_receipt.receipt = json_res["receipt"].get<std::string>();
        out_receipt.size = json_res["size"].get<size_t>();
        return true;
      }
      else if (res)
      {
        std::cerr << "[WARNING] Worker " << host << ":" << port << " rejected upload. HTTP " << res->status << "\n";
      }
    }
    catch (...)
    {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500 * (1 << attempt)));
  }
  std::cerr << "[ERROR] Max retries exhausted for Worker " << host << ":" << port << "\n";
  return false;
}

static void DownloadChunkFromReplica(const std::vector<NodeInfo> &nodes, const std::string &hash, std::vector<char> &chunk_buffer, ICryptoEngine *crypto_engine)
{
  size_t expected_size = chunk_buffer.size();
  for (const auto &node : nodes)
  {
    try
    {
      httplib::Client client(node.advertised_host, node.advertised_port);
      client.set_connection_timeout(2, 0);
      client.set_read_timeout(5, 0);
      client.set_write_timeout(5, 0);

      size_t bytes_written = 0;

      auto res = client.Get("/chunk?hash=" + hash, [&](const char *data, size_t data_length)
                            {
                if (bytes_written + data_length > expected_size) return false;
                std::memcpy(chunk_buffer.data() + bytes_written, data, data_length);
                bytes_written += data_length;
                return true; });

      if (!res || res->status != 200 || bytes_written != expected_size)
      {
        std::cerr << "[FAILOVER] Worker " << node.advertised_host << ":" << node.advertised_port << " returned incomplete data. Trying next.\n";
        continue;
      }

      if (crypto_engine->ComputeHash(chunk_buffer) != hash)
      {
        std::cerr << "[FAILOVER] Hash mismatch from Worker " << node.advertised_host << ":" << node.advertised_port << ". Trying next.\n";
        continue;
      }

      return;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[FAILOVER] Worker " << node.advertised_host << ":" << node.advertised_port << " connection failed: " << e.what() << "\n";
    }
  }
  throw std::runtime_error("All replicas failed or returned corrupted data for chunk: " + hash);
}

// Upload method implementation remains structurally identical to our last refinement
bool ParallelTransferManager::Upload(IFileSlicer *slicer, ILocalStateManager *state_manager,
                                     const std::string &filepath, size_t chunk_size,
                                     const std::vector<std::pair<int, ChunkRoutingPlan>> &pending_tasks, UploadManifest &manifest,
                                     std::atomic<TransferState> *state)
{
  httplib::ThreadPool pool(16);
  std::deque<std::future<bool>> futures;
  Semaphore in_flight_limiter(16);

  std::mutex log_mutex;
  bool all_succeeded = true;
  std::vector<ProgressLogEntry> pending_disk_writes;

  slicer->OpenStream(filepath, chunk_size);
  SlicerStreamGuard guard(slicer);

  for (const auto &task : pending_tasks)
  {
    if (state->load() != TransferState::RUNNING)
      break;

    while (!futures.empty() && futures.front().wait_for(std::chrono::seconds(0)) == std::future_status::ready)
    {
      if (!futures.front().get())
        all_succeeded = false;
      futures.pop_front();
    }
    if (!all_succeeded)
      break;

    int index = task.first;
    const ChunkRoutingPlan &plan = task.second;

    in_flight_limiter.acquire();
    std::vector<char> raw_payload = slicer->ReadChunk(index, chunk_size);
    auto chunk_payload = std::make_shared<std::vector<char>>(std::move(raw_payload));
    auto permit = std::make_shared<InFlightPermit>(in_flight_limiter);

    for (const auto &node : plan.assigned_nodes)
    {
      auto promise = std::make_shared<std::promise<bool>>();
      futures.push_back(promise->get_future());

      pool.enqueue([promise, node, hash = plan.hash, ticket = plan.upload_ticket, expiry = plan.ticket_expiry, index, chunk_payload, permit, &log_mutex, &pending_disk_writes, &manifest, filepath, state_manager]() mutable
                   {
        try
        {
          ChunkReceipt receipt_data;
          bool success = UploadChunkToWorker(node.advertised_host, node.advertised_port, hash, *chunk_payload, ticket, expiry, receipt_data);
          if (success)
          {
            std::lock_guard<std::mutex> lock(log_mutex);
                
                // O(1) RAM Updates
                manifest.completed_chunks[index] = true;
                manifest.chunk_receipts[index] = receipt_data;
                manifest.successfully_uploaded_count++;
                
                pending_disk_writes.push_back({index, receipt_data.receipt, receipt_data.size});
                
                // Batch-Flush to Disk
                if (pending_disk_writes.size() >= 64) {
                    state_manager->AppendProgressLog(filepath, pending_disk_writes);
                    pending_disk_writes.clear();
                }
          }

          // Release memory and resolve promise safely
          chunk_payload.reset();
          permit.reset();
          promise->set_value(success);
        }
        catch (...)
        {
          // Safety net: Guarantee the main thread never deadlocks!
          chunk_payload.reset();
          permit.reset();
          promise->set_exception(std::current_exception());
        } });
    }
  }

  for (auto &fut : futures)
  {
    if (!fut.get())
      all_succeeded = false;
  }
  pool.shutdown();
  if (!pending_disk_writes.empty())
  {
    state_manager->AppendProgressLog(filepath, pending_disk_writes);
    pending_disk_writes.clear();
  }

  return all_succeeded && state->load() == TransferState::RUNNING;
}

// DownloadToFile method implementation remains structurally identical to our last refinement
bool ParallelTransferManager::DownloadToFile(const std::vector<DownloadChunkMeta> &ordered_chunks,
                                             const std::vector<ChunkRoutingPlan> &upload_plans,
                                             const std::string &output_path,
                                             std::atomic<TransferState> *state,
                                             uint64_t &out_transferred_bytes,
                                             size_t &out_chunks_downloaded)
{
  std::unordered_map<std::string, std::vector<NodeInfo>> routing_map;
  for (const auto &plan : upload_plans)
  {
    routing_map[plan.hash] = plan.assigned_nodes;
  }

  size_t total_file_size = 0;
  for (const auto &chunk : ordered_chunks)
  {
    total_file_size += chunk.size;
  }

  std::error_code fs_error;
  bool is_resume = std::filesystem::exists(output_path) && std::filesystem::file_size(output_path, fs_error) == total_file_size;

  if (!is_resume)
  {
    std::ofstream init_file(output_path, std::ios::out | std::ios::binary);
    if (!init_file)
      throw std::runtime_error("Failed to allocate file.");
    if (total_file_size > 0)
    {
      init_file.seekp(total_file_size - 1);
      init_file.write("", 1);
    }
  }

  std::fstream shared_file(output_path, std::ios::in | std::ios::out | std::ios::binary);
  if (!shared_file)
    throw std::runtime_error("Failed to open file stream.");

  std::vector<uint8_t> chunk_completed(ordered_chunks.size(), 0);

  if (is_resume)
  {
    std::cout << "[DEBUG] Executing parallel cryptographic verification on partial file...\n";
    size_t offset = 0;

    // Uncapped hardware concurrency to fully utilize high-core-count machines
    unsigned int thread_count = std::max(1u, std::thread::hardware_concurrency());
    httplib::ThreadPool verify_pool(thread_count);

    std::deque<std::future<void>> verify_futures;
    Semaphore verify_limiter(32);

    // NO MUTEX NEEDED! Threads write to distinct bytes.

    for (size_t i = 0; i < ordered_chunks.size(); ++i)
    {
      if (state->load() != TransferState::RUNNING)
        break;

      while (!verify_futures.empty() && verify_futures.front().wait_for(std::chrono::seconds(0)) == std::future_status::ready)
      {
        verify_futures.front().get();
        verify_futures.pop_front();
      }

      verify_limiter.acquire();

      auto buffer = std::make_shared<std::vector<char>>(ordered_chunks[i].size);
      shared_file.seekg(offset, std::ios::beg);
      shared_file.read(buffer->data(), buffer->size());

      auto permit = std::make_shared<InFlightPermit>(verify_limiter);
      auto promise = std::make_shared<std::promise<void>>();
      verify_futures.push_back(promise->get_future());

      std::string expected_hash = ordered_chunks[i].hash;
      ICryptoEngine *crypto_engine = crypto_engine_.get();

      verify_pool.enqueue([promise, buffer, i, expected_hash, crypto_engine, permit, &chunk_completed]() mutable
                          {
          try {
              if (crypto_engine->ComputeHash(*buffer) == expected_hash) {
                  // LOCK-FREE WRITE: Safe because every index 'i' maps to a unique, non-overlapping byte.
                  chunk_completed[i] = 1; 
              }
              buffer.reset();
              permit.reset();
              promise->set_value();
          } catch (...) {
              buffer.reset();
              permit.reset();
              promise->set_exception(std::current_exception());
          } });

      offset += ordered_chunks[i].size;
    }

    for (auto &fut : verify_futures)
    {
      fut.get();
    }
    verify_pool.shutdown();
  }

  // 1. ADD THE SEMAPHORE
  Semaphore in_flight_limiter(16);
  std::mutex file_mutex;
  httplib::ThreadPool pool(16);

  // 2. SWITCH TO DEQUE (Void type, relying on exceptions for failure)
  std::deque<std::future<void>> futures;
  size_t current_offset = 0;

  uint64_t local_transferred_bytes = 0;
  size_t local_chunks_downloaded = 0;
  bool all_succeeded = true;

  for (size_t i = 0; i < ordered_chunks.size(); ++i)
  {
    const auto &chunk = ordered_chunks[i];

    if (state->load() != TransferState::RUNNING)
      break;

    if (chunk_completed[i])
    {
      current_offset += chunk.size;
      continue;
    }
    local_transferred_bytes += chunk.size;
    local_chunks_downloaded++;

    // 3. DYNAMIC FUTURE REAPING
    while (!futures.empty() && futures.front().wait_for(std::chrono::seconds(0)) == std::future_status::ready)
    {
      try
      {
        futures.front().get();
      }
      catch (...)
      {
        all_succeeded = false;
      }
      futures.pop_front();
    }
    if (!all_succeeded)
      break;
    auto it = routing_map.find(chunk.hash);
    if (it == routing_map.end())
      throw std::runtime_error("Missing network route for hash: " + chunk.hash);

    auto nodes = it->second;

    // 4. BLOCK THE LOOP IF 16 TASKS ARE ALREADY QUEUED
    in_flight_limiter.acquire();
    auto permit = std::make_shared<InFlightPermit>(in_flight_limiter);

    auto promise = std::make_shared<std::promise<void>>();
    futures.push_back(promise->get_future());

    size_t chunk_offset = current_offset;
    ICryptoEngine *crypto_engine = crypto_engine_.get();

    pool.enqueue([promise, nodes, chunk, chunk_offset, &shared_file, &file_mutex, crypto_engine, permit]() mutable
                 {
            try {
                std::vector<char> chunk_buffer(chunk.size);
                DownloadChunkFromReplica(nodes, chunk.hash, chunk_buffer, crypto_engine);

                {
                    std::lock_guard<std::mutex> lock(file_mutex);
                    shared_file.seekp(chunk_offset, std::ios::beg);
                    shared_file.write(chunk_buffer.data(), chunk.size);
                }
                
                permit.reset(); // Release semaphore BEFORE unblocking main thread
                promise->set_value();
            } catch (...) {
                permit.reset();
                promise->set_exception(std::current_exception());
            } });

    current_offset += chunk.size;
  }

  // 5. AWAIT PIPELINE TAIL
  for (auto &fut : futures)
  {
    try
    {
      fut.get(); // Will throw if a tail task failed
    }
    catch (...)
    {
      all_succeeded = false;
    }
  }

  pool.shutdown();
  out_transferred_bytes = local_transferred_bytes;
  out_chunks_downloaded = local_chunks_downloaded;
  return all_succeeded && state->load() == TransferState::RUNNING;
}
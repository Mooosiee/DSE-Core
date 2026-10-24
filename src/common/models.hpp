#pragma once
#include <string>
#include <vector>
#include <unordered_set>

// Lifecycle state enumerator for active data plane transfers.
enum class TransferState
{
  RUNNING,
  PAUSED,
  CANCELED
};

// Represents the network routing coordinates for a physical storage node.
struct NodeInfo
{
  std::string advertised_host;
  int advertised_port;
};

// Maps a requested data block to its allocated replica set.
// Add to ChunkRoutingPlan
struct ChunkRoutingPlan
{
  std::string hash;
  std::vector<NodeInfo> assigned_nodes;

  // Capability Ticket (Authorization)
  std::string upload_ticket;
  long long ticket_expiry;
};

// New struct for cryptographic receipts
struct ChunkReceipt
{
  std::string receipt;
  size_t size;
};

// New struct for the Append-Only Log memory buffer
struct ProgressLogEntry
{
  int index;
  std::string receipt;
  size_t size;
};

// Specifies strict byte boundaries for direct memory access during downloads.
struct DownloadChunkMeta
{
  std::string hash;
  size_t size;
};

// Unified payload returned by the Control Plane download initialization.
struct FileDownloadManifest
{
  long long file_size;
  std::vector<ChunkRoutingPlan> plans;
};

// Ephemeral state tracker for uncommitted upload progress.
struct UploadManifest
{
  std::string filepath;
  long long total_size = 0;
  long long temp_modified_timestamp = 0;
  long long session_timestamp = 0;
  std::vector<std::string> chunk_hashes;
  std::vector<bool> completed_chunks;
  std::vector<ChunkReceipt> chunk_receipts;
  int successfully_uploaded_count = 0;
};
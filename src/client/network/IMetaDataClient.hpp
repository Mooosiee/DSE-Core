#pragma once
#include "../../common/models.hpp"
#include <string>
#include <vector>
#include <unordered_map>

// Abstract interface for Control Plane operations.
class IMetadataClient
{
public:
  virtual ~IMetadataClient() = default;
  virtual void SetAuthToken(const std::string &token) = 0;
  virtual bool CreateUser(const std::string &email, const std::string &password) = 0;
  virtual std::string Login(const std::string &email, const std::string &password) = 0;

  virtual std::vector<ChunkRoutingPlan> InitializeUpload(const std::vector<std::string> &chunk_hashes) = 0;
  virtual bool CommitFile(const std::string &filename, const std::vector<std::string> &chunk_hashes,
                          const std::unordered_map<std::string, ChunkReceipt> &receipts) = 0;
  virtual FileDownloadManifest GetDownloadPlan(const std::string &filename) = 0;
  virtual bool FileExists(const std::string &filename) = 0;
  virtual bool RemoveFile(const std::string &filename) = 0;
};
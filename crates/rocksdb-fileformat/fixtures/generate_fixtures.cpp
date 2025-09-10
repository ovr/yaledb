#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/filter_policy.h>
#include <iostream>
#include <string>
#include <iomanip>
#include <sstream>
#include <vector>
#include <map>
#include <filesystem>
#include <cstdlib>

using namespace rocksdb;

// Mapping for readable names
std::map<ChecksumType, std::string> checksum_names = {
    {ChecksumType::kNoChecksum, "nocsum"},
    {ChecksumType::kCRC32c, "crc32c"},
    {ChecksumType::kxxHash, "xxhash"},
    {ChecksumType::kxxHash64, "xxhash64"},
    {ChecksumType::kXXH3, "xxh3"}
};

std::map<CompressionType, std::string> compression_names = {
    {CompressionType::kNoCompression, "none"},
    {CompressionType::kSnappyCompression, "snappy"},
    {CompressionType::kZlibCompression, "zlib"},
    {CompressionType::kBZip2Compression, "bzip2"},
    {CompressionType::kLZ4Compression, "lz4"},
    {CompressionType::kLZ4HCCompression, "lz4hc"},
    {CompressionType::kXpressCompression, "xpress"},
    {CompressionType::kZSTD, "zstd"}
};

// Helper function to format keys and values
std::string format_key(int i) {
    std::ostringstream oss;
    oss << "key" << std::setfill('0') << std::setw(3) << i;
    return oss.str();
}

std::string format_value(int format_version, const std::string& checksum_name, 
                        const std::string& compression_name, int i) {
    std::ostringstream oss;
    oss << "value_v" << format_version << "_" << checksum_name << "_" << compression_name 
        << "_" << std::setfill('0') << std::setw(3) << i;
    return oss.str();
}

bool generate_sst_file(int format_version, ChecksumType checksum_type, 
                       CompressionType compression_type, const std::string& filename) {
    Options options;
    
    // Configure BlockBasedTableOptions
    BlockBasedTableOptions table_options;
    table_options.format_version = format_version;
    table_options.checksum = checksum_type;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    
    // Set compression type
    options.compression = compression_type;
    
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    
    SstFileWriter writer(EnvOptions(), options);
    Status status = writer.Open(filename);
    if (!status.ok()) {
        std::cerr << "Failed to open SST file writer for " << filename 
                  << ": " << status.ToString() << std::endl;
        return false;
    }
    
    // Generate test data - 50 key-value pairs
    std::string checksum_name = checksum_names[checksum_type];
    std::string compression_name = compression_names[compression_type];
    
    for (int i = 0; i < 50; ++i) {
        std::string key = format_key(i);
        std::string value = format_value(format_version, checksum_name, compression_name, i);
        
        status = writer.Put(key, value);
        if (!status.ok()) {
            std::cerr << "Failed to put key-value pair: " << status.ToString() << std::endl;
            return false;
        }
    }
    
    status = writer.Finish();
    if (!status.ok()) {
        std::cerr << "Failed to finish writing SST file " << filename 
                  << ": " << status.ToString() << std::endl;
        return false;
    }
    
    std::cout << "Generated " << filename 
              << " (v" << format_version 
              << ", " << checksum_name 
              << ", " << compression_name << ")" << std::endl;
    return true;
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]\n";
    std::cout << "Options:\n";
    std::cout << "  --all          Generate all combinations (default)\n";
    std::cout << "  --minimal      Generate minimal set for testing\n";
    std::cout << "  --version V    Generate only for format version V (5,6,7)\n";
    std::cout << "  --checksum C   Generate only for checksum type C (nocsum,crc32c,xxhash,xxhash64,xxh3)\n";
    std::cout << "  --compression C Generate only for compression type C (none,snappy,zlib,lz4,lz4hc,zstd)\n";
    std::cout << "  --help         Show this help\n";
}

std::string build_filename(int version, ChecksumType checksum, CompressionType compression) {
    std::ostringstream oss;
    oss << "sst_files/v" << version << "/" 
        << "v" << version << "_" 
        << checksum_names[checksum] << "_" 
        << compression_names[compression] << ".sst";
    return oss.str();
}

void create_directories() {
    std::filesystem::create_directories("sst_files/v5");
    std::filesystem::create_directories("sst_files/v6");
    std::filesystem::create_directories("sst_files/v7");
}

bool generate_matrix(const std::vector<int>& versions,
                     const std::vector<ChecksumType>& checksums,
                     const std::vector<CompressionType>& compressions) {
    
    create_directories();
    
    int total = versions.size() * checksums.size() * compressions.size();
    int current = 0;
    int failed = 0;
    
    std::cout << "Generating " << total << " SST files..." << std::endl;
    
    for (int version : versions) {
        for (ChecksumType checksum : checksums) {
            for (CompressionType compression : compressions) {
                current++;
                
                std::string filename = build_filename(version, checksum, compression);
                std::cout << "[" << current << "/" << total << "] ";
                
                if (!generate_sst_file(version, checksum, compression, filename)) {
                    std::cerr << "FAILED: " << filename << std::endl;
                    failed++;
                } 
            }
        }
    }
    
    std::cout << "\nGeneration complete!" << std::endl;
    std::cout << "Success: " << (total - failed) << "/" << total << std::endl;
    if (failed > 0) {
        std::cout << "Failed: " << failed << std::endl;
    }
    
    return failed == 0;
}

int main(int argc, char* argv[]) {
    // Default: all combinations
    std::vector<int> versions = {5, 6, 7};
    std::vector<ChecksumType> checksums = {ChecksumType::kNoChecksum, ChecksumType::kCRC32c, ChecksumType::kxxHash, ChecksumType::kxxHash64, ChecksumType::kXXH3};
    std::vector<CompressionType> compressions = {CompressionType::kNoCompression, CompressionType::kSnappyCompression, CompressionType::kZlibCompression, CompressionType::kLZ4Compression, CompressionType::kLZ4HCCompression, CompressionType::kZSTD};
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--minimal") {
            // Minimal set for basic testing - include more compression types
            versions = {5, 6, 7};
            checksums = {ChecksumType::kCRC32c, ChecksumType::kXXH3};
            compressions = {CompressionType::kNoCompression, CompressionType::kSnappyCompression, CompressionType::kLZ4Compression, CompressionType::kZSTD};
        } else if (arg == "--version" && i + 1 < argc) {
            versions = {std::atoi(argv[++i])};
        } else if (arg == "--checksum" && i + 1 < argc) {
            std::string checksum = argv[++i];
            checksums.clear();
            if (checksum == "nocsum") checksums.push_back(ChecksumType::kNoChecksum);
            else if (checksum == "crc32c") checksums.push_back(ChecksumType::kCRC32c);
            else if (checksum == "xxhash") checksums.push_back(ChecksumType::kxxHash);
            else if (checksum == "xxhash64") checksums.push_back(ChecksumType::kxxHash64);
            else if (checksum == "xxh3") checksums.push_back(ChecksumType::kXXH3);
            else {
                std::cerr << "Unknown checksum type: " << checksum << std::endl;
                return 1;
            }
        } else if (arg == "--compression" && i + 1 < argc) {
            std::string compression = argv[++i];
            compressions.clear();
            if (compression == "none") compressions.push_back(CompressionType::kNoCompression);
            else if (compression == "snappy") compressions.push_back(CompressionType::kSnappyCompression);
            else if (compression == "zlib") compressions.push_back(CompressionType::kZlibCompression);
            else if (compression == "lz4") compressions.push_back(CompressionType::kLZ4Compression);
            else if (compression == "lz4hc") compressions.push_back(CompressionType::kLZ4HCCompression);
            else if (compression == "zstd") compressions.push_back(CompressionType::kZSTD);
            else {
                std::cerr << "Unknown compression type: " << compression << std::endl;
                return 1;
            }
        }
    }
    
    std::cout << "Generating RocksDB SST fixture matrix..." << std::endl;
    
    bool success = generate_matrix(versions, checksums, compressions);
    return success ? 0 : 1;
}
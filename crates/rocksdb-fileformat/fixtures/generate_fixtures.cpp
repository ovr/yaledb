#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/filter_policy.h>
#include <iostream>
#include <string>
#include <iomanip>
#include <sstream>

using namespace rocksdb;

// Helper function to format keys and values
std::string format_key(int i) {
    std::ostringstream oss;
    oss << "key" << std::setfill('0') << std::setw(3) << i;
    return oss.str();
}

std::string format_value(int format_version, int i) {
    std::ostringstream oss;
    oss << "value_v" << format_version << "_" << std::setfill('0') << std::setw(3) << i;
    return oss.str();
}

bool generate_sst_file(int format_version, const std::string& filename) {
    Options options;
    
    // Configure BlockBasedTableOptions with specific format_version
    BlockBasedTableOptions table_options;
    table_options.format_version = format_version;
    
    // Set different compression types for variety
    switch (format_version) {
        case 5:
            table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
            options.compression = CompressionType::kSnappyCompression;
            break;
        case 6:
            table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
            options.compression = CompressionType::kLZ4Compression;
            break;
        case 7:
            table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
            options.compression = CompressionType::kZSTD;
            break;
        default:
            std::cerr << "Unsupported format version: " << format_version << std::endl;
            return false;
    }
    
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    
    SstFileWriter writer(EnvOptions(), options);
    Status status = writer.Open(filename);
    if (!status.ok()) {
        std::cerr << "Failed to open SST file writer for " << filename 
                  << ": " << status.ToString() << std::endl;
        return false;
    }
    
    // Generate test data - 50 key-value pairs
    for (int i = 0; i < 50; ++i) {
        std::string key = format_key(i);
        std::string value = format_value(format_version, i);
        
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
    
    std::cout << "Successfully generated " << filename 
              << " with format version " << format_version << std::endl;
    return true;
}

int main() {
    std::cout << "Generating RocksDB SST test fixtures..." << std::endl;
    
    // Generate SST files for format versions 5, 6, and 7
    std::vector<int> format_versions = {5, 6, 7};
    
    for (int version : format_versions) {
        std::string filename = "sst_files/format_v" + std::to_string(version) + ".sst";
        
        if (!generate_sst_file(version, filename)) {
            std::cerr << "Failed to generate fixture for format version " << version << std::endl;
            return 1;
        }
    }
    
    std::cout << "\nAll SST fixtures generated successfully!" << std::endl;
    std::cout << "Files created in sst_files/ directory:" << std::endl;
    std::cout << "  - format_v5.sst (Enhanced Bloom filters)" << std::endl;
    std::cout << "  - format_v6.sst (Improved checksum verification)" << std::endl;
    std::cout << "  - format_v7.sst (Latest format)" << std::endl;
    
    return 0;
}
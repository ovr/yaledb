use rocksdb_fileformat::{CompressionType, FormatVersion, SstFileWriter, SstReader, WriteOptions};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("SstFileWriter Example");

    // Create a temporary directory for our SST file
    let dir = tempdir()?;
    let sst_path = dir.path().join("example.sst");

    // Configure options
    let options = WriteOptions {
        compression: CompressionType::Snappy,
        block_size: 4096,
        block_restart_interval: 16,
        format_version: FormatVersion::V5,
    };

    // Create and use the writer
    let mut writer = SstFileWriter::create(&options);
    writer.open(&sst_path)?;

    println!("Writing data to SST file...");

    // Add some data in sorted order
    for i in 0..100 {
        let key = format!("user:{:04}", i);
        let value = format!(
            "User data for user {} with some additional content to make compression worthwhile",
            i
        );
        writer.put(key.as_bytes(), value.as_bytes())?;
    }

    // Add some entries after the regular users (must maintain sorted order)
    writer.delete(b"user_delete:0001")?;
    writer.delete(b"user_delete:0002")?;

    // Add some merge entries
    writer.merge(b"user_merge:0001", b"additional_data")?;
    writer.merge(b"user_merge:0002", b"more_merge_data")?;

    // Finish writing
    writer.finish()?;
    let file_size = writer.file_size();

    println!("Wrote SST file with {} bytes", file_size);

    // Now try to read it back using SstReader to verify it's properly formatted
    println!("Verifying SST file format...");

    let mut reader = SstReader::open(&sst_path)?;

    // Check basic file properties
    println!("File size: {}", reader.file_size());

    let footer = reader.get_footer();
    let format_version = footer.format_version;
    println!("✓ Format version: {}", format_version);
    println!("✓ Footer read successfully");
    println!(
        "  Index block: offset={}, size={}",
        footer.index_handle.offset, footer.index_handle.size
    );
    println!(
        "  Metaindex block: offset={}, size={}",
        footer.metaindex_handle.offset, footer.metaindex_handle.size
    );

    println!("✅ SST file created and verified successfully!");

    // Print some file statistics
    println!("\nFile Statistics:");
    println!("- Original data entries: 100");
    println!("- Delete entries: 2");
    println!("- Merge entries: 2");
    println!("- Total file size: {} bytes", file_size);
    println!("- Compression: {:?}", options.compression);

    Ok(())
}

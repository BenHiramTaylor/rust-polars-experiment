use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use dotenv::dotenv;
use log::{info, trace};
use polars::prelude::*;

#[derive(Debug)]
struct Opt {
    bucket: String,
    object: String,
    destination: PathBuf,
}

async fn get_object(client: Client, opt: Opt) -> Result<usize, anyhow::Error> {
    trace!("bucket:      {}", opt.bucket);
    trace!("object:      {}", opt.object);
    trace!("destination: {}", opt.destination.display());

    let mut file = File::create(opt.destination.clone())?;

    let mut object = client
        .get_object()
        .bucket(opt.bucket)
        .key(opt.object)
        .send()
        .await?;

    let mut byte_count = 0_usize;
    while let Some(bytes) = object.body.try_next().await? {
        let bytes_len = bytes.len();
        file.write_all(&bytes)?;
        trace!("Intermediate write of {bytes_len}");
        byte_count += bytes_len;
    }

    Ok(byte_count)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    dotenv().ok();

    let file_key = std::env::var("FILE_KEY").expect("Could not load file from .env");
    let bucket = std::env::var("BUCKET_NAME").expect("Could not load file from .env");
    let file_path = Path::new(&file_key);
    let local_file_name = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("temp.csv")
        .to_string();

    if Path::new(&local_file_name).exists() {
        info!("File already exists locally.");
    } else {
        info!("Downloading file from S3");

        let shared_config = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-west-1"))
            .build();
        let client = Client::new(&shared_config);

        get_object(
            client,
            Opt {
                object: file_key,
                bucket,
                destination: local_file_name.parse()?,
            },
        )
        .await?;
    }

    let lazy_frame = LazyCsvReader::new(&local_file_name)
        .with_has_header(true)
        .finish()?;

    // let df = lazy_frame.collect()?;

    let lazy_distinct_codes = lazy_frame
        .group_by([col("SIC_CODE_CATEGORY")])
        .agg([col("SIC_CODE_CATEGORY").count().alias("count")])
        .sort(["SIC_CODE_CATEGORY"], SortMultipleOptions::default());

    let df = lazy_distinct_codes.collect()?;

    info!("{df}");

    info!("Estimated size used: {}", df.estimated_size());

    Ok(())
}

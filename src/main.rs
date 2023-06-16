// we want to mimic the functionality of this bash script:
// #!/bin/bash
// if [ -z "${1}" ]; then
//         echo "empty arg!"
// else
//         echo "operating on ${1}"
//         time nice -n 10 cat ../virtue/data/$1/* > coalesced.txt
//         echo "concatenated files"
//         time nice -n 10 ../marlinflow/target/release/marlinflow-utils txt-to-data coalesced.txt --output coalesced.bin
//         rm coalesced.txt
//         echo "converted to binary format"
//         time nice -n 10 ../marlinflow/target/release/marlinflow-utils shuffle coalesced.bin --output $1.bin
//         rm coalesced.bin
//         echo "shuffled data"
//         time nice -n 10 xz -v -T 0 $1.bin
//         echo "compressed data"
// fi

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_folder = std::env::args()
        .nth(1)
        .ok_or("no path to data folder provided.")?;
    let marlinflow_utils = std::env::args()
        .nth(2)
        .ok_or("no path to marlinflow-utils provided.")?;
    let data_folder = std::path::Path::new(&data_folder);
    let data_files = std::fs::read_dir(data_folder)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && path.extension().map(|ext| ext == "txt") == Some(true) {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // convert and shuffle each data file, one-by-one
    for data_file in &data_files {
        println!("operating on {}", data_file.display());
        let unshuffled_binary_output = data_file.with_extension("unshuf-bin");
        let shuffled_binary_output = data_file.with_extension("bin");
        // convert to binary format using marlinflow-utils
        let mut command = std::process::Command::new(&marlinflow_utils);
        command.arg("txt-to-data");
        command.arg(data_file);
        command.arg("--output");
        command.arg(&unshuffled_binary_output);
        println!("running {:?}", command);
        let status = command.status()?;
        if !status.success() {
            return Err(
                format!("failed to convert {} to binary format", data_file.display()).into(),
            );
        }
        // shuffle using marlinflow-utils
        let mut command = std::process::Command::new(&marlinflow_utils);
        command.arg("shuffle");
        command.arg(&unshuffled_binary_output);
        command.arg("--output");
        command.arg(&shuffled_binary_output);
        println!("running {:?}", command);
        let status = command.status()?;
        if !status.success() {
            return Err(format!("failed to shuffle {}", data_file.display()).into());
        }
    }

    // merge all shuffled binary files into one
    let mut keep_going = String::new();
    println!("merge all shuffled binary files into one? (y/n)");
    std::io::stdin().read_line(&mut keep_going)?;
    if keep_going.trim().to_ascii_lowercase() != "y" {
        return Ok(());
    }

    let mut command = std::process::Command::new(&marlinflow_utils);
    command.arg("interleave");
    for data_file in &data_files {
        let shuffled_binary_output = data_file.with_extension("bin");
        command.arg(&shuffled_binary_output);
    }
    command.arg("--output");
    // output filename is same as data folder name
    let output_filename = data_folder.file_name().ok_or("no data folder name")?;
    let output_path = std::path::Path::new(&output_filename).with_extension("bin");
    command.arg(&output_path);
    println!("running {:?}", command);
    let status = command.status()?;
    if !status.success() {
        return Err("failed to merge shuffled binary files into one".into());
    }

    // compress the merged binary file
    let mut keep_going = String::new();
    println!("compress the merged binary file? (y/n)");
    std::io::stdin().read_line(&mut keep_going)?;
    if keep_going.trim().to_ascii_lowercase() != "y" {
        return Ok(());
    }

    let mut command = std::process::Command::new("xz");
    command.arg("-v");
    command.arg("-T");
    command.arg("0");
    command.arg(&output_path);
    println!("running {:?}", command);
    let status = command.status()?;
    if !status.success() {
        return Err("failed to compress the merged binary file".into());
    }

    Ok(())
}

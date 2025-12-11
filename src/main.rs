use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, Read};

mod pipeline;
use pipeline::{BusinessLogicStage, Pipeline, RecordParserStage, Stage};

fn main() -> Result<()> {
    let input_path = "input.bin";

    // 1. Stage that reads raw bytes from a file
    let reader_stage = FileReaderStage::new(input_path.to_string());

    // 2. Parser → raw bytes → records
    let parser_stage = RecordParserStage;

    // 3. Business logic over parsed records
    let logic_stage = BusinessLogicStage;

    // Compose the pipeline
    let pipeline = Pipeline::new(reader_stage)
        .then(parser_stage)
        .then(logic_stage);

    // Execute pipeline
    let result: Vec<Vec<u8>> = pipeline.run(())?;

    for (i, rec) in result.iter().enumerate() {
        println!("Record #{i}: {:?}", String::from_utf8_lossy(rec));
    }

    Ok(())
}

/// Stage for reading full file into a Vec<u8>
struct FileReaderStage {
    path: String,
}

impl FileReaderStage {
    fn new(path: String) -> Self {
        Self { path }
    }
}

impl Stage<(), Vec<u8>> for FileReaderStage {
    fn run(&self, _input: ()) -> Result<Vec<u8>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(buf)
    }
}

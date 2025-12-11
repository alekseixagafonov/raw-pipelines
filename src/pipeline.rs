use anyhow::{anyhow, Result};
use std::marker::PhantomData;

//
// -------- Base Stage trait ----------
//

pub trait Stage<I, O> {
    fn run(&self, input: I) -> Result<O>;
}

pub type Record = Vec<u8>;

//
// ---------- Parser Stage ------------
//
/// Format:
///    [4 bytes length big-endian] [payload]  * repeated
pub struct RecordParserStage;

impl Stage<Vec<u8>, Vec<Record>> for RecordParserStage {
    fn run(&self, input: Vec<u8>) -> Result<Vec<Record>> {
        let mut res = Vec::new();
        let mut i = 0;

        while i + 4 <= input.len() {
            let len_bytes = &input[i..i + 4];
            let len = u32::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
            i += 4;

            if i + len > input.len() {
                return Err(anyhow!(
                    "truncated record: expected {len} bytes, remaining {}",
                    input.len() - i
                ));
            }

            let payload = input[i..i + len].to_vec();
            i += len;

            res.push(payload);
        }

        if i != input.len() {
            return Err(anyhow!("extra {} bytes at end of input", input.len() - i));
        }

        Ok(res)
    }
}

//
// ---------- Business Logic Stage ------------
//
/// Filters out records <=3 bytes
/// Converts UTF-8 payloads to uppercase
pub struct BusinessLogicStage;

impl Stage<Vec<Record>, Vec<Record>> for BusinessLogicStage {
    fn run(&self, input: Vec<Record>) -> Result<Vec<Record>> {
        let mut out = Vec::new();

        for rec in input {
            if rec.len() <= 3 {
                continue;
            }

            if let Ok(s) = String::from_utf8(rec) {
                out.push(s.to_uppercase().into_bytes());
            }
        }

        Ok(out)
    }
}

pub struct Pipeline<S> {
    pub stage: S,
}

impl<S> Pipeline<S> {
    pub fn new(stage: S) -> Self {
        Self { stage }
    }

    pub fn then<Next, I, M, O>(self, next: Next) -> Pipeline<impl Stage<I, O>>
    where
        S: Stage<I, M> + 'static,
        Next: Stage<M, O> + 'static,
    {
        struct Combined<A, B, M> {
            a: A,
            b: B,
            _marker: PhantomData<M>,
        }

        impl<A, B, I, M, O> Stage<I, O> for Combined<A, B, M>
        where
            A: Stage<I, M>,
            B: Stage<M, O>,
        {
            fn run(&self, input: I) -> Result<O> {
                let mid = self.a.run(input)?;
                self.b.run(mid)
            }
        }

        Pipeline {
            stage: Combined {
                a: self.stage,
                b: next,
                _marker: PhantomData,
            },
        }
    }

    /// Run the composed pipeline
    pub fn run<I, O>(self, input: I) -> Result<O>
    where
        S: Stage<I, O>,
    {
        self.stage.run(input)
    }
}

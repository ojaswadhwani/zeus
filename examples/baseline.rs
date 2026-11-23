use std::env;
use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::io;
use std::path::Path;
use std::time::Instant;
use zeus::{MAX_VALUE_SIZE, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    let [path, count, value_size] = args.as_slice() else {
        return Err("usage: baseline NEW_LOG KEY_COUNT VALUE_BYTES".into());
    };
    let count: u64 = count.parse()?;
    let value_size: usize = value_size.parse()?;
    if count == 0 || !(1..=MAX_VALUE_SIZE).contains(&value_size) {
        return Err("KEY_COUNT and VALUE_BYTES must be positive and VALUE_BYTES at most 1 MiB".into());
    }
    let path = Path::new(path);
    OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut store = Store::open(path)?;
    let value = vec![0x5a; value_size];
    let mut physical_bytes = 0_u64;
    let mut logical_bytes = 0_u128;
    println!("keys={count} key_bytes=8 value_bytes={value_size} payload_bytes={} threads=1 sync=each_write", count as u128 * value_size as u128);

    measure("write", count, |i| store.put(&i.to_be_bytes(), &value))?;
    let initial_bytes = fs::metadata(path)?.len();
    physical_bytes += initial_bytes;
    logical_bytes += count as u128 * (8 + value_size) as u128;
    drop(store);
    let started = Instant::now();
    let mut store = Store::open(path)?;
    println!("reopen_ms={:.2}", started.elapsed().as_secs_f64() * 1000.0);
    print_memory("opened")?;

    measure("read", count, |i| read(&store, shuffled_key(i, count), &value))?;
    measure("mixed", count, |i| {
        let key = shuffled_key(i, count);
        if i % 10 == 0 {
            logical_bytes += (8 + value_size) as u128;
            store.put(&key.to_be_bytes(), &value)
        } else {
            read(&store, shuffled_key(i, count), &value)
        }
    })?;
    let before_compaction = fs::metadata(path)?.len();
    physical_bytes += before_compaction - initial_bytes;
    let started = Instant::now();
    store.compact()?;
    println!("compaction_ms={:.2}", started.elapsed().as_secs_f64() * 1000.0);
    let after_compaction = fs::metadata(path)?.len();
    physical_bytes += after_compaction;
    println!("bytes_before_compaction={before_compaction} bytes_after_compaction={after_compaction} engine_write_amplification={:.3}", physical_bytes as f64 / logical_bytes as f64);
    drop(store);
    let started = Instant::now();
    let store = Store::open(path)?;
    println!("compacted_reopen_ms={:.2}", started.elapsed().as_secs_f64() * 1000.0);
    read(&store, 0, &value)?;
    print_memory("compacted")?;
    Ok(())
}

fn shuffled_key(i: u64, count: u64) -> u64 {
    let mut key = i.wrapping_add(0x9e37_79b9_7f4a_7c15);
    key = (key ^ (key >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    key = (key ^ (key >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (key ^ (key >> 31)) % count
}

fn read(store: &Store, key: u64, expected: &[u8]) -> io::Result<()> {
    let key = key.to_be_bytes();
    match black_box(store.get(&key)?) {
        Some(value) if value.as_slice() == expected => Ok(()),
        _ => Err(io::Error::other("unexpected benchmark value")),
    }
}

fn measure(
    name: &str,
    count: u64,
    mut operation: impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
    let mut samples = Vec::new();
    let stride = count.div_ceil(100_000);
    let started = Instant::now();
    for i in 0..count {
        if i % stride == 0 {
            let sample = Instant::now();
            operation(i)?;
            samples.push(sample.elapsed());
        } else {
            operation(i)?;
        }
    }
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let percentile = |p| samples[(samples.len() * p as usize).div_ceil(100) - 1].as_secs_f64() * 1000.0;
    println!("{name}: ops_per_sec={:.2} p50_ms={:.4} p99_ms={:.4} samples={}", count as f64 / elapsed.as_secs_f64(), percentile(50), percentile(99), samples.len());
    Ok(())
}

fn print_memory(stage: &str) -> io::Result<()> {
    for line in fs::read_to_string("/proc/self/status")?.lines() {
        if line.starts_with("VmRSS:") || line.starts_with("VmHWM:") {
            println!("{stage} {line}");
        }
    }
    Ok(())
}

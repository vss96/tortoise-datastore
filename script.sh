rm -rf sst/
mkdir sst
rm wal.log
touch wal.log
cargo run --release
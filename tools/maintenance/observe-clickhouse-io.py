#!/usr/bin/env python3
"""Read-only local ClickHouse/disk samples; no database log tables are enabled."""
import argparse
import datetime
import json
import pathlib
import subprocess
import time
import urllib.request


def query(sql):
    request = urllib.request.Request(
        "http://127.0.0.1:8123/?readonly=1&max_execution_time=5",
        data=(sql + " FORMAT JSONEachRow").encode(),
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        return [json.loads(line) for line in response if line.strip()]


def disk_stats(device):
    for line in pathlib.Path("/proc/diskstats").read_text().splitlines():
        fields = line.split()
        if fields[2] == device:
            return list(map(int, fields[3:]))
    raise ValueError("disk not found: " + device)


def process_io(pid):
    try:
        return {k: int(v) for k, v in
                (line.split(":", 1) for line in pathlib.Path(f"/proc/{pid}/io").read_text().splitlines())}
    except (OSError, ValueError):
        return {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=300)
    parser.add_argument("--device", default="sda")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600:
        parser.error("seconds must be between 1 and 3600")
    pid = int(subprocess.check_output(["pgrep", "-n", "-x", "clickhouse"]).strip())
    merge_threads = []
    for task in pathlib.Path(f"/proc/{pid}/task").iterdir():
        try:
            if "MergeMutate" in (task / "comm").read_text():
                merge_threads.append(task.name)
        except OSError:
            pass
    args.output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    old_disk = disk_stats(args.device)
    previous_time = started
    with args.output.open("x") as output:
        for index in range(args.seconds):
            time.sleep(max(0, started + index + 1 - time.monotonic()))
            now = time.monotonic()
            disk = disk_stats(args.device)
            elapsed = now - previous_time
            delta = [a - b for a, b in zip(disk, old_disk)]
            row = {
                "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "elapsed": now - started,
                "interval": elapsed,
                "pid": pid,
                "disk_read_bytes_per_second": delta[2] * 512 / elapsed,
                "disk_write_bytes_per_second": delta[6] * 512 / elapsed,
                "disk_util_percent": delta[9] / (elapsed * 10),
                "disk_read_await_ms": delta[3] / delta[0] if delta[0] else 0,
                "disk_write_await_ms": delta[7] / delta[4] if delta[4] else 0,
                "disk_queue_depth": delta[10] / (elapsed * 1000),
                "process_io": process_io(pid),
                "merge_thread_io": {tid: process_io(f"{pid}/task/{tid}") for tid in merge_threads},
            }
            old_disk, previous_time = disk, now
            if index % 2 == 0:
                try:
                    row["merges"] = query("SELECT database,table,elapsed,progress,num_parts,total_size_bytes_compressed,bytes_read_uncompressed,bytes_written_uncompressed,is_mutation,merge_type FROM system.merges ORDER BY elapsed DESC")
                    row["events"] = query("SELECT event,value FROM system.events WHERE event IN ('MergedRows','MergedUncompressedBytes','MergesTimeMilliseconds','MergeTreeDataWriterRows','MergeTreeDataWriterUncompressedBytes','FileSync','FileSyncElapsedMicroseconds','MergesThrottlerBytes','MergesThrottlerSleepMicroseconds','MutationsThrottlerBytes','MutationsThrottlerSleepMicroseconds')")
                except Exception as error:
                    row["query_error"] = str(error)
            if index % 30 == 0 or index == args.seconds - 1:
                try:
                    row["parts"] = query("SELECT database,table,partition_id,count() AS parts,countIf(level=0) AS level_zero_parts,sum(bytes_on_disk) AS bytes,max(bytes_on_disk) AS largest_part FROM system.parts WHERE active GROUP BY database,table,partition_id ORDER BY bytes DESC")
                except Exception as error:
                    row["parts_error"] = str(error)
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            output.flush()
    print(json.dumps({"output": str(args.output.resolve()), "samples": args.seconds}))


if __name__ == "__main__":
    main()

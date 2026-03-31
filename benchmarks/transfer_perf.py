import argparse
import asyncio
import json
import os
import resource
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_ITERATIONS = 24
DEFAULT_PAYLOAD_BYTES = 1024 * 1024
DEFAULT_CONCURRENT_TRANSFERS = 3
DEFAULT_MAX_CACHE_SIZE = 1024 * 1024 * 1024
DEFAULT_MAX_INFLIGHT_BYTES = DEFAULT_MAX_CACHE_SIZE
DEFAULT_UPLOAD_WORKERS = 4
DEFAULT_SENDER_POOL_SIZE = 4
BUFFER_SIZE = 1024 * 1024


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _peak_rss_bytes() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def _mb_per_second(total_bytes: int, elapsed_seconds: float) -> float:
    if elapsed_seconds <= 0:
        return 0.0
    return total_bytes / (1024 * 1024) / elapsed_seconds


def _write_payload(path: Path, payload_bytes: int) -> None:
    pattern = b"telegram-reload-benchmark-payload\n"
    remaining = payload_bytes
    with path.open("wb") as handle:
        while remaining > 0:
            chunk = pattern[: min(len(pattern), remaining)]
            handle.write(chunk)
            remaining -= len(chunk)


def _copy_file(src: Path, dst: Path) -> int:
    dst.parent.mkdir(parents=True, exist_ok=True)
    copied = 0
    with src.open("rb") as reader, dst.open("wb") as writer:
        while True:
            chunk = reader.read(BUFFER_SIZE)
            if not chunk:
                break
            writer.write(chunk)
            copied += len(chunk)
    return copied


class ByteBudget:
    def __init__(self, total_bytes: int):
        self.total_bytes = max(1, total_bytes)
        self._available = self.total_bytes
        self._condition = asyncio.Condition()

    async def acquire(self, amount: int) -> None:
        amount = max(0, amount)
        async with self._condition:
            while amount > self._available:
                await self._condition.wait()
            self._available -= amount

    async def release(self, amount: int) -> None:
        amount = max(0, amount)
        async with self._condition:
            self._available = min(self.total_bytes, self._available + amount)
            self._condition.notify_all()


@dataclass
class BenchmarkCase:
    name: str
    iterations: int
    payload_bytes: int
    files_per_iteration: int
    transfer_shape: str
    concurrent_transfers: int
    max_inflight_bytes: int
    upload_workers: int
    sender_pool_size: int
    cleanup_mode: bool = False


@dataclass
class PreparedItem:
    index: int
    stage_paths: list[Path]
    sink_paths: list[Path]
    bytes_held: int


def _scenario_payload_bytes(payload_bytes: int, minimum_bytes: int) -> int:
    return max(payload_bytes, minimum_bytes)


async def _run_pipeline(case: BenchmarkCase) -> dict[str, Any]:
    queue: asyncio.Queue[PreparedItem | None] = asyncio.Queue(
        maxsize=case.concurrent_transfers
    )
    budget = ByteBudget(case.max_inflight_bytes)
    lane_count = max(1, min(case.upload_workers, case.sender_pool_size))
    residue_before = 0
    cleanup_result = "not-requested"

    with tempfile.TemporaryDirectory(prefix="transfer-perf-") as tmp:
        root = Path(tmp)
        source_dir = root / "source"
        stage_dir = root / "stage"
        sink_dir = root / "sink"
        source_dir.mkdir()
        stage_dir.mkdir()
        sink_dir.mkdir()

        source_paths: list[Path] = []
        payload_size = 0
        for file_index in range(case.files_per_iteration):
            source_path = source_dir / f"payload-{file_index:02d}.bin"
            _write_payload(source_path, case.payload_bytes)
            source_paths.append(source_path)
            payload_size += source_path.stat().st_size

        start = time.perf_counter()
        bytes_transferred = 0

        async def producer() -> None:
            for index in range(case.iterations):
                await budget.acquire(payload_size)
                stage_paths: list[Path] = []
                sink_paths: list[Path] = []
                held_bytes = 0
                for file_index, source_path in enumerate(source_paths):
                    stage_path = stage_dir / f"item-{index:04d}-part-{file_index:02d}.bin"
                    sink_path = sink_dir / f"item-{index:04d}-part-{file_index:02d}.bin"
                    held_bytes += _copy_file(source_path, stage_path)
                    stage_paths.append(stage_path)
                    sink_paths.append(sink_path)
                await queue.put(
                    PreparedItem(
                        index=index,
                        stage_paths=stage_paths,
                        sink_paths=sink_paths,
                        bytes_held=held_bytes,
                    )
                )

            for _ in range(lane_count):
                await queue.put(None)

        async def worker() -> int:
            transferred = 0
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    return transferred
                try:
                    for stage_path, sink_path in zip(item.stage_paths, item.sink_paths):
                        transferred += _copy_file(stage_path, sink_path)
                        if not case.cleanup_mode:
                            stage_path.unlink(missing_ok=True)
                finally:
                    await budget.release(item.bytes_held)
                    queue.task_done()

        producer_task = asyncio.create_task(producer())
        worker_tasks = [asyncio.create_task(worker()) for _ in range(lane_count)]

        try:
            await producer_task
            await queue.join()
            bytes_transferred = sum(await asyncio.gather(*worker_tasks))
        finally:
            if case.cleanup_mode:
                residue_before = len(list(stage_dir.iterdir()))
                for path in stage_dir.iterdir():
                    path.unlink(missing_ok=True)
                residue_after = len(list(stage_dir.iterdir()))
                cleanup_result = "clean" if residue_after == 0 else "residue"
            else:
                cleanup_result = "not-requested"
                residue_after = len(list(stage_dir.iterdir()))

        elapsed = time.perf_counter() - start
        result = {
            **asdict(case),
            "scenario": case.name,
            "synthetic_local": True,
            "bytes_transferred": bytes_transferred,
            "elapsed_seconds": round(elapsed, 6),
            "mb_per_second": round(_mb_per_second(bytes_transferred, elapsed), 6),
            "peak_rss_bytes": _peak_rss_bytes(),
            "cleanup_result": cleanup_result,
            "residue_count": residue_after,
            "residue_count_before_cleanup": residue_before,
            "effective_upload_lanes": lane_count,
        }
        return result


def _default_case(iterations: int, payload_bytes: int) -> BenchmarkCase:
    return BenchmarkCase(
        name="default",
        iterations=iterations,
        payload_bytes=payload_bytes,
        files_per_iteration=1,
        transfer_shape="single",
        concurrent_transfers=_env_int(
            "CONCURRENT_TRANSFERS", DEFAULT_CONCURRENT_TRANSFERS
        ),
        max_inflight_bytes=_env_int(
            "MAX_INFLIGHT_BYTES", DEFAULT_MAX_INFLIGHT_BYTES
        ),
        upload_workers=_env_int("UPLOAD_WORKERS", DEFAULT_UPLOAD_WORKERS),
        sender_pool_size=_env_int("SENDER_POOL_SIZE", DEFAULT_SENDER_POOL_SIZE),
    )


def _build_matrix_cases(iterations: int, payload_bytes: int) -> list[BenchmarkCase]:
    base = _default_case(iterations, payload_bytes)
    return [
        BenchmarkCase(
            name="upload_large_single",
            iterations=max(2, min(base.iterations, 4)),
            payload_bytes=_scenario_payload_bytes(base.payload_bytes, 8 * 1024 * 1024),
            files_per_iteration=1,
            transfer_shape="single",
            concurrent_transfers=max(1, base.concurrent_transfers),
            max_inflight_bytes=max(
                _scenario_payload_bytes(base.payload_bytes, 8 * 1024 * 1024),
                base.max_inflight_bytes,
            ),
            upload_workers=max(1, base.upload_workers),
            sender_pool_size=max(1, base.sender_pool_size),
        ),
        BenchmarkCase(
            name="upload_album",
            iterations=max(2, min(base.iterations, 4)),
            payload_bytes=max(256 * 1024, base.payload_bytes // 2),
            files_per_iteration=5,
            transfer_shape="album",
            concurrent_transfers=max(1, base.concurrent_transfers),
            max_inflight_bytes=max(max(256 * 1024, base.payload_bytes // 2) * 5, base.max_inflight_bytes),
            upload_workers=max(1, base.upload_workers),
            sender_pool_size=max(1, base.sender_pool_size),
        ),
        BenchmarkCase(
            name="upload_many_small",
            iterations=max(base.iterations, 24),
            payload_bytes=max(64 * 1024, base.payload_bytes // 16),
            files_per_iteration=1,
            transfer_shape="many-small",
            concurrent_transfers=max(1, base.concurrent_transfers),
            max_inflight_bytes=max(max(64 * 1024, base.payload_bytes // 16), base.max_inflight_bytes),
            upload_workers=max(1, base.upload_workers),
            sender_pool_size=max(1, base.sender_pool_size),
        ),
        BenchmarkCase(
            name="budget_saturation",
            iterations=max(4, min(base.iterations, 8)),
            payload_bytes=_scenario_payload_bytes(base.payload_bytes, 4 * 1024 * 1024),
            files_per_iteration=1,
            transfer_shape="single",
            concurrent_transfers=max(2, base.concurrent_transfers),
            max_inflight_bytes=_scenario_payload_bytes(base.payload_bytes, 4 * 1024 * 1024),
            upload_workers=max(1, base.upload_workers),
            sender_pool_size=max(1, base.sender_pool_size),
        ),
        BenchmarkCase(
            name="cleanup",
            iterations=max(4, min(base.iterations, 8)),
            payload_bytes=max(512 * 1024, base.payload_bytes),
            files_per_iteration=2,
            transfer_shape="cleanup",
            concurrent_transfers=max(1, base.concurrent_transfers),
            max_inflight_bytes=max(max(512 * 1024, base.payload_bytes) * 2, base.max_inflight_bytes),
            upload_workers=max(1, base.upload_workers),
            sender_pool_size=max(1, base.sender_pool_size),
            cleanup_mode=True,
        ),
    ]


async def _run_matrix(iterations: int, payload_bytes: int) -> dict[str, Any]:
    cases = _build_matrix_cases(iterations, payload_bytes)
    case_results = [await _run_pipeline(case) for case in cases]
    total_bytes = sum(case["bytes_transferred"] for case in case_results)
    total_elapsed = sum(case["elapsed_seconds"] for case in case_results)
    return {
        "scenario": "matrix",
        "synthetic_local": True,
        "bytes_transferred": total_bytes,
        "elapsed_seconds": round(total_elapsed, 6),
        "mb_per_second": round(_mb_per_second(total_bytes, total_elapsed), 6),
        "peak_rss_bytes": _peak_rss_bytes(),
        "cleanup_result": "clean",
        "residue_count": 0,
        "cases": case_results,
    }


async def _run_cleanup(iterations: int, payload_bytes: int) -> dict[str, Any]:
    cleanup_case = _default_case(max(4, min(iterations, 8)), payload_bytes)
    cleanup_case.name = "cleanup"
    cleanup_case.files_per_iteration = 2
    cleanup_case.transfer_shape = "cleanup"
    cleanup_case.payload_bytes = max(512 * 1024, payload_bytes)
    cleanup_case.max_inflight_bytes = max(
        cleanup_case.payload_bytes * cleanup_case.files_per_iteration,
        cleanup_case.max_inflight_bytes,
    )
    cleanup_case.cleanup_mode = True
    result = await _run_pipeline(cleanup_case)
    return {
        **result,
        "scenario": "cleanup",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.transfer_perf",
        description="Synthetic local transfer benchmark harness.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help="Number of synthetic payload transfers per case.",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=DEFAULT_PAYLOAD_BYTES,
        help="Synthetic payload size in bytes.",
    )
    parser.add_argument(
        "--scenario",
        default="baseline",
        choices=["baseline", "matrix", "cleanup"],
        help=(
            "Named synthetic benchmark scenario to run. "
            "'matrix' emits upload_large_single, upload_album, upload_many_small, "
            "budget_saturation, and cleanup cases."
        ),
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output JSON file path.",
    )
    return parser


def _emit_result(result: dict[str, Any], output: str) -> None:
    payload = json.dumps(result, indent=2)
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    else:
        print(payload)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.iterations <= 0:
        raise SystemExit("--iterations must be > 0")
    if args.payload_bytes <= 0:
        raise SystemExit("--payload-bytes must be > 0")

    if args.scenario == "matrix":
        result = asyncio.run(_run_matrix(args.iterations, args.payload_bytes))
    elif args.scenario == "cleanup":
        result = asyncio.run(_run_cleanup(args.iterations, args.payload_bytes))
    else:
        result = asyncio.run(_run_pipeline(_default_case(args.iterations, args.payload_bytes)))
        result["scenario"] = "baseline"

    _emit_result(result, args.output)


if __name__ == "__main__":
    main()

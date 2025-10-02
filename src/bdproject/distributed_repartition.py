import gc
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from functools import reduce
from pathlib import Path
from shutil import rmtree
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from distributed import Actor, Client, worker_client

from bdproject.extra_dask import with_secede


@dataclass
class _CoordinationActor:
    dir_base: Path
    schema: Any
    partitioning: Any
    compression: str
    compression_level: int | dict | None
    rows_per_file: int

    _self_ref: Actor | None = None
    _writers: dict[
        tuple[Any, ...], tuple[Literal[True], Actor] | tuple[Literal[False], Any]
    ] = field(default_factory=dict, init=False)
    _metadata: list[Any] = field(default_factory=list, init=False)

    def set_self(self, self_ref: Actor):
        self._self_ref = self_ref

    def get_writer(self, partition: tuple[Any, ...]) -> Actor:
        if (writer := self._writers.get(partition)) is None:
            prefix_dir, prefix_name = self.partitioning.format(
                reduce(
                    lambda x, y: x & y,
                    (
                        pc.field(name) == value
                        for name, value in zip(
                            self.partitioning.schema.names, partition
                        )
                    ),
                )
            )
            with worker_client(separate_thread=False) as c:
                assert self._self_ref is not None
                writer = (
                    False,
                    c.submit(
                        _WriteActor,
                        self._self_ref,
                        self.dir_base,
                        prefix_dir,
                        prefix_name,
                        self.schema,
                        self.compression,
                        self.compression_level,
                        self.rows_per_file,
                        actor=True,
                        priority=1,
                    ),
                )
                self._writers[partition] = writer

        match writer:
            case False, future:
                with with_secede():
                    writer = future.result()
                self._writers[partition] = writer
            case True, writer:
                pass

        return writer

    def append_metadata(self, metadata: list[Any]) -> Any:
        self._metadata += metadata

    def finish(self) -> Any:
        futures = [writer.end_of_chunks() for writer in self._writers.values()]
        with with_secede():
            for future in futures:
                future.result()
        self._self_ref = None
        self._writers.clear()
        gc.collect()
        return self._metadata


@dataclass
class _WriteActor:
    coordinator: Actor | None
    dir_base: Path
    prefix_dir: str
    prefix_name: str
    schema: Any
    compression: str
    compression_level: int | dict[Any, Any] | None
    rows_per_file: int

    _i: int = field(default=0, init=False)
    _batches: list[Any] = field(default_factory=list, init=False)
    _num_rows: int = field(default=0, init=False)

    def append_chunk(self, batch: Any):
        self._batches.append(batch)
        self._num_rows += batch.num_rows

        while self._num_rows >= self.rows_per_file:
            if self._num_rows == self.rows_per_file:
                self._flush()
            else:
                last_batch = self._batches[-1]
                n_batch_b = self._num_rows - self.rows_per_file
                n_batch_a = last_batch.num_rows - n_batch_b
                batch_a, batch_b = (
                    last_batch.slice(length=n_batch_a),
                    last_batch.slice(offset=n_batch_a),
                )
                self._batches[-1] = batch_a
                self._num_rows -= n_batch_b
                self._flush()
                self._batches.append(batch_b)
                self._num_rows = n_batch_b

    def end_of_chunks(self):
        if self._batches:
            self._flush()
        self.coordinator = None
        gc.collect()

    def _flush(self):
        p_relative = Path(self.prefix_dir) / f"{self.prefix_name}part.{self._i}.parquet"
        p_absolute = self.dir_base / p_relative
        p_absolute.parent.mkdir(parents=True, exist_ok=True)
        metadata_collector = []
        pq.write_table(
            pa.Table.from_batches(self._batches),
            p_absolute,
            row_group_size=self.rows_per_file,
            compression=self.compression,
            compression_level=self.compression_level,
            metadata_collector=metadata_collector,
        )
        metadata_collector[-1].set_file_path(str(p_relative))

        self._i += 1
        self._batches = []
        self._num_rows = 0
        pa.default_memory_pool().release_unused()
        gc.collect()

        assert self.coordinator is not None
        self.coordinator.append_metadata(metadata_collector).result()


def _process_file(
    path: Path,
    preprocess_batch: Callable[[Any], Any],
    coordinator: Actor,
    partitioning_fields: list[str],
):
    pf = pq.ParquetFile(path)
    writers: dict[tuple[Any, ...], Actor] = {}
    for batch in pf.iter_batches(10**6):
        batch = preprocess_batch(batch)
        for group in (
            pa.Table.from_batches((batch,))
            .group_by(partitioning_fields)
            .aggregate([])
            .to_pylist()
        ):
            if any(v is None for v in group.values()):
                continue
            group_expr = reduce(
                lambda x, y: x & y,
                (pc.field(k) == v for k, v in group.items()),
            )
            group_batch = batch.filter(group_expr).drop_columns(partitioning_fields)
            group_partition = tuple(group.values())
            if (group_writer := writers.get(group_partition)) is None:
                with with_secede():
                    group_writer = coordinator.get_writer(group_partition).result()
                writers[group_partition] = group_writer

            group_writer.append_chunk(group_batch).result()


def _process_files(
    src: Path | Iterable[Path],
    dst: Path,
    schema: Any,
    partitioning: Any,
    compress: bool,
    preprocess_batch: Callable[[Any], Any],
    rows_per_file: int,
):
    pds = pq.ParquetDataset(src)
    files = [Path(p) for p in pds.files]
    schema_no_partitioning = reduce(
        lambda schema, k: schema.remove(schema.get_field_index(k)),
        partitioning.schema.names,
        schema,
    )
    rmtree(dst, ignore_errors=True)
    dst.mkdir(parents=True, exist_ok=True)

    with worker_client() as client:
        coordinator = client.submit(
            _CoordinationActor,
            dst,
            schema_no_partitioning,
            partitioning,
            "zstd" if compress else "none",
            6 if compress else None,
            rows_per_file,
            actor=True,
            priority=2,
        ).result()
        coordinator.set_self(coordinator).result()
        client.gather(
            client.map(
                lambda p: _process_file(
                    p,
                    preprocess_batch,
                    coordinator,
                    partitioning.schema.names,
                ),
                files,
                key="_process_file",
            )
        )

        metadata_collector = coordinator.finish().result()
        pq.write_metadata(schema, dst / "_common_metadata")
        pq.write_metadata(schema_no_partitioning, dst / "_metadata", metadata_collector)


def repartition(
    client: Client,
    *,
    src: Path | Iterable[Path],
    dst: Path,
    schema: Any,
    partitioning: Any,
    compress: bool,
    preprocess_batch: Callable[[Any], Any],
    rows_per_file: int,
):
    return client.submit(
        _process_files,
        src if isinstance(src, Path) else list(src),
        dst,
        schema,
        partitioning,
        compress,
        preprocess_batch,
        rows_per_file,
    )

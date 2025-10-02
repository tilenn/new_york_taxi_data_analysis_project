from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow.parquet


def _parquetfile_to_schema_dict(parquetfile: Any) -> dict[str, tuple[Any, bool]]:
    ret = {}
    for i, field in enumerate(parquetfile.schema_arrow):
        has_nulls = False
        for j in range(parquetfile.metadata.num_row_groups):
            col = parquetfile.metadata.row_group(j).column(i)
            if (
                not col.is_stats_set
                or not col.statistics.has_null_count
                or col.statistics.null_count > 0
            ):
                has_nulls = True
                break
        ret[field.name] = field.type, has_nulls
    return ret


def _format_schema_v(v: Any) -> str:
    return f"{'?' if v[1] else ''}{v[0]}"


def _diff_schemas(schema_prev: Any, schema: Any):
    for k in sorted({*schema_prev, *schema}):
        v_prev = schema_prev.get(k)
        v = schema.get(k)
        if v_prev is None and v is not None:
            schema_prev[k] = v
            yield k, f"+ {_format_schema_v(v)}"
        elif v_prev is not None and v is None:
            del schema_prev[k]
            yield k, "-"
        elif v_prev is not None and v is not None and v_prev != v:
            if v[0] == pyarrow.null():
                v = v_prev[0], True
            if v_prev[1] and not v[1]:
                v = v[0], True
            schema_prev[k] = v
            if v_prev != v:
                yield k, f": {_format_schema_v(v_prev)} -> {_format_schema_v(v)}"


def analyze_parquet_schema_evolution(candidate_files: Iterable[str | Path]):
    schema_prev = None
    klen = 0
    for p in candidate_files:
        schema = _parquetfile_to_schema_dict(pyarrow.parquet.ParquetFile(p))
        if schema_prev is None:
            print(f"{p}:")
            klen = max(len(k) for k in schema)
            for k, v in schema.items():
                print(f"{k:{klen}} : {_format_schema_v(v)}")
            schema_prev = schema
        else:
            if diffs := list(_diff_schemas(schema_prev, schema)):
                print()
                print(f"{p}:")
                klen = max(klen, max(len(k) for k, _ in diffs))
                for k, v in diffs:
                    print(f"{k:{klen}} {v}")
    lines = [
        f"{k:{klen}} : {_format_schema_v(v)}" for k, v in sorted(schema_prev.items())
    ]
    print()
    print(f"{' FINAL ':=^{max(len(x) for x in lines)}}")
    print("\n".join(lines))

import pickle
from collections.abc import Iterable
from functools import partial

import dask.dataframe as dd
import pandas as pd
from crick import TDigest

_USER_AGGREGATIONS: dict[str, dd.Aggregation] = {}


def _digest_create(s: pd.Series):
    t = TDigest()
    t.update(s)
    return pickle.dumps(t)


def _digest_merge(s: pd.Series):
    t = TDigest()
    t.merge(*(pickle.loads(t_ser) for t_ser in s))
    return pickle.dumps(t)


def _digest_quantile_one(t_ser: bytes, *, quantile: float):
    return pickle.loads(t_ser).quantile(quantile)


def _digest_quantile_multiple(t_ser: bytes, *, quantile: list[float]):
    t = pickle.loads(t_ser)
    return tuple(t.quantile(q) for q in quantile)


def agg_tdigest(*, quantile: Iterable[float] | float = 0.5):
    """Returns a custom Dask aggregation that calculates approximate quantiles."""
    q = list(quantile) if isinstance(quantile, Iterable) else [quantile]
    if len(q) == 0:
        raise ValueError("no quantiles specified")
    name = f"u_tdigest_{q!r}"

    if agg := _USER_AGGREGATIONS.get(name):
        return agg

    agg = dd.Aggregation(
        name,
        lambda sgb: sgb.apply(lambda s: _digest_create(s)),
        lambda sgb: sgb.apply(_digest_merge),
        lambda s: s.apply(
            partial(_digest_quantile_one, quantile=q[0])
            if len(q) == 1
            else partial(_digest_quantile_multiple, quantile=q)
        ),
    )
    _USER_AGGREGATIONS[name] = agg
    return agg

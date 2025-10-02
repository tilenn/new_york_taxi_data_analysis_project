import pandas as pd
import seaborn as sns

COLOR_YELLOW = sns.color_palette()[1]
COLOR_GREEN = sns.color_palette()[2]
COLOR_FHV = sns.color_palette()[0]
COLOR_FHVHV = sns.color_palette()[4]
LABEL_TO_COLOR = {
    "yellow": COLOR_YELLOW,
    "green": COLOR_GREEN,
    "fhv_small": COLOR_FHV,
    "fhv_big": COLOR_FHVHV,
}

LABEL_TO_HUMAN_READABLE = {
    "yellow": "Yellow",
    "green": "Green",
    "fhv_small": "FHV (Other)",
    "fhv_big": "FHV (Uber/Lyft)",
}
LABEL_HUMAN_READABLE_CATEGORICAL = pd.CategoricalDtype(
    LABEL_TO_HUMAN_READABLE.values(), ordered=True
)

LABEL_HUMAN_READABLE_TO_COLOR = {
    LABEL_TO_HUMAN_READABLE[k]: v for k, v in LABEL_TO_COLOR.items()
}


def label_to_human_readable(s: pd.Series) -> pd.Series:
    return s.map(LABEL_TO_HUMAN_READABLE).astype(LABEL_HUMAN_READABLE_CATEGORICAL)

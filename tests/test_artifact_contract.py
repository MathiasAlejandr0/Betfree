from src.predictor.artifact_contract import collect_e0_artifact_issues
from src.predictor.e0_expert_train import TABULAR_COLS


def test_collect_empty_artifact() -> None:
    assert "dict" in collect_e0_artifact_issues("x")[0].lower()


def test_collect_missing_model() -> None:
    issues = collect_e0_artifact_issues({"feature_names": TABULAR_COLS})
    assert any("model" in x for x in issues)

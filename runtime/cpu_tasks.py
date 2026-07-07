from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import statistics
from typing import Any

from joblib import dump, load
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score

from ml_model.feature_builder import build_feature_vector
from ml_model.model_registry import ModelRegistry


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_cancel_requested(cancel_flag_path: str) -> bool:
    if not cancel_flag_path:
        return False
    return Path(cancel_flag_path).exists()


def _write_log(log_path: str, message: str) -> None:
    if not log_path:
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{_iso_now()} {message}\n")


def _write_progress(progress_path: str, percent: float, status: str, details: dict[str, Any] | None = None) -> None:
    if not progress_path:
        return
    payload = {
        "updated_at": _iso_now(),
        "percent": max(0.0, min(float(percent), 100.0)),
        "status": status,
        "details": details or {},
    }
    path = Path(progress_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def _open_readonly_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _load_training_samples_ro(db_path: str, label_type: str) -> list[dict[str, Any]]:
    supported = {
        "result_5m",
        "result_5m_fallback_15m",
        "result_15m",
        "result_15m_fallback_30m",
        "final_label",
    }
    label_type_norm = str(label_type or "result_15m_fallback_30m").strip().lower()
    if label_type_norm not in supported:
        label_type_norm = "result_15m_fallback_30m"

    with _open_readonly_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                s.features_json,
                s.confidence_score,
                s.timestamp,
                o.final_label,
                o.result_5m,
                o.result_15m,
                o.result_30m,
                o.max_profit_5m,
                o.max_drawdown_5m,
                o.max_profit_15m,
                o.max_drawdown_15m,
                o.max_profit_30m,
                o.max_drawdown_30m
            FROM signals AS s
            INNER JOIN signal_outcomes AS o ON o.signal_id = s.id
            ORDER BY s.timestamp ASC
            """
        ).fetchall()

    samples: list[dict[str, Any]] = []
    for row in rows:
        features = json.loads(str(row["features_json"]))
        if label_type_norm == "result_5m":
            selected_label = str(row["result_5m"] or "").strip().lower()
            label_source = "result_5m"
            max_profit = float(row["max_profit_5m"] or 0.0)
            max_drawdown = float(row["max_drawdown_5m"] or 0.0)
        elif label_type_norm == "result_5m_fallback_15m":
            if row["result_5m"] is not None:
                selected_label = str(row["result_5m"] or "").strip().lower()
                label_source = "result_5m"
                max_profit = float(row["max_profit_5m"] or 0.0)
                max_drawdown = float(row["max_drawdown_5m"] or 0.0)
            else:
                selected_label = str(row["result_15m"] or "").strip().lower()
                label_source = "result_15m"
                max_profit = float(row["max_profit_15m"] or 0.0)
                max_drawdown = float(row["max_drawdown_15m"] or 0.0)
        elif label_type_norm == "result_15m":
            selected_label = str(row["result_15m"] or "").strip().lower()
            label_source = "result_15m"
            max_profit = float(row["max_profit_15m"] or 0.0)
            max_drawdown = float(row["max_drawdown_15m"] or 0.0)
        elif label_type_norm == "final_label":
            selected_label = str(row["final_label"] or "").strip().lower()
            if row["result_15m"] is not None:
                label_source = "result_15m"
                max_profit = float(row["max_profit_15m"] or 0.0)
                max_drawdown = float(row["max_drawdown_15m"] or 0.0)
            else:
                label_source = "result_30m"
                max_profit = float(row["max_profit_30m"] or 0.0)
                max_drawdown = float(row["max_drawdown_30m"] or 0.0)
        else:
            selected_label = str(row["result_15m"] or row["result_30m"] or "").strip().lower()
            if row["result_15m"] is not None:
                label_source = "result_15m"
                max_profit = float(row["max_profit_15m"] or 0.0)
                max_drawdown = float(row["max_drawdown_15m"] or 0.0)
            else:
                label_source = "result_30m"
                max_profit = float(row["max_profit_30m"] or 0.0)
                max_drawdown = float(row["max_drawdown_30m"] or 0.0)

        if selected_label not in {"win", "loss", "neutral"}:
            continue

        samples.append(
            {
                "features": features,
                "label": 1 if selected_label == "win" else 0,
                "final_label": selected_label,
                "label_source": label_source,
                "timestamp": str(row["timestamp"] or ""),
                "confidence_score": float(row["confidence_score"] or 0.0),
                "max_profit_pct": max_profit,
                "max_drawdown_pct": max_drawdown,
            }
        )
    return samples


def _train_model(payload: dict[str, Any], cancel_flag_path: str, progress_path: str, log_path: str) -> dict[str, Any]:
    db_path = str(payload.get("db_path", ""))
    models_dir = Path(str(payload.get("models_dir", "")))
    label_type = str(payload.get("label_type", "result_15m_fallback_30m"))
    if not db_path:
        raise ValueError("db_path es requerido")
    if not models_dir:
        raise ValueError("models_dir es requerido")

    _write_log(log_path, "train_model started")
    _write_progress(progress_path, 5.0, "RUNNING", {"step": "loading_samples"})
    samples = _load_training_samples_ro(db_path, label_type)
    if len(samples) < 20:
        return {
            "trained": False,
            "reason": "No hay suficientes muestras para entrenar (minimo 20)",
            "number_of_samples": len(samples),
        }

    if _is_cancel_requested(cancel_flag_path):
        return {"cancelled": True, "partial_progress": 10.0}

    _write_progress(progress_path, 30.0, "RUNNING", {"step": "split_dataset", "samples": len(samples)})
    split_idx = int(len(samples) * 0.8)
    split_idx = max(1, min(split_idx, len(samples) - 1))
    train_samples = samples[:split_idx]
    test_samples = samples[split_idx:]

    x_train = [build_feature_vector(sample["features"]) for sample in train_samples]
    y_train = [int(sample["label"]) for sample in train_samples]
    x_test = [build_feature_vector(sample["features"]) for sample in test_samples]
    y_test = [int(sample["label"]) for sample in test_samples]

    if _is_cancel_requested(cancel_flag_path):
        return {"cancelled": True, "partial_progress": 40.0}

    _write_progress(progress_path, 60.0, "RUNNING", {"step": "fitting_model"})
    model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
    model.fit(x_train, y_train)
    predictions = [int(value) for value in model.predict(x_test)]

    accuracy = float(accuracy_score(y_test, predictions))
    precision = float(precision_score(y_test, predictions, zero_division=0))
    recall = float(recall_score(y_test, predictions, zero_division=0))

    predicted_trades: list[dict[str, Any]] = [
        sample
        for sample, pred in zip(test_samples, predictions)
        if int(pred) == 1
    ]
    if not predicted_trades:
        predicted_trades = list(test_samples)

    wins = sum(1 for sample in predicted_trades if str(sample.get("final_label", "")) == "win")
    total = len(predicted_trades)
    win_rate = float(wins / total) if total else 0.0
    gross_profit = sum(max(float(sample.get("max_profit_pct", 0.0) or 0.0), 0.0) for sample in predicted_trades)
    gross_loss = sum(abs(min(float(sample.get("max_drawdown_pct", 0.0) or 0.0), 0.0)) for sample in predicted_trades)
    profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else float(gross_profit)
    max_drawdown = min(float(sample.get("max_drawdown_pct", 0.0) or 0.0) for sample in predicted_trades)

    if _is_cancel_requested(cancel_flag_path):
        return {"cancelled": True, "partial_progress": 85.0}

    version = f"general_model_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    metadata = {
        "model_version": version,
        "trained_at": _iso_now(),
        "number_of_samples": len(samples),
        "trained_with_outcomes_count": len(samples),
        "label_type": label_type,
        "dataset_start": str(samples[0].get("timestamp", "") or ""),
        "dataset_end": str(samples[-1].get("timestamp", "") or ""),
    }

    registry = ModelRegistry(str(models_dir))
    artifact_path = registry.save_model(model=model, version=version, metadata=metadata)
    registry.prune_old_versions(keep_last=5)

    _write_progress(progress_path, 100.0, "DONE", {"model_version": version})
    _write_log(log_path, f"train_model done version={version}")

    return {
        "trained": True,
        "model_version": version,
        "artifact_path": str(artifact_path),
        "metadata": metadata,
        "training_run": {
            "timestamp": metadata["trained_at"],
            "model_version": version,
            "asset_scope": "general",
            "dataset_start": metadata["dataset_start"],
            "dataset_end": metadata["dataset_end"],
            "number_of_samples": len(samples),
            "trained_with_outcomes_count": len(samples),
            "label_type": label_type,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown": max_drawdown,
            "approved_for_paper": False,
            "approved_for_live": False,
            "notes": f"Entrenamiento CPU separado (label_type={label_type})",
        },
    }


def _evaluate_model(payload: dict[str, Any], cancel_flag_path: str, progress_path: str, log_path: str) -> dict[str, Any]:
    db_path = str(payload.get("db_path", ""))
    label_type = str(payload.get("label_type", "result_15m_fallback_30m"))
    model_path = str(payload.get("model_path", ""))
    if not db_path:
        raise ValueError("db_path es requerido")

    _write_log(log_path, "evaluate_model started")
    _write_progress(progress_path, 10.0, "RUNNING", {"step": "loading_samples"})
    samples = _load_training_samples_ro(db_path, label_type)
    if len(samples) < 20:
        return {"evaluated": False, "reason": "Muestras insuficientes", "number_of_samples": len(samples)}

    split_idx = int(len(samples) * 0.8)
    split_idx = max(1, min(split_idx, len(samples) - 1))
    test_samples = samples[split_idx:]
    x_test = [build_feature_vector(sample["features"]) for sample in test_samples]
    y_test = [int(sample["label"]) for sample in test_samples]

    if _is_cancel_requested(cancel_flag_path):
        return {"cancelled": True, "partial_progress": 40.0}

    if model_path:
        model_bundle = load(model_path)
        model = model_bundle.get("model") if isinstance(model_bundle, dict) else model_bundle
    else:
        train_samples = samples[:split_idx]
        x_train = [build_feature_vector(sample["features"]) for sample in train_samples]
        y_train = [int(sample["label"]) for sample in train_samples]
        model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
        model.fit(x_train, y_train)

    predictions = [int(value) for value in model.predict(x_test)]
    accuracy = float(accuracy_score(y_test, predictions))
    precision = float(precision_score(y_test, predictions, zero_division=0))
    recall = float(recall_score(y_test, predictions, zero_division=0))
    _write_progress(progress_path, 100.0, "DONE", {"accuracy": accuracy, "precision": precision, "recall": recall})
    _write_log(log_path, "evaluate_model done")
    return {
        "evaluated": True,
        "number_of_samples": len(samples),
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
    }


def _run_backtest(payload: dict[str, Any], cancel_flag_path: str, progress_path: str, log_path: str) -> dict[str, Any]:
    db_path = str(payload.get("db_path", ""))
    label_type = str(payload.get("label_type", "result_15m_fallback_30m"))
    if not db_path:
        raise ValueError("db_path es requerido")

    _write_log(log_path, "run_backtest started")
    samples = _load_training_samples_ro(db_path, label_type)
    if not samples:
        return {"backtest": False, "reason": "No hay muestras"}

    profits: list[float] = []
    wins = 0
    losses = 0
    for idx, sample in enumerate(samples):
        if _is_cancel_requested(cancel_flag_path):
            return {
                "cancelled": True,
                "partial_progress": (idx / max(len(samples), 1)) * 100.0,
                "processed": idx,
            }
        pnl = float(sample.get("max_profit_pct", 0.0) or 0.0) + float(sample.get("max_drawdown_pct", 0.0) or 0.0)
        profits.append(pnl)
        if str(sample.get("final_label", "")) == "win":
            wins += 1
        elif str(sample.get("final_label", "")) == "loss":
            losses += 1
        if idx % 50 == 0:
            _write_progress(progress_path, (idx / max(len(samples), 1)) * 100.0, "RUNNING", {"processed": idx})

    total = len(samples)
    avg_pnl = float(sum(profits) / max(total, 1))
    std_pnl = float(statistics.pstdev(profits)) if len(profits) > 1 else 0.0
    win_rate = float(wins / max(wins + losses, 1))

    _write_progress(progress_path, 100.0, "DONE", {"samples": total, "avg_pnl": avg_pnl})
    _write_log(log_path, "run_backtest done")
    return {
        "backtest": True,
        "number_of_samples": total,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_pnl_pct": avg_pnl,
        "pnl_stddev_pct": std_pnl,
    }


def _generate_historical_features(payload: dict[str, Any], cancel_flag_path: str, progress_path: str, log_path: str) -> dict[str, Any]:
    db_path = str(payload.get("db_path", ""))
    if not db_path:
        raise ValueError("db_path es requerido")

    _write_log(log_path, "generate_historical_features started")
    with _open_readonly_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT asset_type, price, volume, vwap, rsi, atr, spread,
                   percent_change_1m, percent_change_5m, percent_change_15m
            FROM market_snapshots
            ORDER BY timestamp DESC
            LIMIT 10000
            """
        ).fetchall()

    if not rows:
        return {"generated": False, "reason": "Sin snapshots historicos"}

    vectors: list[list[float]] = []
    for idx, row in enumerate(rows):
        if _is_cancel_requested(cancel_flag_path):
            return {
                "cancelled": True,
                "partial_progress": (idx / max(len(rows), 1)) * 100.0,
                "generated": len(vectors),
            }
        features = {
            "asset_type": row["asset_type"],
            "price": float(row["price"] or 0.0),
            "volume": float(row["volume"] or 0.0),
            "vwap": float(row["vwap"] or 0.0),
            "rsi": float(row["rsi"] or 0.0),
            "atr": float(row["atr"] or 0.0),
            "spread": float(row["spread"] or 0.0),
            "percent_change_1m": float(row["percent_change_1m"] or 0.0),
            "percent_change_5m": float(row["percent_change_5m"] or 0.0),
            "percent_change_15m": float(row["percent_change_15m"] or 0.0),
        }
        vectors.append(build_feature_vector(features))
        if idx % 200 == 0:
            _write_progress(progress_path, (idx / max(len(rows), 1)) * 100.0, "RUNNING", {"generated": len(vectors)})

    means = [float(sum(col) / max(len(col), 1)) for col in zip(*vectors)]
    _write_progress(progress_path, 100.0, "DONE", {"generated": len(vectors)})
    _write_log(log_path, "generate_historical_features done")
    return {
        "generated": True,
        "vectors": len(vectors),
        "feature_means": means,
    }


def _optimize_strategy(payload: dict[str, Any], cancel_flag_path: str, progress_path: str, log_path: str) -> dict[str, Any]:
    db_path = str(payload.get("db_path", ""))
    label_type = str(payload.get("label_type", "result_15m_fallback_30m"))
    if not db_path:
        raise ValueError("db_path es requerido")

    samples = _load_training_samples_ro(db_path, label_type)
    if not samples:
        return {"optimized": False, "reason": "Sin muestras"}

    candidates = []
    thresholds_profit = [0.1, 0.2, 0.3, 0.5]
    thresholds_drawdown = [-0.1, -0.2, -0.3, -0.5]
    total = len(thresholds_profit) * len(thresholds_drawdown)
    processed = 0
    for tp in thresholds_profit:
        for sl in thresholds_drawdown:
            if _is_cancel_requested(cancel_flag_path):
                return {
                    "cancelled": True,
                    "partial_progress": (processed / max(total, 1)) * 100.0,
                    "evaluated": processed,
                }
            score = 0.0
            for row in samples:
                p = float(row.get("max_profit_pct", 0.0) or 0.0)
                d = float(row.get("max_drawdown_pct", 0.0) or 0.0)
                if p >= tp:
                    score += 1.0
                if d <= sl:
                    score -= 0.5
            candidates.append({"take_profit_pct": tp, "stop_loss_pct": sl, "score": score})
            processed += 1
            _write_progress(progress_path, (processed / max(total, 1)) * 100.0, "RUNNING", {"evaluated": processed})

    best = max(candidates, key=lambda row: float(row.get("score", 0.0) or 0.0))
    _write_progress(progress_path, 100.0, "DONE", {"best": best})
    _write_log(log_path, "optimize_strategy done")
    return {
        "optimized": True,
        "best": best,
        "tested": candidates,
    }


def execute_cpu_task(
    task_name: str,
    payload: dict[str, Any],
    cancel_flag_path: str = "",
    progress_path: str = "",
    log_path: str = "",
) -> dict[str, Any]:
    task = str(task_name or "").strip().lower()
    _write_log(log_path, f"cpu_task_start task={task}")
    if task == "train_model":
        result = _train_model(payload, cancel_flag_path, progress_path, log_path)
    elif task == "run_backtest":
        result = _run_backtest(payload, cancel_flag_path, progress_path, log_path)
    elif task == "evaluate_model":
        result = _evaluate_model(payload, cancel_flag_path, progress_path, log_path)
    elif task == "generate_historical_features":
        result = _generate_historical_features(payload, cancel_flag_path, progress_path, log_path)
    elif task == "optimize_strategy":
        result = _optimize_strategy(payload, cancel_flag_path, progress_path, log_path)
    else:
        raise ValueError(f"CPU task no soportada: {task_name}")
    _write_log(log_path, f"cpu_task_done task={task}")
    return result

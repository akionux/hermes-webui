"""Task 4: session-visit model catalog must stop re-probing on every page load.

Measured 6–7s per page load on the target box: the frontend hardcodes
``freshness=session_visit`` (static/ui.js, static/sessions.js) and the handler
rebuilds the catalog with one live HTTPS probe per detected provider whenever
the disk cache is older than the freshness horizon. Two dials fix it together:

* the session-visit horizon is now env-tunable and defaults HIGH (1h): inside
  it the disk-cached catalog is returned with no probe at all — model lists go
  stale by at most one hour, which is what users actually experience;
* when a rebuild IS needed, it must not keep the caller waiting multi-seconds:
  the foreground budget default drops 4s → 1s (stale catalog returns sooner,
  the rebuild finishes out-of-band and serves the next call).
"""
import importlib
import os


def _reload_config_fresh(monkeypatch, **env):
    for k in ("HERMES_WEBUI_SESSION_VISIT_MODELS_FRESHNESS_SECONDS",
              "HERMES_WEBUI_MODELS_REBUILD_BUDGET"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import api.config as cfg
    importlib.reload(cfg)
    return cfg


def test_session_visit_freshness_default_is_one_hour(monkeypatch):
    cfg = _reload_config_fresh(monkeypatch)
    assert cfg._SESSION_VISIT_MODELS_FRESHNESS_SECONDS >= 3600.0


def test_session_visit_freshness_env_override(monkeypatch):
    cfg = _reload_config_fresh(
        monkeypatch, HERMES_WEBUI_SESSION_VISIT_MODELS_FRESHNESS_SECONDS="30")
    assert cfg._SESSION_VISIT_MODELS_FRESHNESS_SECONDS == 30.0


def test_rebuild_budget_default_is_one_second(monkeypatch):
    cfg = _reload_config_fresh(monkeypatch)
    assert cfg._LIVE_REBUILD_BUDGET_SECONDS <= 1.0

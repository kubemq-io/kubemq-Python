"""Configuration with YAML loading and API config translation — v2 multi-channel."""

from __future__ import annotations

import logging
import os
import re
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

_config_logger = logging.getLogger("burnin.config")


def _parse_duration(s: str) -> float:
    """Parse a duration string (e.g. '1h', '30m', '5s', '2d') to seconds."""
    if not s:
        return 0.0
    s = s.strip()
    if s == "0":
        return 0.0
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    for suffix, mult in multipliers.items():
        if s.endswith(suffix):
            return float(s[:-1]) * mult
    return float(s)


# ---------------------------------------------------------------------------
# Pattern configuration (v2)
# ---------------------------------------------------------------------------

@dataclass
class PatternConfig:
    """Per-pattern configuration for v2 multi-channel burn-in."""
    enabled: bool = True
    channels: int = 1
    producers_per_channel: int = 1
    consumers_per_channel: int = 1
    consumer_group: bool = False
    senders_per_channel: int = 1
    responders_per_channel: int = 1
    rate: int = 100
    thresholds: Optional[dict[str, float]] = None


# ---------------------------------------------------------------------------
# Warmup configuration (v2)
# ---------------------------------------------------------------------------

@dataclass
class WarmupConfig:
    """Warmup configuration for multi-channel warmup."""
    max_parallel_channels: int = 10
    timeout_per_channel_ms: int = 5000
    warmup_duration: str = ""


# ---------------------------------------------------------------------------
# Sub-configs (preserved from v1 where applicable)
# ---------------------------------------------------------------------------

@dataclass
class BrokerConfig:
    address: str = "localhost:50000"
    client_id_prefix: str = "burnin-python"


@dataclass
class QueueConfig:
    poll_max_messages: int = 10
    poll_wait_timeout_seconds: int = 5
    auto_ack: bool = False
    max_depth: int = 1_000_000


@dataclass
class RPCConfig:
    timeout_ms: int = 5000


@dataclass
class MessageConfig:
    size_mode: str = "fixed"
    size_bytes: int = 1024
    size_distribution: str = "256:80,4096:15,65536:5"
    reorder_window: int = 10_000


@dataclass
class ApiConfig:
    port: int = 8080
    enabled: bool = True


@dataclass
class MetricsConfig:
    port: int = 8888
    report_interval: str = "30s"

    @property
    def report_interval_seconds(self) -> float:
        return _parse_duration(self.report_interval)


@dataclass
class LoggingConfig:
    format: str = "text"
    level: str = "info"


@dataclass
class ForcedDisconnectConfig:
    interval: str = "0"
    duration: str = "5s"

    @property
    def interval_seconds(self) -> float:
        return _parse_duration(self.interval)

    @property
    def duration_seconds(self) -> float:
        return _parse_duration(self.duration)


@dataclass
class RecoveryConfig:
    reconnect_interval: str = "1s"
    reconnect_max_interval: str = "30s"
    reconnect_multiplier: float = 2.0

    @property
    def reconnect_interval_seconds(self) -> float:
        return _parse_duration(self.reconnect_interval)

    @property
    def reconnect_max_interval_seconds(self) -> float:
        return _parse_duration(self.reconnect_max_interval)


@dataclass
class ShutdownConfig:
    drain_timeout_seconds: int = 10
    cleanup_channels: bool = True


@dataclass
class CORSConfig:
    origins: str = "*"


@dataclass
class OutputConfig:
    report_file: str = ""
    sdk_version: str = ""


@dataclass
class ThresholdsConfig:
    max_loss_pct: float = 0.0
    max_events_loss_pct: float = 5.0
    max_duplication_pct: float = 0.1
    max_p99_latency_ms: float = 1000.0
    max_p999_latency_ms: float = 5000.0
    min_throughput_pct: float = 90.0
    max_error_rate_pct: float = 1.0
    max_memory_growth_factor: float = 2.0
    max_downtime_pct: float = 10.0
    max_duration: str = "168h"

    @property
    def max_duration_seconds(self) -> float:
        return _parse_duration(self.max_duration)


# ---------------------------------------------------------------------------
# Main Config (v2)
# ---------------------------------------------------------------------------

ALL_PATTERN_NAMES = ["events", "events_store", "queue_stream", "queue_simple", "commands", "queries"]
PUBSUB_PATTERNS = {"events", "events_store"}
QUEUE_PATTERNS = {"queue_stream", "queue_simple"}
RPC_PATTERNS = {"commands", "queries"}

_DEFAULT_RATES: dict[str, int] = {
    "events": 100, "events_store": 100,
    "queue_stream": 50, "queue_simple": 100,
    "commands": 100, "queries": 100,
}

_DEFAULT_LOSS_PCT: dict[str, float] = {
    "events": 5.0, "events_store": 0.0,
    "queue_stream": 0.0, "queue_simple": 0.0,
}


def _default_patterns() -> dict[str, PatternConfig]:
    """Create default pattern configs for all 6 patterns."""
    return {
        "events": PatternConfig(rate=100),
        "events_store": PatternConfig(rate=100),
        "queue_stream": PatternConfig(rate=50),
        "queue_simple": PatternConfig(rate=100),
        "commands": PatternConfig(rate=100),
        "queries": PatternConfig(rate=100),
    }


@dataclass
class Config:
    version: str = "2"
    broker: BrokerConfig = field(default_factory=BrokerConfig)
    mode: str = "soak"
    duration: str = "1h"
    run_id: str = ""
    patterns: dict[str, PatternConfig] = field(default_factory=_default_patterns)
    warmup: WarmupConfig = field(default_factory=WarmupConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    rpc: RPCConfig = field(default_factory=RPCConfig)
    message: MessageConfig = field(default_factory=MessageConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    forced_disconnect: ForcedDisconnectConfig = field(default_factory=ForcedDisconnectConfig)
    recovery: RecoveryConfig = field(default_factory=RecoveryConfig)
    shutdown: ShutdownConfig = field(default_factory=ShutdownConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    thresholds: ThresholdsConfig = field(default_factory=ThresholdsConfig)
    cors: CORSConfig = field(default_factory=CORSConfig)

    @property
    def duration_seconds(self) -> float:
        return _parse_duration(self.duration)

    @property
    def warmup_duration_seconds(self) -> float:
        return _parse_duration(self.warmup.warmup_duration)

    def validate(self) -> list[str]:
        """Validate config, returning list of errors/warnings."""
        errors: list[str] = []
        warnings: list[str] = []

        if not self.broker.address:
            errors.append("broker.address is required")
        if self.mode not in ("soak", "benchmark"):
            errors.append(f"mode must be 'soak' or 'benchmark', got '{self.mode}'")
        if self.duration and self.duration != "0" and not _valid_duration(self.duration):
            errors.append(f"invalid duration format: {self.duration}")
        if self.duration_seconds <= 0 and self.mode == "soak":
            errors.append("duration must be > 0 for soak mode")
        if self.message.size_mode not in ("fixed", "distribution"):
            errors.append(f"message.size_mode must be 'fixed' or 'distribution', got '{self.message.size_mode}'")
        if self.message.size_bytes < 64:
            errors.append(f"message.size_bytes: must be >= 64, got {self.message.size_bytes}")
        if self.api.port <= 0 or self.api.port > 65535:
            errors.append(f"api.port: must be 1-65535, got {self.api.port}")
        if self.shutdown.drain_timeout_seconds <= 0:
            errors.append(f"shutdown.drain_timeout_seconds: must be > 0, got {self.shutdown.drain_timeout_seconds}")

        # Pattern validation
        enabled_count = 0
        total_workers = 0
        total_rate_pubsub = 0
        total_rate_queues = 0
        total_rate_cq = 0

        for pname, pc in self.patterns.items():
            if not pc.enabled:
                continue
            enabled_count += 1

            # channels
            if not isinstance(pc.channels, int) or isinstance(pc.channels, bool):
                errors.append(f"{pname}.channels: must be integer, got {type(pc.channels).__name__}")
            elif pc.channels < 1 or pc.channels > 1000:
                errors.append(f"{pname}.channels: must be 1-1000, got {pc.channels}")

            # rate
            if pc.rate < 0:
                errors.append(f"{pname}.rate: must be >= 0, got {pc.rate}")

            if pname in RPC_PATTERNS:
                if pc.senders_per_channel < 1:
                    errors.append(f"{pname}.senders_per_channel: must be >= 1, got {pc.senders_per_channel}")
                if pc.responders_per_channel < 1:
                    errors.append(f"{pname}.responders_per_channel: must be >= 1, got {pc.responders_per_channel}")
                if pc.senders_per_channel > 100:
                    warnings.append(f"{pname}.senders_per_channel: {pc.senders_per_channel} exceeds recommended max 100")
                if pc.responders_per_channel > 100:
                    warnings.append(f"{pname}.responders_per_channel: {pc.responders_per_channel} exceeds recommended max 100")
                total_workers += pc.channels * (pc.senders_per_channel + pc.responders_per_channel)
            else:
                if pc.producers_per_channel < 1:
                    errors.append(f"{pname}.producers_per_channel: must be >= 1, got {pc.producers_per_channel}")
                if pc.consumers_per_channel < 1:
                    errors.append(f"{pname}.consumers_per_channel: must be >= 1, got {pc.consumers_per_channel}")
                if pc.producers_per_channel > 100:
                    warnings.append(f"{pname}.producers_per_channel: {pc.producers_per_channel} exceeds recommended max 100")
                if pc.consumers_per_channel > 100:
                    warnings.append(f"{pname}.consumers_per_channel: {pc.consumers_per_channel} exceeds recommended max 100")
                total_workers += pc.channels * (pc.producers_per_channel + pc.consumers_per_channel)

            # Aggregate rate per client type
            if pname in PUBSUB_PATTERNS:
                total_rate_pubsub += pc.channels * pc.rate
            elif pname in QUEUE_PATTERNS:
                total_rate_queues += pc.channels * pc.rate
            elif pname in RPC_PATTERNS:
                total_rate_cq += pc.channels * pc.rate

            # consumer_group type validation (pub/sub only)
            if pname in PUBSUB_PATTERNS and not isinstance(pc.consumer_group, bool):
                errors.append(f"{pname}.consumer_group: must be boolean")

            # Threshold validation
            if pc.thresholds:
                t = pc.thresholds
                if "max_loss_pct" in t and (t["max_loss_pct"] < 0 or t["max_loss_pct"] > 100):
                    errors.append(f"{pname}.thresholds.max_loss_pct: must be 0-100")
                if "max_duplication_pct" in t and (t["max_duplication_pct"] < 0 or t["max_duplication_pct"] > 100):
                    errors.append(f"{pname}.thresholds.max_duplication_pct: must be 0-100")
                if "max_p99_latency_ms" in t and t["max_p99_latency_ms"] <= 0:
                    errors.append(f"{pname}.thresholds.max_p99_latency_ms: must be > 0")
                if "max_p999_latency_ms" in t and t["max_p999_latency_ms"] <= 0:
                    errors.append(f"{pname}.thresholds.max_p999_latency_ms: must be > 0")
                if "min_throughput_pct" in t and (t["min_throughput_pct"] <= 0 or t["min_throughput_pct"] > 100):
                    errors.append(f"{pname}.thresholds.min_throughput_pct: must be > 0 and <= 100")
                if "max_memory_growth_factor" in t and t["max_memory_growth_factor"] < 1.0:
                    errors.append(f"{pname}.thresholds.max_memory_growth_factor: must be >= 1.0")
                if "max_error_rate_pct" in t and (t["max_error_rate_pct"] < 0 or t["max_error_rate_pct"] > 100):
                    errors.append(f"{pname}.thresholds.max_error_rate_pct: must be 0-100")
                if "max_downtime_pct" in t and (t["max_downtime_pct"] < 0 or t["max_downtime_pct"] > 100):
                    errors.append(f"{pname}.thresholds.max_downtime_pct: must be 0-100")

        if enabled_count == 0:
            errors.append("at least one pattern must be enabled")

        # Resource guard warnings
        if total_workers > 500:
            warnings.append(f"high worker count: {total_workers} -- may impact system resources")

        est_memory_mb = total_workers * (self.message.reorder_window * 8 / 1024 / 1024 + 0.5)
        if est_memory_mb > 4096:
            warnings.append(f"memory warning: estimated {est_memory_mb / 1024:.1f}GB overhead for {total_workers} workers -- ensure sufficient system memory")

        if total_rate_pubsub > 50000:
            warnings.append(f"high aggregate rate {total_rate_pubsub} msgs/s through pubSubClient -- may cause transport bottleneck")
        if total_rate_queues > 50000:
            warnings.append(f"high aggregate rate {total_rate_queues} msgs/s through queuesClient -- may cause transport bottleneck")
        if total_rate_cq > 50000:
            warnings.append(f"high aggregate rate {total_rate_cq} msgs/s through cqClient -- may cause transport bottleneck")

        # Global threshold validation
        th = self.thresholds
        if th.max_loss_pct < 0 or th.max_loss_pct > 100:
            errors.append("thresholds.max_loss_pct: must be 0-100")
        if th.max_duplication_pct < 0 or th.max_duplication_pct > 100:
            errors.append("thresholds.max_duplication_pct: must be 0-100")
        if th.max_p99_latency_ms <= 0:
            errors.append("thresholds.max_p99_latency_ms: must be > 0")
        if th.max_p999_latency_ms <= 0:
            errors.append("thresholds.max_p999_latency_ms: must be > 0")
        if th.min_throughput_pct <= 0 or th.min_throughput_pct > 100:
            errors.append("thresholds.min_throughput_pct: must be > 0 and <= 100")
        if th.max_memory_growth_factor < 1.0:
            errors.append("thresholds.max_memory_growth_factor: must be >= 1.0")
        if th.max_error_rate_pct < 0 or th.max_error_rate_pct > 100:
            errors.append("thresholds.max_error_rate_pct: must be 0-100")
        if th.max_downtime_pct < 0 or th.max_downtime_pct > 100:
            errors.append("thresholds.max_downtime_pct: must be 0-100")

        # Log warnings
        for w in warnings:
            _config_logger.warning(w)

        return errors


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------

_NESTED_TYPES: dict[str, type] = {
    "broker": BrokerConfig,
    "api": ApiConfig,
    "queue": QueueConfig,
    "rpc": RPCConfig,
    "message": MessageConfig,
    "metrics": MetricsConfig,
    "logging": LoggingConfig,
    "forced_disconnect": ForcedDisconnectConfig,
    "recovery": RecoveryConfig,
    "shutdown": ShutdownConfig,
    "output": OutputConfig,
    "thresholds": ThresholdsConfig,
    "cors": CORSConfig,
    "warmup": WarmupConfig,
}


def _from_dict(dc_class: type, data: dict[str, Any], path: str = "") -> Any:
    """Recursively build a dataclass from a dict, warning on unknown keys."""
    if not data:
        return dc_class()
    known_fields = set(dc_class.__dataclass_fields__)
    for key in data:
        if key not in known_fields:
            full_path = f"{path}.{key}" if path else key
            _config_logger.warning("unknown config key '%s' -- ignored", full_path)
    field_vals = {}
    for f in known_fields:
        if f not in data:
            continue
        val = data[f]
        nested_type = _NESTED_TYPES.get(f)
        if isinstance(val, dict) and nested_type is not None:
            child_path = f"{path}.{f}" if path else f
            field_vals[f] = _from_dict(nested_type, val, child_path)
        else:
            field_vals[f] = val
    return dc_class(**field_vals)


def _parse_pattern_config(data: dict[str, Any], pattern_name: str) -> PatternConfig:
    """Parse a single pattern config from a dict."""
    pc = PatternConfig()
    pc.enabled = data.get("enabled", True)
    pc.channels = data.get("channels", 1)
    pc.rate = data.get("rate", _DEFAULT_RATES.get(pattern_name, 100))
    pc.thresholds = data.get("thresholds", None)

    if pattern_name in RPC_PATTERNS:
        pc.senders_per_channel = data.get("senders_per_channel", 10)
        pc.responders_per_channel = data.get("responders_per_channel", 4)
    else:
        pc.producers_per_channel = data.get("producers_per_channel", 1)
        pc.consumers_per_channel = data.get("consumers_per_channel", 1)
        if pattern_name in PUBSUB_PATTERNS:
            pc.consumer_group = data.get("consumer_group", False)

    return pc


def _find_config_file(cli_path: str) -> str:
    """Find config file: env var -> CLI flag -> defaults."""
    import os
    env_path = os.environ.get("BURNIN_CONFIG_FILE", "")
    if env_path:
        return env_path
    if cli_path:
        return cli_path
    for candidate in ["./burnin-config.yaml", "/etc/burnin/config.yaml"]:
        if Path(candidate).is_file():
            return candidate
    return ""


def load_config(cli_path: str = "") -> Config:
    """Load config from YAML file. No env var overrides in v2."""
    config_path = _find_config_file(cli_path)

    if config_path and Path(config_path).is_file():
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

        # Build Config from raw YAML
        cfg = Config()
        cfg.version = str(raw.get("version", "2"))
        cfg.mode = raw.get("mode", "soak")
        cfg.duration = raw.get("duration", "1h")
        cfg.run_id = raw.get("run_id", "")

        # Nested configs
        for nested_name in ("broker", "api", "queue", "rpc", "message", "metrics", "logging",
                            "forced_disconnect", "recovery", "shutdown", "output",
                            "thresholds", "cors", "warmup"):
            if nested_name in raw and isinstance(raw[nested_name], dict):
                nested_type = _NESTED_TYPES[nested_name]
                setattr(cfg, nested_name, _from_dict(nested_type, raw[nested_name], nested_name))

        # Parse patterns
        patterns_raw = raw.get("patterns", {})
        if patterns_raw:
            cfg.patterns = {}
            for pname in ALL_PATTERN_NAMES:
                if pname in patterns_raw:
                    cfg.patterns[pname] = _parse_pattern_config(patterns_raw[pname], pname)
                else:
                    cfg.patterns[pname] = PatternConfig(
                        enabled=False,
                        rate=_DEFAULT_RATES.get(pname, 100),
                    )
    else:
        cfg = Config()

    # Environment variable override for broker address
    env_addr = os.environ.get("KUBEMQ_BROKER_ADDRESS", "")
    if env_addr:
        cfg.broker.address = env_addr

    # Fallback: if api.port is still the default (8080) and metrics.port differs,
    # use metrics.port as the authoritative HTTP server port.
    if cfg.api.port == 8080 and cfg.metrics.port != 8080:
        _config_logger.info(
            "api.port not set explicitly; falling back to metrics.port=%d",
            cfg.metrics.port,
        )
        cfg.api.port = cfg.metrics.port

    # Auto-generate run_id if empty
    if not cfg.run_id:
        cfg.run_id = secrets.token_hex(4)

    # Default warmup for benchmark mode
    if cfg.mode == "benchmark" and not cfg.warmup.warmup_duration:
        cfg.warmup.warmup_duration = "60s"

    return cfg


# ---------------------------------------------------------------------------
# Per-pattern thresholds resolved for verdict
# ---------------------------------------------------------------------------

@dataclass
class PatternThresholds:
    """Per-pattern threshold overrides resolved from API config."""
    max_loss_pct: float = 0.0
    max_p99_latency_ms: float = 1000.0
    max_p999_latency_ms: float = 5000.0


# ---------------------------------------------------------------------------
# Run context
# ---------------------------------------------------------------------------

@dataclass
class RunContext:
    """Per-run context derived from API config, alongside the flat Config."""
    enabled_patterns: set[str] = field(default_factory=set)
    pattern_thresholds: dict[str, PatternThresholds] = field(default_factory=dict)
    starting_timeout_seconds: int = 60
    all_patterns_enabled: bool = True
    run_id: str = ""
    channel_names: dict[str, list[str]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# v1 detection
# ---------------------------------------------------------------------------

_V1_OLD_FIELD_NAMES = {"producers", "consumers", "senders", "responders"}

_DURATION_RE = re.compile(r"^\d+[smhd]$")


def _valid_duration(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    if s == "0":
        return True
    return bool(_DURATION_RE.match(s))


def _detect_v1_format(body: dict[str, Any]) -> list[str]:
    """Detect v1 config format and return error messages."""
    errors: list[str] = []

    # Layer 1: top-level v1 keys
    if "concurrency" in body:
        errors.append("detected v1 field: concurrency -- use patterns block instead")
    if "rates" in body:
        errors.append("detected v1 field: rates -- use patterns block instead")

    # Layer 2: old field names in patterns
    patterns_body = body.get("patterns", {})
    for pname, pcfg in patterns_body.items():
        if not isinstance(pcfg, dict):
            continue
        for old_field in _V1_OLD_FIELD_NAMES:
            if old_field in pcfg:
                errors.append(f"detected v1 field: patterns.{pname}.{old_field} -- use {old_field}_per_channel")

    return errors


# ---------------------------------------------------------------------------
# API config translation (v2)
# ---------------------------------------------------------------------------

def translate_api_config(
    body: dict[str, Any],
    startup_cfg: Config,
) -> tuple[Config, RunContext, list[str]]:
    """Translate v2 API config -> internal Config + RunContext.

    Returns (config, run_context, validation_errors).
    If validation_errors is non-empty, the run should NOT be started (400 response).
    """
    errors: list[str] = []

    # v1 detection
    v1_errors = _detect_v1_format(body)
    if v1_errors:
        return Config(), RunContext(), [
            "v1 config format not supported. Update to v2 patterns format."
        ] + v1_errors

    # Version field (optional but recognized)
    version = body.get("version", "2")
    if str(version) not in ("2", "2.0", "2.1"):
        errors.append(f"unsupported config version: {version}")

    cfg = Config()
    # Broker: default from startup, allow API override
    broker_body = body.get("broker", {})
    broker_address = startup_cfg.broker.address
    if isinstance(broker_body, dict) and broker_body.get("address"):
        broker_address = str(broker_body["address"])
    cfg.broker = BrokerConfig(
        address=broker_address,
        client_id_prefix=startup_cfg.broker.client_id_prefix,
    )
    cfg.recovery = startup_cfg.recovery
    cfg.logging = startup_cfg.logging
    cfg.api = startup_cfg.api
    cfg.metrics = MetricsConfig(port=startup_cfg.metrics.port,
                                report_interval=startup_cfg.metrics.report_interval)
    cfg.output = startup_cfg.output
    cfg.cors = startup_cfg.cors

    # --- Top-level fields ---
    mode = body.get("mode", "soak")
    if mode not in ("soak", "benchmark"):
        errors.append(f"mode must be 'soak' or 'benchmark', got '{mode}'")
    cfg.mode = mode

    duration = body.get("duration", "1h")
    if duration != "0" and not _valid_duration(duration):
        errors.append(f"duration must match \\d+(s|m|h|d) or be '0', got '{duration}'")
    cfg.duration = duration

    run_id = body.get("run_id", "")
    if not run_id:
        run_id = secrets.token_hex(4)
    cfg.run_id = run_id

    # Warmup config
    warmup_body = body.get("warmup", {})
    warmup_duration = warmup_body.get("warmup_duration", body.get("warmup_duration", ""))
    if not warmup_duration:
        warmup_duration = "60s" if mode == "benchmark" else "0s"
    cfg.warmup = WarmupConfig(
        max_parallel_channels=warmup_body.get("max_parallel_channels", 10),
        timeout_per_channel_ms=warmup_body.get("timeout_per_channel_ms", 5000),
        warmup_duration=warmup_duration,
    )

    starting_timeout = body.get("starting_timeout_seconds", 60)
    if isinstance(starting_timeout, (int, float)) and starting_timeout <= 0:
        errors.append("starting_timeout_seconds must be > 0")
    starting_timeout = int(starting_timeout) if isinstance(starting_timeout, (int, float)) else 60

    # --- Patterns (v2) ---
    patterns_body = body.get("patterns", {})
    enabled: set[str] = set()
    pt_map: dict[str, PatternThresholds] = {}
    cfg.patterns = {}

    for pname in ALL_PATTERN_NAMES:
        pcfg_body = patterns_body.get(pname, {})
        is_enabled = pcfg_body.get("enabled", True)
        if is_enabled:
            enabled.add(pname)

        pc = PatternConfig()
        pc.enabled = is_enabled
        pc.channels = pcfg_body.get("channels", 1)
        pc.rate = pcfg_body.get("rate", _DEFAULT_RATES.get(pname, 100))

        # Validate channels
        if is_enabled:
            if not isinstance(pc.channels, int) or isinstance(pc.channels, bool):
                errors.append(f"{pname}.channels: must be integer, got {type(pc.channels).__name__}")
            elif pc.channels < 1 or pc.channels > 1000:
                errors.append(f"{pname}.channels: must be 1-1000, got {pc.channels}")

            if pc.rate < 0:
                errors.append(f"{pname}.rate: must be >= 0, got {pc.rate}")
            if mode == "soak" and pc.rate <= 0:
                errors.append(f"patterns.{pname}.rate: must be > 0, got {pc.rate}")

        if pname in RPC_PATTERNS:
            pc.senders_per_channel = pcfg_body.get("senders_per_channel", 10)
            pc.responders_per_channel = pcfg_body.get("responders_per_channel", 4)
            if is_enabled and pc.senders_per_channel < 1:
                errors.append(f"{pname}.senders_per_channel: must be >= 1, got {pc.senders_per_channel}")
            if is_enabled and pc.responders_per_channel < 1:
                errors.append(f"{pname}.responders_per_channel: must be >= 1, got {pc.responders_per_channel}")
        else:
            pc.producers_per_channel = pcfg_body.get("producers_per_channel", 1)
            pc.consumers_per_channel = pcfg_body.get("consumers_per_channel", 1)
            if is_enabled and pc.producers_per_channel < 1:
                errors.append(f"{pname}.producers_per_channel: must be >= 1, got {pc.producers_per_channel}")
            if is_enabled and pc.consumers_per_channel < 1:
                errors.append(f"{pname}.consumers_per_channel: must be >= 1, got {pc.consumers_per_channel}")
            if pname in PUBSUB_PATTERNS:
                cg_val = pcfg_body.get("consumer_group", False)
                if not isinstance(cg_val, bool):
                    errors.append(f"{pname}.consumer_group: must be boolean")
                pc.consumer_group = cg_val

        # Per-pattern thresholds
        t_cfg = pcfg_body.get("thresholds", {})
        loss_default = _DEFAULT_LOSS_PCT.get(pname, 0.0)
        pt = PatternThresholds(
            max_loss_pct=t_cfg.get("max_loss_pct", loss_default),
            max_p99_latency_ms=t_cfg.get("max_p99_latency_ms", 1000.0),
            max_p999_latency_ms=t_cfg.get("max_p999_latency_ms", 5000.0),
        )
        if pt.max_loss_pct < 0 or pt.max_loss_pct > 100:
            errors.append(f"patterns.{pname}.thresholds.max_loss_pct: must be 0-100, got {pt.max_loss_pct}")
        pt_map[pname] = pt
        pc.thresholds = t_cfg if t_cfg else None
        cfg.patterns[pname] = pc

    if not enabled:
        errors.append("at least one pattern must be enabled")

    # --- Queue config ---
    q = body.get("queue", {})
    cfg.queue = QueueConfig(
        poll_max_messages=q.get("poll_max_messages", 10),
        poll_wait_timeout_seconds=q.get("poll_wait_timeout_seconds", 5),
        auto_ack=q.get("auto_ack", False),
        max_depth=q.get("max_depth", 1_000_000),
    )

    # --- RPC config ---
    r = body.get("rpc", {})
    cfg.rpc = RPCConfig(timeout_ms=r.get("timeout_ms", 5000))
    if cfg.rpc.timeout_ms <= 0:
        errors.append("rpc.timeout_ms must be > 0")

    # --- Message config ---
    m = body.get("message", {})
    size_mode = m.get("size_mode", "fixed")
    size_bytes = m.get("size_bytes", 1024)
    reorder = m.get("reorder_window", 10_000)
    if size_mode not in ("fixed", "distribution"):
        errors.append(f"message.size_mode must be 'fixed' or 'distribution', got '{size_mode}'")
    if size_bytes < 64:
        errors.append(f"message.size_bytes must be >= 64, got {size_bytes}")
    if reorder < 100:
        errors.append(f"message.reorder_window must be >= 100, got {reorder}")
    cfg.message = MessageConfig(
        size_mode=size_mode, size_bytes=size_bytes,
        size_distribution=m.get("size_distribution", "256:80,4096:15,65536:5"),
        reorder_window=reorder,
    )

    # --- Global thresholds ---
    th = body.get("thresholds", {})
    cfg.thresholds = ThresholdsConfig(
        max_loss_pct=th.get("max_loss_pct", 0.0),
        max_events_loss_pct=th.get("max_events_loss_pct", 5.0),
        max_duplication_pct=th.get("max_duplication_pct", 0.1),
        max_p99_latency_ms=th.get("max_p99_latency_ms", 1000.0),
        max_p999_latency_ms=th.get("max_p999_latency_ms", 5000.0),
        max_error_rate_pct=th.get("max_error_rate_pct", 1.0),
        max_memory_growth_factor=th.get("max_memory_growth_factor", 2.0),
        max_downtime_pct=th.get("max_downtime_pct", 10.0),
        min_throughput_pct=th.get("min_throughput_pct", 90.0),
        max_duration=th.get("max_duration", "168h"),
    )
    for fname in ("max_duplication_pct", "max_error_rate_pct", "max_downtime_pct", "min_throughput_pct"):
        val = getattr(cfg.thresholds, fname)
        if val < 0 or val > 100:
            errors.append(f"thresholds.{fname}: must be 0-100, got {val}")

    # --- Forced disconnect ---
    fd = body.get("forced_disconnect", {})
    cfg.forced_disconnect = ForcedDisconnectConfig(
        interval=fd.get("interval", "0"),
        duration=fd.get("duration", "5s"),
    )

    # --- Shutdown ---
    sd = body.get("shutdown", {})
    drain = sd.get("drain_timeout_seconds", 10)
    if isinstance(drain, (int, float)) and drain <= 0:
        errors.append("shutdown.drain_timeout_seconds must be > 0")
    cfg.shutdown = ShutdownConfig(
        drain_timeout_seconds=int(drain) if isinstance(drain, (int, float)) else 10,
        cleanup_channels=sd.get("cleanup_channels", True),
    )

    # --- Metrics interval override ---
    met = body.get("metrics", {})
    if "report_interval" in met:
        cfg.metrics.report_interval = met["report_interval"]

    ctx = RunContext(
        enabled_patterns=enabled,
        pattern_thresholds=pt_map,
        starting_timeout_seconds=starting_timeout,
        all_patterns_enabled=len(enabled) == len(ALL_PATTERN_NAMES),
        run_id=run_id,
    )

    return cfg, ctx, errors

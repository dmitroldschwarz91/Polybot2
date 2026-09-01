"""Structured logging — refactored SLog.

Preserves the original's colourised console output + rotating JSON file output,
but is now a proper configurable logger instead of a module singleton.
"""

from __future__ import annotations

import io
import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


class JSONFmt(logging.Formatter):
    def format(self, r: logging.LogRecord) -> str:
        o = {
            "ts": datetime.fromtimestamp(r.created, tz=timezone.utc).isoformat(),
            "level": r.levelname,
            "msg": r.getMessage(),
        }
        extra = getattr(r, "extra", None)
        if isinstance(extra, dict):
            o.update(extra)
        return json.dumps(o, ensure_ascii=False, default=str)


class HumanFmt(logging.Formatter):
    C = {
        "DEBUG": "\033[36m", "INFO": "\033[32m", "WARNING": "\033[33m",
        "ERROR": "\033[31m", "CRITICAL": "\033[35m",
    }
    R = "\033[0m"

    def format(self, r: logging.LogRecord) -> str:
        c = self.C.get(r.levelname, "")
        ts = self.formatTime(r, "%H:%M:%S")
        m = f"{ts} {c}[{r.levelname:7}]{self.R} {r.getMessage()}"
        extra = getattr(r, "extra", None)
        if extra:
            m += f" {c}({' | '.join(f'{k}={v}' for k, v in extra.items())}){self.R}"
        return m


class _SafeStreamHandler(logging.StreamHandler):
    """StreamHandler that never crashes on Unicode (Windows cp1251 console).

    The default Windows console encoding (cp1251/cp1252) cannot represent the
    box-drawing / symbol characters we use in log lines (✓ ✗ → …). A bare
    StreamHandler.emit() then raises UnicodeEncodeError, which the logging
    module only reports to stderr and SILENTLY DROPS the record. We catch the
    error and re-emit with unrepresentable characters replaced, so no log line
    is ever lost. (_get_stdout() reconfiguring stdout to UTF-8 usually keeps
    the symbols intact; this replacement is the last-resort safety net.)
    """

    def emit(self, r: logging.LogRecord) -> None:
        # NOTE: we must NOT call super().emit() here — the base StreamHandler
        # wraps stream.write in its own try/except and swallows the
        # UnicodeEncodeError via handleError(), so our except below would never
        # run. We do the format+write ourselves and intercept the encoding
        # error before logging can drop the record.
        try:
            msg = self.format(r)
            self.stream.write(msg + self.terminator)
            self.flush()
        except UnicodeEncodeError:
            # console can't encode some chars (✓ ✗ → …) — re-emit with
            # unrepresentable characters replaced, so the line is not lost.
            enc = getattr(self.stream, "encoding", None) or "utf-8"
            safe = self.format(r).encode(enc, errors="replace").decode(enc, errors="replace")
            try:
                self.stream.write(safe + self.terminator)
                self.flush()
            except Exception:
                self.handleError(r)
        except RecursionError:
            raise
        except Exception:
            self.handleError(r)


class StructuredLogger:
    """Thin wrapper exposing the original `.info(msg, **kwargs)` API."""

    # Reuse a single stdout wrapper so it is never GC'd (which would close
    # the underlying sys.stdout.buffer and break other loggers).
    _stdout_wrapper = None

    def __init__(self, name: str, level: int = logging.INFO,
                 json_file: Optional[str] = None) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()
        self.logger.propagate = False

        ch = _SafeStreamHandler(self._get_stdout())
        ch.setFormatter(HumanFmt())
        self.logger.addHandler(ch)

        if json_file:
            Path(json_file).parent.mkdir(parents=True, exist_ok=True)
            # encoding="utf-8" is MANDATORY on Windows: RotatingFileHandler
            # otherwise opens the file with the locale default (cp1251) and
            # raises UnicodeEncodeError on ✓/✗/→ exactly like the console.
            fh = RotatingFileHandler(json_file, maxBytes=10 * 1024 * 1024,
                                     backupCount=5, encoding="utf-8")
            fh.setFormatter(JSONFmt())
            self.logger.addHandler(fh)

    @classmethod
    def _get_stdout(cls):
        if cls._stdout_wrapper is None:
            # Best option: keep the terminal's OWN encoding (so it can render
            # what it understands — no mojibake), but switch the error handler
            # to 'replace'. The default is 'strict', which is what crashes on
            # ✓/✗/→ on a cp1251 console. With 'replace' those become '?'.
            cur_enc = getattr(sys.stdout, "encoding", None) or "utf-8"
            try:
                sys.stdout.reconfigure(encoding=cur_enc, errors="replace")  # type: ignore[attr-defined]
                cls._stdout_wrapper = sys.stdout
            except (AttributeError, ValueError):
                # Fallback: wrap the raw buffer with the same encoding + replace.
                try:
                    cls._stdout_wrapper = io.TextIOWrapper(
                        sys.stdout.buffer, encoding=cur_enc, errors="replace",
                        line_buffering=True,
                    )
                except AttributeError:
                    cls._stdout_wrapper = sys.stdout
        return cls._stdout_wrapper

    def _log(self, lv: int, m: str, **k) -> None:
        self.logger.log(lv, m, extra={"extra": k} if k else {})

    def info(self, m: str, **k) -> None: self._log(logging.INFO, m, **k)
    def warning(self, m: str, **k) -> None: self._log(logging.WARNING, m, **k)
    def error(self, m: str, **k) -> None: self._log(logging.ERROR, m, **k)
    def debug(self, m: str, **k) -> None: self._log(logging.DEBUG, m, **k)


def build_logger(name: str, json_file: Optional[str] = None) -> StructuredLogger:
    return StructuredLogger(name, json_file=json_file)

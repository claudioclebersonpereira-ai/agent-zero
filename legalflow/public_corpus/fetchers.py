from __future__ import annotations

import atexit
import os
import time
from dataclasses import dataclass
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlparse, urlencode

import ssl


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: int | None
    text: str | None
    error: str | None = None


class Fetcher(Protocol):
    def fetch_text(self, url: str) -> FetchResult: ...
    def post_form(self, url: str, form: dict[str, str], *, headers: dict[str, str] | None = None) -> FetchResult: ...


class HttpFetcher:
    def __init__(self, *, user_agent: str | None = None, timeout_s: float = 30.0):
        self._timeout_s = timeout_s
        self._user_agent = user_agent or "legalflow-ingest/0.1"
        self._ssl_context = _build_ssl_context()

    def fetch_text(self, url: str) -> FetchResult:
        req = Request(url, headers={"User-Agent": self._user_agent})
        try:
            with urlopen(req, timeout=self._timeout_s, context=self._ssl_context) as resp:  # noqa: S310
                status = getattr(resp, "status", None)
                data = resp.read()
                encoding = resp.headers.get_content_charset() or "utf-8"
                text = data.decode(encoding, "replace")
                return FetchResult(url=url, status=status, text=text)
        except HTTPError as e:
            try:
                body = e.read().decode("utf-8", "replace")
            except Exception:
                body = None
            return FetchResult(url=url, status=e.code, text=body, error=str(e))
        except URLError as e:
            return FetchResult(url=url, status=None, text=None, error=str(e))
        except Exception as e:
            return FetchResult(url=url, status=None, text=None, error=str(e))

    def post_form(self, url: str, form: dict[str, str], *, headers: dict[str, str] | None = None) -> FetchResult:
        data = urlencode(form).encode("utf-8")
        req_headers = {"User-Agent": self._user_agent, "Content-Type": "application/x-www-form-urlencoded"}
        if headers:
            req_headers.update(headers)
        req = Request(url, data=data, headers=req_headers)
        try:
            with urlopen(req, timeout=self._timeout_s, context=self._ssl_context) as resp:  # noqa: S310
                status = getattr(resp, "status", None)
                raw = resp.read()
                encoding = resp.headers.get_content_charset() or "utf-8"
                text = raw.decode(encoding, "replace")
                return FetchResult(url=url, status=status, text=text)
        except HTTPError as e:
            try:
                body = e.read().decode("utf-8", "replace")
            except Exception:
                body = None
            return FetchResult(url=url, status=e.code, text=body, error=str(e))
        except URLError as e:
            return FetchResult(url=url, status=None, text=None, error=str(e))
        except Exception as e:
            return FetchResult(url=url, status=None, text=None, error=str(e))


class PlaywrightFetcher:
    def __init__(self, *, timeout_ms: int = 45000):
        self._timeout_ms = timeout_ms
        self._started = False
        self._pw = None
        self._browser = None
        self._ctx = None
        self._primed: set[str] = set()

    def _ensure_started(self) -> None:
        if self._started:
            return
        from python.helpers import files
        from python.helpers.playwright import ensure_playwright_binary

        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", files.get_abs_path("tmp/playwright"))
        pw_binary = ensure_playwright_binary()

        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True, executable_path=str(pw_binary))
        self._ctx = self._browser.new_context(ignore_https_errors=True)
        atexit.register(self.close)
        self._started = True

    def close(self) -> None:
        ctx, browser, pw = self._ctx, self._browser, self._pw
        self._ctx = None
        self._browser = None
        self._pw = None
        self._started = False
        try:
            if ctx is not None:
                ctx.close()
        finally:
            try:
                if browser is not None:
                    browser.close()
            finally:
                if pw is not None:
                    pw.stop()

    def _prime(self, url: str) -> None:
        self._ensure_started()
        assert self._ctx is not None
        page = self._ctx.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            # Some STF endpoints only allow XHR POSTs after a full browser-like
            # navigation (JS/WAF challenge tokens). Give it a moment to settle.
            try:
                page.wait_for_load_state("networkidle", timeout=3_000)
            except Exception:
                pass
            time.sleep(0.8)
        finally:
            page.close()

    def fetch_text(self, url: str) -> FetchResult:
        try:
            self._ensure_started()
            assert self._ctx is not None

            # STF classic search UI: request API alone often isn't enough to
            # establish cookies / challenge tokens required for subsequent XHRs.
            if "portal.stf.jus.br/jurisprudencia/aplicacaosumula.asp" in url:
                page = self._ctx.new_page()
                try:
                    nav = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
                    time.sleep(0.4)
                    html = page.content()
                    status = getattr(nav, "status", None) if nav is not None else 200
                    return FetchResult(url=url, status=status, text=html)
                finally:
                    page.close()

            # Prefer Playwright's request API (fast, no full page navigation).
            resp = self._ctx.request.get(url, timeout=self._timeout_ms)
            try:
                text = resp.text()
            except Exception:
                text = None

            if text:
                return FetchResult(url=url, status=getattr(resp, "status", None), text=text)

            # Fallback: render the page (useful when content is produced via JS/WAF).
            page = self._ctx.new_page()
            try:
                nav = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
                time.sleep(0.2)
                html = page.content()
                status = getattr(nav, "status", None) if nav is not None else 200
                return FetchResult(url=url, status=status, text=html)
            finally:
                page.close()
        except Exception as e:
            return FetchResult(url=url, status=None, text=None, error=str(e))

    def post_form(self, url: str, form: dict[str, str], *, headers: dict[str, str] | None = None) -> FetchResult:
        try:
            self._ensure_started()
            assert self._ctx is not None

            referer = (headers or {}).get("Referer") if headers else None
            if referer and referer not in self._primed:
                try:
                    self._prime(referer)
                finally:
                    self._primed.add(referer)

            resp = self._ctx.request.post(url, form=form, headers=headers, timeout=self._timeout_ms)
            if getattr(resp, "status", None) == 403 and referer:
                # Retry once after an explicit prime (handles intermittent STF/WAF behavior).
                try:
                    self._prime(referer)
                    resp = self._ctx.request.post(url, form=form, headers=headers, timeout=self._timeout_ms)
                except Exception:
                    pass
            try:
                text = resp.text()
            except Exception:
                text = None
            return FetchResult(url=url, status=getattr(resp, "status", None), text=text)
        except Exception as e:
            return FetchResult(url=url, status=None, text=None, error=str(e))


class AutoFetcher:
    def __init__(self):
        self._http = HttpFetcher()
        self._pw = PlaywrightFetcher()

    def fetch_text(self, url: str) -> FetchResult:
        first = self._http.fetch_text(url)
        host = (urlparse(url).hostname or "").lower()
        should_try_playwright = False
        if first.status == 403:
            should_try_playwright = True
        elif host.endswith("stf.jus.br") and (first.status is None or (first.status and first.status >= 400)):
            # STF often blocks/breaks from this environment (403) and can also
            # fail certificate validation depending on the local Python install.
            should_try_playwright = True
        elif first.text is None and first.error:
            if "CERTIFICATE_VERIFY_FAILED" in first.error:
                should_try_playwright = True

        if should_try_playwright:
            second = self._pw.fetch_text(url)
            if second.text:
                return second
        return first

    def post_form(self, url: str, form: dict[str, str], *, headers: dict[str, str] | None = None) -> FetchResult:
        first = self._http.post_form(url, form, headers=headers)
        host = (urlparse(url).hostname or "").lower()
        should_try_playwright = False
        if first.status == 403:
            should_try_playwright = True
        elif host.endswith("stf.jus.br") and (first.status is None or (first.status and first.status >= 400)):
            should_try_playwright = True
        elif first.text is None and first.error:
            if "CERTIFICATE_VERIFY_FAILED" in first.error:
                should_try_playwright = True

        if should_try_playwright:
            second = self._pw.post_form(url, form, headers=headers)
            if second.text:
                return second
        return first


def _build_ssl_context() -> ssl.SSLContext:
    """
    Build an SSL context that uses certifi when available.

    Some environments (notably macOS/Homebrew Python) may not have a usable
    system CA bundle configured, causing CERTIFICATE_VERIFY_FAILED for some sites.
    """
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()

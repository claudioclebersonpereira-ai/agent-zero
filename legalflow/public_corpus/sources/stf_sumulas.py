from __future__ import annotations

import json
import re
import ssl
from dataclasses import dataclass
from datetime import date
from typing import Iterator
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.request import HTTPSHandler, HTTPCookieProcessor, Request, build_opener

from ..fetchers import Fetcher
from ..model import DiscoveredDoc, ParsedDoc


_HREF_RE = re.compile(r'href="(?P<href>[^"]+)"', re.IGNORECASE)
_SUMULA_NUM_RE = re.compile(r"sumula\s*(?:n[ºo]\s*)?(\d+)", re.IGNORECASE)
_TEXT_BLOCK_RE = re.compile(r'<div[^>]+class="texto-sumula"[^>]*>(?P<body>.*?)</div>', re.IGNORECASE | re.DOTALL)

_SEARCH_PAGE_URL = "https://portal.stf.jus.br/jurisprudencia/aplicacaosumula.asp"
_SEARCH_API_URL = "https://portal.stf.jus.br/jurisprudencia/aplicacaosumulapesquisa.asp"
_DETAIL_URL = "https://portal.stf.jus.br/jurisprudencia/sumariosumulas.asp"

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _strip_tags(html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\xa0", " ")
    return "\n".join(line.strip() for line in text.splitlines()).strip()


def parse_stf_listing(html: str, *, base_url: str, doc_type: str) -> list[DiscoveredDoc]:
    docs: list[DiscoveredDoc] = []
    for m in _HREF_RE.finditer(html):
        href = m.group("href")
        if "sumula" not in href.lower():
            continue
        num_m = re.search(r"(?:sumula=|sumula\s*)(\d+)", href, flags=re.IGNORECASE)
        if not num_m:
            continue
        num = int(num_m.group(1))
        url = urljoin(base_url, href)
        doc_id = f"stf:{doc_type}:{num}"
        docs.append(
            DiscoveredDoc(
                source="stf",
                doc_type=doc_type,
                doc_id=doc_id,
                url=url,
                title=f"STF {doc_type.replace('_', ' ')} {num}",
                published_date=None,
                extra={"number": num},
            )
        )
    # stable de-dupe
    seen: set[str] = set()
    out: list[DiscoveredDoc] = []
    for d in docs:
        if d.doc_id in seen:
            continue
        seen.add(d.doc_id)
        out.append(d)
    return out


def parse_stf_sumula_detail(html: str, *, discovered: DiscoveredDoc) -> ParsedDoc:
    title = discovered.title or "STF Súmula"
    # Best-effort: pull a dedicated text block if present; otherwise fallback to stripped page text.
    block_m = _TEXT_BLOCK_RE.search(html)
    if block_m:
        statement = _strip_tags(block_m.group("body"))
    else:
        statement = _strip_tags(html)

    num = None
    m = _SUMULA_NUM_RE.search(statement) or _SUMULA_NUM_RE.search(title)
    if m:
        num = int(m.group(1))
        if discovered.doc_type == "sumula_vinculante":
            title = f"Súmula Vinculante {num}"
        else:
            title = f"Súmula {num}"

    body = f"# {title}\n\n{statement.strip()}\n"
    return ParsedDoc(
        source=discovered.source,
        doc_type=discovered.doc_type,
        doc_id=discovered.doc_id,
        url=discovered.url,
        title=title,
        published_date=None,
        body_markdown=body,
        citations=[discovered.url],
        extra={"number": num} if num is not None else {},
    )


def _discover_via_search_api(
    *, fetcher: Fetcher, doc_type: str, base: str, limit: int | None = None
) -> list[DiscoveredDoc]:
    """
    STF sumulas discovery fallback.

    The historic listing pages may return a 404-like shell or WAF challenge
    content in some environments. The STF search UI (`aplicacaosumula.asp`)
    exposes a JSON POST endpoint (`aplicacaosumulapesquisa.asp`) that resolves
    a sumula number -> internal link id -> detail page.

    This fallback uses the configured Fetcher for the POST endpoint. In
    environments where STF blocks non-browser clients or Python TLS fails,
    AutoFetcher/PlaywrightFetcher provides a browser-like request path.
    """
    yielded = 0
    consecutive_misses = 0
    consecutive_failures = 0
    max_consecutive_misses = 25
    max_number = 2000
    discovered: list[DiscoveredDoc] = []

    # Best-effort: establish an ASP session first (important for classic flows).
    try:
        fetcher.fetch_text(_SEARCH_PAGE_URL)
    except Exception:
        pass

    for number in range(1, max_number + 1):
        if limit is not None and yielded >= limit:
            return discovered

        resp = fetcher.post_form(
            _SEARCH_API_URL,
            {
                "base": base,
                "texto": "",
                "numero": str(number),
                "ramo": "",
            },
            headers=None,
        )

        # Treat non-200s as transient failures (WAF/rate-limit/cert issues) and
        # do not let them prematurely trigger the "end-of-range" heuristic.
        if resp.status != 200 or not resp.text:
            consecutive_failures += 1
            continue

        consecutive_failures = 0
        try:
            payload = json.loads(resp.text or "[]")
        except Exception:
            payload = []
        item = next(
            (
                x
                for x in payload
                if isinstance(x, dict)
                and str(x.get("link", "")).strip()
                and str(x.get("num", "")).strip()
            ),
            None,
        )
        if not item:
            consecutive_misses += 1
        else:
            consecutive_misses = 0
            link = str(item.get("link", "")).strip()
            title = str(item.get("num", "")).strip() or f"STF {doc_type} {number}"
            comment = str(item.get("comentario", "")).strip()

            url = f"{_DETAIL_URL}?base={base}&sumula={link}"
            doc_id = f"stf:{doc_type}:{number}"
            discovered.append(
                DiscoveredDoc(
                    source="stf",
                    doc_type=doc_type,
                    doc_id=doc_id,
                    url=url,
                    title=title,
                    published_date=None,
                    extra={
                        "number": number,
                        "link": link,
                        "comment": comment,
                        "base": base,
                    },
                )
            )
            yielded += 1

        if number >= 50 and consecutive_misses >= max_consecutive_misses:
            return discovered

    return discovered


def _discover_via_search_api_playwright(  # pragma: no cover
    *, doc_type: str, base: str, limit: int | None = None
) -> list[DiscoveredDoc]:
    try:
        from python.helpers import files
        from python.helpers.playwright import ensure_playwright_binary

        import os

        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", files.get_abs_path("tmp/playwright"))
        pw_binary = ensure_playwright_binary()

        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    yielded = 0
    consecutive_misses = 0
    max_consecutive_misses = 25
    max_number = 2000
    discovered: list[DiscoveredDoc] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=str(pw_binary))
        try:
            ctx = browser.new_context(ignore_https_errors=True, user_agent=_BROWSER_UA)
            page = ctx.new_page()
            page.goto(_SEARCH_PAGE_URL, wait_until="domcontentloaded", timeout=45_000)
            page.wait_for_timeout(250)

            for number in range(1, max_number + 1):
                if limit is not None and yielded >= limit:
                    return discovered

                resp = ctx.request.post(
                    _SEARCH_API_URL,
                    form={"base": base, "texto": "", "numero": str(number), "ramo": ""},
                    headers={"Referer": _SEARCH_PAGE_URL, "X-Requested-With": "XMLHttpRequest"},
                    timeout=15_000,
                )
                if resp.status != 200:
                    consecutive_misses += 1
                    continue

                try:
                    payload = json.loads(resp.text() or "[]")
                except Exception:
                    payload = []

                item = next(
                    (
                        x
                        for x in payload
                        if isinstance(x, dict)
                        and str(x.get("link", "")).strip()
                        and str(x.get("num", "")).strip()
                    ),
                    None,
                )
                if not item:
                    consecutive_misses += 1
                    continue

                consecutive_misses = 0
                link = str(item.get("link", "")).strip()
                title = str(item.get("num", "")).strip() or f"STF {doc_type} {number}"
                comment = str(item.get("comentario", "")).strip()

                url = f"{_DETAIL_URL}?base={base}&sumula={link}"
                doc_id = f"stf:{doc_type}:{number}"
                discovered.append(
                    DiscoveredDoc(
                        source="stf",
                        doc_type=doc_type,
                        doc_id=doc_id,
                        url=url,
                        title=title,
                        published_date=None,
                        extra={"number": number, "link": link, "comment": comment, "base": base},
                    )
                )
                yielded += 1

                if number >= 50 and consecutive_misses >= max_consecutive_misses:
                    return discovered
        finally:
            browser.close()

    return discovered


@dataclass
class STFSumulasSource:
    fetcher: Fetcher
    today: date
    sumulas_url: str = "https://portal.stf.jus.br/jurisprudencia/sumulas.asp"
    vinculantes_url: str = "https://portal.stf.jus.br/jurisprudencia/sumulasVinculantes.asp"

    def discover(self, *, limit: int | None = None) -> Iterator[DiscoveredDoc]:
        # Important: discovery must not invoke Playwright while parsing is in
        # progress (parsing may already be using a long-lived Playwright
        # instance via AutoFetcher). To avoid nested Playwright sync sessions,
        # compute the full discovery list up-front, then yield it.
        discovered_all: list[DiscoveredDoc] = []

        for doc_type, url, fallback_base in (
            ("sumula", self.sumulas_url, "30"),
            ("sumula_vinculante", self.vinculantes_url, "26"),
        ):
            remaining = None if limit is None else max(0, limit - len(discovered_all))
            if remaining is not None and remaining <= 0:
                break

            res = self.fetcher.fetch_text(url)
            docs = parse_stf_listing(res.text or "", base_url=url, doc_type=doc_type) if res.text else []

            # Fallback per doc_type: sumulas may work while vinculantes is blocked.
            if not docs:
                docs = _discover_via_search_api(fetcher=self.fetcher, doc_type=doc_type, base=fallback_base, limit=remaining)

            if remaining is not None:
                docs = docs[:remaining]
            discovered_all.extend(docs)

        for d in discovered_all:
            yield d

    def parse(self, discovered: DiscoveredDoc) -> ParsedDoc:
        res = self.fetcher.fetch_text(discovered.url)
        if not res.text:
            raise ValueError(f"Failed to fetch STF: {discovered.url} ({res.status}) {res.error}")
        return parse_stf_sumula_detail(res.text, discovered=discovered)

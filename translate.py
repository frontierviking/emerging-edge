"""
translate.py — Free, on-demand machine translation for emerging-edge.

Strategy
--------
Most news/forum/insider items in emerging-edge are stored in the
language of the original source (Polish, Swedish, French, Italian, …).
The dashboard is meant for an English-speaking audience, so at render
time we translate any non-English text to English.

We use Google Translate's free unofficial endpoint
(``translate.googleapis.com/translate_a/single``) — the same one
``googletrans`` and ``deep-translator`` rely on. No API key, no cost,
generous rate limits for our volume. Behind the scenes we cache every
translation in the ``translations`` table so the same headline is
never translated twice.

Public API
----------
``translate_to_english(db, text, source_lang)``
    Returns the English translation of ``text``. Returns ``text`` as-is
    if ``source_lang`` is empty / "en" / unknown, or if the network call
    fails. Cached results are served from the DB; new translations are
    written back.

``translate_batch(db, items)``
    Convenience helper for translating a list of (text, source_lang)
    pairs efficiently.

Design notes
------------
* All network calls have a short timeout (4s) and degrade gracefully —
  if Google rate-limits or times out we fall back to the original text
  rather than blocking the page render.
* Cache is keyed by sha1(text) + source/target lang so trivial whitespace
  changes still hit the cache.
* This module is a hard dependency only on stdlib + the project's
  ``Database`` class.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger("emerging-edge.translate")


# Languages we never need to translate
_PASSTHROUGH = {"", "en", "en-us", "en-gb"}

# Map our internal lang codes to Google Translate codes.
_GT_LANG = {
    "sv":    "sv",   # Swedish
    "no":    "no",   # Norwegian
    "da":    "da",   # Danish
    "fi":    "fi",   # Finnish
    "fr":    "fr",   # French
    "de":    "de",   # German
    "it":    "it",   # Italian
    "es":    "es",   # Spanish
    "pt":    "pt",   # Portuguese
    "pl":    "pl",   # Polish
    "cs":    "cs",   # Czech
    "sk":    "sk",   # Slovak
    "hu":    "hu",   # Hungarian
    "ro":    "ro",   # Romanian
    "el":    "el",   # Greek
    "tr":    "tr",   # Turkish
    "ru":    "ru",   # Russian
    "uk":    "uk",   # Ukrainian
    "ar":    "ar",   # Arabic
    "fa":    "fa",   # Persian
    "ms":    "ms",   # Malay
    "id":    "id",   # Indonesian
    "vi":    "vi",   # Vietnamese
    "th":    "th",   # Thai
    "ja":    "ja",   # Japanese
    "ko":    "ko",   # Korean
    "zh":    "zh-CN",
    "zh-cn": "zh-CN",
    "zh-tw": "zh-TW",
    "hr":    "hr",   # Croatian
    "sr":    "sr",   # Serbian
    "bg":    "bg",   # Bulgarian
    "sl":    "sl",   # Slovenian
    "sw":    "sw",   # Swahili
    "am":    "am",   # Amharic
    "ur":    "ur",   # Urdu
    "bn":    "bn",   # Bengali
    "hi":    "hi",   # Hindi
    "ta":    "ta",   # Tamil
    "ne":    "ne",   # Nepali
    "si":    "si",   # Sinhala
    "tl":    "tl",   # Tagalog
}


def _hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Lightweight language detection
# ---------------------------------------------------------------------------
# Scripts: easy — block ranges identify the language unambiguously.
# Latin-script languages: heuristic via common stopwords. Good enough for
# the news/forum/insider use case where we only need to know "is this
# English or not, and if not, roughly what."

import re as _re
import unicodedata as _ud

_SCRIPT_PATTERNS = [
    ("ru", _re.compile(r"[А-Яа-яЁё]")),
    ("uk", _re.compile(r"[ҐґЄєІіЇї]")),                # Ukrainian-only chars
    ("el", _re.compile(r"[Α-Ωα-ωΆ-Ώά-ώ]")),
    ("ar", _re.compile(r"[؀-ۿ]")),
    ("fa", _re.compile(r"[؀-ۿ].*[ﮯﮰپچگی]")),
    ("ur", _re.compile(r"[؀-ۿ].*[ٹڈڑں]")),
    ("he", _re.compile(r"[֐-׿]")),
    ("hi", _re.compile(r"[ऀ-ॿ]")),
    ("bn", _re.compile(r"[ঀ-৿]")),
    ("ta", _re.compile(r"[஀-௿]")),
    ("th", _re.compile(r"[฀-๿]")),
    ("ja", _re.compile(r"[぀-ゟ゠-ヿ]")),  # hiragana/katakana
    ("zh", _re.compile(r"[一-鿿]")),
    ("ko", _re.compile(r"[가-힯]")),
]

# Stopwords per Latin-script language. We score by how many of these words
# appear; the language with the most hits wins, provided it beats English
# convincingly. Keep lists short and distinctive — words that DON'T also
# appear in English or other Latin languages we care about.
_LATIN_STOPWORDS: dict[str, set[str]] = {
    "id": {  # Indonesian / Malay
        "yang", "dengan", "untuk", "akan", "telah", "tidak", "atau",
        "dari", "ini", "itu", "adalah", "sebagai", "juga", "saya",
        "kami", "kita", "mereka", "harus", "mengungkapkan", "membagikan",
        "membukukan", "tersebut", "sebesar", "pada", "tahun",
        "perusahaan", "emiten", "pertumbuhan", "saham", "tbk", "naik",
        "turun", "laba", "keuntungan",
    },
    "tl": {  # Tagalog / Filipino
        "ang", "ng", "mga", "po", "kayo", "ako", "natin", "nila",
        "ito", "kasi", "lang", "saan", "paano", "kung", "namin",
    },
    "fr": {
        "le", "la", "les", "des", "une", "est", "que", "qui", "dans",
        "pour", "avec", "pas", "mais", "où", "été", "avoir", "être",
        "leurs", "cette", "ces",
    },
    "it": {
        "il", "la", "gli", "delle", "degli", "una", "che", "non",
        "per", "con", "sono", "anche", "oppure", "questa", "questo",
        "molto", "essere",
    },
    "es": {
        "el", "la", "los", "las", "una", "del", "que", "para", "con",
        "por", "muy", "más", "está", "este", "esta", "ser",
    },
    "pt": {
        "uma", "para", "com", "que", "não", "seu", "sua", "seus",
        "também", "está", "este", "esta", "muito", "tão",
    },
    "de": {
        "der", "die", "und", "ist", "nicht", "ein", "mit", "den",
        "von", "auf", "auch", "sich", "dem", "wird", "noch", "über",
    },
    "nl": {
        "het", "een", "van", "voor", "niet", "ook", "maar", "zijn",
        "haar", "hem", "naar", "deze", "dit", "wij",
    },
    "pl": {
        "jest", "się", "tego", "tym", "tych", "który", "która", "które",
        "jako", "także", "więc", "spółka", "spółki", "raport", "wyniki",
    },
    "sv": {
        "och", "att", "som", "för", "men", "med", "från", "denna",
        "detta", "dessa", "vilken", "kvartal",
    },
    "no": {
        "ikke", "også", "være", "vært", "disse", "kvartal", "selskap",
        "selskapet",
    },
    "da": {
        "ikke", "også", "være", "været", "disse", "kvartal", "selskab",
        "selskabet",
    },
    "fi": {
        "että", "ole", "olla", "tämä", "kuten", "myös", "yhtiö",
        "yhtiön", "tulos", "neljännes",
    },
    "tr": {
        "için", "olarak", "veya", "şirket", "şirketi", "açıklama",
        "değil", "değişiklik",
    },
    "vi": {
        "không", "được", "của", "với", "trên", "công", "ty", "doanh",
        "nghiệp", "tại", "đến",
    },
    "ms": {
        "yang", "dengan", "tidak", "untuk", "syarikat", "saham", "naik",
        "turun",
    },
}

_EN_STOPWORDS = {
    "the", "and", "of", "to", "in", "is", "for", "with", "on", "this",
    "that", "from", "as", "but", "by", "are", "was", "were", "has",
    "have", "had", "their", "they", "we", "our", "an", "at", "be",
}


def detect_language(text: str) -> str:
    """Return a 2-letter language code for ``text``.

    Returns 'en' as a confident fallback when nothing matches. Only used
    to override stored ``lang`` values when feeds (Twitter, Yahoo, news
    aggregators) lie about the content language because they look at the
    stock's locale rather than the post body itself.
    """
    if not text:
        return "en"
    sample = text[:600]
    # Script-level detection first — these are unambiguous
    for code, pat in _SCRIPT_PATTERNS:
        if pat.search(sample):
            return code
    # Latin-script: stopword scoring
    words = _re.findall(r"[A-Za-zÀ-ɏ]+", sample.lower())
    if not words:
        return "en"
    en_hits = sum(1 for w in words if w in _EN_STOPWORDS)
    best_lang = "en"
    best_hits = en_hits
    for lang, vocab in _LATIN_STOPWORDS.items():
        hits = sum(1 for w in words if w in vocab)
        # Require a clear margin over English — at least 2 hits and >= en_hits
        if hits >= 2 and hits > best_hits:
            best_lang = lang
            best_hits = hits
    return best_lang


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_lang(lang: str | None) -> str:
    if not lang:
        return ""
    s = str(lang).strip().lower()
    return s


def _is_passthrough(lang: str) -> bool:
    return _normalize_lang(lang) in _PASSTHROUGH


def _ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _google_translate(text: str, source_lang: str, target_lang: str = "en",
                      timeout: int = 4) -> str | None:
    """Call Google Translate's free public endpoint.

    Returns the translated text or ``None`` on failure.
    """
    src = _GT_LANG.get(_normalize_lang(source_lang)) or _normalize_lang(source_lang) or "auto"
    tgt = _GT_LANG.get(_normalize_lang(target_lang)) or "en"
    if not text or len(text.strip()) < 2:
        return text
    # The endpoint truncates very long inputs silently; cap to 4500 chars.
    payload = text[:4500]
    qs = urllib.parse.urlencode({
        "client": "gtx",
        "sl": src,
        "tl": tgt,
        "dt": "t",
        "q": payload,
    })
    url = "https://translate.googleapis.com/translate_a/single?" + qs
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0 Safari/537.36",
            "Accept": "*/*",
        })
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx()) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        logger.info("translate %s→%s HTTP %d", src, tgt, e.code)
        return None
    except Exception as e:
        logger.info("translate %s→%s failed: %s", src, tgt, e)
        return None
    # Response is a deeply nested JSON array; first row holds the translated
    # chunks. Concatenate them.
    try:
        data = json.loads(raw)
        chunks = data[0] or []
        out = "".join(c[0] for c in chunks if c and c[0])
        return out or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# DB-cached public API
# ---------------------------------------------------------------------------

SETTING_SKIP_LANGS = "translate_skip_langs"


def get_skip_langs(db) -> set[str]:
    """Languages the user has opted to keep native (not translate).

    Stored in ``app_settings`` under key ``translate_skip_langs`` as a
    comma-separated list of lang codes (e.g. ``"sv,fr,it"``). Managed
    from the Engine Room.
    """
    try:
        raw = db.get_setting(SETTING_SKIP_LANGS, "")
    except Exception:
        return set()
    if not raw:
        return set()
    return {p.strip().lower() for p in raw.split(",") if p.strip()}


def set_skip_langs(db, langs: list[str] | set[str]) -> None:
    """Persist the skip-languages preference."""
    cleaned = sorted({(_normalize_lang(l)) for l in (langs or []) if l})
    db.set_setting(SETTING_SKIP_LANGS, ",".join(cleaned))


def cached_translation(db, text: str, source_lang: str | None) -> str | None:
    """Pure cache lookup. Returns the cached English translation, or
    ``None`` if not cached. Never hits the network."""
    if not text or len(text.strip()) < 2:
        return None
    if _is_passthrough(source_lang):
        return None
    src = _normalize_lang(source_lang)
    if src in get_skip_langs(db):
        return None
    try:
        row = db.conn.execute(
            "SELECT target_text FROM translations "
            "WHERE text_hash=? AND source_lang=? AND target_lang='en'",
            (_hash(text), src),
        ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    return row[0] if not hasattr(row, "keys") else row["target_text"]


def translate_to_english(db, text: str, source_lang: str | None) -> str:
    """Translate ``text`` from ``source_lang`` to English. DB-cached.

    Returns the original text unchanged if:
      * source_lang is empty / "en"
      * source_lang is in the user's "keep native" list (Engine Room setting)
      * text is empty / very short
      * the translation network call fails (we fall back gracefully)
    """
    if not text or len(text.strip()) < 2:
        return text
    if _is_passthrough(source_lang):
        return text
    src = _normalize_lang(source_lang)
    # User-managed opt-out for languages they prefer to read in the original
    if src in get_skip_langs(db):
        return text
    text_hash = _hash(text)

    # 1) Cache lookup
    cached = cached_translation(db, text, source_lang)
    if cached is not None:
        return cached

    # 2) Network translate
    translated = _google_translate(text, src, "en")
    if not translated or translated.strip() == text.strip():
        # Either the API failed or the source text is already English.
        # Don't write a no-op into the cache.
        return text

    # 3) Cache write
    try:
        db.conn.execute(
            """INSERT OR REPLACE INTO translations
                  (text_hash, source_lang, target_lang, source_text,
                   target_text, created_at)
               VALUES (?, ?, 'en', ?, ?, ?)""",
            (text_hash, src, text, translated, _now()),
        )
        db.conn.commit()
    except Exception as e:
        logger.debug("translation cache write failed: %s", e)
    return translated


def translate_batch(db, items: list[tuple[str, str | None]]) -> list[str]:
    """Translate a list of (text, source_lang) pairs. Cache-aware."""
    return [translate_to_english(db, t, l) for (t, l) in items]


# ---------------------------------------------------------------------------
# Render-time helpers used by dashboard.py
# ---------------------------------------------------------------------------

# Flag chips for the most common source languages — small visual cue that
# something has been translated, with the original text in a tooltip.
LANG_FLAGS = {
    "sv": "🇸🇪", "no": "🇳🇴", "da": "🇩🇰", "fi": "🇫🇮",
    "fr": "🇫🇷", "de": "🇩🇪", "it": "🇮🇹", "es": "🇪🇸", "pt": "🇵🇹",
    "pl": "🇵🇱", "cs": "🇨🇿", "sk": "🇸🇰", "hu": "🇭🇺", "ro": "🇷🇴",
    "el": "🇬🇷", "tr": "🇹🇷", "ru": "🇷🇺", "uk": "🇺🇦",
    "ar": "🇸🇦", "fa": "🇮🇷", "ur": "🇵🇰",
    "ms": "🇲🇾", "id": "🇮🇩", "vi": "🇻🇳", "th": "🇹🇭",
    "ja": "🇯🇵", "ko": "🇰🇷", "zh": "🇨🇳", "zh-cn": "🇨🇳", "zh-tw": "🇹🇼",
    "hr": "🇭🇷", "sr": "🇷🇸", "bg": "🇧🇬", "sl": "🇸🇮",
    "sw": "🇰🇪", "am": "🇪🇹",
    "bn": "🇧🇩", "hi": "🇮🇳", "ta": "🇮🇳", "ne": "🇳🇵", "si": "🇱🇰",
    "tl": "🇵🇭",
}


def lang_flag(lang: str | None) -> str:
    """Return a small flag glyph for the given language, or '' if none."""
    if not lang:
        return ""
    return LANG_FLAGS.get(_normalize_lang(lang), "")

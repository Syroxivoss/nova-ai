"""
NOVA AUTONOMOUS ECOSYSTEM - V13 TITAN ENGINE
Full-Scale LLM Pre-Training + Alignment Pipeline
Full expanded version copied from `pyhton.py`.

Note: ASCII-only header to avoid encoding issues in some editors.
Entry point: NovaTitanOrchestratorV3 (see bottom of file).
"""


from __future__ import annotations

import ast
import bz2
import gzip
import html
import importlib
import json
import hashlib
import logging
import math
import os
import random
import re
import subprocess
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict, Counter, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, asdict
from enum import Enum
from itertools import islice, chain
from typing import Iterator, Optional, List, Dict, Tuple, Any, Sequence, Iterable, TypeAlias
from urllib.parse import urljoin, urlparse

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("NovaTitan")

USER_AGENT = "NovaTitan/2.0 (+https://example.invalid)"
FASTTEXT_MODEL_URL = (
    "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz"
)


def optional_import(name: str) -> Any:
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


def progress(iterable: Iterable[Any], total: Optional[int] = None, desc: str = "") -> Iterable[Any]:
    tqdm_mod = optional_import("tqdm")
    tqdm_cls = getattr(tqdm_mod, "tqdm", None) if tqdm_mod else None
    if tqdm_cls:
        return tqdm_cls(iterable, total=total, desc=desc)
    return iterable


Tensor: TypeAlias = Any
DType: TypeAlias = Any
TorchModule: TypeAlias = Any


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def strip_html(text: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return html.unescape(text)


def safe_filename(value: str, max_len: int = 120) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("._")
    return cleaned[:max_len] or "artifact"


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class LanguagePrediction:
    primary_language: str
    confidence: float
    distribution: Dict[str, float] = field(default_factory=dict)
    is_mixed: bool = False

# ─────────────────────────────────────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────────────────────────────────────

def get_nova_base() -> Path:
    base = Path.cwd() / "Nova_Autonomous_Ecosystem" / "V13_Titan_Engine"
    base.mkdir(parents=True, exist_ok=True)
    return base


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 1 — DATA COLLECTION SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

class DataSource(Enum):
    COMMON_CRAWL   = "common_crawl"
    WIKIPEDIA      = "wikipedia"
    STACK_EXCHANGE = "stack_exchange"
    GITHUB         = "github"
    CODE_SEARCH_NET= "code_search_net"
    BIGCODE        = "bigcode"
    CVE            = "cve"
    EXPLOIT_DB     = "exploit_db"
    BUG_BOUNTY     = "bug_bounty"
    METASPLOIT     = "metasploit"
    ARXIV          = "arxiv"
    PUBMED         = "pubmed"
    OSCAR          = "oscar"
    CC100          = "cc100"
    MC4            = "mc4"
    GSM8K          = "gsm8k"
    MATH_DATASET   = "math_dataset"


@dataclass
class RawDocument:
    source: str
    category: str
    language: str
    text: str
    url: str = ""
    title: str = ""
    license: str = "unknown"
    repo_stars: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    token_count: int = 0
    quality_score: float = 0.0


class WebCrawler:
    """Real HTTP crawler with bounded BFS and article extraction."""

    BOILERPLATE_PATTERNS = [
        r"(cookie policy|privacy policy|terms of service)",
        r"(subscribe to our newsletter|sign up for updates)",
        r"(share this article|follow us on|like us on)",
        r"(copyright\s?\d{4}|all rights reserved|powered by)",
        r"(navigation|skip to content|back to top)",
    ]

    def __init__(
        self,
        max_depth: int = 2,
        politeness_delay: float = 1.0,
        max_pages: int = 50,
        timeout: int = 20,
        same_domain_only: bool = True,
    ):
        self.max_depth = max_depth
        self.politeness_delay = politeness_delay
        self.max_pages = max_pages
        self.timeout = timeout
        self.same_domain_only = same_domain_only
        self.visited: set = set()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def crawl(self, seed_urls: List[str]) -> Iterator[RawDocument]:
        trafilatura_mod = optional_import("trafilatura")
        queue: deque[Tuple[str, int, str]] = deque(
            (url, 0, urlparse(url).netloc) for url in seed_urls if url
        )
        pages_seen = 0

        while queue and pages_seen < self.max_pages:
            url, depth, root_domain = queue.popleft()
            if url in self.visited:
                continue
            self.visited.add(url)

            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
            except Exception as exc:
                log.warning("Web crawl failed for %s: %s", url, exc)
                continue

            html_text = response.text
            if trafilatura_mod:
                text = trafilatura_mod.extract(
                    html_text,
                    url=url,
                    include_comments=False,
                    include_tables=False,
                ) or ""
            else:
                text = strip_html(html_text)

            for pattern in self.BOILERPLATE_PATTERNS:
                text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
            text = normalize_space(text)

            if len(text) >= 200:
                title_match = re.search(r"(?is)<title>(.*?)</title>", html_text)
                title = normalize_space(strip_html(title_match.group(1))) if title_match else ""
                pages_seen += 1
                yield RawDocument(
                    source="web_crawl",
                    category="general_web",
                    language="unknown",
                    text=text,
                    url=url,
                    title=title,
                )

            if depth >= self.max_depth:
                time.sleep(self.politeness_delay)
                continue

            for href in re.findall(r'(?is)href=["\'](.*?)["\']', html_text):
                if href.startswith("#") or href.startswith("javascript:"):
                    continue
                absolute = urljoin(url, href)
                if not absolute.startswith(("http://", "https://")):
                    continue
                if self.same_domain_only and urlparse(absolute).netloc != root_domain:
                    continue
                if absolute not in self.visited:
                    queue.append((absolute, depth + 1, root_domain))

            time.sleep(self.politeness_delay)


class GitHubCrawler:
    """Streams code files from GitHub Search + Trees API."""

    ALLOWED_LICENSES = {
        "mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause",
        "isc", "lgpl-2.1", "mpl-2.0", "unlicense", "unknown",
    }
    EXTENSIONS = {
        ".py": "python", ".js": "javascript", ".ts": "typescript",
        ".rs": "rust", ".go": "go", ".java": "java", ".c": "c",
        ".cc": "cpp", ".cpp": "cpp", ".sh": "bash", ".sol": "solidity",
    }

    def __init__(
        self,
        min_stars: int = 50,
        token: str = "",
        per_query_repos: int = 3,
        max_files_per_repo: int = 20,
        timeout: int = 30,
    ):
        self.min_stars = min_stars
        self.per_query_repos = per_query_repos
        self.max_files_per_repo = max_files_per_repo
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def crawl_repos(self, queries: List[str]) -> Iterator[RawDocument]:
        for query in queries:
            yield from self._stream_query(query)

    def _stream_query(self, query: str) -> Iterator[RawDocument]:
        try:
            response = self.session.get(
                "https://api.github.com/search/repositories",
                params={
                    "q": query,
                    "sort": "stars",
                    "order": "desc",
                    "per_page": self.per_query_repos,
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
        except Exception as exc:
            log.warning("GitHub search failed for %s: %s", query, exc)
            return

        for repo in response.json().get("items", []):
            stars = int(repo.get("stargazers_count", 0))
            license_name = ((repo.get("license") or {}).get("spdx_id") or "unknown").lower()
            if stars < self.min_stars or license_name not in self.ALLOWED_LICENSES:
                continue
            yield from self._stream_repo_files(repo, license_name)

    def _stream_repo_files(self, repo: Dict[str, Any], license_name: str) -> Iterator[RawDocument]:
        full_name = repo["full_name"]
        default_branch = repo.get("default_branch", "main")
        try:
            response = self.session.get(
                f"https://api.github.com/repos/{full_name}/git/trees/{default_branch}",
                params={"recursive": "1"},
                timeout=self.timeout,
            )
            response.raise_for_status()
        except Exception as exc:
            log.warning("GitHub tree failed for %s: %s", full_name, exc)
            return

        emitted = 0
        for entry in response.json().get("tree", []):
            if entry.get("type") != "blob":
                continue
            path = entry.get("path", "")
            language = self.EXTENSIONS.get(Path(path).suffix.lower())
            if not language:
                continue
            raw_url = f"https://raw.githubusercontent.com/{full_name}/{default_branch}/{path}"
            try:
                file_response = self.session.get(raw_url, timeout=self.timeout)
                file_response.raise_for_status()
            except Exception:
                continue

            content = file_response.text
            if len(content) < 80:
                continue

            emitted += 1
            yield RawDocument(
                source="github",
                category="code_ast",
                language=language,
                text=content,
                url=raw_url,
                title=path,
                license=license_name,
                repo_stars=int(repo.get("stargazers_count", 0)),
                metadata={
                    "repo": full_name,
                    "path": path,
                    "programming_language": language,
                    "html_url": repo.get("html_url", ""),
                },
            )
            if emitted >= self.max_files_per_repo:
                break


class DatasetImporter:
    """Imports real streaming datasets from HuggingFace datasets."""

    SOURCE_CONFIGS: Dict[DataSource, Dict[str, Any]] = {
        DataSource.GSM8K: {
            "dataset_name": "openai/gsm8k",
            "subset": "main",
            "split": "train",
            "fields": ("question", "answer"),
            "category": "math_reasoning",
        },
        DataSource.MATH_DATASET: {
            "dataset_name": "hendrycks/competition_math",
            "split": "train",
            "fields": ("problem", "solution"),
            "category": "math_reasoning",
        },
        DataSource.BIGCODE: {
            "dataset_name": "bigcode/the-stack",
            "split": "train",
            "fields": ("content", "code"),
            "category": "code_ast",
        },
        DataSource.OSCAR: {
            "dataset_name": "oscar-corpus/OSCAR-2301",
            "subset": "en",
            "split": "train",
            "fields": ("text",),
            "category": "multilingual",
        },
    }

    def __init__(self, max_documents: int = 100):
        self.max_documents = max_documents

    def stream(self, source: DataSource) -> Iterator[RawDocument]:
        datasets_mod = optional_import("datasets")
        if not datasets_mod:
            log.warning("Skipping %s: install datasets for streaming imports", source.value)
            return

        cfg = self.SOURCE_CONFIGS.get(source)
        if not cfg:
            log.warning("No HuggingFace config registered for %s", source.value)
            return

        try:
            dataset = datasets_mod.load_dataset(
                cfg["dataset_name"],
                cfg.get("subset"),
                split=cfg.get("split", "train"),
                streaming=True,
            )
        except Exception as exc:
            log.warning("Dataset load failed for %s: %s", source.value, exc)
            return

        for record in islice(dataset, self.max_documents):
            parts = []
            fields_payload: Dict[str, Any] = {}
            for field in cfg.get("fields", ("text",)):
                value = record.get(field)
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())
                    fields_payload[field] = value.strip()
            text_value = "\n\n".join(parts)
            if len(text_value) < 40:
                continue
            metadata = {
                key: record.get(key)
                for key in ("lang", "license", "max_stars_count", "path")
                if key in record
            }
            if fields_payload:
                metadata["fields"] = fields_payload
                if len(fields_payload) >= 2:
                    field_items = list(fields_payload.items())
                    metadata["question"] = field_items[0][1]
                    metadata["answer"] = field_items[1][1]
            yield RawDocument(
                source=source.value,
                category=cfg.get("category", self._cat(source)),
                language=str(record.get("lang", "unknown")),
                text=text_value,
                metadata=metadata,
            )

    @staticmethod
    def _cat(source: DataSource) -> str:
        mapping = {
            DataSource.GITHUB: "code_ast", DataSource.CODE_SEARCH_NET: "code_ast",
            DataSource.CVE: "cyber_sec", DataSource.EXPLOIT_DB: "cyber_sec",
            DataSource.ARXIV: "science", DataSource.GSM8K: "math_reasoning",
            DataSource.OSCAR: "multilingual", DataSource.CC100: "multilingual",
        }
        return mapping.get(source, "general_web")


class LanguageDetector:
    """fastText-backed language detector with mixed-language support."""

    HEURISTIC_SIGNATURES = {
        "tr": {"bir", "ve", "ile", "icin", "olan", "daha"},
        "de": {"und", "die", "der", "ist", "nicht", "ein"},
        "fr": {"les", "des", "une", "pour", "est", "pas"},
        "es": {"los", "las", "una", "para", "con", "por"},
        "zh": {"?", "?", "?", "?"},
        "ar": {"??", "??", "???", "???", "???"},
        "en": {"the", "and", "for", "that", "with", "this"},
    }

    def __init__(self, model_path: str = "", allow_download: bool = False, top_k: int = 3):
        self.model_path = Path(model_path) if model_path else get_nova_base() / "models" / "lid.176.ftz"
        self.allow_download = allow_download
        self.top_k = top_k
        self.fasttext = optional_import("fasttext")
        self._model = None
        self._load_attempted = False

    def detect(self, text: str) -> str:
        return self.analyze(text).primary_language

    def analyze(self, text: str) -> LanguagePrediction:
        if not text.strip():
            return LanguagePrediction("unknown", 0.0, {})
        if self._looks_like_code(text):
            return LanguagePrediction("code", 1.0, {"code": 1.0}, False)

        model = self._load_model()
        distribution = self._predict_fasttext(text) if model else self._predict_heuristic(text)
        language, confidence = max(distribution.items(), key=lambda item: item[1])
        scores = sorted(distribution.values(), reverse=True)
        return LanguagePrediction(
            primary_language=language,
            confidence=confidence,
            distribution=distribution,
            is_mixed=len(scores) > 1 and scores[1] >= 0.20,
        )

    def _load_model(self):
        if self._load_attempted:
            return self._model
        self._load_attempted = True
        if not self.fasttext:
            return None
        if not self.model_path.exists() and self.allow_download:
            self.model_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with requests.get(FASTTEXT_MODEL_URL, stream=True, timeout=60) as response:
                    response.raise_for_status()
                    with open(self.model_path, "wb") as handle:
                        for chunk in response.iter_content(chunk_size=1 << 20):
                            if chunk:
                                handle.write(chunk)
            except Exception as exc:
                log.warning("fastText model download failed: %s", exc)
                return None
        if not self.model_path.exists():
            return None
        try:
            self._model = self.fasttext.load_model(str(self.model_path))
        except Exception as exc:
            log.warning("fastText model load failed: %s", exc)
            self._model = None
        return self._model

    def _predict_fasttext(self, text: str) -> Dict[str, float]:
        model = self._load_model()
        if not model:
            return self._predict_heuristic(text)
        distribution: Dict[str, float] = defaultdict(float)
        total = 0.0
        for segment in self._segments(text):
            labels, probs = model.predict(segment.replace("\n", " "), k=self.top_k)
            for label, prob in zip(labels, probs):
                lang = label.replace("__label__", "")
                distribution[lang] += float(prob)
                total += float(prob)
        if not distribution:
            return {"unknown": 1.0}
        ranked = sorted(distribution.items(), key=lambda item: item[1], reverse=True)[: self.top_k]
        return {
            lang: round(score / (total or 1.0), 4)
            for lang, score in ranked
        }

    def _predict_heuristic(self, text: str) -> Dict[str, float]:
        lowered = text.lower()
        words = set(re.findall(r"\w+", lowered))
        scores = {}
        for lang, signature in self.HEURISTIC_SIGNATURES.items():
            scores[lang] = float(len(words & signature))
        if max(scores.values(), default=0.0) <= 0.0:
            return {"en": 1.0}
        total = sum(scores.values()) or 1.0
        return {lang: round(score / total, 4) for lang, score in scores.items() if score > 0}

    @staticmethod
    def _segments(text: str) -> List[str]:
        parts = [part.strip() for part in re.split(r"\n{2,}", text) if part.strip()]
        if not parts:
            parts = [text]
        result = []
        for part in parts:
            if len(part) <= 500:
                result.append(part)
                continue
            for start in range(0, len(part), 400):
                piece = part[start:start + 500].strip()
                if piece:
                    result.append(piece)
        return result[:10]

    @staticmethod
    def _looks_like_code(text: str) -> bool:
        lines = text.splitlines()[:50]
        if not lines:
            return False
        markers = sum(
            1 for line in lines
            if re.search(r'[{}();=<>]|\b(def|class|import|function|const|let|var|return)\b', line)
        )
        return markers >= max(3, len(lines) // 5)


class BoilerplateRemover:
    PATTERNS = [
        re.compile(pattern, re.IGNORECASE) for pattern in [
            r"(cookie policy|accept cookies|gdpr|privacy policy)",
            r"(subscribe|newsletter|sign[\s-]?up)",
            r"(share this|follow us|facebook|twitter|instagram)",
            r"(all rights reserved|copyright\s?\d{4}|powered by)",
            r"(navigation|skip to|back to top|breadcrumb)",
            r"(advertisement|sponsored|promoted)",
            r"(loading\.\.\.|please wait)",
        ]
    ]

    def remove(self, text: str) -> str:
        lines = text.splitlines()
        kept = [line for line in lines if not any(pattern.search(line) for pattern in self.PATTERNS)]
        return "\n".join(kept)


class EncodingNormalizer:
    def normalize(self, text: str) -> str:
        text = unicodedata.normalize("NFC", text)
        return "".join(
            ch for ch in text
            if unicodedata.category(ch)[0] != "C" or ch in "\n\t"
        )


class NovaCleaner:
    def __init__(self, min_chars: int = 120):
        self.lang_detect = LanguageDetector()
        self.boilerplate = BoilerplateRemover()
        self.enc_normalizer = EncodingNormalizer()
        self.min_chars = min_chars

    def clean(self, doc: RawDocument) -> Optional[RawDocument]:
        text = self.enc_normalizer.normalize(doc.text).replace("\r\n", "\n")
        text = re.sub(r"https?://\S+", "<URL>", text)

        if doc.category == "code_ast":
            text = "\n".join(line.rstrip() for line in text.splitlines()).strip()
            if len(text.splitlines()) < 5:
                return None
            doc.metadata["programming_language"] = (
                doc.metadata.get("programming_language") or doc.language or "unknown"
            )
            doc.language = "code"
        else:
            text = strip_html(text)
            text = self.boilerplate.remove(text)
            text = re.sub(r"[\U00010000-\U0010ffff]", "", text, flags=re.UNICODE)
            text = normalize_space(text)
            if len(text) < self.min_chars:
                return None
            prediction = self.lang_detect.analyze(text)
            doc.language = prediction.primary_language
            doc.metadata["language_confidence"] = round(prediction.confidence, 4)
            doc.metadata["language_distribution"] = prediction.distribution
            doc.metadata["mixed_language"] = prediction.is_mixed

        doc.text = text
        return doc


class LegacyMinHashDeduplicator:
    """
    MinHash LSH deduplication — catches near-duplicate documents
    even if wording slightly differs.
    """
    NUM_PERM = 128
    JACCARD_THRESHOLD = 0.85
    SHINGLE_SIZE = 5

    def __init__(self):
        self.exact_hashes: set = set()
        self.band_buckets: Dict[int, Dict[tuple, str]] = defaultdict(dict)
        self._coefficients = self._generate_coefficients()
        self.PRIME = (1 << 61) - 1
        self.MAX_HASH = (1 << 32) - 1
        self.num_bands = 32
        self.rows_per_band = self.NUM_PERM // self.num_bands

    def _generate_coefficients(self) -> List[Tuple[int, int]]:
        rng = random.Random(42)
        return [(rng.randint(1, (1 << 31) - 1), rng.randint(0, (1 << 31) - 1))
                for _ in range(self.NUM_PERM)]

    def _shingles(self, text: str) -> set:
        words = text.split()
        return {" ".join(words[i:i + self.SHINGLE_SIZE])
                for i in range(max(1, len(words) - self.SHINGLE_SIZE + 1))}

    def _minhash(self, shingles: set) -> List[int]:
        signature = [self.MAX_HASH] * self.NUM_PERM
        for shingle in shingles:
            h = int(hashlib.md5(shingle.encode()).hexdigest(), 16) % self.PRIME
            for i, (a, b) in enumerate(self._coefficients):
                val = ((a * h + b) % self.PRIME) & self.MAX_HASH
                if val < signature[i]:
                    signature[i] = val
        return signature

    def is_duplicate(self, text: str, doc_id: str = "") -> bool:
        # 1) Exact dedup
        exact_h = hashlib.sha256(text.encode()).hexdigest()
        if exact_h in self.exact_hashes:
            return True
        self.exact_hashes.add(exact_h)

        # 2) MinHash near-dedup
        shingles = self._shingles(text)
        sig = self._minhash(shingles)
        for band_idx in range(self.num_bands):
            start = band_idx * self.rows_per_band
            band = tuple(sig[start: start + self.rows_per_band])
            bucket = self.band_buckets[band_idx]
            if band in bucket:
                return True  # near-duplicate found
            bucket[band] = doc_id or exact_h
        return False


class LegacySimHashDeduplicator:
    """SimHash for paragraph-level deduplication."""

    BITS = 64
    THRESHOLD = 3  # hamming distance ≤ 3 → duplicate

    def __init__(self):
        self.fingerprints: List[int] = []

    def _simhash(self, text: str) -> int:
        v = [0] * self.BITS
        for word in text.split():
            h = int(hashlib.md5(word.encode()).hexdigest(), 16)
            for i in range(self.BITS):
                v[i] += 1 if (h >> i) & 1 else -1
        return sum(1 << i for i in range(self.BITS) if v[i] > 0)

    def _hamming(self, a: int, b: int) -> int:
        return bin(a ^ b).count("1")

    def is_duplicate(self, text: str) -> bool:
        fp = self._simhash(text)
        for existing in self.fingerprints:
            if self._hamming(fp, existing) <= self.THRESHOLD:
                return True
        self.fingerprints.append(fp)
        return False


class LegacySemanticDeduplicator:
    """Combines exact + MinHash + SimHash strategies."""

    def __init__(self):
        self.minhash = LegacyMinHashDeduplicator()
        self.simhash = LegacySimHashDeduplicator()

    def is_duplicate(self, text: str, doc_id: str = "") -> bool:
        return self.minhash.is_duplicate(text, doc_id) or \
               self.simhash.is_duplicate(text)


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 4 — COMPREHENSIVE QUALITY SCORING
# ═════════════════════════════════════════════════════════════════════════════

class LegacyQualityScorer:
    TOXIC_PATTERNS = re.compile(
        r"\b(spam|click here|buy now|free money|xxx|hate|kill yourself)\b",
        re.IGNORECASE,
    )
    GRAMMAR_PENALTIES = re.compile(r"[A-Z]{5,}|(\?\?|\!\!){2,}|\.{4,}")

    def score(self, doc: RawDocument) -> float:
        text = doc.text
        words = text.split()
        n = max(1, len(words))

        # Length score (logarithmic, saturates at 10k tokens)
        length_score = min(1.0, math.log1p(n) / math.log1p(10_000))

        # Vocabulary richness (type-token ratio, capped)
        ttr = len(set(words)) / n
        info_density = min(1.0, ttr * 2)

        # Sentence count signal
        sentences = re.split(r"[.!?]+", text)
        avg_sent_len = n / max(1, len(sentences))
        structure_score = 1.0 if 5 < avg_sent_len < 50 else 0.5

        # Toxicity penalty
        toxicity_penalty = 0.4 if self.TOXIC_PATTERNS.search(text) else 0.0

        # Grammar irregularities
        grammar_penalty = min(0.3, len(self.GRAMMAR_PENALTIES.findall(text)) * 0.05)

        # Perplexity proxy (character-level entropy)
        freq = Counter(text)
        total = len(text)
        entropy = -sum((c / total) * math.log2(c / total) for c in freq.values() if c)
        perplexity_score = min(1.0, entropy / 5.0)  # normalised

        # Spam signal: repetitive content
        if n > 20:
            bigrams = [f"{words[i]} {words[i+1]}" for i in range(n - 1)]
            bigram_repeat_rate = 1 - len(set(bigrams)) / max(1, len(bigrams))
            spam_penalty = min(0.5, bigram_repeat_rate * 1.5)
        else:
            spam_penalty = 0.0

        total_score = (
            length_score    * 0.20 +
            info_density    * 0.20 +
            structure_score * 0.15 +
            perplexity_score* 0.15 +
            (1 - toxicity_penalty) * 0.15 +
            (1 - grammar_penalty)  * 0.10 +
            (1 - spam_penalty)     * 0.05
        )
        return round(total_score, 4)


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 5 — MULTILINGUAL SUPPORT
# ═════════════════════════════════════════════════════════════════════════════

class MultilingualBalancer:
    TARGET_LANG_RATIOS = {
        "en": 0.50, "zh": 0.08, "de": 0.06, "fr": 0.06,
        "es": 0.06, "ar": 0.05, "tr": 0.05, "ru": 0.05,
        "ja": 0.04, "other": 0.05,
    }

    def __init__(self, total_tokens: int):
        self.total_tokens = total_tokens
        self.lang_tokens: Dict[str, int] = defaultdict(int)

    def accept(self, lang: str, token_count: int) -> bool:
        key = lang if lang in self.TARGET_LANG_RATIOS else "other"
        limit = self.total_tokens * self.TARGET_LANG_RATIOS[key]
        if self.lang_tokens[key] < limit:
            self.lang_tokens[key] += token_count
            return True
        return False

    def report(self) -> Dict[str, float]:
        total = max(1, sum(self.lang_tokens.values()))
        return {lang: tokens / total for lang, tokens in self.lang_tokens.items()}


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 6 — REASONING DATASET PIPELINE
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class ReasoningExample:
    problem: str
    chain_of_thought: str
    answer: str
    domain: str  # math | logic | algorithm | science


class ChainOfThoughtFormatter:
    DOMAINS = ["math", "logic", "algorithm", "science"]

    def format(self, example: ReasoningExample) -> str:
        return (
            f"Problem: {example.problem}\n\n"
            f"Let me think step by step:\n{example.chain_of_thought}\n\n"
            f"Answer: {example.answer}"
        )

    def create_synthetic(self, n: int = 5) -> List[ReasoningExample]:
        """Generate placeholder CoT examples."""
        templates = [
            ("If x + 5 = 12, find x.", "x + 5 = 12 → x = 12 - 5 = 7", "7", "math"),
            ("Is 'All A are B, All B are C, therefore All A are C' valid?",
             "This is a valid syllogism (transitive property).", "Yes", "logic"),
            ("Find the sum of first N natural numbers.",
             "The formula is N*(N+1)/2. Derived by pairing terms.", "N*(N+1)/2", "algorithm"),
        ]
        return [ReasoningExample(*templates[i % len(templates)]) for i in range(n)]


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 7 — CODE DATASET PIPELINE
# ═════════════════════════════════════════════════════════════════════════════

class LegacyCodeDatasetPipeline:
    SUPPORTED_LANGS = {"python", "javascript", "rust", "go", "c", "cpp",
                       "java", "typescript", "bash", "solidity"}

    LICENSE_WHITELIST = {"mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause",
                         "unlicense", "isc"}

    LANG_EXTENSIONS = {
        ".py": "python", ".js": "javascript", ".rs": "rust", ".go": "go",
        ".c": "c", ".cpp": "cpp", ".java": "java", ".ts": "typescript",
        ".sh": "bash", ".sol": "solidity",
    }

    def detect_language(self, filename: str) -> Optional[str]:
        suffix = Path(filename).suffix.lower()
        return self.LANG_EXTENSIONS.get(suffix)

    def is_acceptable(self, doc: RawDocument) -> bool:
        if doc.license.lower() not in self.LICENSE_WHITELIST:
            return False
        if doc.repo_stars < 50:
            return False
        if len(doc.text.splitlines()) < 5:
            return False
        return True

    def extract_ast_features(self, code: str, lang: str) -> Dict:
        """Returns lightweight AST-level features (real impl. uses tree-sitter)."""
        lines = code.splitlines()
        return {
            "language": lang,
            "line_count": len(lines),
            "has_docstring": '"""' in code or "'''" in code,
            "function_count": code.count("def ") + code.count("function "),
            "class_count": code.count("class "),
            "comment_ratio": sum(1 for l in lines if l.strip().startswith(("#", "//", "*")))
                             / max(1, len(lines)),
        }

    def deduplicate_code(self, snippets: List[str]) -> List[str]:
        """Line-level code deduplication via hash."""
        seen: set = set()
        result = []
        for s in snippets:
            h = hashlib.sha256(s.encode()).hexdigest()
            if h not in seen:
                seen.add(h)
                result.append(s)
        return result


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 8 — LONG CONTEXT DATASET
# ═════════════════════════════════════════════════════════════════════════════

class LegacyLongContextDataset:
    SOURCES = ["books", "research_papers", "legal_documents", "technical_manuals"]

    def __init__(self, context_window: int = 32_768):
        self.context_window = context_window

    def build_long_chunks(self, documents: List[str]) -> List[str]:
        """Concatenate related documents into long context sequences."""
        chunks, current, length = [], [], 0
        for doc in documents:
            tokens = doc.split()
            if length + len(tokens) > self.context_window:
                if current:
                    chunks.append(" ".join(current))
                current, length = tokens[: self.context_window], len(tokens)
            else:
                current.extend(tokens)
                length += len(tokens)
        if current:
            chunks.append(" ".join(current))
        return chunks


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 9 — TOKENIZER TRAINING PIPELINE
# ═════════════════════════════════════════════════════════════════════════════
# NOTE: TokenizerConfig, BPETokenizerTrainer, and estimate_tokens are defined
# in the production tokenizer utilities section near the end of the file.


# =============================================================================
# MODULE 10 — POSITIONAL ENCODING STRATEGY
# =============================================================================

@dataclass
class PositionalEncodingConfig:
    strategy: str = "rope"          # rope | alibi | learned | sinusoidal
    max_position: int = 32_768
    rope_theta: float = 10_000.0
    rope_scaling: Optional[Dict] = None   # {"type": "linear", "factor": 4}
    alibi_num_heads: int = 32

    def describe(self) -> str:
        if self.strategy == "rope":
            extra = f", scaling={self.rope_scaling}" if self.rope_scaling else ""
            return f"RoPE: theta={self.rope_theta}, max_pos={self.max_position}{extra}"
        if self.strategy == "alibi":
            return f"ALiBi: {self.alibi_num_heads} heads, max_pos={self.max_position}"
        return f"{self.strategy}: max_pos={self.max_position}"


# =============================================================================
# MODULE 11 — MODEL ARCHITECTURE
# =============================================================================

@dataclass
class LegacyModelArchitectureConfig:
    name: str = "NovaTitan-250M"
    hidden_size: int = 1024
    num_layers: int = 24
    num_attention_heads: int = 16
    num_kv_heads: int = 8              # Grouped-Query Attention (GQA)
    intermediate_size: int = 4096
    vocab_size: int = 65_536
    max_seq_len: int = 32_768
    attention_type: str = "gqa"        # mha | gqa | mqa
    ffn_type: str = "swiglu"           # gelu | swiglu | geglu
    normalization: str = "rmsnorm"     # layernorm | rmsnorm
    norm_eps: float = 1e-5
    tie_embeddings: bool = True
    use_flash_attention: bool = True
    positional_encoding: "PositionalEncodingConfig" = field(
        default_factory=lambda: PositionalEncodingConfig(strategy="rope")
    )

    def parameter_count(self) -> int:
        embed = self.vocab_size * self.hidden_size
        attn  = 4 * self.hidden_size ** 2 * self.num_layers
        ffn   = 3 * self.hidden_size * self.intermediate_size * self.num_layers
        return embed + attn + ffn

    def summary(self) -> str:
        p = self.parameter_count() / 1e6
        return (
            f"{self.name}  ~{p:.0f}M params  layers={self.num_layers}  "
            f"heads={self.num_attention_heads}(kv={self.num_kv_heads})  "
            f"hidden={self.hidden_size}  ffn={self.ffn_type}  "
            f"pos={self.positional_encoding.strategy}"
        )


# =============================================================================
# MODULE 12 — TRAINING PIPELINE
# =============================================================================

@dataclass
class LegacyTrainingConfig:
    precision: str = "bf16"
    gradient_checkpointing: bool = True
    distributed_backend: str = "accelerate"
    num_gpus: int = 1
    optimizer: str = "adamw"
    learning_rate: float = 3e-4
    weight_decay: float = 0.1
    beta1: float = 0.9
    beta2: float = 0.95
    gradient_clip: float = 1.0
    warmup_steps: int = 200
    max_steps: int = 2000
    lr_schedule: str = "cosine_with_warmup"
    min_lr_ratio: float = 0.1
    batch_size_per_gpu: int = 2
    gradient_accumulation_steps: int = 4
    save_every_n_steps: int = 250
    eval_every_n_steps: int = 250
    max_train_sequences: int = 10000
    dataloader_workers: int = 0


class LegacyNovaTrainerPipeline:
    def __init__(self, model_cfg: "LegacyModelArchitectureConfig", train_cfg: "LegacyTrainingConfig"):
        self.model_cfg = model_cfg
        self.train_cfg = train_cfg

    @property
    def effective_batch_size(self) -> int:
        return self.train_cfg.batch_size_per_gpu * max(1, self.train_cfg.num_gpus) * self.train_cfg.gradient_accumulation_steps

    def total_tokens(self) -> int:
        return self.effective_batch_size * self.model_cfg.max_seq_len * self.train_cfg.max_steps

    def init_training(self) -> Dict[str, Any]:
        return {
            "model": self.model_cfg.summary(),
            "backend": self.train_cfg.distributed_backend,
            "precision": self.train_cfg.precision,
            "effective_batch_size": self.effective_batch_size,
            "planned_total_tokens": self.total_tokens(),
            "target_tokens": None,
            "max_steps": self.train_cfg.max_steps,
            "torch_available": optional_import("torch") is not None,
            "accelerate_available": optional_import("accelerate") is not None,
            "deepspeed_available": optional_import("deepspeed") is not None,
        }

    def lr_at_step(self, step: int) -> float:
        if step < self.train_cfg.warmup_steps:
            return self.train_cfg.learning_rate * step / max(1, self.train_cfg.warmup_steps)
        progress = (step - self.train_cfg.warmup_steps) / max(1, self.train_cfg.max_steps - self.train_cfg.warmup_steps)
        cosine = 0.5 * (1.0 + math.cos(math.pi * progress))
        return (self.train_cfg.min_lr_ratio + (1 - self.train_cfg.min_lr_ratio) * cosine) * self.train_cfg.learning_rate

    def train(self, chunks: Sequence[DocumentChunk], output_dir: Path, token_counter: Optional[TokenCounter] = None) -> Dict[str, Any]:
        torch_mod = optional_import("torch")
        if not torch_mod:
            raise RuntimeError("Install torch to run the real training loop.")
        transformers_mod = optional_import("transformers")
        accelerate_mod = optional_import("accelerate")
        token_counter = token_counter or DEFAULT_TOKEN_COUNTER
        sequences = self._build_sequences(chunks, token_counter)
        if not sequences:
            raise RuntimeError("No training sequences were produced.")
        tensor = torch_mod.tensor(sequences, dtype=torch_mod.long)
        dataset = torch_mod.utils.data.TensorDataset(tensor)
        dataloader = torch_mod.utils.data.DataLoader(
            dataset,
            batch_size=self.train_cfg.batch_size_per_gpu,
            shuffle=True,
            num_workers=self.train_cfg.dataloader_workers,
        )
        model = self._build_model(torch_mod, transformers_mod, int(tensor.max().item()) + 1)
        optimizer = torch_mod.optim.AdamW(
            model.parameters(),
            lr=self.train_cfg.learning_rate,
            betas=(self.train_cfg.beta1, self.train_cfg.beta2),
            weight_decay=self.train_cfg.weight_decay,
        )
        accelerator = accelerate_mod.Accelerator() if accelerate_mod and self.train_cfg.distributed_backend == "accelerate" else None
        device = "cuda" if torch_mod.cuda.is_available() else "cpu"
        if accelerator:
            model, optimizer, dataloader = accelerator.prepare(model, optimizer, dataloader)
        else:
            model.to(device)
        output_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_dir = output_dir / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        losses = []
        optimizer.zero_grad()
        step = 0
        for batch in dataloader:
            sequence = batch[0]
            if not accelerator:
                sequence = sequence.to(device)
            if transformers_mod and hasattr(model, "config"):
                outputs = model(input_ids=sequence[:, :-1], labels=sequence[:, 1:])
                loss = outputs.loss / self.train_cfg.gradient_accumulation_steps
            else:
                logits = model(sequence[:, :-1])
                vocab_dim = logits.size(-1)
                loss = torch_mod.nn.functional.cross_entropy(
                    logits.reshape(-1, vocab_dim),
                    sequence[:, 1:].reshape(-1),
                ) / self.train_cfg.gradient_accumulation_steps
            if accelerator:
                accelerator.backward(loss)
            else:
                loss.backward()
            if (step + 1) % self.train_cfg.gradient_accumulation_steps == 0:
                if accelerator:
                    accelerator.clip_grad_norm_(model.parameters(), self.train_cfg.gradient_clip)
                else:
                    torch_mod.nn.utils.clip_grad_norm_(model.parameters(), self.train_cfg.gradient_clip)
                for param_group in optimizer.param_groups:
                    param_group["lr"] = self.lr_at_step(step)
                optimizer.step()
                optimizer.zero_grad()
            losses.append(float(loss.detach().cpu()) * self.train_cfg.gradient_accumulation_steps)
            step += 1
            if step % self.train_cfg.save_every_n_steps == 0:
                self._save_checkpoint(model, optimizer, checkpoint_dir / f"step_{step:06d}.pt", accelerator, torch_mod)
            if step >= self.train_cfg.max_steps:
                break
        self._save_checkpoint(model, optimizer, checkpoint_dir / "final.pt", accelerator, torch_mod)
        return {
            "steps": step,
            "mean_loss": round(sum(losses) / max(1, len(losses)), 6),
            "checkpoints": sorted(str(path) for path in checkpoint_dir.glob("*.pt")),
        }

    def _build_sequences(self, chunks: Sequence[DocumentChunk], token_counter: TokenCounter) -> List[List[int]]:
        max_len = self.model_cfg.max_seq_len + 1
        sequences = []
        for chunk in chunks:
            ids = token_counter.encode(chunk.text)
            if len(ids) < 16:
                continue
            for start in range(0, len(ids) - 1, self.model_cfg.max_seq_len):
                seq = ids[start:start + max_len]
                if len(seq) < 16:
                    continue
                if len(seq) < max_len:
                    seq = seq + [0] * (max_len - len(seq))
                sequences.append(seq[:max_len])
                if len(sequences) >= self.train_cfg.max_train_sequences:
                    return sequences
        return sequences

    def _build_model(self, torch_mod, transformers_mod, vocab_size: int):
        if transformers_mod:
            config = transformers_mod.GPT2Config(
                vocab_size=max(self.model_cfg.vocab_size, vocab_size),
                n_positions=self.model_cfg.max_seq_len,
                n_ctx=self.model_cfg.max_seq_len,
                n_embd=self.model_cfg.hidden_size,
                n_layer=self.model_cfg.num_layers,
                n_head=self.model_cfg.num_attention_heads,
            )
            return transformers_mod.GPT2LMHeadModel(config)
        class TinyCausalLM(torch_mod.nn.Module):
            def __init__(self, vocab: int, hidden: int, layers: int, heads: int):
                super().__init__()
                self.embed = torch_mod.nn.Embedding(vocab, hidden)
                encoder_layer = torch_mod.nn.TransformerEncoderLayer(d_model=hidden, nhead=heads, batch_first=True)
                self.encoder = torch_mod.nn.TransformerEncoder(encoder_layer, num_layers=max(1, min(layers, 6)))
                self.head = torch_mod.nn.Linear(hidden, vocab)
            def forward(self, input_ids):
                hidden_states = self.embed(input_ids)
                encoded = self.encoder(hidden_states)
                return self.head(encoded)
        return TinyCausalLM(max(self.model_cfg.vocab_size, vocab_size), min(self.model_cfg.hidden_size, 768), min(self.model_cfg.num_layers, 6), min(self.model_cfg.num_attention_heads, 8))

    @staticmethod
    def _save_checkpoint(model, optimizer, path: Path, accelerator, torch_mod):
        state_dict = accelerator.unwrap_model(model).state_dict() if accelerator else model.state_dict()
        torch_mod.save({"model": state_dict, "optimizer": optimizer.state_dict()}, path)


# =============================================================================
# MODULE 13 - DATASET SHARDING & STREAMING
# =============================================================================

class LegacyDatasetShardManager:
    """
    Writes large datasets as sharded JSONL files.
    Production: use Apache Arrow / HuggingFace datasets .save_to_disk() sharding.

    Install:  pip install pyarrow datasets
    """
    def __init__(self, shard_size_tokens: int = 500_000_000):
        self.shard_size_tokens = shard_size_tokens
        self.shards: List[Path] = []

    def write_shards(self, packed_chunks: "Iterator[str]", output_dir: Path) -> int:
        output_dir.mkdir(parents=True, exist_ok=True)
        shard_idx, current_tokens, f = 0, 0, None
        try:
            for chunk in packed_chunks:
                tok = estimate_tokens(chunk)
                if f is None or current_tokens + tok > self.shard_size_tokens:
                    if f:
                        f.close()
                    shard_path = output_dir / f"shard_{shard_idx:05d}.jsonl"
                    self.shards.append(shard_path)
                    f = open(shard_path, "w", encoding="utf-8")
                    shard_idx += 1
                    current_tokens = 0
                f.write(json.dumps({"text": chunk}) + "\n")
                current_tokens += tok
        finally:
            if f and not f.closed:
                f.close()
        log.info("Wrote %d shards → %s", shard_idx, output_dir)
        return shard_idx

    def stream_shards(self) -> "Iterator[str]":
        for shard_path in self.shards:
            with open(shard_path, encoding="utf-8") as f:
                for line in f:
                    yield json.loads(line)["text"]


# =============================================================================
# MODULE 14 — CURRICULUM TRAINING
# =============================================================================

class CurriculumScheduler:
    """
    Sorts training data easy → hard (short/general first, expert last).
    Inspired by: https://arxiv.org/abs/2205.01068
    """
    PHASES = [
        {"name": "ph1_warmup",    "max_tokens": 512,
         "categories": ["general_web", "conversation"],  "ratio": 0.20},
        {"name": "ph2_domain",    "max_tokens": 2_048,
         "categories": ["general_web", "multilingual"], "ratio": 0.30},
        {"name": "ph3_technical", "max_tokens": 4_096,
         "categories": ["code_ast", "math_reasoning"],  "ratio": 0.30},
        {"name": "ph4_expert",    "max_tokens": 8_192,
         "categories": ["cyber_sec", "math_reasoning"], "ratio": 0.20},
    ]

    def assign_phase(self, token_count: int, category: str) -> str:
        for ph in self.PHASES:
            if token_count <= ph["max_tokens"] and category in ph["categories"]:
                return ph["name"]
        return "ph4_expert"

    def sort_dataset(self, documents: Sequence[RawDocument]) -> List[RawDocument]:
        order = {ph["name"]: i for i, ph in enumerate(self.PHASES)}
        def _phase(doc: RawDocument) -> str:
            token_count = doc.token_count or estimate_tokens(doc.text)
            return self.assign_phase(token_count, doc.category)
        return sorted(documents, key=lambda d: order.get(_phase(d), 99))


# =============================================================================
# MODULE 15 — RLHF PIPELINE
# =============================================================================

@dataclass
class InstructionExample:
    system: str
    user: str
    assistant: str
    source: str = "synthetic"
    preference_chosen: str = ""
    preference_rejected: str = ""


class RLHFPipeline:
    """
    3-stage RLHF:
      Stage 1 SFT  — Supervised Fine-Tuning on instruction pairs
      Stage 2 RM   — Reward Model trained on preference pairs
      Stage 3 PPO  — RL optimization against reward model score

    Production libraries: trl (HuggingFace), OpenRLHF, DeepSpeed-Chat
    Install: pip install trl
    """
    SYSTEM_PROMPTS = {
        "general":  "You are a helpful, harmless, and honest AI assistant.",
        "code":     "You are an expert software engineer. Write clean, efficient code.",
        "security": "You are a cybersecurity expert. Analyze threats and defenses.",
        "math":     "You are a math tutor. Solve problems step by step.",
    }

    def __init__(self):
        self.sft_dataset: List[InstructionExample] = []
        self.preference_dataset: List[InstructionExample] = []

    def add_sft(self, ex: InstructionExample):
        self.sft_dataset.append(ex)

    def add_preference(self, ex: InstructionExample):
        assert ex.preference_chosen and ex.preference_rejected
        self.preference_dataset.append(ex)

    def format_sft(self, ex: InstructionExample) -> str:
        return (
            f"[SYS]{ex.system}[/SYS]\n"
            f"[USER]{ex.user}[/USER]\n"
            f"[ASSISTANT]{ex.assistant}[/ASSISTANT]"
        )

    def format_preference(self, ex: InstructionExample) -> Dict:
        return {
            "prompt":   f"[SYS]{ex.system}[/SYS]\n[USER]{ex.user}[/USER]\n",
            "chosen":   f"[ASSISTANT]{ex.preference_chosen}[/ASSISTANT]",
            "rejected": f"[ASSISTANT]{ex.preference_rejected}[/ASSISTANT]",
        }

    def save(self, output_dir: Path):
        output_dir.mkdir(parents=True, exist_ok=True)
        sft_path = output_dir / "sft_dataset.jsonl"
        pref_path = output_dir / "preference_dataset.jsonl"
        with open(sft_path, "w", encoding="utf-8") as f:
            for ex in self.sft_dataset:
                f.write(json.dumps({"text": self.format_sft(ex),
                                    "source": ex.source}) + "\n")
        with open(pref_path, "w", encoding="utf-8") as f:
            for ex in self.preference_dataset:
                f.write(json.dumps(self.format_preference(ex)) + "\n")
        log.info("RLHF data saved | SFT=%d | Preference=%d",
                 len(self.sft_dataset), len(self.preference_dataset))


class InstructionDatasetBuilder:
    """Converts cleaned documents into simple instruction/response pairs."""
    def __init__(self, max_assistant_chars: int = 1200, min_text_chars: int = 160):
        self.max_assistant_chars = max_assistant_chars
        self.min_text_chars = min_text_chars

    def build_from_document(self, doc: RawDocument) -> List[InstructionExample]:
        qa = self._extract_qa(doc)
        if qa:
            question, answer = qa
            return [InstructionExample(
                system=self._system_for(doc),
                user=question,
                assistant=answer,
                source=doc.source,
            )]
        if doc.category == "code_ast":
            return []
        summary = self._summarize(doc.text)
        if not summary:
            return []
        return [InstructionExample(
            system=self._system_for(doc),
            user=self._prompt_for(doc),
            assistant=summary,
            source=doc.source,
        )]

    def _extract_qa(self, doc: RawDocument) -> Optional[Tuple[str, str]]:
        meta = doc.metadata or {}
        question = meta.get("question")
        answer = meta.get("answer")
        if isinstance(question, str) and isinstance(answer, str) and question.strip() and answer.strip():
            return question.strip(), answer.strip()
        text = doc.text or ""
        if "Question:" in text and "Answer:" in text:
            parts = re.split(r"\bAnswer:\s*", text, maxsplit=1)
            if len(parts) == 2:
                q = re.sub(r"^Question:\s*", "", parts[0]).strip()
                a = parts[1].strip()
                if q and a:
                    return q, a
        return None

    def _summarize(self, text: str) -> str:
        if not text or len(text) < self.min_text_chars:
            return ""
        sentences = re.split(r"(?<=[.!?])\s+", text.strip())
        out = []
        total = 0
        for sent in sentences:
            if not sent:
                continue
            if total + len(sent) > self.max_assistant_chars and out:
                break
            out.append(sent)
            total += len(sent) + 1
            if total >= self.max_assistant_chars:
                break
        return " ".join(out).strip()

    def _system_for(self, doc: RawDocument) -> str:
        if doc.category == "math_reasoning":
            return RLHFPipeline.SYSTEM_PROMPTS["math"]
        if doc.category == "cyber_sec":
            return RLHFPipeline.SYSTEM_PROMPTS["security"]
        if doc.category == "code_ast":
            return RLHFPipeline.SYSTEM_PROMPTS["code"]
        return RLHFPipeline.SYSTEM_PROMPTS["general"]

    def _prompt_for(self, doc: RawDocument) -> str:
        if doc.category == "cyber_sec":
            return "Summarize the vulnerability, its impact, and mitigation steps."
        if doc.category == "science":
            return "Summarize the research text in plain language."
        if doc.category == "math_reasoning":
            return "Explain the solution step by step."
        return "Summarize the text and list key points."


# =============================================================================
# MODULE 16 — SAFETY DATASET
# =============================================================================

class SafetyFilter:
    """
    Filters harmful content and builds a safety/refusal dataset.
    Categories: jailbreak prompts, prompt injection, harmful instructions,
                malicious code generation, PII leakage.
    Production: Perspective API, OpenAI moderation endpoint, Detoxify.
    Install: pip install detoxify
    """
    JAILBREAK_PATTERNS = re.compile(
        r"(ignore (previous|all) instructions|pretend you are|"
        r"do anything now|dan prompt|bypass safety|"
        r"act as if you have no (restrictions|limits))",
        re.IGNORECASE,
    )
    HARMFUL_PATTERNS = re.compile(
        r"(how to (make|build|create|synthesize) "
        r"(bomb|weapon|poison|malware|ransomware)|"
        r"credit card (dump|skimmer)|child (porn|abuse))",
        re.IGNORECASE,
    )
    PII_PATTERNS = re.compile(
        r"\b(\d{3}[-.\s]?\d{2}[-.\s]?\d{4}|"           # SSN
        r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b|"    # CC
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b",  # email
        re.IGNORECASE,
    )

    def is_safe(self, text: str) -> bool:
        if self.JAILBREAK_PATTERNS.search(text):
            return False
        if self.HARMFUL_PATTERNS.search(text):
            return False
        return True

    def redact_pii(self, text: str) -> str:
        return self.PII_PATTERNS.sub("<|PII|>", text)

    def build_refusal(self, harmful_prompt: str) -> InstructionExample:
        return InstructionExample(
            system=RLHFPipeline.SYSTEM_PROMPTS["general"],
            user=harmful_prompt,
            assistant=(
                "I'm sorry, I can't help with that request. "
                "If you have a legitimate need, please rephrase it."
            ),
            source="safety_synthetic",
        )


@dataclass
class EvalModelSpec:
    model_backend: str = "hf"
    model_args: Dict[str, Any] = field(default_factory=dict)
    device: str = "cuda"
    batch_size: str = "auto"
    extra_cli_args: List[str] = field(default_factory=list)


# =============================================================================
# MODULE 17 — REAL EVALUATION SYSTEM
# =============================================================================

@dataclass
class BenchmarkResult:
    name: str
    score: Optional[float]
    num_samples: int
    metric: str
    target: float
    status: str = "skipped"
    notes: str = ""


class LegacyEvaluator:
    BENCHMARK_CONFIGS = {
        "MMLU": {"task": "mmlu", "num_samples": 14042, "metric": "accuracy", "metric_keys": ["acc,none", "acc"], "target": 0.70},
        "HellaSwag": {"task": "hellaswag", "num_samples": 10003, "metric": "accuracy", "metric_keys": ["acc,none", "acc_norm,none", "acc"], "target": 0.85},
        "GSM8K": {"task": "gsm8k", "num_samples": 1319, "metric": "exact_match", "metric_keys": ["exact_match,strict-match", "exact_match"], "target": 0.55},
        "HumanEval": {"task": "humaneval", "num_samples": 164, "metric": "pass@1", "metric_keys": ["pass@1,create_test", "pass@1"], "target": 0.40},
    }

    def run_benchmarks(self, model_spec: Optional[EvalModelSpec] = None, output_dir: Optional[Path] = None) -> Dict[str, BenchmarkResult]:
        if model_spec is None:
            return {
                name: BenchmarkResult(name, None, cfg["num_samples"], cfg["metric"], cfg["target"], "skipped", "provide EvalModelSpec to run lm-eval")
                for name, cfg in self.BENCHMARK_CONFIGS.items()
            }
        if not optional_import("lm_eval"):
            return {
                name: BenchmarkResult(name, None, cfg["num_samples"], cfg["metric"], cfg["target"], "skipped", "install lm-eval")
                for name, cfg in self.BENCHMARK_CONFIGS.items()
            }
        output_dir = output_dir or (get_nova_base() / "evals" / safe_filename(now_utc()))
        output_dir.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            "-m",
            "lm_eval",
            "--model",
            model_spec.model_backend,
            "--model_args",
            ",".join(f"{key}={value}" for key, value in model_spec.model_args.items()),
            "--tasks",
            ",".join(cfg["task"] for cfg in self.BENCHMARK_CONFIGS.values()),
            "--device",
            model_spec.device,
            "--batch_size",
            model_spec.batch_size,
            "--output_path",
            str(output_dir),
        ] + model_spec.extra_cli_args
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            notes = (exc.stderr or exc.stdout or str(exc)).strip()[:500]
            return {
                name: BenchmarkResult(name, None, cfg["num_samples"], cfg["metric"], cfg["target"], "failed", notes)
                for name, cfg in self.BENCHMARK_CONFIGS.items()
            }
        payload = {}
        for candidate in sorted(output_dir.rglob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
                if "results" in payload:
                    break
            except Exception:
                continue
        results = payload.get("results", {})
        parsed = {}
        for name, cfg in self.BENCHMARK_CONFIGS.items():
            task_result = results.get(cfg["task"], {})
            score = None
            for metric_key in cfg["metric_keys"]:
                if metric_key in task_result:
                    score = float(task_result[metric_key])
                    break
            parsed[name] = BenchmarkResult(
                name=name,
                score=score,
                num_samples=cfg["num_samples"],
                metric=cfg["metric"],
                target=cfg["target"],
                status="completed" if score is not None else "missing",
                notes="" if score is not None else "metric not found",
            )
        return parsed

    def print_report(self, results: Dict[str, BenchmarkResult]):
        rich_mod = optional_import("rich")
        if rich_mod:
            try:
                from rich.console import Console
                from rich.table import Table
                table = Table(title="Benchmark Report")
                table.add_column("Benchmark", justify="left")
                table.add_column("Score", justify="right")
                table.add_column("Target", justify="right")
                table.add_column("Metric", justify="left")
                table.add_column("Status", justify="left")
                for result in results.values():
                    score_text = f"{result.score:.3f}" if result.score is not None else "-"
                    table.add_row(
                        result.name,
                        score_text,
                        f"{result.target:.2f}",
                        result.metric,
                        result.status,
                    )
                Console().print(table)
                return
            except Exception as exc:
                log.warning("Rich report failed: %s", exc)
        print("\n" + "=" * 72)
        print(f"{'Benchmark':<16} {'Score':>10} {'Target':>10} {'Metric':<15} Status")
        print("-" * 72)
        for result in results.values():
            score_text = f"{result.score:.3f}" if result.score is not None else "-"
            print(f"{result.name:<16} {score_text:>10} {result.target:>10.2f} {result.metric:<15} {result.status}")
        print("=" * 72 + "\n")


# =============================================================================
# MODULE 18 - TOOL USE & MEMORY DATASET
# =============================================================================

@dataclass
class ToolUseExample:
    system: str
    user: str
    tool_call: Dict          # {"name": "search_web", "args": {...}}
    tool_response: str
    final_answer: str


class ToolUseDatasetBuilder:
    """
    Builds a dataset for teaching the model to:
      - Call external APIs / tools
      - Parse and use tool responses
      - Maintain multi-turn memory via a scratchpad
    """
    TOOL_CATALOG = {
        "search_web":    "Search the internet for current information.",
        "run_code":      "Execute Python code and return stdout/stderr.",
        "read_file":     "Read a local file and return its contents.",
        "query_db":      "Run SQL query against a database.",
        "calc":          "Evaluate a mathematical expression.",
        "fetch_cve":     "Retrieve CVE details by CVE-ID.",
    }

    def create_example(self, user_query: str, tool_name: str,
                       tool_args: Dict, tool_response: str,
                       final_answer: str) -> ToolUseExample:
        assert tool_name in self.TOOL_CATALOG, f"Unknown tool: {tool_name}"
        return ToolUseExample(
            system=f"You have access to tools: {list(self.TOOL_CATALOG.keys())}",
            user=user_query, tool_call={"name": tool_name, "args": tool_args},
            tool_response=tool_response, final_answer=final_answer,
        )

    def synthetic_samples(self) -> List[ToolUseExample]:
        return [
            self.create_example(
                "What is the square root of 144?", "calc",
                {"expr": "sqrt(144)"}, "12.0", "The square root of 144 is 12."),
            self.create_example(
                "Run this Python snippet: print(2**10)", "run_code",
                {"code": "print(2**10)"}, "1024", "The output is 1024."),
        ]


class ConversationMemoryDataset:
    """Builds long multi-turn conversations to teach memory retention."""
    def __init__(self, max_turns: int = 20):
        self.max_turns = max_turns

    def build_example(self, turns: List[Dict]) -> str:
        assert len(turns) <= self.max_turns
        lines = []
        for t in turns:
            role = t.get("role", "user").upper()
            lines.append(f"[{role}] {t['content']}")
        return "\n".join(lines)


# =============================================================================
# MODULE 19 — DATASET VERSIONING
# =============================================================================

class DatasetVersionManager:
    """
    Tracks dataset versions and experiment metadata.
    Production: DVC (Data Version Control) or MLflow.
    Install: pip install dvc mlflow
    """
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.registry_path = base_dir / "dataset_registry.json"
        self.registry: Dict = self._load()

    def _load(self) -> Dict:
        if self.registry_path.exists():
            return json.loads(self.registry_path.read_text(encoding="utf-8"))
        return {"versions": []}

    def _save(self):
        self.registry_path.write_text(
            json.dumps(self.registry, indent=2), encoding="utf-8")

    def register(self, version: str, stats: Dict, notes: str = "") -> Dict:
        entry = {
            "version": version,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "stats": stats,
            "notes": notes,
        }
        self.registry["versions"].append(entry)
        self._save()
        log.info("Registered dataset version %s | stats=%s", version, stats)
        return entry

    def latest(self) -> Optional[Dict]:
        vs = self.registry.get("versions", [])
        return vs[-1] if vs else None


# =============================================================================
# MODULE 20 — COMPUTE PLAN
# =============================================================================

@dataclass
class ComputePlan:
    """
    GPU resource planning for LLM training.
    Reference: https://huggingface.co/blog/estimate-token-training
    """
    gpu_model: str = "A100-80GB"
    num_gpus: int = 8
    vram_per_gpu_gb: float = 80.0
    interconnect: str = "NVLink"        # NVLink | InfiniBand | PCIe
    # Chinchilla scaling law: flops = 6 * N * D  (N=params, D=tokens)
    target_params: int = 250_000_000    # 250M
    target_tokens: int = 5_000_000_000  # 5B (Chinchilla-optimal for 250M)

    def chinchilla_optimal_tokens(self) -> int:
        return 20 * self.target_params   # 20x param count rule of thumb

    def training_flops(self) -> float:
        return 6.0 * self.target_params * self.target_tokens

    def estimated_hours(self, gpu_flops_per_s: float = 312e12,
                        mfu: float = 0.45) -> float:
        """
        Estimate wall-clock training time.
        Default: A100 = 312 TFLOP/s bfloat16, MFU (model flop utilization) = 45%.
        """
        effective_flops = gpu_flops_per_s * self.num_gpus * mfu
        hours = self.training_flops() / effective_flops / 3600.0
        return hours

    def vram_requirement_gb(self, precision_bytes: int = 2) -> float:
        """
        Rough VRAM estimate for parameters + gradients + optimizer states (AdamW).
        AdamW stores momentum + variance: 4 + 4 = 8 bytes extra per param (fp32 states).
        """
        param_bytes = self.target_params * precision_bytes
        optimizer_bytes = self.target_params * 8   # fp32 adam states
        activation_gb = 2.0                        # rough activation cache
        return (param_bytes + optimizer_bytes) / 1e9 + activation_gb

    def report(self) -> str:
        opt_tok = self.chinchilla_optimal_tokens() / 1e9
        flops = self.training_flops() / 1e21
        hours = self.estimated_hours()
        vram  = self.vram_requirement_gb()
        avail = self.vram_per_gpu_gb * self.num_gpus
        return (
            f"\n{'='*60}\n"
            f"  COMPUTE PLAN — {self.gpu_model} x{self.num_gpus}\n"
            f"{'='*60}\n"
            f"  Model params:          {self.target_params/1e6:.0f}M\n"
            f"  Target tokens:         {self.target_tokens/1e9:.1f}B\n"
            f"  Chinchilla optimal:    {opt_tok:.1f}B tokens\n"
            f"  Training FLOPs:        {flops:.2f} ZFLOPs\n"
            f"  Est. training time:    {hours:.1f}h (MFU=45%)\n"
            f"  Required VRAM:         {vram:.1f}GB\n"
            f"  Available VRAM:        {avail:.0f}GB\n"
            f"  Interconnect:          {self.interconnect}\n"
            f"{'='*60}\n"
        )


# =============================================================================
# MODULE 21 — PARALLEL PIPELINE WORKERS
# =============================================================================

class PipelineStats:
    def __init__(self):
        self.total_docs = 0
        self.passed_clean = 0
        self.passed_dedup = 0
        self.passed_quality = 0
        self.passed_lang = 0
        self.tokens_by_category: Dict[str, int] = defaultdict(int)
        self.docs_by_source: Dict[str, int] = defaultdict(int)
        self.rejects: Dict[str, int] = defaultdict(int)

    def reject(self, reason: str) -> None:
        self.rejects[reason] += 1

    def report(self) -> str:
        lines = [
            "\n" + "═" * 55,
            f"  PIPELINE STATS",
            "─" * 55,
            f"  Total ingested:  {self.total_docs:>10,}",
            f"  After cleaning:  {self.passed_clean:>10,}  ({self._pct(self.passed_clean):.1f}%)",
            f"  After dedup:     {self.passed_dedup:>10,}  ({self._pct(self.passed_dedup):.1f}%)",
            f"  After quality:   {self.passed_quality:>10,}  ({self._pct(self.passed_quality):.1f}%)",
            f"  After lang:      {self.passed_lang:>10,}  ({self._pct(self.passed_lang):.1f}%)",
            "─" * 55,
            "  Tokens by category:",
        ]
        for cat, toks in sorted(self.tokens_by_category.items()):
            lines.append(f"    {cat:<20} {toks:>12,} tokens")
        lines += [
            "─" * 55,
            "  Docs by source:",
        ]
        for src, cnt in sorted(self.docs_by_source.items()):
            lines.append(f"    {src:<20} {cnt:>10,} docs")
        lines.append("═" * 55)
        return "\n".join(lines)

    def _pct(self, n: int) -> float:
        return 100.0 * n / max(1, self.total_docs)


# =============================================================================
# REAL DATASET PARSERS (Module 1 Extended)
# =============================================================================

class WikipediaDumpParser:
    def __init__(self, dump_path: str, lang: str = "en"):
        self.dump_path = Path(dump_path) if dump_path else Path()
        self.lang = lang

    def stream_articles(self) -> Iterator[RawDocument]:
        if not self.dump_path or not self.dump_path.exists():
            return
        mwparser_mod = optional_import("mwparserfromhell")
        namespace = "{http://www.mediawiki.org/xml/export-0.10/}"
        opener = bz2.open if self.dump_path.suffix == ".bz2" else open
        with opener(self.dump_path, "rb") as handle:
            for _, elem in ET.iterparse(handle, events=("end",)):
                if elem.tag != f"{namespace}page":
                    continue
                title = elem.findtext(f"{namespace}title", default="")
                ns = elem.findtext(f"{namespace}ns", default="0")
                text_node = elem.find(f".//{namespace}text")
                raw_text = text_node.text if text_node is not None and text_node.text else ""
                elem.clear()
                if ns != "0" or not title or ":" in title or len(raw_text) < 200:
                    continue
                if mwparser_mod:
                    try:
                        cleaned = mwparser_mod.parse(raw_text).strip_code(normalize=True, collapse=True)
                    except Exception:
                        cleaned = raw_text
                else:
                    cleaned = raw_text
                cleaned = normalize_space(re.sub(r"(?s)\{\{.*?\}\}", " ", cleaned))
                if len(cleaned) < 200:
                    continue
                yield RawDocument(
                    source="wikipedia",
                    category="general_web",
                    language=self.lang,
                    text=cleaned,
                    url=f"https://{self.lang}.wikipedia.org/wiki/{title.replace(' ', '_')}",
                    title=title,
                )


class CommonCrawlParser:
    def __init__(self, warc_paths: List[str]):
        self.warc_paths = [Path(path) for path in warc_paths if path]

    def stream(self) -> Iterator[RawDocument]:
        archive_mod = optional_import("warcio.archiveiterator")
        trafilatura_mod = optional_import("trafilatura")
        if not self.warc_paths or not archive_mod:
            return
        for warc_path in self.warc_paths:
            if not warc_path.exists():
                continue
            opener = gzip.open if warc_path.suffix == ".gz" else open
            with opener(warc_path, "rb") as handle:
                for record in archive_mod.ArchiveIterator(handle):
                    if getattr(record, "rec_type", "") != "response":
                        continue
                    url = record.rec_headers.get_header("WARC-Target-URI") or ""
                    try:
                        html_text = record.content_stream().read().decode("utf-8", errors="ignore")
                    except Exception:
                        continue
                    text_value = trafilatura_mod.extract(html_text, url=url) if trafilatura_mod else strip_html(html_text)
                    text_value = normalize_space(text_value or "")
                    if len(text_value) < 200:
                        continue
                    yield RawDocument(
                        source="common_crawl",
                        category="general_web",
                        language="unknown",
                        text=text_value,
                        url=url,
                    )


class StackExchangeParser:
    def __init__(self, posts_xml_path: str, min_score: int = 5):
        self.posts_xml_path = Path(posts_xml_path) if posts_xml_path else Path()
        self.min_score = min_score

    def stream_qa_pairs(self) -> Iterator[RawDocument]:
        if not self.posts_xml_path or not self.posts_xml_path.exists():
            return
        questions = {}
        for _, elem in ET.iterparse(str(self.posts_xml_path), events=("end",)):
            if elem.tag != "row":
                continue
            score = int(elem.get("Score", "0") or "0")
            post_type = elem.get("PostTypeId", "")
            body = normalize_space(strip_html(elem.get("Body", "")))
            title = normalize_space(strip_html(elem.get("Title", "")))
            if post_type == "1" and score >= self.min_score:
                questions[elem.get("Id", "")] = {
                    "title": title,
                    "body": body,
                    "tags": elem.get("Tags", ""),
                }
            elif post_type == "2" and score >= self.min_score:
                parent = elem.get("ParentId", "")
                question = questions.get(parent)
                if question and body:
                    text_value = "\n".join([
                        f"Question: {question['title']}",
                        question["body"],
                        "Answer:",
                        body,
                    ]).strip()
                    if len(text_value) >= 160:
                        yield RawDocument(
                            source="stack_exchange",
                            category="math_reasoning",
                            language="en",
                            text=text_value,
                            title=question["title"],
                            metadata={
                                "tags": question["tags"],
                                "question": question["title"] + "\n" + question["body"],
                                "answer": body,
                            },
                        )
            elem.clear()


class SecurityDatasetCollector:
    NVD_API = "https://services.nvd.nist.gov/rest/json/cves/2.0"

    def __init__(self, api_key: str = "", timeout: int = 30, results_per_page: int = 2000):
        self.timeout = timeout
        self.results_per_page = min(results_per_page, 2000)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        if api_key:
            self.session.headers["apiKey"] = api_key

    def stream_cves(self, start_year: int = 2020, max_documents: int = 200) -> Iterator[RawDocument]:
        emitted = 0
        current_year = time.gmtime().tm_year
        for year in range(start_year, current_year + 1):
            start_index = 0
            while emitted < max_documents:
                try:
                    response = self.session.get(
                        self.NVD_API,
                        params={
                            "pubStartDate": f"{year}-01-01T00:00:00.000",
                            "pubEndDate": f"{year}-12-31T23:59:59.999",
                            "resultsPerPage": self.results_per_page,
                            "startIndex": start_index,
                        },
                        timeout=self.timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:
                    log.warning("NVD fetch failed for %s: %s", year, exc)
                    break
                items = payload.get("vulnerabilities", [])
                if not items:
                    break
                for item in items:
                    cve = item.get("cve", {})
                    cve_id = cve.get("id", "")
                    descriptions = [d.get("value", "") for d in cve.get("descriptions", []) if d.get("lang") == "en"]
                    weaknesses = [desc.get("value", "") for weakness in cve.get("weaknesses", []) for desc in weakness.get("description", []) if desc.get("lang") == "en"]
                    metrics = cve.get("metrics", {})
                    references = [ref.get("url", "") for ref in cve.get("references", [])]
                    text_value = "\n".join([
                        f"CVE: {cve_id}",
                        f"Description: {' '.join(descriptions)}",
                        f"Weaknesses: {', '.join(weaknesses)}",
                        f"Metrics: {json.dumps(metrics)[:500]}",
                        f"References: {' '.join(references[:5])}",
                    ]).strip()
                    if len(text_value) < 120:
                        continue
                    emitted += 1
                    yield RawDocument(
                        source="nvd_cve",
                        category="cyber_sec",
                        language="en",
                        text=text_value,
                        url=f"https://nvd.nist.gov/vuln/detail/{cve_id}",
                        title=cve_id,
                        metadata={"metrics": metrics, "weaknesses": weaknesses},
                    )
                    if emitted >= max_documents:
                        return
                start_index += len(items)


# =============================================================================
# MASTER ORCHESTRATOR
# =============================================================================

@dataclass
class LegacyInferenceConfig:
    backend: str = "transformers"
    model_name_or_path: str = ""
    tensor_parallel_size: int = 1
    dtype: str = "auto"
    max_model_len: int = 4096


class LegacyInferenceEngine:
    def __init__(self, config: LegacyInferenceConfig):
        self.config = config
        self._loaded = False
        self._backend: Dict[str, Any] = {}

    def generate(self, prompt: str, max_new_tokens: int = 256, temperature: float = 0.7) -> str:
        self._ensure_loaded()
        backend = self._backend
        if not backend:
            raise RuntimeError("Inference backend was not initialized.")
        if self.config.backend == "vllm":
            sampling = backend["sampling_cls"](temperature=temperature, max_tokens=max_new_tokens)
            outputs = backend["llm"].generate([prompt], sampling)
            return outputs[0].outputs[0].text.strip()
        tokenizer = backend["tokenizer"]
        model = backend["model"]
        torch_mod = backend["torch"]
        device = backend["device"]
        encoded = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=self.config.max_model_len)
        encoded = {key: value.to(device) for key, value in encoded.items()}
        with torch_mod.no_grad():
            output = model.generate(
                **encoded,
                do_sample=temperature > 0,
                temperature=temperature,
                max_new_tokens=max_new_tokens,
                pad_token_id=tokenizer.eos_token_id,
            )
        return tokenizer.decode(output[0], skip_special_tokens=True)

    def _ensure_loaded(self):
        if self._loaded:
            return
        self._loaded = True
        if self.config.backend == "vllm":
            vllm_mod = optional_import("vllm")
            if not vllm_mod:
                raise RuntimeError("Install vllm for vLLM inference.")
            self._backend = {
                "llm": vllm_mod.LLM(
                    model=self.config.model_name_or_path,
                    tensor_parallel_size=self.config.tensor_parallel_size,
                    dtype=self.config.dtype,
                    max_model_len=self.config.max_model_len,
                ),
                "sampling_cls": vllm_mod.SamplingParams,
            }
            return
        transformers_mod = optional_import("transformers")
        torch_mod = optional_import("torch")
        if not transformers_mod or not torch_mod:
            raise RuntimeError("Install transformers and torch for HF inference.")
        tokenizer = transformers_mod.AutoTokenizer.from_pretrained(self.config.model_name_or_path)
        model = transformers_mod.AutoModelForCausalLM.from_pretrained(self.config.model_name_or_path)
        device = "cuda" if torch_mod.cuda.is_available() else "cpu"
        model.to(device)
        model.eval()
        self._backend = {"tokenizer": tokenizer, "model": model, "torch": torch_mod, "device": device}

# NOTE: RetrievalConfig and VectorRetriever are defined in the RAG section near the end of the file.


class CorpusBalancer:
    CATEGORY_RATIOS = {
        "cyber_sec": 0.10,
        "code_ast": 0.25,
        "math_reasoning": 0.15,
        "science": 0.15,
        "general_web": 0.20,
        "multilingual": 0.05,
        "conversation": 0.05,
        "tool_use": 0.05,
    }

    def __init__(self, target_tokens: int):
        self.target_tokens = target_tokens
        self.vault: Dict[str, List[RawDocument]] = defaultdict(list)
        self.current_tokens: Dict[str, int] = defaultdict(int)

    def add_document(self, doc: RawDocument) -> bool:
        token_count = doc.token_count or estimate_tokens(doc.text)
        limit = int(self.target_tokens * self.CATEGORY_RATIOS.get(doc.category, 0.05))
        if self.current_tokens[doc.category] + token_count <= limit:
            self.vault[doc.category].append(doc)
            self.current_tokens[doc.category] += token_count
            return True
        return False

    def total_tokens(self) -> int:
        return sum(self.current_tokens.values())


class ContextPacker:
    def __init__(self, context_window: int = 4096, overlap: int = 512):
        self.context_window = context_window
        self.overlap = overlap
        self.chunker = SemanticChunker(context_window, overlap)

    def pack(self, documents: Sequence[RawDocument]) -> Iterator[DocumentChunk]:
        for index, doc in enumerate(documents):
            for piece_idx, piece in enumerate(self.chunker.chunk_text(doc.text)):
                yield DocumentChunk(
                    chunk_id=f"{safe_filename(doc.source)}_{index:06d}_{piece_idx:04d}",
                    text=piece,
                    source=doc.source,
                    category=doc.category,
                    metadata={"url": doc.url, "title": doc.title, **doc.metadata},
                    token_count=estimate_tokens(piece),
                )


class NovaTitanOrchestrator:
    def __init__(self, target_tokens: int = 5_000_000_000):
        self.base_dir = get_nova_base()
        self.target_tokens = target_tokens
        self.parallel = ParallelDataPipeline(max(1, min(8, (os.cpu_count() or 4) // 2)))
        self.tokenizer_cfg = TokenizerConfig()
        self.token_counter = TokenCounter(
            tokenizer_path=self.tokenizer_cfg.tokenizer_json_path,
            pretrained_name_or_path=self.tokenizer_cfg.pretrained_name_or_path,
            sentencepiece_model_path=self.tokenizer_cfg.sentencepiece_model_path,
        )
        set_default_token_counter(self.token_counter)

        self.cleaner = NovaCleaner()
        self.dedup = SemanticDeduplicator()
        self.scorer = QualityScorer()
        self.balancer = CorpusBalancer(target_tokens)
        self.ml_balancer = MultilingualBalancer(target_tokens)
        self.packer = ContextPacker(context_window=4096, overlap=512)
        self.curriculum = CurriculumScheduler()
        self.safety = SafetyFilter()
        self.shard_mgr = DatasetShardManager(shard_size_tokens=500_000_000, storage_format="auto")
        self.evaluator = Evaluator()
        self.rlhf = RLHFPipeline()
        self.tool_builder = ToolUseDatasetBuilder()
        self.instruction_builder = InstructionDatasetBuilder()
        self.instruction_shards = InstructionShardManager(shard_size_tokens=200_000_000)
        self.version_mgr = DatasetVersionManager(self.base_dir)
        self.stats = PipelineStats()

        self.model_cfg = ModelArchitectureConfig(max_seq_len=4096)
        self.train_cfg = TrainingConfig()
        self.train_cfg.target_tokens = int(os.getenv("NOVA_TRAIN_TOKENS", "0") or 0)
        self.train_cfg.infinite_streaming = os.getenv("NOVA_INFINITE_TRAINING", "1") == "1"
        self.trainer = NovaTrainerPipeline(self.model_cfg, self.train_cfg)
        self.compute = ComputePlan(target_tokens=target_tokens)

        self.web_crawler = WebCrawler(max_depth=2, politeness_delay=0.5, max_pages=25)
        self.github_crawler = GitHubCrawler(
            min_stars=50,
            token=os.getenv("GITHUB_TOKEN", ""),
            per_query_repos=3,
            max_files_per_repo=15,
        )
        self.dataset_importer = DatasetImporter(max_documents=100)
        self.wiki_parser = WikipediaDumpParser(os.getenv("NOVA_WIKIPEDIA_DUMP", ""), lang="en")
        self.cc_parser = CommonCrawlParser([])
        self.se_parser = StackExchangeParser(os.getenv("NOVA_STACKEXCHANGE_POSTS", ""))
        self.sec_collector = SecurityDatasetCollector(api_key=os.getenv("NVD_API_KEY", ""))
        self.arxiv_collector = ArxivCollector(max_results=40)

    def _collect_all(self) -> Iterator[RawDocument]:
        seed_urls = [url for url in os.getenv("NOVA_WEB_SEEDS", "").split(",") if url]
        github_queries = [
            "stars:>5000 language:Python machine learning",
            "stars:>5000 language:TypeScript llm",
        ]
        arxiv_queries = ["cat:cs.AI OR cat:cs.LG", "cat:cs.CR"]
        if seed_urls:
            yield from self.web_crawler.crawl(seed_urls)
        yield from self.github_crawler.crawl_repos(github_queries)
        yield from self.arxiv_collector.stream(arxiv_queries)
        yield from self.sec_collector.stream_cves(start_year=max(2020, time.gmtime().tm_year - 1), max_documents=100)
        yield from self.wiki_parser.stream_articles()
        yield from self.cc_parser.stream()
        yield from self.se_parser.stream_qa_pairs()
        for source in (DataSource.GSM8K, DataSource.MATH_DATASET, DataSource.BIGCODE, DataSource.OSCAR):
            yield from self.dataset_importer.stream(source)

    def _prepare_document(self, raw_doc: RawDocument) -> Tuple[str, Optional[RawDocument]]:
        cleaned = self.cleaner.clean(raw_doc)
        if cleaned is None:
            return ("clean", None)
        if not self.safety.is_safe(cleaned.text):
            self.rlhf.add_sft(self.safety.build_refusal(cleaned.text[:1000]))
            return ("unsafe", None)
        cleaned.text = self.safety.redact_pii(cleaned.text)
        cleaned.token_count = estimate_tokens(cleaned.text)
        score = self.scorer.score(cleaned)
        if score < 0.45:
            return ("quality", None)
        return ("ok", cleaned)

    def stream_processed(self, data_stream: Iterator[RawDocument]) -> Iterator[RawDocument]:
        for status, doc in self.parallel.map(data_stream, self._prepare_document):
            self.stats.total_docs += 1
            if status != "ok" or doc is None:
                self.stats.reject(status)
                continue
            self.stats.passed_clean += 1
            if self.dedup.is_duplicate(doc.text, doc_id=doc.url or doc.title or doc.source):
                self.stats.reject("dedup")
                continue
            self.stats.passed_dedup += 1
            self.stats.passed_quality += 1
            if not self.ml_balancer.accept(doc.language, doc.token_count):
                self.stats.reject("lang_balance")
                continue
            self.stats.passed_lang += 1
            if self.balancer.add_document(doc):
                self.stats.tokens_by_category[doc.category] += doc.token_count
                self.stats.docs_by_source[doc.source] += 1
                yield doc
            else:
                self.stats.reject("category_balance")

    def process_stream(self, data_stream: Iterator[RawDocument]) -> List[RawDocument]:
        return list(self.stream_processed(data_stream))

    def execute_pipeline(self, version: str = "v2.0") -> Dict:
        log.info("=" * 60)
        log.info("Nova Titan Orchestrator - target %.1fB tokens", self.target_tokens / 1e9)
        log.info("Model: %s", self.model_cfg.summary())
        log.info("%s", self.compute.report())

        use_streaming = os.getenv("NOVA_STREAMING", "1") == "1"
        buffer_docs = int(os.getenv("NOVA_CURRICULUM_BUFFER_DOCS", "800"))
        sample_limit = int(os.getenv("NOVA_TRAIN_SAMPLE_CHUNKS", "5000"))

        corpus_dir = self.base_dir / "corpus_shards"
        tokenizer_corpus = self.base_dir / "tokenizer_corpus.txt"
        instruction_dir = self.base_dir / "instruction_shards"

        self.instruction_shards.open(instruction_dir)
        packed_chunks: List[DocumentChunk] = []
        processed_docs_count = 0
        chunk_counter = 0

        def _flush_buffer(buf: List[RawDocument], tokenizer_handle) -> Iterator[DocumentChunk]:
            nonlocal chunk_counter
            sorted_docs = self.curriculum.sort_dataset(buf)
            for chunk in self.packer.pack(sorted_docs):
                tokenizer_handle.write(chunk.text.replace("\n", " ") + "\n")
                if len(packed_chunks) < sample_limit:
                    packed_chunks.append(chunk)
                chunk_counter += 1
                yield chunk

        if use_streaming:
            processed_iter = self.stream_processed(self._collect_all())

            def chunk_iter(tokenizer_handle):
                nonlocal processed_docs_count
                buffer: List[RawDocument] = []
                for doc in processed_iter:
                    processed_docs_count += 1
                    for ex in self.instruction_builder.build_from_document(doc):
                        self.instruction_shards.write_example(ex)
                    buffer.append(doc)
                    if len(buffer) >= buffer_docs:
                        yield from _flush_buffer(buffer, tokenizer_handle)
                        buffer.clear()
                if buffer:
                    yield from _flush_buffer(buffer, tokenizer_handle)

            with open(tokenizer_corpus, "w", encoding="utf-8") as handle:
                n_shards = self.shard_mgr.write_shards(chunk_iter(handle), corpus_dir)
        else:
            processed_docs = self.process_stream(self._collect_all())
            processed_docs_count = len(processed_docs)
            for doc in processed_docs:
                for ex in self.instruction_builder.build_from_document(doc):
                    self.instruction_shards.write_example(ex)
            sorted_docs = self.curriculum.sort_dataset(processed_docs)
            packed_chunks = list(self.packer.pack(sorted_docs))
            chunk_counter = len(packed_chunks)
            n_shards = self.shard_mgr.write_shards(iter(packed_chunks), corpus_dir)
            with open(tokenizer_corpus, "w", encoding="utf-8") as handle:
                for chunk in packed_chunks:
                    handle.write(chunk.text.replace("\n", " ") + "\n")

        instruction_stats = self.instruction_shards.close()

        tokenizer_trainer = BPETokenizerTrainer(self.tokenizer_cfg)
        tok_result = tokenizer_trainer.train(tokenizer_corpus)
        if tok_result.get("status") == "trained":
            if tok_result.get("sentencepiece_model"):
                self.token_counter = TokenCounter(sentencepiece_model_path=tok_result["sentencepiece_model"])
                set_default_token_counter(self.token_counter)
            else:
                tokenizer_json = tok_result.get("tokenizer_json")
                if tokenizer_json:
                    self.token_counter = TokenCounter(tokenizer_path=tokenizer_json)
                    set_default_token_counter(self.token_counter)
                else:
                    log.warning("Tokenizer training completed without model path.")

        training_info = self.trainer.init_training()
        training_run = None
        if os.getenv("NOVA_RUN_TRAINING", "0") == "1":
            if use_streaming and self.shard_mgr.shards:
                if self.train_cfg.infinite_streaming or self.train_cfg.target_tokens:
                    train_source: Iterable[DocumentChunk] = self.shard_mgr.stream_shards_forever()
                else:
                    train_source = self.shard_mgr.stream_shards()
                training_run = self.trainer.train(
                    train_source,
                    self.base_dir / "training_runs" / safe_filename(version),
                    token_counter=self.token_counter,
                )
            elif packed_chunks:
                training_run = self.trainer.train(
                    packed_chunks,
                    self.base_dir / "training_runs" / safe_filename(version),
                    token_counter=self.token_counter,
                )

        eval_spec = None
        eval_model_path = os.getenv("NOVA_EVAL_MODEL", "")
        if eval_model_path:
            eval_spec = EvalModelSpec(model_backend="hf", model_args={"pretrained": eval_model_path})
        eval_results = self.evaluator.run_benchmarks(
            model_spec=eval_spec,
            output_dir=self.base_dir / "evals" / safe_filename(version),
        )
        self.evaluator.print_report(eval_results)

        rlhf_dir = self.base_dir / "rlhf"
        for example in self.tool_builder.synthetic_samples():
            self.rlhf.add_sft(InstructionExample(
                system=example.system,
                user=example.user,
                assistant=example.final_answer,
                source="tool_use",
            ))
        self.rlhf.save(rlhf_dir)

        retrieval_manifest = self.base_dir / "retrieval_corpus.jsonl"
        with open(retrieval_manifest, "w", encoding="utf-8") as handle:
            if use_streaming:
                for chunk in self.shard_mgr.stream_shards():
                    handle.write(json.dumps(asdict(chunk), ensure_ascii=False) + "\n")
            else:
                for chunk in packed_chunks:
                    handle.write(json.dumps(asdict(chunk), ensure_ascii=False) + "\n")

        print(self.stats.report())
        stats_dict = {
            "total_docs": self.stats.total_docs,
            "accepted_docs": processed_docs_count,
            "corpus_chunks": chunk_counter,
            "n_shards": n_shards,
            "tokens_by_category": dict(self.stats.tokens_by_category),
            "docs_by_source": dict(self.stats.docs_by_source),
            "lang_distribution": self.ml_balancer.report(),
            "tokenizer": tok_result,
            "training_plan": training_info,
            "training_run": training_run,
            "instruction_shards": instruction_stats,
        }
        self.version_mgr.register(version, stats_dict, notes="productionized pipeline update")
        return {
            "corpus_chunks": chunk_counter,
            "n_shards": n_shards,
            "eval": {
                name: {"score": result.score, "status": result.status, "target": result.target}
                for name, result in eval_results.items()
            },
            "tokenizer": tok_result,
            "training": training_run or training_info,
            "retrieval_manifest": str(retrieval_manifest),
            "instruction_shards": {"dir": str(instruction_dir), "splits": instruction_stats},
            "stats": stats_dict,
        }




# =============================================================================
# PRODUCTION OVERRIDES - CORE PIPELINE
# =============================================================================

@dataclass
class DocumentChunk:
    chunk_id: str
    text: str
    source: str
    category: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    token_count: int = 0

# NOTE: TokenizerConfig is defined in the tokenizer utilities section near the end of the file.


class TokenCounter:
    def __init__(self, tokenizer_path: str = "", pretrained_name_or_path: str = "", sentencepiece_model_path: str = ""):
        self.tokenizer_path = tokenizer_path
        self.pretrained_name_or_path = pretrained_name_or_path
        self.sentencepiece_model_path = sentencepiece_model_path
        self._backend = ""
        self._tokenizer: Any = None
        self._loaded = False

    def _load(self):
        if self._loaded:
            return
        self._loaded = True
        tokenizers_mod = optional_import("tokenizers")
        if tokenizers_mod and self.tokenizer_path and Path(self.tokenizer_path).exists():
            try:
                self._tokenizer = tokenizers_mod.Tokenizer.from_file(self.tokenizer_path)
                self._backend = "tokenizers"
                return
            except Exception as exc:
                log.warning("Tokenizer JSON load failed: %s", exc)
        spm_mod = optional_import("sentencepiece")
        if spm_mod and self.sentencepiece_model_path and Path(self.sentencepiece_model_path).exists():
            try:
                self._tokenizer = spm_mod.SentencePieceProcessor(model_file=self.sentencepiece_model_path)
                self._backend = "sentencepiece"
                return
            except Exception as exc:
                log.warning("SentencePiece load failed: %s", exc)
        if self.pretrained_name_or_path:
            transformers_mod = optional_import("transformers")
            if transformers_mod:
                try:
                    self._tokenizer = transformers_mod.AutoTokenizer.from_pretrained(self.pretrained_name_or_path)
                    self._backend = "transformers"
                    return
                except Exception as exc:
                    log.warning("Pretrained tokenizer load failed: %s", exc)

    def encode(self, text: str) -> List[int]:
        self._load()
        if self._backend == "tokenizers":
            assert self._tokenizer is not None
            return self._tokenizer.encode(text).ids
        if self._backend == "sentencepiece":
            assert self._tokenizer is not None
            return list(self._tokenizer.encode(text, out_type=int))
        if self._backend == "transformers":
            assert self._tokenizer is not None
            return self._tokenizer.encode(text, add_special_tokens=False)
        pieces = re.findall(r"[A-Za-z0-9_]+|[^\w\s]", text, flags=re.UNICODE)
        return [int(hashlib.blake2b(piece.encode("utf-8"), digest_size=4).hexdigest(), 16) for piece in pieces]

    def count(self, text: str) -> int:
        return len(self.encode(text))


DEFAULT_TOKEN_COUNTER = TokenCounter(
    tokenizer_path=os.getenv("NOVA_TOKENIZER_JSON", ""),
    pretrained_name_or_path=os.getenv("NOVA_TOKENIZER", ""),
    sentencepiece_model_path=os.getenv("NOVA_SPM_MODEL", ""),
)


def set_default_token_counter(counter: TokenCounter) -> None:
    global DEFAULT_TOKEN_COUNTER
    DEFAULT_TOKEN_COUNTER = counter


def estimate_tokens(text: str) -> int:
    return DEFAULT_TOKEN_COUNTER.count(text)
# NOTE: BPETokenizerTrainer is defined in the tokenizer utilities section near the end of the file.


class MinHashDeduplicator:
    NUM_PERM = 128
    JACCARD_THRESHOLD = 0.85
    SHINGLE_SIZE = 5

    def __init__(self):
        self.exact_hashes: set = set()
        self.signatures: Dict[str, List[int]] = {}
        self.band_buckets: List[Dict[Tuple[int, ...], set]] = [defaultdict(set) for _ in range(32)]
        self._coefficients = self._generate_coefficients()
        self.prime = (1 << 61) - 1
        self.max_hash = (1 << 32) - 1
        self.rows_per_band = self.NUM_PERM // 32
        self.datasketch = optional_import("datasketch")
        self._lsh = None
        self._minhash_cls = None
        if self.datasketch:
            try:
                self._minhash_cls = self.datasketch.MinHash
                self._lsh = self.datasketch.MinHashLSH(threshold=self.JACCARD_THRESHOLD, num_perm=self.NUM_PERM)
            except Exception as exc:
                log.warning("datasketch disabled: %s", exc)

    def _generate_coefficients(self) -> List[Tuple[int, int]]:
        rng = random.Random(42)
        return [(rng.randint(1, (1 << 31) - 1), rng.randint(0, (1 << 31) - 1)) for _ in range(self.NUM_PERM)]

    def _shingles(self, text: str) -> set:
        words = re.findall(r"\w+", normalize_space(text.lower()))
        if len(words) < self.SHINGLE_SIZE:
            return {" ".join(words)} if words else set()
        return {" ".join(words[i:i + self.SHINGLE_SIZE]) for i in range(len(words) - self.SHINGLE_SIZE + 1)}

    def _minhash(self, shingles: set) -> List[int]:
        signature = [self.max_hash] * self.NUM_PERM
        for shingle in shingles:
            h = int(hashlib.md5(shingle.encode("utf-8")).hexdigest(), 16) % self.prime
            for idx, (a, b) in enumerate(self._coefficients):
                value = ((a * h + b) % self.prime) & self.max_hash
                if value < signature[idx]:
                    signature[idx] = value
        return signature

    def is_duplicate(self, text: str, doc_id: str = "") -> bool:
        normalized = normalize_space(text.lower())
        exact_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        if exact_hash in self.exact_hashes:
            return True
        key = doc_id or exact_hash
        shingles = self._shingles(normalized)
        if not shingles:
            return False
        if self._lsh and self._minhash_cls:
            minhash = self._minhash_cls(num_perm=self.NUM_PERM)
            for shingle in shingles:
                minhash.update(shingle.encode("utf-8"))
            if self._lsh.query(minhash):
                return True
            self._lsh.insert(key, minhash)
            self.exact_hashes.add(exact_hash)
            return False
        signature = self._minhash(shingles)
        candidates = set()
        for band_idx in range(32):
            start = band_idx * self.rows_per_band
            band = tuple(signature[start:start + self.rows_per_band])
            candidates.update(self.band_buckets[band_idx].get(band, set()))
        for candidate in candidates:
            other = self.signatures.get(candidate)
            if not other:
                continue
            similarity = sum(1 for left, right in zip(signature, other) if left == right) / len(signature)
            if similarity >= self.JACCARD_THRESHOLD:
                return True
        self.signatures[key] = signature
        for band_idx in range(32):
            start = band_idx * self.rows_per_band
            band = tuple(signature[start:start + self.rows_per_band])
            self.band_buckets[band_idx][band].add(key)
        self.exact_hashes.add(exact_hash)
        return False


class SimHashDeduplicator:
    BITS = 64
    THRESHOLD = 3

    def __init__(self, num_bands: int = 4):
        self.num_bands = num_bands
        self.segment_bits = self.BITS // self.num_bands
        self.fingerprints: Dict[str, int] = {}
        self.band_buckets: List[Dict[int, set]] = [defaultdict(set) for _ in range(self.num_bands)]

    def _simhash(self, text: str) -> int:
        weights = Counter(re.findall(r"\w+", text.lower()))
        vector = [0] * self.BITS
        for token, weight in weights.items():
            hashed = int(hashlib.blake2b(token.encode("utf-8"), digest_size=8).hexdigest(), 16)
            for idx in range(self.BITS):
                vector[idx] += weight if ((hashed >> idx) & 1) else -weight
        return sum(1 << idx for idx, value in enumerate(vector) if value >= 0)

    def is_duplicate(self, text: str, doc_id: str = "") -> bool:
        key = doc_id or hashlib.sha256(text.encode("utf-8")).hexdigest()
        fingerprint = self._simhash(text)
        mask = (1 << self.segment_bits) - 1
        candidates = set()
        for band_idx in range(self.num_bands):
            segment = (fingerprint >> (band_idx * self.segment_bits)) & mask
            candidates.update(self.band_buckets[band_idx].get(segment, set()))
        for candidate in candidates:
            if (fingerprint ^ self.fingerprints[candidate]).bit_count() <= self.THRESHOLD:
                return True
        self.fingerprints[key] = fingerprint
        for band_idx in range(self.num_bands):
            segment = (fingerprint >> (band_idx * self.segment_bits)) & mask
            self.band_buckets[band_idx][segment].add(key)
        return False


class SemanticDeduplicator:
    def __init__(self):
        self.minhash = MinHashDeduplicator()
        self.simhash = SimHashDeduplicator()

    def is_duplicate(self, text: str, doc_id: str = "") -> bool:
        return self.minhash.is_duplicate(text, doc_id) or self.simhash.is_duplicate(text, doc_id)


class QualityScorer:
    TOXIC_PATTERNS = re.compile(r"\b(spam|click here|buy now|free money|xxx|hate|kill yourself)\b", re.IGNORECASE)
    GRAMMAR_PENALTIES = re.compile(r"[A-Z]{8,}|(\?\?|!!){2,}|\.{5,}")

    def __init__(self, perplexity_threshold: float = 200.0):
        self.perplexity_threshold = perplexity_threshold
        self._torch = None
        self._transformers = None
        self._ppl_model = None
        self._ppl_tokenizer = None
        self._device = "cpu"

    def score(self, doc: RawDocument) -> float:
        words = re.findall(r"\w+", doc.text.lower())
        n_words = max(1, len(words))
        sentences = [s for s in re.split(r"[.!?]+", doc.text) if s.strip()]
        length_score = min(1.0, math.log1p(n_words) / math.log1p(10000))
        diversity_score = clamp((len(set(words)) / n_words) * 1.8, 0.0, 1.0)
        structure_score = 1.0 if 6 <= n_words / max(1, len(sentences)) <= 40 else 0.55
        repetition_penalty = 0.0
        if n_words > 20:
            bigrams = [f"{words[i]} {words[i + 1]}" for i in range(n_words - 1)]
            repetition_penalty = clamp(1.0 - (len(set(bigrams)) / max(1, len(bigrams))), 0.0, 0.6)
        grammar_penalty = min(0.35, len(self.GRAMMAR_PENALTIES.findall(doc.text)) * 0.05)
        toxicity_penalty = 0.4 if self.TOXIC_PATTERNS.search(doc.text) else 0.0
        perplexity_score = 1.0
        perplexity = None if doc.category == "code_ast" else self._perplexity(doc.text)
        if perplexity is not None and perplexity > self.perplexity_threshold:
            perplexity_score = max(0.0, 1.0 - ((perplexity - self.perplexity_threshold) / 300.0))
        total = (
            length_score * 0.20 + diversity_score * 0.18 + structure_score * 0.12 +
            (1 - repetition_penalty) * 0.15 + (1 - grammar_penalty) * 0.10 +
            (1 - toxicity_penalty) * 0.15 + perplexity_score * 0.10
        )
        doc.quality_score = round(clamp(total, 0.0, 1.0), 4)
        doc.metadata["quality"] = {"score": doc.quality_score, "perplexity": None if perplexity is None else round(perplexity, 4)}
        return doc.quality_score

    def _perplexity(self, text: str) -> Optional[float]:
        if not self._ensure_perplexity_model():
            return None
        assert self._ppl_tokenizer is not None
        assert self._ppl_model is not None
        assert self._torch is not None
        encoded = self._ppl_tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
        input_ids = encoded["input_ids"].to(self._device)
        with self._torch.no_grad():
            outputs = self._ppl_model(input_ids=input_ids, labels=input_ids)
            loss = float(outputs.loss.detach().cpu())
        return float(math.exp(min(loss, 20)))

    def _ensure_perplexity_model(self) -> bool:
        if self._ppl_model is not None:
            return True
        self._torch = optional_import("torch")
        self._transformers = optional_import("transformers")
        if not self._torch or not self._transformers:
            return False
        try:
            self._ppl_tokenizer = self._transformers.AutoTokenizer.from_pretrained("gpt2")
            self._ppl_model = self._transformers.AutoModelForCausalLM.from_pretrained("gpt2")
            self._device = "cuda" if self._torch.cuda.is_available() else "cpu"
            self._ppl_model.to(self._device)
            self._ppl_model.eval()
            return True
        except Exception as exc:
            log.warning("Perplexity model load failed: %s", exc)
            self._ppl_model = None
            return False


class CodeDatasetPipeline:
    SUPPORTED_LANGS = {"python", "javascript", "rust", "go", "c", "cpp", "java", "typescript", "bash", "solidity"}
    LICENSE_WHITELIST = {"mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause", "unlicense", "isc", "unknown"}
    LANG_EXTENSIONS = {
        ".py": "python", ".js": "javascript", ".rs": "rust", ".go": "go",
        ".c": "c", ".cpp": "cpp", ".java": "java", ".ts": "typescript",
        ".sh": "bash", ".sol": "solidity",
    }

    def __init__(self):
        self.tree_sitter_bundle = optional_import("tree_sitter_languages")
        self.parsers: Dict[str, Any] = {}

    def detect_language(self, filename: str) -> Optional[str]:
        return self.LANG_EXTENSIONS.get(Path(filename).suffix.lower())

    def is_acceptable(self, doc: RawDocument) -> bool:
        return doc.license.lower() in self.LICENSE_WHITELIST and (doc.repo_stars == 0 or doc.repo_stars >= 50) and len(doc.text.splitlines()) >= 5

    def extract_ast_features(self, code: str, lang: str) -> Dict:
        if lang == "python":
            try:
                tree = ast.parse(code)
            except SyntaxError:
                tree = None
            if tree is not None:
                functions, classes, imports, docstrings = [], [], [], []
                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        functions.append(node.name)
                        doc = ast.get_docstring(node)
                        if doc:
                            docstrings.append(doc.splitlines()[0][:200])
                    elif isinstance(node, ast.ClassDef):
                        classes.append(node.name)
                        doc = ast.get_docstring(node)
                        if doc:
                            docstrings.append(doc.splitlines()[0][:200])
                    elif isinstance(node, ast.Import):
                        imports.extend(alias.name for alias in node.names)
                    elif isinstance(node, ast.ImportFrom):
                        base = node.module or ""
                        imports.extend(f"{base}.{alias.name}".strip(".") for alias in node.names)
                return {"language": lang, "line_count": len(code.splitlines()), "function_count": len(functions), "class_count": len(classes), "import_count": len(imports), "has_docstring": bool(docstrings), "functions": functions[:100], "classes": classes[:100], "imports": imports[:100], "docstrings": docstrings[:50]}
        parser = self._parser(lang)
        if parser is not None:
            return self._tree_sitter_features(code, lang, parser)
        return {"language": lang, "line_count": len(code.splitlines()), "has_docstring": code.count(chr(34) * 3) > 0 or code.count(chr(39) * 3) > 0, "function_count": len(re.findall(r"\b(def|function)\b", code)), "class_count": len(re.findall(r"\bclass\b", code)), "imports": re.findall(r"^\s*(?:import|from|#include)\s+.*$", code, flags=re.MULTILINE)[:100]}

    def deduplicate_code(self, snippets: List[str]) -> List[str]:
        seen = set()
        result = []
        for snippet in snippets:
            normalized = re.sub(r"\s+", " ", re.sub(r"(?m)^\s*(#|//).*$", "", snippet)).strip()
            digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
            if digest in seen:
                continue
            seen.add(digest)
            result.append(snippet)
        return result

    def _parser(self, lang: str):
        if lang in self.parsers:
            return self.parsers[lang]
        if not self.tree_sitter_bundle:
            return None
        try:
            parser = self.tree_sitter_bundle.get_parser(lang)
            self.parsers[lang] = parser
            return parser
        except Exception:
            return None

    def _tree_sitter_features(self, code: str, lang: str, parser: Any) -> Dict:
        code_bytes = code.encode("utf-8", errors="ignore")
        tree = parser.parse(code_bytes)
        stack = [tree.root_node]
        functions, classes, imports = [], [], []
        while stack:
            node = stack.pop()
            node_type = getattr(node, "type", "")
            snippet = normalize_space(code_bytes[node.start_byte:node.end_byte].decode("utf-8", errors="ignore"))[:160]
            if node_type in {"function_definition", "function_declaration", "method_definition"}:
                functions.append(snippet)
            elif node_type in {"class_definition", "class_declaration"}:
                classes.append(snippet)
            elif node_type in {"import_statement", "import_declaration"}:
                imports.append(snippet)
            stack.extend(getattr(node, "children", []))
        return {"language": lang, "line_count": len(code.splitlines()), "function_count": len(functions), "class_count": len(classes), "import_count": len(imports), "has_docstring": False, "functions": functions[:100], "classes": classes[:100], "imports": imports[:100], "docstrings": []}


class SemanticChunker:
    def __init__(self, context_window: int = 4096, overlap: int = 512, token_counter: Optional[TokenCounter] = None):
        self.context_window = context_window
        self.overlap = overlap
        self.token_counter = token_counter or DEFAULT_TOKEN_COUNTER

    def chunk_text(self, text: str) -> List[str]:
        units = [part.strip() for part in re.split(r"\n{2,}", text) if part.strip()]
        if not units:
            units = [text.strip()]
        chunks, current = [], ""
        current_tokens = 0
        for unit in units:
            unit_tokens = self.token_counter.count(unit)
            if current and current_tokens + unit_tokens > self.context_window:
                chunks.append(current.strip())
                overlap_words = current.split()[-self.overlap:]
                current = (" ".join(overlap_words) + "\n\n" + unit).strip() if overlap_words else unit
                current_tokens = self.token_counter.count(current)
            else:
                current = (current + "\n\n" + unit).strip() if current else unit
                current_tokens += unit_tokens
        if current:
            chunks.append(current.strip())
        return chunks


class LongContextDataset:
    SOURCES = ["books", "research_papers", "legal_documents", "technical_manuals"]

    def __init__(self, context_window: int = 32768, overlap: int = 512):
        self.context_window = context_window
        self.overlap = overlap
        self.chunker = SemanticChunker(context_window, overlap)

    def build_long_chunks(self, documents: List[str]) -> List[str]:
        chunks = []
        for document in documents:
            chunks.extend(self.chunker.chunk_text(document))
        return chunks



class DatasetShardManager:
    def __init__(self, shard_size_tokens: int = 500_000_000, storage_format: str = "auto"):
        self.shard_size_tokens = shard_size_tokens
        self.storage_format = storage_format
        self.shards: List[Path] = []
        self.pyarrow = optional_import("pyarrow")
        self.parquet = optional_import("pyarrow.parquet")

    def _format(self) -> str:
        if self.storage_format == "auto":
            return "parquet" if self.pyarrow and self.parquet else "jsonl"
        return self.storage_format

    def write_shards(self, packed_chunks: Iterator[DocumentChunk], output_dir: Path) -> int:
        output_dir.mkdir(parents=True, exist_ok=True)
        self.shards = []
        return self._write_parquet(packed_chunks, output_dir) if self._format() == "parquet" else self._write_jsonl(packed_chunks, output_dir)

    def stream_shards(self) -> Iterator[DocumentChunk]:
        for shard_path in self.shards:
            if shard_path.suffix == ".parquet":
                table = self.parquet.read_table(shard_path)
                for row in table.to_pylist():
                    yield DocumentChunk(**row)
            else:
                with open(shard_path, encoding="utf-8") as handle:
                    for line in handle:
                        yield DocumentChunk(**json.loads(line))

    def stream_shards_forever(self) -> Iterator[DocumentChunk]:
        while self.shards:
            yield from self.stream_shards()

    def _write_jsonl(self, packed_chunks: Iterator[DocumentChunk], output_dir: Path) -> int:
        shard_idx = 0
        current_tokens = 0
        handle = None
        try:
            for chunk in packed_chunks:
                if handle is None or current_tokens + chunk.token_count > self.shard_size_tokens:
                    if handle:
                        handle.close()
                    shard_path = output_dir / f"shard_{shard_idx:05d}.jsonl"
                    self.shards.append(shard_path)
                    handle = open(shard_path, "w", encoding="utf-8")
                    shard_idx += 1
                    current_tokens = 0
                handle.write(json.dumps(asdict(chunk), ensure_ascii=False) + "\n")
                current_tokens += chunk.token_count
        finally:
            if handle and not handle.closed:
                handle.close()
        return shard_idx

    def _write_parquet(self, packed_chunks: Iterator[DocumentChunk], output_dir: Path) -> int:
        shard_idx = 0
        current_tokens = 0
        rows = []
        def flush():
            nonlocal shard_idx, current_tokens, rows
            if not rows:
                return
            shard_path = output_dir / f"shard_{shard_idx:05d}.parquet"
            self.parquet.write_table(self.pyarrow.Table.from_pylist(rows), shard_path)
            self.shards.append(shard_path)
            shard_idx += 1
            current_tokens = 0
            rows = []
        for chunk in packed_chunks:
            if rows and current_tokens + chunk.token_count > self.shard_size_tokens:
                flush()
            rows.append(asdict(chunk))
            current_tokens += chunk.token_count
        flush()
        return shard_idx


class InstructionShardManager:
    """Shards instruction examples into train/val/test JSONL splits."""
    def __init__(
        self,
        shard_size_tokens: int = 200_000_000,
        split_ratios: Optional[Dict[str, float]] = None,
        seed: int = 42,
    ):
        self.shard_size_tokens = shard_size_tokens
        self.split_ratios = split_ratios or {"train": 0.90, "val": 0.05, "test": 0.05}
        self.rng = random.Random(seed)
        self._writers: Dict[str, "_InstructionShardWriter"] = {}

    def open(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        self._writers = {}
        for split in self.split_ratios:
            self._writers[split] = _InstructionShardWriter(
                output_dir / split,
                shard_size_tokens=self.shard_size_tokens,
                split_name=split,
            )

    def write_example(self, ex: InstructionExample) -> None:
        split = self._pick_split()
        writer = self._writers.get(split)
        if not writer:
            return
        writer.write(ex)

    def close(self) -> Dict[str, int]:
        counts = {}
        for split, writer in self._writers.items():
            counts[split] = writer.close()
        self._writers = {}
        return counts

    def _pick_split(self) -> str:
        roll = self.rng.random()
        cumulative = 0.0
        for split, ratio in self.split_ratios.items():
            cumulative += ratio
            if roll <= cumulative:
                return split
        return "train"


class _InstructionShardWriter:
    def __init__(self, output_dir: Path, shard_size_tokens: int, split_name: str):
        self.output_dir = output_dir
        self.shard_size_tokens = shard_size_tokens
        self.split_name = split_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._handle = None
        self._shard_idx = 0
        self._current_tokens = 0

    def write(self, ex: InstructionExample) -> None:
        text = self._format_text(ex)
        token_count = estimate_tokens(text)
        if self._handle is None or self._current_tokens + token_count > self.shard_size_tokens:
            self._rotate()
        if self._handle is None:
            return
        row = {
            "text": text,
            "system": ex.system,
            "user": ex.user,
            "assistant": ex.assistant,
            "source": ex.source,
            "token_count": token_count,
        }
        self._handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        self._current_tokens += token_count

    def close(self) -> int:
        if self._handle and not self._handle.closed:
            self._handle.close()
        return self._shard_idx

    def _rotate(self) -> None:
        if self._handle:
            self._handle.close()
        shard_path = self.output_dir / f"{self.split_name}_shard_{self._shard_idx:05d}.jsonl"
        self._handle = open(shard_path, "w", encoding="utf-8")
        self._shard_idx += 1
        self._current_tokens = 0

    @staticmethod
    def _format_text(ex: InstructionExample) -> str:
        return (
            f"[SYS]{ex.system}[/SYS]\n"
            f"[USER]{ex.user}[/USER]\n"
            f"[ASSISTANT]{ex.assistant}[/ASSISTANT]"
        )


class ParallelDataPipeline:
    def __init__(self, workers: int = 4):
        self.workers = max(1, workers)

    def map(self, iterable: Iterable[Any], fn) -> Iterator[Any]:
        if self.workers == 1:
            for item in iterable:
                yield fn(item)
            return
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            pending = set()
            iterator = iter(iterable)
            exhausted = False
            while pending or not exhausted:
                while not exhausted and len(pending) < self.workers * 2:
                    try:
                        item = next(iterator)
                    except StopIteration:
                        exhausted = True
                        break
                    pending.add(executor.submit(fn, item))
                if not pending:
                    continue
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    yield future.result()



class ArxivCollector:
    API_URL = "http://export.arxiv.org/api/query"

    def __init__(self, max_results: int = 40, timeout: int = 30):
        self.max_results = max_results
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def stream(self, queries: Sequence[str]) -> Iterator[RawDocument]:
        for query in queries:
            try:
                response = self.session.get(
                    self.API_URL,
                    params={"search_query": query, "start": 0, "max_results": self.max_results},
                    timeout=self.timeout,
                )
                response.raise_for_status()
            except Exception as exc:
                log.warning("arXiv fetch failed for %s: %s", query, exc)
                continue
            root = ET.fromstring(response.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry in root.findall("atom:entry", ns):
                title = normalize_space(entry.findtext("atom:title", "", ns))
                summary = normalize_space(entry.findtext("atom:summary", "", ns))
                url = normalize_space(entry.findtext("atom:id", "", ns))
                authors = [normalize_space(author.findtext("atom:name", "", ns)) for author in entry.findall("atom:author", ns)]
                text_value = "\n".join([
                    f"Title: {title}",
                    f"Authors: {', '.join(author for author in authors if author)}",
                    f"Abstract: {summary}",
                ]).strip()
                if len(text_value) < 120:
                    continue
                yield RawDocument(
                    source="arxiv",
                    category="science",
                    language="en",
                    text=text_value,
                    url=url,
                    title=title,
                    metadata={"authors": authors, "query": query},
                )

# =============================================================================
# PRODUCTION OVERRIDES - ADVANCED LLM STACK
# =============================================================================

_TORCH = optional_import("torch")


@dataclass
class ModelArchitectureConfig:
    name: str = "NovaTitan-250M"
    hidden_size: int = 1024
    num_layers: int = 24
    num_attention_heads: int = 16
    num_kv_heads: int = 8
    intermediate_size: int = 4096
    vocab_size: int = 65_536
    max_seq_len: int = 32_768
    attention_type: str = "gqa"
    ffn_type: str = "swiglu"
    normalization: str = "rmsnorm"
    norm_eps: float = 1e-5
    tie_embeddings: bool = True
    use_flash_attention: bool = True
    use_kv_cache: bool = True
    attention_dropout: float = 0.0
    residual_dropout: float = 0.0
    use_bias: bool = False
    rope_fraction: float = 1.0
    sliding_window: Optional[int] = None
    positional_encoding: "PositionalEncodingConfig" = field(
        default_factory=lambda: PositionalEncodingConfig(strategy="rope", max_position=32_768)
    )

    def parameter_count(self) -> int:
        embed = self.vocab_size * self.hidden_size
        attn = 4 * self.hidden_size ** 2 * self.num_layers
        ffn = 3 * self.hidden_size * self.intermediate_size * self.num_layers
        return embed + attn + ffn

    def summary(self) -> str:
        p = self.parameter_count() / 1e6
        return (
            f"{self.name}  ~{p:.0f}M params  layers={self.num_layers}  "
            f"heads={self.num_attention_heads}(kv={self.num_kv_heads})  "
            f"hidden={self.hidden_size}  ffn={self.ffn_type}  "
            f"pos={self.positional_encoding.describe()}  "
            f"flash={self.use_flash_attention}"
        )


@dataclass
class CausalLMOutput:
    logits: Any
    loss: Optional[Any] = None
    past_key_values: Optional[List[Tuple[Any, Any]]] = None


if _TORCH:
    _F = _TORCH.nn.functional

    class _RMSNorm(_TORCH.nn.Module):
        def __init__(self, hidden_size: int, eps: float = 1e-6):
            super().__init__()
            self.weight = _TORCH.nn.Parameter(_TORCH.ones(hidden_size))
            self.eps = eps

        def forward(self, x: "Tensor") -> "Tensor":
            norm = x.pow(2).mean(-1, keepdim=True)
            x = x * _TORCH.rsqrt(norm + self.eps)
            return self.weight * x


    class _SwiGLU(_TORCH.nn.Module):
        def __init__(self, hidden_size: int, intermediate_size: int, bias: bool = False):
            super().__init__()
            self.w1 = _TORCH.nn.Linear(hidden_size, intermediate_size, bias=bias)
            self.w2 = _TORCH.nn.Linear(hidden_size, intermediate_size, bias=bias)
            self.w3 = _TORCH.nn.Linear(intermediate_size, hidden_size, bias=bias)

        def forward(self, x: "Tensor") -> "Tensor":
            return self.w3(_F.silu(self.w1(x)) * self.w2(x))


    class _RotaryEmbedding(_TORCH.nn.Module):
        def __init__(self, dim: int, max_position: int, base: float = 10000.0, scaling: Optional[Dict] = None):
            super().__init__()
            inv_freq = 1.0 / (base ** (_TORCH.arange(0, dim, 2).float() / dim))
            self.register_buffer("inv_freq", inv_freq, persistent=False)
            self.max_position = max_position
            self.scaling = scaling or {}
            self._cos_cached = None
            self._sin_cached = None

        def _build_cache(self, seq_len: int, device: str, dtype: "DType"):
            positions = _TORCH.arange(seq_len, device=device, dtype=self.inv_freq.dtype)
            if self.scaling.get("type") == "linear":
                factor = float(self.scaling.get("factor", 1.0))
                if factor > 1.0:
                    positions = positions / factor
            freqs = _TORCH.einsum("i,j->ij", positions, self.inv_freq)
            emb = _TORCH.cat([freqs, freqs], dim=-1)
            self._cos_cached = emb.cos().to(dtype=dtype)
            self._sin_cached = emb.sin().to(dtype=dtype)

        def _get_cos_sin(self, seq_len: int, device: str, dtype: "DType"):
            if self._cos_cached is None or self._cos_cached.size(0) < seq_len or self._cos_cached.device != _TORCH.device(device):
                self._build_cache(seq_len, device, dtype)
            assert self._cos_cached is not None and self._sin_cached is not None
            return self._cos_cached[:seq_len], self._sin_cached[:seq_len]

        @staticmethod
        def _rotate_half(x: "Tensor") -> "Tensor":
            x1 = x[..., ::2]
            x2 = x[..., 1::2]
            return _TORCH.cat([-x2, x1], dim=-1)

        def apply_rotary(self, q: "Tensor", k: "Tensor", position_ids: Optional["Tensor"] = None):
            b, h, t, d = q.shape
            cos, sin = self._get_cos_sin(t, q.device, q.dtype)
            if position_ids is not None:
                flat = position_ids.reshape(-1)
                cos = cos.index_select(0, flat).view(b, t, d)
                sin = sin.index_select(0, flat).view(b, t, d)
                cos = cos.unsqueeze(1)
                sin = sin.unsqueeze(1)
            else:
                cos = cos.unsqueeze(0).unsqueeze(0)
                sin = sin.unsqueeze(0).unsqueeze(0)
            q = (q * cos) + (self._rotate_half(q) * sin)
            k = (k * cos) + (self._rotate_half(k) * sin)
            return q, k


    class _KVCacheManager:
        def __init__(self, num_layers: int):
            self.num_layers = num_layers
            self.cache: List[Optional[Tuple["Tensor", "Tensor"]]] = [None] * num_layers

        def get(self, layer_idx: int) -> Optional[Tuple["Tensor", "Tensor"]]:
            return self.cache[layer_idx]

        def update(self, layer_idx: int, key: "Tensor", value: "Tensor") -> Tuple["Tensor", "Tensor"]:
            existing = self.cache[layer_idx]
            if existing is None:
                self.cache[layer_idx] = (key, value)
            else:
                prev_k, prev_v = existing
                self.cache[layer_idx] = (_TORCH.cat([prev_k, key], dim=2), _TORCH.cat([prev_v, value], dim=2))
            updated = self.cache[layer_idx]
            assert updated is not None
            return updated

        def clear(self) -> None:
            self.cache = [None] * self.num_layers


    class _RoPEAttention(_TORCH.nn.Module):
        def __init__(self, config: ModelArchitectureConfig):
            super().__init__()
            self.num_heads = config.num_attention_heads
            self.num_kv_heads = config.num_kv_heads or config.num_attention_heads
            self.head_dim = config.hidden_size // config.num_attention_heads
            self.scale = self.head_dim ** -0.5
            self.dropout_p = config.attention_dropout
            self.use_flash_attention = config.use_flash_attention
            self.sliding_window = config.sliding_window
            self.q_proj = _TORCH.nn.Linear(config.hidden_size, self.num_heads * self.head_dim, bias=config.use_bias)
            self.k_proj = _TORCH.nn.Linear(config.hidden_size, self.num_kv_heads * self.head_dim, bias=config.use_bias)
            self.v_proj = _TORCH.nn.Linear(config.hidden_size, self.num_kv_heads * self.head_dim, bias=config.use_bias)
            self.o_proj = _TORCH.nn.Linear(config.hidden_size, config.hidden_size, bias=config.use_bias)
            rope_dim = int(self.head_dim * config.rope_fraction)
            self.rotary = None
            if config.positional_encoding.strategy == "rope" and rope_dim > 0:
                self.rotary = _RotaryEmbedding(
                    dim=rope_dim,
                    max_position=config.positional_encoding.max_position,
                    base=config.positional_encoding.rope_theta,
                    scaling=config.positional_encoding.rope_scaling,
                )
            self.flash_attn_func = None
            if self.use_flash_attention:
                flash_attn_mod = optional_import("flash_attn")
                if flash_attn_mod and hasattr(flash_attn_mod, "flash_attn_func"):
                    self.flash_attn_func = flash_attn_mod.flash_attn_func

        def _repeat_kv(self, tensor: "Tensor") -> "Tensor":
            if self.num_kv_heads == self.num_heads:
                return tensor
            repeat_factor = self.num_heads // self.num_kv_heads
            return tensor.repeat_interleave(repeat_factor, dim=1)

        def forward(
            self,
            x: "Tensor",
            position_ids: Optional["Tensor"] = None,
            past_key_value: Optional[Tuple["Tensor", "Tensor"]] = None,
            use_cache: bool = False,
        ) -> Tuple["Tensor", Optional[Tuple["Tensor", "Tensor"]]]:
            b, t, _ = x.size()
            q = self.q_proj(x).view(b, t, self.num_heads, self.head_dim).transpose(1, 2)
            k = self.k_proj(x).view(b, t, self.num_kv_heads, self.head_dim).transpose(1, 2)
            v = self.v_proj(x).view(b, t, self.num_kv_heads, self.head_dim).transpose(1, 2)
            if self.rotary:
                rope_dim = self.rotary.inv_freq.size(0) * 2
                q_rope, k_rope = q[..., :rope_dim], k[..., :rope_dim]
                q_pass, k_pass = q[..., rope_dim:], k[..., rope_dim:]
                q_rope, k_rope = self.rotary.apply_rotary(q_rope, k_rope, position_ids)
                q = _TORCH.cat([q_rope, q_pass], dim=-1)
                k = _TORCH.cat([k_rope, k_pass], dim=-1)
            if past_key_value is not None:
                past_k, past_v = past_key_value
                k = _TORCH.cat([past_k, k], dim=2)
                v = _TORCH.cat([past_v, v], dim=2)
            if self.sliding_window and k.size(2) > self.sliding_window:
                k = k[:, :, -self.sliding_window:, :]
                v = v[:, :, -self.sliding_window:, :]
            k = self._repeat_kv(k)
            v = self._repeat_kv(v)
            if self.flash_attn_func is not None:
                q_t = q.transpose(1, 2)
                k_t = k.transpose(1, 2)
                v_t = v.transpose(1, 2)
                attn_out = self.flash_attn_func(
                    q_t,
                    k_t,
                    v_t,
                    dropout_p=self.dropout_p if self.training else 0.0,
                    causal=True,
                ).transpose(1, 2)
            else:
                attn_out = _F.scaled_dot_product_attention(
                    q,
                    k,
                    v,
                    attn_mask=None,
                    dropout_p=self.dropout_p if self.training else 0.0,
                    is_causal=True,
                )
            attn_out = attn_out.transpose(1, 2).contiguous().view(b, t, -1)
            output = self.o_proj(attn_out)
            new_kv = (k, v) if use_cache else None
            return output, new_kv


    class _TransformerBlock(_TORCH.nn.Module):
        def __init__(self, config: ModelArchitectureConfig):
            super().__init__()
            norm_cls = _RMSNorm if config.normalization == "rmsnorm" else _TORCH.nn.LayerNorm
            self.norm1 = norm_cls(config.hidden_size, eps=config.norm_eps)
            self.attn = _RoPEAttention(config)
            self.norm2 = norm_cls(config.hidden_size, eps=config.norm_eps)
            if config.ffn_type == "swiglu":
                self.mlp = _SwiGLU(config.hidden_size, config.intermediate_size, bias=config.use_bias)
            else:
                self.mlp = _TORCH.nn.Sequential(
                    _TORCH.nn.Linear(config.hidden_size, config.intermediate_size, bias=config.use_bias),
                    _TORCH.nn.GELU(),
                    _TORCH.nn.Linear(config.intermediate_size, config.hidden_size, bias=config.use_bias),
                )
            self.dropout = _TORCH.nn.Dropout(config.residual_dropout)

        def forward(
            self,
            x: "Tensor",
            position_ids: Optional["Tensor"],
            past_key_value: Optional[Tuple["Tensor", "Tensor"]],
            use_cache: bool,
        ):
            residual = x
            x = self.norm1(x)
            attn_out, kv = self.attn(x, position_ids=position_ids, past_key_value=past_key_value, use_cache=use_cache)
            x = residual + self.dropout(attn_out)
            residual = x
            x = self.norm2(x)
            x = residual + self.dropout(self.mlp(x))
            return x, kv


    class _NovaCausalLM(_TORCH.nn.Module):
        def __init__(self, config: ModelArchitectureConfig):
            super().__init__()
            self.config = config
            self.embed_tokens = _TORCH.nn.Embedding(config.vocab_size, config.hidden_size)
            self.blocks = _TORCH.nn.ModuleList([_TransformerBlock(config) for _ in range(config.num_layers)])
            norm_cls = _RMSNorm if config.normalization == "rmsnorm" else _TORCH.nn.LayerNorm
            self.norm = norm_cls(config.hidden_size, eps=config.norm_eps)
            self.lm_head = _TORCH.nn.Linear(config.hidden_size, config.vocab_size, bias=False)
            if config.tie_embeddings:
                self.lm_head.weight = self.embed_tokens.weight
            self.gradient_checkpointing = False

        def set_gradient_checkpointing(self, enable: bool = True):
            self.gradient_checkpointing = enable

        def forward(
            self,
            input_ids: "Tensor",
            labels: Optional["Tensor"] = None,
            use_cache: bool = False,
            past_key_values: Optional[List[Tuple["Tensor", "Tensor"]]] = None,
        ) -> CausalLMOutput:
            b, t = input_ids.size()
            device = input_ids.device
            position_ids = _TORCH.arange(t, device=device).unsqueeze(0).expand(b, -1)
            hidden_states = self.embed_tokens(input_ids)
            new_past: Optional[List[Tuple["Tensor", "Tensor"]]] = [] if use_cache else None
            for idx, block in enumerate(self.blocks):
                past_kv = past_key_values[idx] if past_key_values is not None else None
                if self.gradient_checkpointing and self.training and not use_cache:
                    def _checkpoint_fn(x):
                        return block(x, position_ids, None, False)[0]
                    hidden_states = _TORCH.utils.checkpoint.checkpoint(_checkpoint_fn, hidden_states)
                    kv = None
                else:
                    hidden_states, kv = block(hidden_states, position_ids, past_kv, use_cache)
                if use_cache and new_past is not None and kv is not None:
                    new_past.append(kv)
            hidden_states = self.norm(hidden_states)
            logits = self.lm_head(hidden_states)
            loss = None
            if labels is not None:
                loss = _F.cross_entropy(
                    logits.view(-1, logits.size(-1)),
                    labels.view(-1),
                    ignore_index=-100,
                )
            return CausalLMOutput(logits=logits, loss=loss, past_key_values=new_past if use_cache else None)

        @_TORCH.no_grad()
        def generate(
            self,
            input_ids: "Tensor",
            max_new_tokens: int = 128,
            temperature: float = 0.7,
            top_k: int = 0,
            use_cache: bool = True,
        ) -> "Tensor":
            self.eval()
            past = None
            for _ in range(max_new_tokens):
                outputs = self(input_ids, use_cache=use_cache, past_key_values=past)
                logits = outputs.logits[:, -1, :]
                past = outputs.past_key_values
                if temperature <= 0:
                    next_token = logits.argmax(dim=-1, keepdim=True)
                else:
                    logits = logits / max(temperature, 1e-5)
                    if top_k > 0:
                        values, indices = _TORCH.topk(logits, k=top_k, dim=-1)
                        probs = _TORCH.softmax(values, dim=-1)
                        next_token = indices.gather(-1, _TORCH.multinomial(probs, num_samples=1))
                    else:
                        probs = _TORCH.softmax(logits, dim=-1)
                        next_token = _TORCH.multinomial(probs, num_samples=1)
                input_ids = _TORCH.cat([input_ids, next_token], dim=-1)
            return input_ids

        def to_pipeline_sequential(self):
            class EmbedStage(_TORCH.nn.Module):
                def __init__(self, embed):
                    super().__init__()
                    self.embed = embed

                def forward(self, input_ids):
                    b, t = input_ids.size()
                    position_ids = _TORCH.arange(t, device=input_ids.device).unsqueeze(0).expand(b, -1)
                    return (self.embed(input_ids), position_ids)

            class BlockStage(_TORCH.nn.Module):
                def __init__(self, block):
                    super().__init__()
                    self.block = block

                def forward(self, inputs):
                    hidden, position_ids = inputs
                    hidden, _ = self.block(hidden, position_ids, None, False)
                    return (hidden, position_ids)

            class NormHeadStage(_TORCH.nn.Module):
                def __init__(self, norm, head):
                    super().__init__()
                    self.norm = norm
                    self.head = head

                def forward(self, inputs):
                    hidden, _ = inputs
                    hidden = self.norm(hidden)
                    return self.head(hidden)

            modules = [EmbedStage(self.embed_tokens)]
            modules.extend(BlockStage(block) for block in self.blocks)
            modules.append(NormHeadStage(self.norm, self.lm_head))
            return _TORCH.nn.Sequential(*modules)

    RMSNorm = _RMSNorm
    SwiGLU = _SwiGLU
    RotaryEmbedding = _RotaryEmbedding
    KVCacheManager = _KVCacheManager
    RoPEAttention = _RoPEAttention
    TransformerBlock = _TransformerBlock
    NovaCausalLM = _NovaCausalLM

else:
    class _TorchRequired:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("Install torch to use this component.")

    RMSNorm: Any = _TorchRequired
    SwiGLU: Any = _TorchRequired
    RotaryEmbedding: Any = _TorchRequired
    KVCacheManager: Any = _TorchRequired
    RoPEAttention: Any = _TorchRequired
    TransformerBlock: Any = _TorchRequired
    NovaCausalLM: Any = _TorchRequired


# =============================================================================
# TRAINING ENGINE ENHANCEMENTS
# =============================================================================

@dataclass
class TrainingConfig:
    precision: str = "bf16"
    gradient_checkpointing: bool = True
    distributed_backend: str = "accelerate"
    num_gpus: int = 1
    optimizer: str = "adamw"
    learning_rate: float = 3e-4
    weight_decay: float = 0.1
    beta1: float = 0.9
    beta2: float = 0.95
    gradient_clip: float = 1.0
    warmup_steps: int = 200
    max_steps: int = 2000
    lr_schedule: str = "cosine_with_warmup"
    min_lr_ratio: float = 0.1
    batch_size_per_gpu: int = 2
    gradient_accumulation_steps: int = 4
    save_every_n_steps: int = 250
    eval_every_n_steps: int = 250
    max_train_sequences: int = 10000
    dataloader_workers: int = 0
    dynamic_batch_size: bool = True
    max_batch_size: int = 8
    min_batch_size: int = 1
    auto_batch_max_trials: int = 6
    onecycle_pct_start: float = 0.1
    onecycle_div_factor: float = 25.0
    onecycle_final_div_factor: float = 1e4
    grad_noise_scale: bool = True
    streaming: bool = True
    shuffle_buffer: int = 1000
    warmup_sequences: int = 64
    target_tokens: int = 0
    log_every_n_steps: int = 50
    log_gpu_every_n_steps: int = 200
    infinite_streaming: bool = True
    fsdp_wrap_min_params: int = 1_000_000
    tp_size: int = 1
    pp_size: int = 1
    deepspeed_config: Optional[Dict[str, Any]] = None


class AutoBatchOptimizer:
    def __init__(self, max_batch_size: int = 8, min_batch_size: int = 1, max_trials: int = 6):
        self.max_batch_size = max_batch_size
        self.min_batch_size = min_batch_size
        self.max_trials = max_trials

    def tune(self, sequences: List["Tensor"], model: "TorchModule", device: str) -> int:
        torch_mod = optional_import("torch")
        if not torch_mod or device != "cuda":
            return min(self.max_batch_size, len(sequences))
        batch_size = min(self.max_batch_size, len(sequences))
        trials = 0
        while batch_size >= self.min_batch_size and trials < self.max_trials:
            try:
                batch = _TORCH.stack(sequences[:batch_size]).to(device)
                outputs = model(batch[:, :-1], labels=batch[:, 1:])
                loss = outputs.loss if hasattr(outputs, "loss") else None
                if loss is None:
                    logits = outputs.logits
                    loss = _F.cross_entropy(logits.reshape(-1, logits.size(-1)), batch[:, 1:].reshape(-1))
                loss.backward()
                model.zero_grad(set_to_none=True)
                return batch_size
            except RuntimeError as exc:
                if "out of memory" in str(exc).lower():
                    batch_size = max(self.min_batch_size, batch_size // 2)
                    torch_mod.cuda.empty_cache()
                    trials += 1
                    continue
                raise
        return max(self.min_batch_size, batch_size)


class MixedPrecisionManager:
    def __init__(self, precision: str, device: str):
        self.precision = precision
        self.device = device
        self.enabled = device == "cuda" and precision in ("bf16", "fp16")
        self.dtype = None
        if self.enabled:
            self.dtype = _TORCH.bfloat16 if precision == "bf16" else _TORCH.float16
        self.scaler = _TORCH.cuda.amp.GradScaler(enabled=self.enabled and precision == "fp16") if _TORCH else None

    def autocast(self):
        from contextlib import nullcontext
        if not self.enabled:
            return nullcontext()
        return _TORCH.cuda.amp.autocast(dtype=self.dtype)


class GradientNoiseScaleMonitor:
    def __init__(self):
        self.history: List[float] = []

    def update(self, micro_norms: List[float]) -> Optional[float]:
        if len(micro_norms) < 2:
            return None
        mean = sum(micro_norms) / len(micro_norms)
        var = sum((n - mean) ** 2 for n in micro_norms) / len(micro_norms)
        gns = var / max(mean ** 2, 1e-12)
        self.history.append(gns)
        return gns


if _TORCH:
    class Lion(_TORCH.optim.Optimizer):
        def __init__(self, params, lr: float = 1e-4, betas: Tuple[float, float] = (0.9, 0.99), weight_decay: float = 0.0):
            defaults = dict(lr=lr, betas=betas, weight_decay=weight_decay)
            super().__init__(params, defaults)

        @_TORCH.no_grad()
        def step(self, closure=None):
            loss = None
            if closure is not None:
                loss = closure()
            for group in self.param_groups:
                lr = group["lr"]
                beta1, beta2 = group["betas"]
                wd = group["weight_decay"]
                for p in group["params"]:
                    if p.grad is None:
                        continue
                    grad = p.grad
                    if wd != 0:
                        p.data.mul_(1 - lr * wd)
                    state = self.state[p]
                    if len(state) == 0:
                        state["exp_avg"] = _TORCH.zeros_like(p)
                    exp_avg = state["exp_avg"]
                    exp_avg.mul_(beta1).add_(grad, alpha=1 - beta1)
                    update = exp_avg.sign()
                    p.add_(update, alpha=-lr)
                    exp_avg.mul_(beta2).add_(grad, alpha=1 - beta2)
            return loss


class DistributedManager:
    def __init__(self, train_cfg: TrainingConfig):
        self.train_cfg = train_cfg
        self.world_size = int(os.getenv("WORLD_SIZE", "1"))
        self.rank = int(os.getenv("RANK", "0"))
        self.local_rank = int(os.getenv("LOCAL_RANK", "0"))

    def init_process_group(self, torch_mod):
        if self.world_size <= 1:
            return
        if not torch_mod.distributed.is_available():
            return
        if torch_mod.distributed.is_initialized():
            return
        backend = "nccl" if torch_mod.cuda.is_available() else "gloo"
        torch_mod.distributed.init_process_group(backend=backend, rank=self.rank, world_size=self.world_size)

    def device(self, torch_mod) -> str:
        if torch_mod.cuda.is_available():
            torch_mod.cuda.set_device(self.local_rank)
            return "cuda"
        return "cpu"


class NovaTrainerPipeline:
    def __init__(self, model_cfg: ModelArchitectureConfig, train_cfg: TrainingConfig):
        self.model_cfg = model_cfg
        self.train_cfg = train_cfg
        self.grad_monitor = GradientNoiseScaleMonitor() if train_cfg.grad_noise_scale else None

    @property
    def effective_batch_size(self) -> int:
        return self.train_cfg.batch_size_per_gpu * max(1, self.train_cfg.num_gpus) * self.train_cfg.gradient_accumulation_steps

    def total_tokens(self) -> int:
        return self.effective_batch_size * self.model_cfg.max_seq_len * self.train_cfg.max_steps

    def init_training(self) -> Dict[str, Any]:
        return {
            "model": self.model_cfg.summary(),
            "backend": self.train_cfg.distributed_backend,
            "precision": self.train_cfg.precision,
            "effective_batch_size": self.effective_batch_size,
            "planned_total_tokens": self.total_tokens(),
            "torch_available": optional_import("torch") is not None,
            "accelerate_available": optional_import("accelerate") is not None,
            "deepspeed_available": optional_import("deepspeed") is not None,
        }

    def _lr_at_step(self, step: int) -> float:
        if step < self.train_cfg.warmup_steps:
            return self.train_cfg.learning_rate * step / max(1, self.train_cfg.warmup_steps)
        progress = (step - self.train_cfg.warmup_steps) / max(1, self.train_cfg.max_steps - self.train_cfg.warmup_steps)
        cosine = 0.5 * (1.0 + math.cos(math.pi * progress))
        return (self.train_cfg.min_lr_ratio + (1 - self.train_cfg.min_lr_ratio) * cosine) * self.train_cfg.learning_rate

    def _build_optimizer(self, torch_mod, model):
        opt = self.train_cfg.optimizer.lower()
        if opt == "lion" and "Lion" in globals():
            return Lion(model.parameters(), lr=self.train_cfg.learning_rate, betas=(self.train_cfg.beta1, self.train_cfg.beta2), weight_decay=self.train_cfg.weight_decay)
        if opt == "adafactor":
            transformers_mod = optional_import("transformers")
            if transformers_mod and hasattr(transformers_mod, "Adafactor"):
                return transformers_mod.Adafactor(model.parameters(), lr=self.train_cfg.learning_rate, weight_decay=self.train_cfg.weight_decay, relative_step=False)
        return torch_mod.optim.AdamW(
            model.parameters(),
            lr=self.train_cfg.learning_rate,
            betas=(self.train_cfg.beta1, self.train_cfg.beta2),
            weight_decay=self.train_cfg.weight_decay,
        )

    def _build_scheduler(self, torch_mod, optimizer):
        schedule = self.train_cfg.lr_schedule.lower()
        if schedule == "onecycle":
            steps = max(1, self.train_cfg.max_steps)
            return torch_mod.optim.lr_scheduler.OneCycleLR(
                optimizer,
                max_lr=self.train_cfg.learning_rate,
                total_steps=steps,
                pct_start=self.train_cfg.onecycle_pct_start,
                div_factor=self.train_cfg.onecycle_div_factor,
                final_div_factor=self.train_cfg.onecycle_final_div_factor,
            )
        if schedule in ("cosine", "cosine_with_warmup"):
            return torch_mod.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda=lambda s: self._lr_at_step(s) / self.train_cfg.learning_rate)
        if schedule == "linear":
            return torch_mod.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda=lambda s: max(0.0, 1.0 - s / max(1, self.train_cfg.max_steps)))
        return None

    def _iter_sequences(self, chunks: Iterable[DocumentChunk], token_counter: TokenCounter, vocab_size: int) -> Iterator[List[int]]:
        max_len = self.model_cfg.max_seq_len + 1
        for chunk in chunks:
            ids = token_counter.encode(chunk.text)
            if not ids:
                continue
            if vocab_size and max(ids) >= vocab_size:
                unk_id = 1
                ids = [token if token < vocab_size else unk_id for token in ids]
            if len(ids) < 16:
                continue
            for start in range(0, len(ids) - 1, self.model_cfg.max_seq_len):
                seq = ids[start:start + max_len]
                if len(seq) < 16:
                    continue
                if len(seq) < max_len:
                    seq = seq + [0] * (max_len - len(seq))
                yield seq[:max_len]

    def _build_model(self, torch_mod, vocab_size: int):
        cfg = ModelArchitectureConfig(**{**asdict(self.model_cfg), "vocab_size": max(self.model_cfg.vocab_size, vocab_size)})
        model = NovaCausalLM(cfg)
        if self.train_cfg.gradient_checkpointing:
            model.set_gradient_checkpointing(True)
        return model

    def _grad_norm(self, model) -> float:
        total = 0.0
        for p in model.parameters():
            if p.grad is None:
                continue
            param_norm = float(p.grad.data.norm(2))
            total += param_norm ** 2
        return math.sqrt(total)

    def _apply_tensor_parallel(self, torch_mod, model, dist: DistributedManager):
        if dist.world_size < 2:
            return model
        tp_mod = optional_import("torch.distributed.tensor.parallel")
        if not tp_mod:
            log.warning("Tensor parallel not available: missing torch.distributed.tensor.parallel")
            return model
        parallelize_module = getattr(tp_mod, "parallelize_module", None)
        ColwiseParallel = getattr(tp_mod, "ColwiseParallel", None)
        RowwiseParallel = getattr(tp_mod, "RowwiseParallel", None)
        if not parallelize_module or not ColwiseParallel or not RowwiseParallel:
            log.warning("Tensor parallel not available: missing parallel helpers")
            return model
        mesh_mod = optional_import("torch.distributed.device_mesh") or optional_import("torch.distributed._tensor")
        DeviceMesh = getattr(mesh_mod, "DeviceMesh", None) if mesh_mod else None
        if DeviceMesh is None:
            log.warning("Tensor parallel not available: missing DeviceMesh")
            return model
        mesh = DeviceMesh("cuda" if torch_mod.cuda.is_available() else "cpu", list(range(dist.world_size)))
        for block in getattr(model, "blocks", []):
            try:
                parallelize_module(block.attn, mesh, {"q_proj": ColwiseParallel(), "k_proj": ColwiseParallel(), "v_proj": ColwiseParallel(), "o_proj": RowwiseParallel()})
                if hasattr(block.mlp, "w1"):
                    parallelize_module(block.mlp, mesh, {"w1": ColwiseParallel(), "w2": ColwiseParallel(), "w3": RowwiseParallel()})
            except Exception as exc:
                log.warning("Tensor parallelization failed: %s", exc)
                break
        return model

    def _apply_pipeline_parallel(self, torch_mod, model, dist: DistributedManager):
        if dist.world_size < 2:
            return model
        pipe_mod = optional_import("torch.distributed.pipeline.sync")
        Pipe = getattr(pipe_mod, "Pipe", None) if pipe_mod else None
        if Pipe is None:
            log.warning("Pipeline parallel not available: missing torch.distributed.pipeline.sync")
            return model
        if hasattr(model, "to_pipeline_sequential"):
            seq = model.to_pipeline_sequential()
            return Pipe(seq, chunks=max(1, self.train_cfg.gradient_accumulation_steps))
        return model

    def _wrap_distributed(self, torch_mod, model, optimizer, dist: DistributedManager):
        backend = self.train_cfg.distributed_backend.lower()
        deepspeed_engine = None
        if backend.startswith("deepspeed"):
            deepspeed_mod = optional_import("deepspeed")
            if not deepspeed_mod:
                raise RuntimeError("Install deepspeed for ZeRO-3 training.")
            ds_config = self.train_cfg.deepspeed_config or {
                "train_batch_size": self.effective_batch_size,
                "train_micro_batch_size_per_gpu": self.train_cfg.batch_size_per_gpu,
                "gradient_accumulation_steps": self.train_cfg.gradient_accumulation_steps,
                "zero_optimization": {"stage": 3, "overlap_comm": True, "reduce_scatter": True, "contiguous_gradients": True},
                "bf16": {"enabled": self.train_cfg.precision == "bf16"},
                "fp16": {"enabled": self.train_cfg.precision == "fp16"},
            }
            model, optimizer, _, _ = deepspeed_mod.initialize(
                model=model,
                model_parameters=model.parameters(),
                optimizer=optimizer,
                config=ds_config,
                dist_init_required=False,
            )
            deepspeed_engine = model
            return model, optimizer, deepspeed_engine
        if backend == "fsdp" and dist.world_size > 1:
            fsdp_mod = optional_import("torch.distributed.fsdp")
            wrap_mod = optional_import("torch.distributed.fsdp.wrap")
            FSDP = getattr(fsdp_mod, "FullyShardedDataParallel", None) if fsdp_mod else None
            MixedPrecision = getattr(fsdp_mod, "MixedPrecision", None) if fsdp_mod else None
            size_based_auto_wrap_policy = getattr(wrap_mod, "size_based_auto_wrap_policy", None) if wrap_mod else None
            if not FSDP or not MixedPrecision or not size_based_auto_wrap_policy:
                log.warning("FSDP unavailable: missing torch.distributed.fsdp modules")
            else:
                mp_policy = None
                if self.train_cfg.precision in ("bf16", "fp16"):
                    dtype = torch_mod.bfloat16 if self.train_cfg.precision == "bf16" else torch_mod.float16
                    mp_policy = MixedPrecision(param_dtype=dtype, reduce_dtype=dtype, buffer_dtype=dtype)
                policy = size_based_auto_wrap_policy(min_num_params=self.train_cfg.fsdp_wrap_min_params)
                model = FSDP(model, auto_wrap_policy=policy, mixed_precision=mp_policy, device_id=dist.local_rank if torch_mod.cuda.is_available() else None)
        elif backend == "ddp" and dist.world_size > 1:
            model = torch_mod.nn.parallel.DistributedDataParallel(
                model,
                device_ids=[dist.local_rank] if torch_mod.cuda.is_available() else None,
            )
        elif backend == "tensor_parallel":
            model = self._apply_tensor_parallel(torch_mod, model, dist)
        elif backend == "pipeline_parallel":
            model = self._apply_pipeline_parallel(torch_mod, model, dist)
        return model, optimizer, deepspeed_engine

    def train(self, chunks: Iterable[DocumentChunk], output_dir: Path, token_counter: Optional[TokenCounter] = None) -> Dict[str, Any]:
        torch_mod = optional_import("torch")
        if not torch_mod:
            raise RuntimeError("Install torch to run the training loop.")
        token_counter = token_counter or DEFAULT_TOKEN_COUNTER
        dist = DistributedManager(self.train_cfg)
        dist.init_process_group(torch_mod)
        device = dist.device(torch_mod)
        vocab_size = self.model_cfg.vocab_size
        target_tokens = max(0, int(self.train_cfg.target_tokens))
        max_steps = self.train_cfg.max_steps if self.train_cfg.max_steps > 0 else None

        def _is_reiterable(obj: Any) -> bool:
            try:
                iterator = iter(obj)
            except TypeError:
                return False
            return iterator is not obj

        infinite_streaming = bool(self.train_cfg.streaming and self.train_cfg.infinite_streaming)
        if infinite_streaming and not _is_reiterable(chunks):
            log.warning("Infinite streaming requested but chunk source is not re-iterable; falling back to finite stream.")
            infinite_streaming = False
        if target_tokens > 0 and not self.train_cfg.streaming:
            log.warning("Target tokens set but streaming is disabled; training will stop when data is exhausted.")

        if self.train_cfg.streaming:
            def _sequence_source() -> Iterator[List[int]]:
                while True:
                    for seq in self._iter_sequences(iter(chunks), token_counter, vocab_size):
                        yield seq
                    if not infinite_streaming:
                        break

            seq_iter = _sequence_source()
            warmup_count = max(self.train_cfg.max_batch_size, self.train_cfg.warmup_sequences)
            warmup = list(islice(seq_iter, warmup_count))
            if not warmup:
                raise RuntimeError("No training sequences were produced.")
            if self.train_cfg.dynamic_batch_size:
                auto_batch = AutoBatchOptimizer(self.train_cfg.max_batch_size, self.train_cfg.min_batch_size, self.train_cfg.auto_batch_max_trials)
                model_probe = self._build_model(torch_mod, vocab_size).to(device)
                probe_tensors = [_TORCH.tensor(seq, dtype=_TORCH.long) for seq in warmup[: min(len(warmup), self.train_cfg.max_batch_size)]]
                tuned = auto_batch.tune(probe_tensors, model_probe, device)
                self.train_cfg.batch_size_per_gpu = tuned
                del model_probe
                torch_mod.cuda.empty_cache() if device == "cuda" else None

            def _shuffle_buffer(iterator: Iterator[List[int]], buffer_size: int) -> Iterator[List[int]]:
                if buffer_size <= 0:
                    yield from iterator
                    return
                buf: List[List[int]] = []
                for item in iterator:
                    buf.append(item)
                    if len(buf) >= buffer_size:
                        random.shuffle(buf)
                        while buf:
                            yield buf.pop()
                if buf:
                    random.shuffle(buf)
                    while buf:
                        yield buf.pop()

            full_iter: Iterator[List[int]] = chain(warmup, seq_iter)
            sequence_cap = None
            if not infinite_streaming and target_tokens == 0 and self.train_cfg.max_train_sequences > 0:
                sequence_cap = self.train_cfg.max_train_sequences
            if sequence_cap is not None:
                full_iter = islice(full_iter, sequence_cap)
            full_iter = _shuffle_buffer(full_iter, self.train_cfg.shuffle_buffer)

            class _SequenceDataset(torch_mod.utils.data.IterableDataset):
                def __iter__(self):
                    for seq in full_iter:
                        yield torch_mod.tensor(seq, dtype=torch_mod.long)

            dataset = _SequenceDataset()
            dataloader = torch_mod.utils.data.DataLoader(
                dataset,
                batch_size=self.train_cfg.batch_size_per_gpu,
                shuffle=False,
                num_workers=0,
                drop_last=True,
            )
        else:
            sequences = list(islice(self._iter_sequences(chunks, token_counter, vocab_size), self.train_cfg.max_train_sequences))
            if not sequences:
                raise RuntimeError("No training sequences were produced.")
            if self.train_cfg.dynamic_batch_size:
                auto_batch = AutoBatchOptimizer(self.train_cfg.max_batch_size, self.train_cfg.min_batch_size, self.train_cfg.auto_batch_max_trials)
                model_probe = self._build_model(torch_mod, vocab_size).to(device)
                probe_tensors = [_TORCH.tensor(seq, dtype=_TORCH.long) for seq in sequences[: min(len(sequences), self.train_cfg.max_batch_size)]]
                tuned = auto_batch.tune(probe_tensors, model_probe, device)
                self.train_cfg.batch_size_per_gpu = tuned
                del model_probe
                torch_mod.cuda.empty_cache() if device == "cuda" else None

            dataset = torch_mod.utils.data.TensorDataset(torch_mod.tensor(sequences, dtype=torch_mod.long))
            dataloader = torch_mod.utils.data.DataLoader(
                dataset,
                batch_size=self.train_cfg.batch_size_per_gpu,
                shuffle=True,
                num_workers=self.train_cfg.dataloader_workers,
                drop_last=True,
            )

        model = self._build_model(torch_mod, vocab_size).to(device)
        optimizer = self._build_optimizer(torch_mod, model)
        scheduler = self._build_scheduler(torch_mod, optimizer)
        model, optimizer, deepspeed_engine = self._wrap_distributed(torch_mod, model, optimizer, dist)
        assert _TORCH is not None
        mp = MixedPrecisionManager(self.train_cfg.precision, device)

        output_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_dir = output_dir / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)

        losses = []
        micro_norms: List[float] = []
        step = 0
        tokens_seen = 0
        start_time = time.time()
        last_log_time = start_time
        last_log_tokens = 0
        optimizer.zero_grad(set_to_none=True)
        loop_total = max_steps if max_steps is not None else None
        for batch in progress(dataloader, total=loop_total, desc="train"):
            seq = batch[0] if isinstance(batch, (list, tuple)) else batch
            seq = seq.to(device)
            with mp.autocast():
                outputs = model(seq[:, :-1], labels=seq[:, 1:])
                loss = outputs.loss / self.train_cfg.gradient_accumulation_steps
            loss_value = float(loss.detach().cpu()) * self.train_cfg.gradient_accumulation_steps
            if deepspeed_engine:
                deepspeed_engine.backward(loss)
                deepspeed_engine.step()
            else:
                if mp.scaler:
                    mp.scaler.scale(loss).backward()
                else:
                    loss.backward()
            micro_norms.append(self._grad_norm(model))
            if (step + 1) % self.train_cfg.gradient_accumulation_steps == 0 and not deepspeed_engine:
                if self.train_cfg.gradient_clip:
                    if mp.scaler:
                        mp.scaler.unscale_(optimizer)
                    torch_mod.nn.utils.clip_grad_norm_(model.parameters(), self.train_cfg.gradient_clip)
                if mp.scaler:
                    mp.scaler.step(optimizer)
                    mp.scaler.update()
                else:
                    optimizer.step()
                optimizer.zero_grad(set_to_none=True)
                if scheduler is not None:
                    scheduler.step()
                if self.grad_monitor:
                    gns = self.grad_monitor.update(micro_norms)
                    if gns is not None:
                        log.info("Gradient noise scale: %.6f", gns)
                micro_norms = []
            losses.append(loss_value)
            batch_tokens = int(seq.shape[0] * max(0, seq.shape[1] - 1))
            tokens_seen += batch_tokens
            step += 1
            if self.train_cfg.log_every_n_steps and step % self.train_cfg.log_every_n_steps == 0:
                now = time.time()
                interval_tokens = tokens_seen - last_log_tokens
                interval_time = max(1e-6, now - last_log_time)
                tok_per_sec = interval_tokens / interval_time
                ppl = math.exp(min(20.0, loss_value))
                log.info(
                    "step=%d loss=%.4f ppl=%.2f tokens=%d tok/s=%.0f",
                    step,
                    loss_value,
                    ppl,
                    tokens_seen,
                    tok_per_sec,
                )
                last_log_time = now
                last_log_tokens = tokens_seen
            if self.train_cfg.log_gpu_every_n_steps and step % self.train_cfg.log_gpu_every_n_steps == 0:
                if torch_mod.cuda.is_available():
                    alloc = torch_mod.cuda.memory_allocated() / (1024 ** 2)
                    reserved = torch_mod.cuda.memory_reserved() / (1024 ** 2)
                    log.info("gpu_mem_mb alloc=%.0f reserved=%.0f", alloc, reserved)
            if step % self.train_cfg.save_every_n_steps == 0:
                self._save_checkpoint(model, optimizer, scheduler, checkpoint_dir / f"step_{step:06d}.pt", torch_mod)
            if target_tokens and tokens_seen >= target_tokens:
                log.info("Target tokens reached: %d", tokens_seen)
                break
            if max_steps is not None and step >= max_steps:
                break
        self._save_checkpoint(model, optimizer, scheduler, checkpoint_dir / "final.pt", torch_mod)
        return {
            "steps": step,
            "mean_loss": round(sum(losses) / max(1, len(losses)), 6),
            "checkpoints": sorted(str(path) for path in checkpoint_dir.glob("*.pt")),
        }

    @staticmethod
    def _save_checkpoint(model, optimizer, scheduler, path: Path, torch_mod):
        payload = {"model": model.state_dict(), "optimizer": optimizer.state_dict()}
        if scheduler is not None:
            payload["scheduler"] = scheduler.state_dict()
        torch_mod.save(payload, path)


# =============================================================================
# DATASET PIPELINE ENHANCEMENTS
# =============================================================================

class LicenseDetector:
    SPDX_IDS = {
        "mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause", "mpl-2.0", "lgpl-2.1",
        "lgpl-3.0", "gpl-2.0", "gpl-3.0", "agpl-3.0", "unlicense", "isc", "cc0-1.0",
    }
    PATTERNS = {
        "mit": re.compile(r"\bmit license\b", re.I),
        "apache-2.0": re.compile(r"apache license(?:,)? version 2\.0", re.I),
        "bsd-3-clause": re.compile(r"bsd 3-clause", re.I),
        "gpl-3.0": re.compile(r"gnu (?:general public license|gpl) v?3", re.I),
        "mpl-2.0": re.compile(r"mozilla public license(?:,)? v?2\.0", re.I),
        "cc0-1.0": re.compile(r"creative commons zero", re.I),
    }

    def detect(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        meta_license = (metadata or {}).get("license")
        if meta_license:
            key = meta_license.lower().strip()
            if key in self.SPDX_IDS:
                return key
        for spdx, pattern in self.PATTERNS.items():
            if pattern.search(text):
                return spdx
        return "unknown"


class ToxicityClassifier:
    def __init__(self, threshold: float = 0.7, model_name: str = "unitary/unbiased-toxic-roberta"):
        self.threshold = threshold
        self.model_name = model_name
        self._pipeline = None

    def _load(self):
        if self._pipeline is not None:
            return
        transformers_mod = optional_import("transformers")
        if not transformers_mod:
            return
        try:
            self._pipeline = transformers_mod.pipeline("text-classification", model=self.model_name, truncation=True)
        except Exception as exc:
            log.warning("Toxicity model load failed: %s", exc)
            self._pipeline = None

    def score(self, text: str) -> float:
        self._load()
        if self._pipeline:
            out = self._pipeline(text[:2000])[0]
            if out["label"].lower().startswith("toxic"):
                return float(out["score"])
            return 1.0 - float(out["score"])
        return 1.0 if re.search(r"\b(hate|kill|rape|terrorist)\b", text, flags=re.I) else 0.0


class QualityClassifier:
    def __init__(self, model_name: str = ""):
        self.model_name = model_name or os.getenv("NOVA_QUALITY_MODEL", "")
        self._model = None
        self._tokenizer = None

    def _load(self):
        if self._model is not None:
            return
        transformers_mod = optional_import("transformers")
        if not transformers_mod or not self.model_name:
            return
        try:
            self._tokenizer = transformers_mod.AutoTokenizer.from_pretrained(self.model_name)
            self._model = transformers_mod.AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self._model.eval()
        except Exception as exc:
            log.warning("Quality model load failed: %s", exc)
            self._model = None

    def score(self, text: str) -> Optional[float]:
        self._load()
        if not self._model or not self._tokenizer:
            return None
        torch_mod = optional_import("torch")
        if not torch_mod:
            return None
        encoded = self._tokenizer(text[:2000], return_tensors="pt", truncation=True)
        with torch_mod.no_grad():
            logits = self._model(**encoded).logits
        if logits.size(-1) == 1:
            return float(torch_mod.sigmoid(logits)[0].item())
        probs = torch_mod.softmax(logits, dim=-1)
        return float(probs[0, -1].item())


class EmbeddingDeduplicator:
    def __init__(self, threshold: float = 0.92, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.threshold = threshold
        self.model_name = model_name
        self.faiss = optional_import("faiss")
        self.numpy = optional_import("numpy")
        self.embedder = None
        self.index = None
        self.vectors = []

    def _embed(self, text: str) -> Optional[List[float]]:
        st_mod = optional_import("sentence_transformers")
        if not st_mod:
            return None
        if self.embedder is None:
            try:
                self.embedder = st_mod.SentenceTransformer(self.model_name)
            except Exception as exc:
                log.warning("Embedding model load failed: %s", exc)
                self.embedder = None
        if self.embedder is None:
            return None
        vector = self.embedder.encode([text], normalize_embeddings=True)[0]
        return vector.tolist() if hasattr(vector, "tolist") else list(vector)

    def is_duplicate(self, text: str) -> bool:
        vector = self._embed(text)
        if vector is None or not self.faiss or not self.numpy:
            return False
        vec = self.numpy.array([vector], dtype="float32")
        if self.index is None:
            self.index = self.faiss.IndexFlatIP(vec.shape[1])
            self.index.add(vec)
            self.vectors.append(vector)
            return False
        _, indices = self.index.search(vec, 1)
        if indices.size > 0 and indices[0][0] >= 0:
            existing = self.numpy.array([self.vectors[indices[0][0]]], dtype="float32")
            score = float(self.numpy.dot(vec, existing.T)[0][0])
            if score >= self.threshold:
                return True
        self.index.add(vec)
        self.vectors.append(vector)
        return False


class AdvancedDeduplicator:
    def __init__(self):
        self.lexical = SemanticDeduplicator()
        self.embedding = EmbeddingDeduplicator()

    def is_duplicate(self, text: str, doc_id: str = "") -> bool:
        if self.lexical.is_duplicate(text, doc_id):
            return True
        return self.embedding.is_duplicate(text)


class AdaptiveMultilingualBalancer:
    TARGET_LANG_RATIOS = MultilingualBalancer.TARGET_LANG_RATIOS if "MultilingualBalancer" in globals() else {
        "en": 0.50, "zh": 0.08, "de": 0.06, "fr": 0.06,
        "es": 0.06, "ar": 0.05, "tr": 0.05, "ru": 0.05,
        "ja": 0.04, "other": 0.05,
    }

    def __init__(self, total_tokens: int, tolerance: float = 0.05, min_accept_prob: float = 0.1):
        self.total_tokens = total_tokens
        self.lang_tokens: Dict[str, int] = defaultdict(int)
        self.tolerance = tolerance
        self.min_accept_prob = min_accept_prob

    def accept(self, lang: str, token_count: int) -> bool:
        key = lang if lang in self.TARGET_LANG_RATIOS else "other"
        total = max(1, sum(self.lang_tokens.values()))
        current_ratio = self.lang_tokens[key] / total
        target = self.TARGET_LANG_RATIOS.get(key, 0.01)
        if current_ratio <= target + self.tolerance:
            self.lang_tokens[key] += token_count
            return True
        overshoot = current_ratio - target
        prob = max(self.min_accept_prob, 1.0 - overshoot / max(target, 1e-6))
        if random.random() < prob:
            self.lang_tokens[key] += token_count
            return True
        return False

    def report(self) -> Dict[str, float]:
        total = max(1, sum(self.lang_tokens.values()))
        return {lang: tokens / total for lang, tokens in self.lang_tokens.items()}


# =============================================================================
# TOKENIZER UTILITIES
# =============================================================================

@dataclass
class TokenizerConfig:
    algorithm: str = field(default_factory=lambda: os.getenv("NOVA_TOKENIZER_ALGO", "bpe"))
    vocab_size: int = 65_536
    min_frequency: int = 2
    tokenizer_json_path: str = ""
    sentencepiece_model_path: str = ""
    pretrained_name_or_path: str = field(default_factory=lambda: os.getenv("NOVA_TOKENIZER", ""))
    special_tokens: List[str] = field(default_factory=lambda: [
        "<|pad|>", "<|unk|>", "<|bos|>", "<|eos|>",
        "<|sys|>", "<|tool|>", "<|code|>", "<|math|>",
        "[SYS]", "[/SYS]", "[USER]", "[/USER]", "[ASSISTANT]", "[/ASSISTANT]",
    ])
    unicode_normalization: str = "NFKC"
    byte_fallback: bool = True
    prune_vocab_size: int = 0
    benchmark_samples: int = 200


class TokenizerBenchmark:
    def __init__(self, token_counter: TokenCounter):
        self.token_counter = token_counter

    def evaluate(self, samples: List[str]) -> Dict[str, float]:
        if not samples:
            return {"avg_tokens_per_word": 0.0, "avg_chars_per_token": 0.0, "tokens_per_1k_chars": 0.0}
        total_tokens = sum(self.token_counter.count(s) for s in samples)
        total_words = sum(len(re.findall(r"\w+", s)) for s in samples)
        total_chars = sum(len(s) for s in samples)
        avg_tokens_per_word = total_tokens / max(1, total_words)
        avg_chars_per_token = total_chars / max(1, total_tokens)
        tokens_per_1k_chars = (total_tokens / max(1, total_chars)) * 1000.0
        return {
            "avg_tokens_per_word": round(avg_tokens_per_word, 4),
            "avg_chars_per_token": round(avg_chars_per_token, 4),
            "tokens_per_1k_chars": round(tokens_per_1k_chars, 2),
        }


class BPETokenizerTrainer:
    def __init__(self, config: TokenizerConfig):
        self.config = config

    def train(self, corpus_file: Path) -> dict:
        if self.config.algorithm.lower() == "sentencepiece":
            spm_mod = optional_import("sentencepiece")
            if not spm_mod:
                return {"status": "skipped", "reason": "install sentencepiece"}
            model_prefix = corpus_file.parent / "tokenizer_spm"
            user_symbols = ",".join(self.config.special_tokens) if self.config.special_tokens else ""
            args = [
                f"--input={corpus_file}",
                f"--model_prefix={model_prefix}",
                f"--vocab_size={self.config.vocab_size}",
                "--model_type=bpe",
                "--character_coverage=1.0",
                "--unk_id=0",
                "--bos_id=1",
                "--eos_id=2",
                "--pad_id=3",
            ]
            if user_symbols:
                args.append(f"--user_defined_symbols={user_symbols}")
            spm_mod.SentencePieceTrainer.train(" ".join(args))
            model_path = f"{model_prefix}.model"
            self.config.sentencepiece_model_path = model_path
            return {"status": "trained", "sentencepiece_model": model_path, "vocab_size": self.config.vocab_size}
        tokenizers_mod = optional_import("tokenizers")
        if not tokenizers_mod:
            return {"status": "skipped", "reason": "install tokenizers"}
        model_kwargs: Dict[str, Any] = {"unk_token": "<|unk|>"}
        try:
            model_kwargs["byte_fallback"] = bool(self.config.byte_fallback)
        except Exception:
            pass
        tokenizer = tokenizers_mod.Tokenizer(tokenizers_mod.models.BPE(**model_kwargs))
        normalizer_name = (self.config.unicode_normalization or "").upper()
        normalizer_cls = getattr(tokenizers_mod.normalizers, normalizer_name, None) if normalizer_name else None
        if normalizer_cls is not None:
            tokenizer.normalizer = tokenizers_mod.normalizers.Sequence([normalizer_cls()])
        tokenizer.pre_tokenizer = tokenizers_mod.pre_tokenizers.ByteLevel(add_prefix_space=False)
        tokenizer.decoder = tokenizers_mod.decoders.ByteLevel()
        trainer = tokenizers_mod.trainers.BpeTrainer(
            vocab_size=self.config.vocab_size,
            min_frequency=self.config.min_frequency,
            special_tokens=self.config.special_tokens,
            show_progress=True,
        )
        tokenizer.train([str(corpus_file)], trainer)
        output_path = corpus_file.parent / "tokenizer.json"
        tokenizer.save(str(output_path))
        self.config.tokenizer_json_path = str(output_path)
        result = {"status": "trained", "tokenizer_json": str(output_path), "vocab_size": len(tokenizer.get_vocab())}
        if self.config.prune_vocab_size and self.config.prune_vocab_size < self.config.vocab_size:
            result["pruned_to"] = self.prune(corpus_file, self.config.prune_vocab_size).get("vocab_size")
        return result

    def prune(self, corpus_file: Path, target_vocab_size: int) -> dict:
        original = self.config.vocab_size
        self.config.vocab_size = target_vocab_size
        result = self.train(corpus_file)
        self.config.vocab_size = original
        return result

    def analyze_vocabulary(self) -> dict:
        return {"status": "available" if self.config.tokenizer_json_path else "missing"}


# =============================================================================
# EVALUATION ENHANCEMENTS
# =============================================================================

class Evaluator:
    BENCHMARK_CONFIGS = {
        "MMLU": {"task": "mmlu", "num_samples": 14042, "metric": "accuracy", "metric_keys": ["acc,none", "acc"], "target": 0.70},
        "HellaSwag": {"task": "hellaswag", "num_samples": 10003, "metric": "accuracy", "metric_keys": ["acc,none", "acc_norm,none", "acc"], "target": 0.85},
        "GSM8K": {"task": "gsm8k", "num_samples": 1319, "metric": "exact_match", "metric_keys": ["exact_match,strict-match", "exact_match"], "target": 0.55},
        "HumanEval": {"task": "humaneval", "num_samples": 164, "metric": "pass@1", "metric_keys": ["pass@1,create_test", "pass@1"], "target": 0.40},
        "BBH": {"task": "bbh", "num_samples": 6511, "metric": "accuracy", "metric_keys": ["acc,none", "acc"], "target": 0.60},
        "ARC-Easy": {"task": "arc_easy", "num_samples": 2251, "metric": "accuracy", "metric_keys": ["acc,none", "acc"], "target": 0.80},
        "ARC-Challenge": {"task": "arc_challenge", "num_samples": 1172, "metric": "accuracy", "metric_keys": ["acc,none", "acc"], "target": 0.60},
        "AGIEval": {"task": "agieval", "num_samples": 3000, "metric": "accuracy", "metric_keys": ["acc,none", "acc"], "target": 0.55},
        "TruthfulQA": {"task": "truthfulqa_mc", "num_samples": 817, "metric": "mc1", "metric_keys": ["mc1", "mc2"], "target": 0.45},
        "IFEval": {"task": "ifeval", "num_samples": 1000, "metric": "strict", "metric_keys": ["strict", "loose"], "target": 0.60},
        "ToxiGen": {"task": "toxigen", "num_samples": 10000, "metric": "acc", "metric_keys": ["acc,none", "acc"], "target": 0.95},
    }

    def run_benchmarks(self, model_spec: Optional[EvalModelSpec] = None, output_dir: Optional[Path] = None) -> Dict[str, BenchmarkResult]:
        if model_spec is None:
            return {
                name: BenchmarkResult(name, None, cfg["num_samples"], cfg["metric"], cfg["target"], "skipped", "provide EvalModelSpec to run lm-eval")
                for name, cfg in self.BENCHMARK_CONFIGS.items()
            }
        if not optional_import("lm_eval"):
            return {
                name: BenchmarkResult(name, None, cfg["num_samples"], cfg["metric"], cfg["target"], "skipped", "install lm-eval")
                for name, cfg in self.BENCHMARK_CONFIGS.items()
            }
        output_dir = output_dir or (get_nova_base() / "evals" / safe_filename(now_utc()))
        output_dir.mkdir(parents=True, exist_ok=True)
        results = {}
        for name, cfg in self.BENCHMARK_CONFIGS.items():
            command = [
                sys.executable,
                "-m",
                "lm_eval",
                "--model",
                model_spec.model_backend,
                "--model_args",
                ",".join(f"{key}={value}" for key, value in model_spec.model_args.items()),
                "--tasks",
                cfg["task"],
                "--device",
                model_spec.device,
                "--batch_size",
                model_spec.batch_size,
                "--output_path",
                str(output_dir / safe_filename(name)),
            ] + model_spec.extra_cli_args
            try:
                subprocess.run(command, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as exc:
                notes = (exc.stderr or exc.stdout or str(exc)).strip()[:500]
                results[name] = BenchmarkResult(name, None, cfg["num_samples"], cfg["metric"], cfg["target"], "failed", notes)
                continue
            payload = {}
            for candidate in sorted((output_dir / safe_filename(name)).rglob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
                try:
                    payload = json.loads(candidate.read_text(encoding="utf-8"))
                    if "results" in payload:
                        break
                except Exception:
                    continue
            task_result = payload.get("results", {}).get(cfg["task"], {})
            score = None
            for metric_key in cfg["metric_keys"]:
                if metric_key in task_result:
                    score = float(task_result[metric_key])
                    break
            results[name] = BenchmarkResult(
                name=name,
                score=score,
                num_samples=cfg["num_samples"],
                metric=cfg["metric"],
                target=cfg["target"],
                status="completed" if score is not None else "missing",
                notes="" if score is not None else "metric not found",
            )
        return results

    def print_report(self, results: Dict[str, BenchmarkResult]):
        print("\n" + "=" * 72)
        print(f"{'Benchmark':<16} {'Score':>10} {'Target':>10} {'Metric':<15} Status")
        print("-" * 72)
        for result in results.values():
            score_text = f"{result.score:.3f}" if result.score is not None else "-"
            print(f"{result.name:<16} {score_text:>10} {result.target:>10.2f} {result.metric:<15} {result.status}")
        print("=" * 72 + "\n")


# =============================================================================
# RAG ENHANCEMENTS
# =============================================================================

class BM25Retriever:
    def __init__(self):
        self.rank_bm25 = optional_import("rank_bm25")
        self.corpus = []
        self.index = None

    def build(self, texts: List[str]):
        if not self.rank_bm25:
            return
        self.corpus = [re.findall(r"\w+", t.lower()) for t in texts]
        self.index = self.rank_bm25.BM25Okapi(self.corpus)

    def search(self, query: str, k: int = 5) -> List[int]:
        if not self.index:
            return []
        tokens = re.findall(r"\w+", query.lower())
        scores = self.index.get_scores(tokens)
        ranked = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
        return ranked[:k]


class CrossEncoderReranker:
    def __init__(self, model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"):
        self.model_name = model_name
        self.reranker = None

    def _load(self):
        if self.reranker is not None:
            return
        st_mod = optional_import("sentence_transformers")
        if not st_mod or not hasattr(st_mod, "CrossEncoder"):
            return
        try:
            self.reranker = st_mod.CrossEncoder(self.model_name)
        except Exception as exc:
            log.warning("Cross-encoder load failed: %s", exc)
            self.reranker = None

    def rerank(self, query: str, docs: List[str]) -> List[int]:
        self._load()
        if not self.reranker:
            return list(range(len(docs)))
        pairs = [(query, doc) for doc in docs]
        scores = self.reranker.predict(pairs)
        return sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)


class QueryRewriter:
    def __init__(self, model_name: str = "t5-small"):
        self.model_name = model_name
        self.pipeline = None

    def _load(self):
        if self.pipeline is not None:
            return
        transformers_mod = optional_import("transformers")
        if not transformers_mod:
            return
        try:
            self.pipeline = transformers_mod.pipeline("text2text-generation", model=self.model_name)
        except Exception as exc:
            log.warning("Query rewriter load failed: %s", exc)
            self.pipeline = None

    def rewrite(self, query: str) -> str:
        self._load()
        if not self.pipeline:
            return normalize_space(query.lower())
        output = self.pipeline(f"Rewrite search query: {query}", max_length=64)[0]["generated_text"]
        return normalize_space(output)


class MetadataScorer:
    def score(self, chunk: DocumentChunk) -> float:
        score = 0.0
        if chunk.metadata.get("source"):
            score += 0.05
        if chunk.metadata.get("title"):
            score += 0.05
        score += min(0.3, chunk.token_count / 5000.0)
        return score


@dataclass
class RetrievalConfig:
    embedding_backend: str = "hashing"
    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    use_faiss: bool = True
    embedding_dim: int = 384
    use_bm25: bool = True
    use_reranker: bool = True


class VectorRetriever:
    def __init__(self, config: RetrievalConfig):
        self.config = config
        self.faiss = optional_import("faiss")
        self.numpy = optional_import("numpy")
        self.embedder = None
        self.chunks: List[DocumentChunk] = []
        self.vectors: List[List[float]] = []
        self.index = None
        self.bm25 = BM25Retriever() if config.use_bm25 else None
        self.reranker = CrossEncoderReranker() if config.use_reranker else None
        self.metadata = MetadataScorer()

    def build(self, chunks: Sequence[DocumentChunk]) -> None:
        self.chunks = list(chunks)
        self.vectors = [self._embed(chunk.text) for chunk in self.chunks]
        if self.bm25:
            self.bm25.build([chunk.text for chunk in self.chunks])
        if self.config.use_faiss and self.faiss and self.numpy and self.vectors:
            matrix = self.numpy.array(self.vectors, dtype="float32")
            norms = self.numpy.linalg.norm(matrix, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            matrix /= norms
            self.index = self.faiss.IndexFlatIP(matrix.shape[1])
            self.index.add(matrix)

    def search(self, query: str, k: int = 5) -> List[DocumentChunk]:
        if not self.chunks:
            return []
        query_vec = self._embed(query)
        candidates: Dict[int, float] = {}
        if self.index is not None and self.numpy is not None:
            matrix = self.numpy.array([query_vec], dtype="float32")
            norms = self.numpy.linalg.norm(matrix, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            matrix /= norms
            scores, indices = self.index.search(matrix, min(k * 3, len(self.chunks)))
            for score, idx in zip(scores[0], indices[0]):
                if idx >= 0:
                    candidates[idx] = float(score)
        if self.bm25:
            for rank, idx in enumerate(self.bm25.search(query, k=k * 3)):
                candidates[idx] = candidates.get(idx, 0.0) + (1.0 / (rank + 1))
        scored = []
        for idx, score in candidates.items():
            chunk = self.chunks[idx]
            score += self.metadata.score(chunk)
            scored.append((score, chunk))
        if self.reranker:
            ordered = self.reranker.rerank(query, [chunk.text for _, chunk in scored])
            scored = [scored[i] for i in ordered]
        scored.sort(key=lambda item: item[0], reverse=True)
        return [chunk for _, chunk in scored[:k]]

    def _embed(self, text: str) -> List[float]:
        if self.config.embedding_backend == "sentence_transformers":
            embedder = self._embedder()
            if embedder is not None:
                vector = embedder.encode([text], normalize_embeddings=True)[0]
                return vector.tolist() if hasattr(vector, "tolist") else list(vector)
        vector = [0.0] * self.config.embedding_dim
        for token, weight in Counter(re.findall(r"\w+", text.lower())).items():
            digest = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
            index = digest % self.config.embedding_dim
            sign = 1.0 if (digest >> 8) & 1 else -1.0
            vector[index] += weight * sign
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]

    def _embedder(self):
        if self.embedder is not None:
            return self.embedder
        st_mod = optional_import("sentence_transformers")
        if not st_mod:
            return None
        try:
            self.embedder = st_mod.SentenceTransformer(self.config.embedding_model_name)
        except Exception as exc:
            log.warning("SentenceTransformer load failed: %s", exc)
            self.embedder = None
        return self.embedder


# =============================================================================
# INFERENCE ENGINE ENHANCEMENTS
# =============================================================================

@dataclass
class InferenceConfig:
    backend: str = "transformers"
    model_name_or_path: str = ""
    tensor_parallel_size: int = 1
    dtype: str = "auto"
    max_model_len: int = 4096
    temperature: float = 0.7
    draft_model_name_or_path: str = ""
    enable_speculative: bool = False
    enable_continuous_batching: bool = False
    quantization: str = ""
    max_batch_size: int = 8
    top_k: int = 50
    top_p: float = 0.9
    repetition_penalty: float = 1.1


class MemoryManager:
    def __init__(self, reserve_ratio: float = 0.85):
        self.reserve_ratio = reserve_ratio

    def suggest_max_new_tokens(self, desired: int) -> int:
        torch_mod = optional_import("torch")
        if not torch_mod or not torch_mod.cuda.is_available():
            return desired
        free, total = torch_mod.cuda.mem_get_info()
        if free / max(1, total) < (1.0 - self.reserve_ratio):
            return max(8, desired // 2)
        return desired


class SpeculativeDecoder:
    def __init__(self, target_model, draft_model, tokenizer):
        self.target_model = target_model
        self.draft_model = draft_model
        self.tokenizer = tokenizer

    @staticmethod
    def _apply_repetition_penalty(logits, input_ids, penalty: float):
        if penalty is None or penalty == 1.0:
            return logits
        torch_mod = _TORCH or optional_import("torch")
        if not torch_mod:
            raise RuntimeError("Install torch to use repetition penalty.")
        unique_tokens = torch_mod.unique(input_ids)
        for token_id in unique_tokens:
            idx = int(token_id.item())
            token_logits = logits[..., idx]
            logits[..., idx] = torch_mod.where(token_logits < 0, token_logits * penalty, token_logits / penalty)
        return logits

    @staticmethod
    def _filter_top_k_top_p(logits, top_k: int, top_p: float):
        torch_mod = _TORCH or optional_import("torch")
        if not torch_mod:
            raise RuntimeError("Install torch to use top-k/top-p sampling.")
        if top_k and top_k > 0:
            top_k = min(top_k, logits.size(-1))
            values, _ = torch_mod.topk(logits, top_k)
            min_values = values[..., -1, None]
            logits = torch_mod.where(logits < min_values, torch_mod.full_like(logits, float("-inf")), logits)
        if top_p and 0 < top_p < 1.0:
            sorted_logits, sorted_indices = torch_mod.sort(logits, descending=True)
            probs = torch_mod.softmax(sorted_logits, dim=-1)
            cumulative_probs = torch_mod.cumsum(probs, dim=-1)
            sorted_mask = cumulative_probs > top_p
            sorted_mask[..., 0] = False
            sorted_logits = sorted_logits.masked_fill(sorted_mask, float("-inf"))
            logits = torch_mod.zeros_like(logits).scatter(-1, sorted_indices, sorted_logits)
        return logits

    def _sample_next_token(
        self,
        logits,
        input_ids,
        temperature: float,
        top_k: int,
        top_p: float,
        repetition_penalty: float,
    ):
        torch_mod = _TORCH or optional_import("torch")
        if not torch_mod:
            raise RuntimeError("Install torch to sample tokens.")
        if temperature <= 0:
            return logits.argmax(dim=-1)
        logits = logits / max(temperature, 1e-5)
        logits = self._apply_repetition_penalty(logits, input_ids, repetition_penalty)
        logits = self._filter_top_k_top_p(logits, top_k, top_p)
        probs = torch_mod.softmax(logits, dim=-1)
        return torch_mod.multinomial(probs, num_samples=1).squeeze(1)

    def generate(
        self,
        prompt: str,
        max_new_tokens: int,
        temperature: float = 0.7,
        draft_tokens: int = 4,
        top_k: int = 0,
        top_p: float = 1.0,
        repetition_penalty: float = 1.0,
    ) -> str:
        torch_mod = optional_import("torch")
        if not torch_mod:
            raise RuntimeError("Install torch to use speculative decoding.")
        device = next(self.target_model.parameters()).device
        input_ids = self.tokenizer(prompt, return_tensors="pt").input_ids.to(device)
        prompt_len = input_ids.size(1)
        for _ in range(max_new_tokens):
            draft_out = self.draft_model.generate(
                input_ids,
                max_new_tokens=draft_tokens,
                do_sample=temperature > 0,
                temperature=temperature,
                top_k=top_k if top_k and top_k > 0 else None,
                top_p=top_p if top_p and top_p < 1.0 else None,
                repetition_penalty=repetition_penalty if repetition_penalty and repetition_penalty != 1.0 else None,
                pad_token_id=self.tokenizer.eos_token_id,
            )
            proposed = draft_out[0, input_ids.size(1):]
            for token in proposed:
                logits = self.target_model(input_ids).logits[:, -1, :]
                next_token = self._sample_next_token(
                    logits,
                    input_ids,
                    temperature=temperature,
                    top_k=top_k,
                    top_p=top_p,
                    repetition_penalty=repetition_penalty,
                )
                input_ids = _TORCH.cat([input_ids, next_token.unsqueeze(1)], dim=-1)
                if next_token.item() != token.item():
                    break
                if input_ids.size(1) - prompt_len >= max_new_tokens:
                    break
            if input_ids.size(1) - prompt_len >= max_new_tokens:
                break
        return self.tokenizer.decode(input_ids[0], skip_special_tokens=True)


class InferenceEngine:
    PRESETS = {
        "balanced": {"top_k": 50, "top_p": 0.9, "repetition_penalty": 1.1, "temperature": 0.7},
        "creative": {"top_k": 100, "top_p": 0.95, "repetition_penalty": 1.05, "temperature": 0.9},
        "strict": {"top_k": 20, "top_p": 0.8, "repetition_penalty": 1.2, "temperature": 0.5},
    }

    def __init__(self, config: InferenceConfig):
        self.config = config
        self._loaded = False
        self._backend: Dict[str, Any] = {}
        self.memory = MemoryManager()

    def generate(
        self,
        prompt: Any,
        max_new_tokens: int = 256,
        temperature: Optional[float] = None,
        top_k: Optional[int] = None,
        top_p: Optional[float] = None,
        repetition_penalty: Optional[float] = None,
        preset: Optional[str] = None,
    ) -> Any:
        self._ensure_loaded()
        backend = self._backend
        if not backend:
            raise RuntimeError("Inference backend was not initialized.")
        if preset:
            preset_cfg = self.PRESETS.get(preset.lower())
            if preset_cfg:
                if temperature is None:
                    temperature = preset_cfg.get("temperature")
                if top_k is None:
                    top_k = preset_cfg.get("top_k")
                if top_p is None:
                    top_p = preset_cfg.get("top_p")
                if repetition_penalty is None:
                    repetition_penalty = preset_cfg.get("repetition_penalty")
            else:
                log.warning("Unknown preset '%s'. Available: %s", preset, ", ".join(sorted(self.PRESETS)))
        temperature = self.config.temperature if temperature is None else temperature
        top_k = self.config.top_k if top_k is None else top_k
        top_p = self.config.top_p if top_p is None else top_p
        repetition_penalty = self.config.repetition_penalty if repetition_penalty is None else repetition_penalty
        max_new_tokens = self.memory.suggest_max_new_tokens(max_new_tokens)
        if isinstance(prompt, list):
            return self._batch_generate(prompt, max_new_tokens, temperature, top_k, top_p, repetition_penalty)
        if self.config.enable_speculative and backend.get("draft_model"):
            decoder = SpeculativeDecoder(backend["model"], backend["draft_model"], backend["tokenizer"])
            return decoder.generate(
                prompt,
                max_new_tokens=max_new_tokens,
                temperature=temperature,
                top_k=top_k or 0,
                top_p=top_p or 1.0,
                repetition_penalty=repetition_penalty or 1.0,
            )
        if self.config.backend == "vllm":
            sampling_kwargs = {"temperature": temperature, "max_tokens": max_new_tokens}
            if top_k and top_k > 0:
                sampling_kwargs["top_k"] = top_k
            if top_p and top_p < 1.0:
                sampling_kwargs["top_p"] = top_p
            if repetition_penalty and repetition_penalty != 1.0:
                sampling_kwargs["repetition_penalty"] = repetition_penalty
            sampling = backend["sampling_cls"](**sampling_kwargs)
            outputs = backend["llm"].generate([prompt], sampling)
            return outputs[0].outputs[0].text.strip()
        tokenizer = backend["tokenizer"]
        model = backend["model"]
        torch_mod = backend["torch"]
        device = backend["device"]
        encoded = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=self.config.max_model_len)
        encoded = {key: value.to(device) for key, value in encoded.items()}
        with torch_mod.no_grad():
            output = model.generate(
                **encoded,
                do_sample=temperature > 0,
                temperature=temperature,
                top_k=top_k or 0,
                top_p=top_p or 1.0,
                repetition_penalty=repetition_penalty or 1.0,
                max_new_tokens=max_new_tokens,
                pad_token_id=tokenizer.eos_token_id,
            )
        return tokenizer.decode(output[0], skip_special_tokens=True)

    def _batch_generate(
        self,
        prompts: List[str],
        max_new_tokens: int,
        temperature: float,
        top_k: int,
        top_p: float,
        repetition_penalty: float,
    ) -> List[str]:
        backend = self._backend
        if not backend:
            raise RuntimeError("Inference backend was not initialized.")
        tokenizer = backend["tokenizer"]
        model = backend["model"]
        torch_mod = backend["torch"]
        device = backend["device"]
        encoded = tokenizer(prompts, return_tensors="pt", truncation=True, padding=True, max_length=self.config.max_model_len)
        encoded = {key: value.to(device) for key, value in encoded.items()}
        with torch_mod.no_grad():
            output = model.generate(
                **encoded,
                do_sample=temperature > 0,
                temperature=temperature,
                top_k=top_k or 0,
                top_p=top_p or 1.0,
                repetition_penalty=repetition_penalty or 1.0,
                max_new_tokens=max_new_tokens,
                pad_token_id=tokenizer.eos_token_id,
            )
        return [tokenizer.decode(row, skip_special_tokens=True) for row in output]

    def _ensure_loaded(self):
        if self._loaded:
            return
        self._loaded = True
        if self.config.backend == "vllm":
            vllm_mod = optional_import("vllm")
            if not vllm_mod:
                raise RuntimeError("Install vllm for vLLM inference.")
            self._backend = {
                "llm": vllm_mod.LLM(
                    model=self.config.model_name_or_path,
                    tensor_parallel_size=self.config.tensor_parallel_size,
                    dtype=self.config.dtype,
                    max_model_len=self.config.max_model_len,
                ),
                "sampling_cls": vllm_mod.SamplingParams,
            }
            return
        transformers_mod = optional_import("transformers")
        torch_mod = optional_import("torch")
        if not transformers_mod or not torch_mod:
            raise RuntimeError("Install transformers and torch for HF inference.")
        quant = self.config.quantization.lower()
        if quant in ("int8", "int4") and not optional_import("bitsandbytes"):
            log.warning("bitsandbytes not available; falling back to full precision.")
            quant = ""
        kwargs = {}
        if quant in ("int8", "int4"):
            kwargs["load_in_8bit"] = quant == "int8"
            kwargs["load_in_4bit"] = quant == "int4"
            kwargs["device_map"] = "auto"
        tokenizer = transformers_mod.AutoTokenizer.from_pretrained(self.config.model_name_or_path)
        model = transformers_mod.AutoModelForCausalLM.from_pretrained(self.config.model_name_or_path, **kwargs)
        device = "cuda" if torch_mod.cuda.is_available() else "cpu"
        if device == "cpu" and quant == "int8_dynamic":
            model = torch_mod.quantization.quantize_dynamic(model, {_TORCH.nn.Linear}, dtype=_TORCH.qint8)
        model.to(device)
        model.eval()
        draft_model = None
        if self.config.enable_speculative and self.config.draft_model_name_or_path:
            draft_model = transformers_mod.AutoModelForCausalLM.from_pretrained(self.config.draft_model_name_or_path)
            draft_model.to(device)
            draft_model.eval()
        self._backend = {"tokenizer": tokenizer, "model": model, "draft_model": draft_model, "torch": torch_mod, "device": device}


# =============================================================================
# RLHF ENHANCEMENTS
# =============================================================================

class RewardModelTrainer:
    def __init__(self, model_name: str = "distilbert-base-uncased"):
        self.model_name = model_name

    def train(self, preference_dataset: List[InstructionExample], output_dir: Path) -> str:
        transformers_mod = optional_import("transformers")
        torch_mod = optional_import("torch")
        if not transformers_mod or not torch_mod:
            raise RuntimeError("Install transformers + torch to train reward model.")
        tokenizer = transformers_mod.AutoTokenizer.from_pretrained(self.model_name)
        model = transformers_mod.AutoModelForSequenceClassification.from_pretrained(self.model_name, num_labels=2)
        optimizer = torch_mod.optim.AdamW(model.parameters(), lr=2e-5)
        model.train()
        for ex in preference_dataset:
            chosen = tokenizer(ex.preference_chosen, return_tensors="pt", truncation=True)
            rejected = tokenizer(ex.preference_rejected, return_tensors="pt", truncation=True)
            labels = torch_mod.tensor([1, 0])
            inputs = {k: torch_mod.cat([chosen[k], rejected[k]], dim=0) for k in chosen.keys()}
            logits = model(**inputs).logits
            loss = _F.cross_entropy(logits, labels)
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
        output_dir.mkdir(parents=True, exist_ok=True)
        model.save_pretrained(output_dir)
        tokenizer.save_pretrained(output_dir)
        return str(output_dir)


class DPOTrainerLite:
    def __init__(self, beta: float = 0.1):
        self.beta = beta

    def train(self, policy_model_name: str, preference_dataset: List[InstructionExample], output_dir: Path) -> str:
        transformers_mod = optional_import("transformers")
        torch_mod = optional_import("torch")
        if not transformers_mod or not torch_mod:
            raise RuntimeError("Install transformers + torch to run DPO.")
        tokenizer = transformers_mod.AutoTokenizer.from_pretrained(policy_model_name)
        model = transformers_mod.AutoModelForCausalLM.from_pretrained(policy_model_name)
        model.train()
        optimizer = torch_mod.optim.AdamW(model.parameters(), lr=1e-5)
        for ex in preference_dataset:
            prompt = f"{ex.system}\n{ex.user}\n"
            chosen = prompt + ex.preference_chosen
            rejected = prompt + ex.preference_rejected
            chosen_ids = tokenizer(chosen, return_tensors="pt", truncation=True)
            rejected_ids = tokenizer(rejected, return_tensors="pt", truncation=True)
            chosen_logits = model(**chosen_ids).logits[:, :-1, :]
            rejected_logits = model(**rejected_ids).logits[:, :-1, :]
            chosen_labels = chosen_ids["input_ids"][:, 1:]
            rejected_labels = rejected_ids["input_ids"][:, 1:]
            chosen_logp = -_F.cross_entropy(chosen_logits.reshape(-1, chosen_logits.size(-1)), chosen_labels.reshape(-1), reduction="mean")
            rejected_logp = -_F.cross_entropy(rejected_logits.reshape(-1, rejected_logits.size(-1)), rejected_labels.reshape(-1), reduction="mean")
            loss = -_F.logsigmoid(self.beta * (chosen_logp - rejected_logp))
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
        output_dir.mkdir(parents=True, exist_ok=True)
        model.save_pretrained(output_dir)
        tokenizer.save_pretrained(output_dir)
        return str(output_dir)


class PPOTrainerLite:
    def __init__(self, kl_coeff: float = 0.1):
        self.kl_coeff = kl_coeff

    def train(self, policy_model_name: str, reward_model_path: str, prompts: List[str], output_dir: Path) -> str:
        transformers_mod = optional_import("transformers")
        torch_mod = optional_import("torch")
        if not transformers_mod or not torch_mod:
            raise RuntimeError("Install transformers + torch to run PPO.")
        tokenizer = transformers_mod.AutoTokenizer.from_pretrained(policy_model_name)
        policy = transformers_mod.AutoModelForCausalLM.from_pretrained(policy_model_name)
        reward_model = transformers_mod.AutoModelForSequenceClassification.from_pretrained(reward_model_path)
        optimizer = torch_mod.optim.AdamW(policy.parameters(), lr=1e-6)
        policy.train()
        for prompt in prompts:
            inputs = tokenizer(prompt, return_tensors="pt")
            generated = policy.generate(**inputs, max_new_tokens=64, do_sample=True, temperature=0.8)
            with torch_mod.no_grad():
                reward = reward_model(generated).logits[:, -1]
            logits = policy(generated).logits[:, :-1, :]
            labels = generated[:, 1:]
            logp = -_F.cross_entropy(logits.reshape(-1, logits.size(-1)), labels.reshape(-1), reduction="mean")
            loss = -(reward.mean() - self.kl_coeff * logp)
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
        output_dir.mkdir(parents=True, exist_ok=True)
        policy.save_pretrained(output_dir)
        tokenizer.save_pretrained(output_dir)
        return str(output_dir)


# =============================================================================
# SELF-IMPROVEMENT UTILITIES
# =============================================================================

class SelfPlayGenerator:
    def __init__(self, engine: InferenceEngine):
        self.engine = engine

    def generate(self, n: int = 5) -> List[InstructionExample]:
        samples = []
        for _ in range(n):
            prompt = "Create a challenging reasoning question and answer it succinctly."
            out = self.engine.generate(prompt, max_new_tokens=256, temperature=0.8)
            samples.append(InstructionExample(system=RLHFPipeline.SYSTEM_PROMPTS["general"], user=prompt, assistant=out))
        return samples


class SyntheticDataGenerator:
    def __init__(self):
        self.templates = [
            ("Solve: {a} + {b}", lambda a, b: str(a + b)),
            ("Write a Python function to compute {n}th Fibonacci.", lambda n, _: f"def fib(n):\\n    a,b=0,1\\n    for _ in range(n):\\n        a,b=b,a+b\\n    return a"),
        ]

    def generate(self, n: int = 10) -> List[InstructionExample]:
        out = []
        for i in range(n):
            template, fn = self.templates[i % len(self.templates)]
            a = random.randint(1, 100)
            b = random.randint(1, 100)
            prompt = template.format(a=a, b=b, n=a)
            answer = fn(a, b)
            out.append(InstructionExample(system=RLHFPipeline.SYSTEM_PROMPTS["general"], user=prompt, assistant=answer))
        return out


class AutoCurriculumScheduler:
    def __init__(self):
        self.weights = {"easy": 0.4, "medium": 0.4, "hard": 0.2}

    def update(self, eval_scores: Dict[str, float]) -> Dict[str, float]:
        score = sum(eval_scores.values()) / max(1, len(eval_scores))
        if score < 0.4:
            self.weights = {"easy": 0.6, "medium": 0.3, "hard": 0.1}
        elif score < 0.7:
            self.weights = {"easy": 0.4, "medium": 0.4, "hard": 0.2}
        else:
            self.weights = {"easy": 0.2, "medium": 0.4, "hard": 0.4}
        return self.weights


class DistillationTrainer:
    def __init__(self, temperature: float = 2.0):
        self.temperature = temperature

    def distill(self, teacher_name: str, student_name: str, texts: List[str], output_dir: Path) -> str:
        transformers_mod = optional_import("transformers")
        torch_mod = optional_import("torch")
        if not transformers_mod or not torch_mod:
            raise RuntimeError("Install transformers + torch to distill.")
        tokenizer = transformers_mod.AutoTokenizer.from_pretrained(teacher_name)
        teacher = transformers_mod.AutoModelForCausalLM.from_pretrained(teacher_name)
        student = transformers_mod.AutoModelForCausalLM.from_pretrained(student_name)
        optimizer = torch_mod.optim.AdamW(student.parameters(), lr=2e-5)
        teacher.eval()
        student.train()
        for text in texts:
            enc = tokenizer(text, return_tensors="pt", truncation=True)
            with torch_mod.no_grad():
                t_logits = teacher(**enc).logits / self.temperature
            s_logits = student(**enc).logits / self.temperature
            loss = _F.kl_div(_F.log_softmax(s_logits, dim=-1), _F.softmax(t_logits, dim=-1), reduction="batchmean") * (self.temperature ** 2)
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
        output_dir.mkdir(parents=True, exist_ok=True)
        student.save_pretrained(output_dir)
        tokenizer.save_pretrained(output_dir)
        return str(output_dir)


# =============================================================================
# ORCHESTRATOR V3
# =============================================================================

class NovaTitanOrchestratorV3(NovaTitanOrchestrator):
    def __init__(self, target_tokens: int = 5_000_000_000):
        super().__init__(target_tokens=target_tokens)
        self.tokenizer_cfg = TokenizerConfig()
        self.token_counter = TokenCounter(
            tokenizer_path=self.tokenizer_cfg.tokenizer_json_path,
            pretrained_name_or_path=self.tokenizer_cfg.pretrained_name_or_path,
            sentencepiece_model_path=self.tokenizer_cfg.sentencepiece_model_path,
        )
        set_default_token_counter(self.token_counter)

        self.license_detector = LicenseDetector()
        self.toxicity = ToxicityClassifier()
        self.quality_model = QualityClassifier()
        self.dedup = AdvancedDeduplicator()
        self.ml_balancer = AdaptiveMultilingualBalancer(target_tokens)

        self.model_cfg = ModelArchitectureConfig(max_seq_len=8192)
        self.train_cfg = TrainingConfig()
        self.train_cfg.target_tokens = int(os.getenv("NOVA_TRAIN_TOKENS", "0") or 0)
        self.train_cfg.infinite_streaming = os.getenv("NOVA_INFINITE_TRAINING", "1") == "1"
        self.trainer = NovaTrainerPipeline(self.model_cfg, self.train_cfg)
        self.packer = ContextPacker(context_window=self.model_cfg.max_seq_len, overlap=512)
        self.evaluator = Evaluator()

    def _prepare_document(self, raw_doc: RawDocument) -> Tuple[str, Optional[RawDocument]]:
        cleaned = self.cleaner.clean(raw_doc)
        if cleaned is None:
            return ("clean", None)
        if cleaned.license in ("", "unknown"):
            cleaned.license = self.license_detector.detect(cleaned.text, cleaned.metadata)
        tox_score = self.toxicity.score(cleaned.text)
        if tox_score >= self.toxicity.threshold:
            self.rlhf.add_sft(self.safety.build_refusal(cleaned.text[:1000]))
            return ("toxic", None)
        cleaned.text = self.safety.redact_pii(cleaned.text)
        cleaned.token_count = estimate_tokens(cleaned.text)
        score = self.scorer.score(cleaned)
        qual_score = self.quality_model.score(cleaned.text)
        if qual_score is not None:
            score = 0.6 * score + 0.4 * qual_score
        if score < 0.45:
            return ("quality", None)
        return ("ok", cleaned)

    def execute_pipeline(self, version: str = "v2.1") -> Dict:
        results = super().execute_pipeline(version=version)
        if results.get("tokenizer") and isinstance(results["tokenizer"], dict):
            samples = []
            retrieval_path = results.get("retrieval_manifest", "")
            if retrieval_path and Path(retrieval_path).exists():
                with open(retrieval_path, encoding="utf-8") as handle:
                    for _, line in zip(range(self.tokenizer_cfg.benchmark_samples), handle):
                        try:
                            samples.append(json.loads(line).get("text", ""))
                        except Exception:
                            continue
            benchmark = TokenizerBenchmark(self.token_counter).evaluate(samples)
            results["tokenizer"]["benchmark"] = benchmark
        return results


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    orchestrator = NovaTitanOrchestratorV3(target_tokens=5_000_000_000)
    results = orchestrator.execute_pipeline(version="v2.1")
    print(json.dumps(results, indent=2, default=str, ensure_ascii=False))

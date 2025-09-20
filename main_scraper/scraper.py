import os
import json
import re
import subprocess
import scrapy
from scrapy.crawler import CrawlerProcess
import httpx
from pathlib import Path
from urllib.parse import urljoin, urlparse

# =================== knobs ===================
# Depth counting: seed pages start at depth 0
MAX_DEPTH = 2                  # root (0) + 2 hops; set 1 if you want just one hop
MAX_FANOUT_INDEX = 2           # max links scheduled from an index-like page
EXTRA_LINKS_ON_FOLLOW = 1      # when GPT picks a link, also schedule up to this many extras
LINKS_FOR_GPT = 30             # links shown to GPT (cap tokens)
EXTRACT_LINKS_LIMIT = 80       # how many <a> to scan from the DOM
MAX_PAGES_BUDGET = 350         # emergency stop for huge sites
# =============================================

# -------- paths / config --------
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.json"

def load_config(path: Path = CONFIG_PATH):
    if not path.exists():
        default_config = {
            "gpt_api_key": "your-openai-api-key-here",
            "openai_model": "gpt-4o-mini",
            "output_path": "digest.json"
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default_config, f, indent=2)
        print(f"Created default config file at {path}")
        print("Please update it with your actual OpenAI API key!")
        return default_config
    
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# Load config safely
try:
    CFG = load_config()
    GPT_API_KEY = CFG.get("gpt_api_key")
except Exception as e:
    print(f"Error loading config: {e}")
    CFG = {}
    GPT_API_KEY = None

# Resolve output path relative to base directory
OUTPUT_PATH = (BASE_DIR / CFG.get("output_path", "digest.json")).resolve()

# Simple pipeline path
PIPELINE_PATH = "__main__.CollectPipeline"


# ----------------- helper utils (no content keywords) -----------------

def normalize_netloc(netloc: str) -> str:
    netloc = netloc.split(":")[0].lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc

def same_site(url: str, base_netloc: str) -> bool:
    try:
        n1 = normalize_netloc(urlparse(url).netloc)
        n2 = normalize_netloc(base_netloc)
        if not n1 or not n2:
            return False
        return n1 == n2 or n1.endswith("." + n2) or n2.endswith("." + n1)
    except Exception:
        return False

def extract_links_from_response(response: scrapy.http.Response, limit: int = EXTRACT_LINKS_LIMIT):
    links = []
    try:
        for a in response.css("a"):
            href = a.attrib.get("href")
            if not href:
                continue
            abs_url = response.urljoin(href)
            text = (a.xpath("string(.)").get() or "").strip()
            if not text and not abs_url:
                continue
            links.append({"text": text[:200], "href": abs_url})
            if len(links) >= limit:
                break
    except Exception:
        pass
    return links

def structural_score_link(href: str, text: str, base_url: str) -> int:
    """
    Purely structural (no content keywords):
    - same site
    - not mailto/javascript/anchor
    - prefer deeper paths
    - prefer non-root paths
    - prefer links with more descriptive anchor text
    - slight bonus for .html/.htm
    """
    score = 0

    # same site check happens before calling this (for speed),
    # but we keep a tiny safeguard here
    if not same_site(href, urlparse(base_url).netloc):
        return -999

    l = href.lower()
    if l.startswith("mailto:") or l.startswith("javascript:") or l.startswith("#"):
        return -999

    path = urlparse(href).path or "/"
    # depth bonus
    depth = len([p for p in path.strip("/").split("/") if p])
    if depth >= 1:
        score += 1
    if depth >= 2:
        score += 1
    if depth >= 3:
        score += 1

    # non-root bonus
    if path not in ("/", ""):
        score += 1

    # anchor text length (descriptiveness)
    tlen = len(text or "")
    if tlen >= 15:
        score += 1
    if tlen >= 30:
        score += 1
    if tlen >= 60:
        score += 1

    # extension hint (not a content keyword)
    if path.endswith(".html") or path.endswith(".htm"):
        score += 1

    return score

def filter_candidate_links_broad(links, base_url, max_out=MAX_FANOUT_INDEX):
    """
    Keep likely content links using only structural signals.
    """
    base_netloc = urlparse(base_url).netloc
    scored = []
    seen = set()
    for link in links:
        href = (link.get("href") or "").strip()
        text = (link.get("text") or "").strip()
        if not href or href in seen:
            continue
        seen.add(href)

        if not same_site(href, base_netloc):
            continue

        score = structural_score_link(href, text, base_url)
        if score <= -999:
            continue
        scored.append((score, {"text": text, "href": href}))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:max_out]]

def package_body_and_links_for_gpt(body_text: str, links: list):
    """Give GPT a compact JSON payload it can read reliably."""
    body_snip = body_text[:8000]
    payload = {"body": body_snip, "links": links[:LINKS_FOR_GPT]}
    return json.dumps(payload, ensure_ascii=False)


class ScienceTechSpider(scrapy.Spider):
    name = "sci_tech_spider"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.visited_urls = set()

    def start_requests(self):
        urls_file = BASE_DIR / "web_search.txt"

        if not urls_file.exists():
            self.logger.error(f"No URL list found: {urls_file}")
            return

        with open(urls_file, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]

        if not urls:
            self.logger.warning(f"No URLs found inside {urls_file}")
            return

        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                meta={"breadcrumbs": [url], "depth": 0},
                dont_filter=False
            )

    def handle_error(self, failure):
        url = getattr(failure.request, "url", "unknown")
        self.logger.error(f"Error crawling: {url} -> {failure.getErrorMessage()}")

        rendered = self.puppeteer(url)
        if not rendered:
            self.logger.warning(f"Puppeteer failed, skipping {url}")
            return

        text_content = (rendered.get("body", "") or "").strip()
        if len(text_content) < 100:
            self.logger.warning(f"No usable content for {url}")
            return

        links = rendered.get("links", []) if isinstance(rendered, dict) else []
        content_for_gpt = package_body_and_links_for_gpt(text_content, links)

        breadcrumbs = failure.request.meta.get("breadcrumbs", [url])
        depth = int(failure.request.meta.get("depth", 0))

        next_action = self.gpt_navigation_decision(content_for_gpt, url, breadcrumbs, depth)

        if not next_action or next_action.get("action") == "decide":
            item = self.gpt_sci_tech_relevance_check(text_content, url)
            if item:
                self.logger.info(f"✅ Yielding relevant item (error path): {item.get('url', 'unknown')}")
                yield item
            else:
                candidates = filter_candidate_links_broad(links, url, max_out=MAX_FANOUT_INDEX) if links else []
                for cand in candidates:
                    href = cand.get("href")
                    if href and href not in self.visited_urls and depth < MAX_DEPTH:
                        yield scrapy.Request(
                            url=href,
                            callback=self.parse,
                            errback=self.handle_error,
                            meta={"breadcrumbs": breadcrumbs + [href], "depth": depth + 1},
                            dont_filter=False
                        )
        elif next_action.get("action") == "follow_link":
            next_url = next_action.get("url")
            if next_url and depth < MAX_DEPTH:
                yield scrapy.Request(
                    url=next_url,
                    callback=self.parse,
                    errback=self.handle_error,
                    meta={"breadcrumbs": breadcrumbs + [next_url], "depth": depth + 1},
                    dont_filter=False
                )
            else:
                candidates = filter_candidate_links_broad(links, url, max_out=MAX_FANOUT_INDEX) if links else []
                for cand in candidates:
                    href = cand.get("href")
                    if href and href not in self.visited_urls and depth < MAX_DEPTH:
                        yield scrapy.Request(
                            url=href,
                            callback=self.parse,
                            errback=self.handle_error,
                            meta={"breadcrumbs": breadcrumbs + [href], "depth": depth + 1},
                            dont_filter=False
                        )

    def parse(self, response):
        current_url = response.url

        # Mark visited only when actually parsing the response
        if current_url in self.visited_urls:
            return
        self.visited_urls.add(current_url)

        depth = int(response.meta.get("depth", 0))
        self.logger.info(f"Navigating (depth={depth}) -> {current_url}")

        # Prefer puppeteer render for dynamic sites; fall back to Scrapy body
        rendered = self.puppeteer(current_url)
        if not rendered:
            self.logger.debug(f"Puppeteer failed, using raw HTML for {current_url}")
            text_content = response.text.strip()
            links = extract_links_from_response(response, limit=EXTRACT_LINKS_LIMIT)
        else:
            text_content = (rendered.get("body", "") or "").strip()
            links = rendered.get("links") if isinstance(rendered, dict) and isinstance(rendered.get("links"), list) else extract_links_from_response(response, limit=EXTRACT_LINKS_LIMIT)

        if len(text_content) < 100:
            self.logger.warning(f"Insufficient content for {current_url}")
            return

        breadcrumbs = response.meta.get("breadcrumbs", [current_url])

        # Always provide GPT a compact {body, links} payload so it can choose
        content_for_gpt = package_body_and_links_for_gpt(text_content, links)

        # 1) Ask GPT whether to decide here or follow a link (but keep deterministic fallback)
        next_action = self.gpt_navigation_decision(content_for_gpt, current_url, breadcrumbs, depth)

        # 2) If GPT says "decide", run relevance; else follow link(s)
        if not next_action or next_action.get("action") == "decide":
            item = self.gpt_sci_tech_relevance_check(text_content, current_url)
            if item:
                self.logger.info(f"✅ Yielding relevant item from: {current_url}")
                yield item
            else:
                if depth < MAX_DEPTH:
                    candidates = filter_candidate_links_broad(links, current_url, max_out=MAX_FANOUT_INDEX)
                    for cand in candidates:
                        href = cand.get("href")
                        if href and href not in self.visited_urls:
                            yield scrapy.Request(
                                url=href,
                                callback=self.parse,
                                errback=self.handle_error,
                                meta={"breadcrumbs": breadcrumbs + [href], "depth": depth + 1},
                                dont_filter=False
                            )
        elif next_action.get("action") == "follow_link":
            picked = next_action.get("url")
            scheduled = 0
            if picked and picked not in self.visited_urls and depth < MAX_DEPTH:
                scheduled += 1
                yield scrapy.Request(
                    url=picked,
                    callback=self.parse,
                    errback=self.handle_error,
                    meta={"breadcrumbs": breadcrumbs + [picked], "depth": depth + 1},
                    dont_filter=False
                )
            if depth < MAX_DEPTH and EXTRA_LINKS_ON_FOLLOW > 0:
                extras = filter_candidate_links_broad(links, current_url, max_out=EXTRA_LINKS_ON_FOLLOW + 1)
                for cand in extras:
                    href = cand.get("href")
                    if href and href != picked and href not in self.visited_urls and scheduled < (1 + EXTRA_LINKS_ON_FOLLOW):
                        scheduled += 1
                        yield scrapy.Request(
                            url=href,
                            callback=self.parse,
                            errback=self.handle_error,
                            meta={"breadcrumbs": breadcrumbs + [href], "depth": depth + 1},
                            dont_filter=False
                        )
        else:
            self.logger.warning(f"Unknown action from GPT: {next_action.get('action')}")

    def puppeteer(self, url: str):
        node_bin = os.environ.get("NODE_BIN", "node")
        # Look for puppeteer.js in the same directory as this script, else base dir
        script_path = Path(__file__).resolve().parent / "puppeteer.js"
        if not script_path.exists():
            script_path = BASE_DIR / "puppeteer.js"

        if not script_path.exists():
            self.logger.error(f"Puppeteer script not found at {script_path}")
            return None

        try:
            result = subprocess.run(
                [node_bin, str(script_path), url],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=90
            )
            if result.returncode == 0 and result.stdout.strip():
                # Expecting JSON like {"body": "...", "links":[{"text":"...","href":"..."}]}
                return json.loads(result.stdout)
            else:
                self.logger.error(
                    f"Puppeteer error for {url} (rc={result.returncode}):\n"
                    f"STDOUT: {result.stdout[:500]}\nSTDERR: {result.stderr[:500]}"
                )
                return None
        except FileNotFoundError:
            self.logger.error(f"Node not found at {node_bin}. Set NODE_BIN or install Node.")
        except Exception as e:
            self.logger.error(f"Puppeteer call failed for {url}: {e}")
        return None

    def safe_parse_gpt_json(self, text, url):
        """Safely extract and parse JSON from GPT response."""
        text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text.strip(), flags=re.MULTILINE)
        text = text.strip()
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if not match:
            self.logger.error(f"No JSON object found in GPT response for {url}: {text[:500]}")
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode failed for {url}: {e} — Raw: {match.group(0)[:200]}")
            return None

    def gpt_navigation_decision(self, content: str, current_url: str, breadcrumbs: list, depth: int = 0):
        """content is a JSON string: {'body': '...', 'links': [{'text','href'}, ...]}"""
        if not GPT_API_KEY:
            self.logger.error("Missing GPT API key")
            return None

        if depth >= MAX_DEPTH:
            return {"action": "decide", "url": current_url, "breadcrumbs": breadcrumbs}

        try:
            data = json.loads(content)
            body = data.get("body", "")
            links = data.get("links", []) or []
            link_text = "\nLinks:\n" + "\n".join(
                [f"- [{(lk.get('text') or '')[:80]}]({lk.get('href', '')})" for lk in links[:LINKS_FOR_GPT]]
            )
            content_for_model = f"{body}\n{link_text}"
        except Exception:
            content_for_model = content

        system_prompt = (
            "You help a crawler move from index pages to story pages about science and technology.\n"
            "Return ONLY JSON with keys:\n"
            '  action: "decide" | "follow_link"\n'
            '  url: absolute URL if action == "follow_link" (must be one of the provided links)\n'
            "  reason: brief note\n"
            "  breadcrumbs: echo the breadcrumbs you were given\n\n"
            "Pick 'decide' if the CURRENT page looks like a specific article/story (not a generic landing page).\n"
            "Pick 'follow_link' if the page looks like an index and any link appears to lead to a specific article/story.\n"
            "Be permissive and pragmatic. Do not invent links."
        )

        user_content = f"Current URL: {current_url}\n\nPage Content & Links:\n{content_for_model[:9000]}"
        self.logger.debug(f"GPT Navigation Prompt (url={current_url}):\n{user_content[:1000]}")

        payload = {
            "model": CFG.get("openai_model", "gpt-4o-mini"),
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.2,
            "max_tokens": 350
        }

        try:
            r = httpx.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {GPT_API_KEY}"},
                json=payload,
                timeout=60
            )
            r.raise_for_status()
            completion = r.json()["choices"][0]["message"]["content"].strip()
            self.logger.debug(f"GPT Navigation Response (url={current_url}):\n{completion}")

            parsed = self.safe_parse_gpt_json(completion, current_url)
            if not parsed:
                return None

            action = parsed.get("action")
            if action == "follow_link":
                link_url = parsed.get("url")
                if not link_url:
                    return None
                if not urlparse(link_url).scheme:
                    parsed["url"] = urljoin(current_url, link_url)
                else:
                    parsed["url"] = link_url
                prev_crumbs = parsed.get("breadcrumbs") or breadcrumbs
                parsed["breadcrumbs"] = prev_crumbs + [parsed["url"]]
                parsed["depth"] = len(parsed["breadcrumbs"])
            else:
                parsed["breadcrumbs"] = breadcrumbs

            return parsed

        except Exception as e:
            self.logger.error(f"GPT navigation failed for {current_url}: {e}")
            return None

    def gpt_sci_tech_relevance_check(self, content: str, url: str):
        if not GPT_API_KEY:
            self.logger.error("Missing gpt_api_key in config.json")
            return None

        # Only skip obvious non-content scaffolding
        scaffolding_terms = [
            "privacy policy", "terms of use", "about us", "contact",
            "advertise with us", "copyright", "login", "sign up",
            "cookie policy", "accessibility statement"
        ]
        if any(term in (content or "").lower() for term in scaffolding_terms):
            self.logger.debug(f"Skipping scaffolding page: {url}")
            return None

        system_prompt = (
            "You are an inclusive curator of science & technology content.\n"
            "Keep anything that is about science or technology—broadly defined. When unsure, mark it as relevant.\n\n"
            "Return ONLY JSON with:\n"
            '  "is_relevant": bool,\n'
            '  "summary": 3–4 clear sentences,\n'
            '  "url": string\n\n'
            "Reject only obvious non-content pages (admin/login/privacy/terms), empty stubs, or pure navigation pages with no article."
        )

        self.logger.debug(f"GPT Relevance Prompt (url={url}):\n{content[:1000]}")

        payload = {
            "model": CFG.get("openai_model", "gpt-4o-mini"),
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content[:9000]}
            ],
            "temperature": 0.2,
            "max_tokens": 500
        }

        try:
            r = httpx.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {GPT_API_KEY}"},
                json=payload,
                timeout=60
            )
            r.raise_for_status()
            output = r.json()["choices"][0]["message"]["content"].strip()
            self.logger.debug(f"GPT Relevance Response (url={url}):\n{output}")
            
            parsed = self.safe_parse_gpt_json(output, url)
            if not parsed:
                return None

            if not isinstance(parsed, dict):
                self.logger.error(f"GPT returned non-dict for {url}: {parsed}")
                return None

            if "is_relevant" not in parsed:
                self.logger.error(f"GPT response missing 'is_relevant' for {url}: {parsed}")
                return None

            if parsed.get("is_relevant"):
                parsed["url"] = url
                self.logger.info(f"✅ Relevant: {url}")
                return parsed
            else:
                self.logger.info(f"Not relevant per GPT: {url} — {parsed.get('summary','')[:120]}")
                return None

        except Exception as e:
            self.logger.error(f"GPT evaluation failed for {url}: {e}")
            return None


class CollectPipeline:
    def __init__(self):
        self.results = []

    def process_item(self, item, spider):
        self.results.append(item)
        return item

    def close_spider(self, spider):
        # Debug: all relevant items we found
        debug_path = BASE_DIR / "debug_all_results.json"
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2)

        # Final: only relevant science/tech updates
        relevant_entries = [i for i in self.results if i.get("is_relevant")]
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(relevant_entries, f, indent=2)

        spider.logger.info(f"Wrote {len(relevant_entries)} relevant entries to {OUTPUT_PATH}")


# ---------- runner helpers ----------
SCRAPY_SETTINGS = {
    "LOG_LEVEL": "DEBUG",
    "ITEM_PIPELINES": {PIPELINE_PATH: 1},
    "DOWNLOAD_TIMEOUT": 45,
    "RETRY_ENABLED": True,
    "RETRY_TIMES": 2,
    "COOKIES_ENABLED": False,
    "HTTPERROR_ALLOW_ALL": True,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    # Crawl guardrails
    "DEPTH_LIMIT": MAX_DEPTH,
    "CLOSESPIDER_PAGECOUNT": MAX_PAGES_BUDGET,
}

def run_spider():
    process = CrawlerProcess(settings=SCRAPY_SETTINGS)
    process.crawl(ScienceTechSpider)
    process.start()

def main():
    if not GPT_API_KEY or GPT_API_KEY == "your-openai-api-key-here":
        print("❌ ERROR: Invalid or missing GPT_API_KEY in config.json")
        return
    run_spider()
    return str(OUTPUT_PATH)

if __name__ == "__main__":
    main()

// puppeteer.js — hardened version for science news scraping
const puppeteer = require('puppeteer');
// Optional: install with `npm install puppeteer-extra puppeteer-extra-plugin-stealth`
// const puppeteer = require('puppeteer-extra');
// const stealth = require('puppeteer-extra-plugin-stealth');
// puppeteer.use(stealth());

async function withTimeout(promise, ms) {
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('Operation timed out')), ms)
  );
  return Promise.race([promise, timeout]);
}

async function autoScroll(page, maxScrolls = 15, scrollDelay = 1200) {
  for (let i = 0; i < maxScrolls; i++) {
    const prevHeight = await page.evaluate('document.body.scrollHeight');
    await page.evaluate(() => window.scrollBy(0, window.innerHeight * 0.7));
    await new Promise(r => setTimeout(r, scrollDelay));
    const newHeight = await page.evaluate('document.body.scrollHeight');
    if (newHeight === prevHeight) break;
  }
}

async function main() {
  const url = process.argv[2];
  if (!url) {
    console.error('Usage: node puppeteer.js <url>');
    process.exit(1);
  }

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: true, // Use 'true' for stability on servers
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage', // Critical for low-memory environments
        '--disable-gpu',
        '--single-process', // Save memory
        '--disable-web-security',
        '--allow-running-insecure-content',
        '--disable-features=TranslateUI',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-blink-features=AutomationControlled'
      ],
      defaultViewport: { width: 1280, height: 800 },
      timeout: 60000
    });

    const page = await browser.newPage();

    // Realistic user agent
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    );

    // Anti-detection
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
      window.chrome = { runtime: {} };
      navigator.permissions = {
        query: async () => ({ state: 'granted' }),
      };
    });

    // Go to page with flexible wait
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 55000 }).catch(() => {});

    // Wait for body to exist
    try {
      await page.waitForSelector('body', { visible: true, timeout: 15000 });
    } catch (err) {
      console.warn(`[WARN] Body not loaded quickly for ${url}`);
    }

    // Auto-scroll with timeout protection
    try {
      await withTimeout(autoScroll(page, 10, 1000), 25000); // Max 25s scrolling
    } catch (err) {
      console.warn(`[WARN] Scroll timed out for ${url}`);
    }

    // Extract structured data
    const data = await page.evaluate(() => {
      const getText = () => {
        if (!document.body) return '';
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        const parts = [];
        let node;
        while ((node = walker.nextNode())) {
          const t = (node.nodeValue || '').trim();
          if (t) parts.push(t);
        }
        return parts.join(' ').replace(/\s+/g, ' ').trim().slice(0, 50000);
      };

      const getLinks = () => {
        const links = Array.from(document.querySelectorAll('a[href]'))
          .map(a => {
            const href = a.getAttribute('href');
            if (!href) return null;
            try {
              const absUrl = new URL(href, location.href).href;
              const text = a.textContent.trim().replace(/\s+/g, ' ').substring(0, 200);
              return { href: absUrl, text };
            } catch {
              return null;
            }
          })
          .filter(link => link && link.text && link.href.startsWith('http'))
          .slice(0, 100);
        return links.length > 0 ? links : null;
      };

      return {
        title: document.title?.trim() || '',
        url: location.href,
        body: getText(),
        links: getLinks()
      };
    });

    // Ensure minimal content length before sending
    if (!data.body || data.body.length < 100) {
      console.error('[ERROR] No usable content extracted');
      process.exit(3);
    }

    process.stdout.write(JSON.stringify(data));
    await browser.close();
    process.exit(0);

  } catch (err) {
    console.error(`[PUPPETEER ERROR] ${err.message || err.toString()}`);
    if (browser) {
      try {
        await browser.close();
      } catch {}
    }
    process.exit(2);
  }
}

main();
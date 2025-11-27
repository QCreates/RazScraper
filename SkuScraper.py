import asyncio
import json
import pandas as pd
from playwright.async_api import async_playwright
import requests
import traceback

BASE_URL = "https://www.razimports.com/in-stock/ready-to-ship?p={page}&product_list_limit=36"
NUM_TABS = 12

# GLOBAL STOP FLAG
stop_event = asyncio.Event()


# ---------------------------------------------
# Extract IDs via JavaScript inside real browser
# ---------------------------------------------
async def extract_ids_js(page):
    try:
        data = await page.evaluate(
            """() => {
                if (!window.staticImpressions) return null;
                return window.staticImpressions['category.products.list'] || null;
            }"""
        )
        if not data:
            return []
        return [item["id"] for item in data]
    except:
        return []


# ---------------------------------------------
# Auto-save results to partial Excel
# ---------------------------------------------
def save_partial_results(results):
    try:
        all_ids = []
        for p, ids in results.items():
            if isinstance(ids, list) and ids and ids != ["REPEAT"]:
                all_ids.extend(ids)

        all_ids = sorted(set(all_ids))

        df = pd.DataFrame({"sku": all_ids})
        df.to_excel("raz_skus.xlsx", index=False)

        print(f"[AUTO-SAVE] {len(all_ids)} SKUs saved to raz_skus.xlsx")
    except:
        print("[ERROR] Could not save partial data")
        traceback.print_exc()


# ---------------------------------------------
# Worker: each tab loads assigned pages
# ---------------------------------------------
async def tab_worker(name, page, page_queue, results, first_sku):
    while True:

        # If another worker already triggered a stop → exit immediately
        if stop_event.is_set():
            return

        page_num = await page_queue.get()

        # Shutdown signal
        if page_num is None:
            page_queue.task_done()
            return

        # Stop if global stop occurred while waiting
        if stop_event.is_set():
            page_queue.task_done()
            return

        url = BASE_URL.format(page=page_num)
        print(f"[Tab {name}] Loading page {page_num}: {url}")

        try:
            await page.goto(url, timeout=60000, wait_until="networkidle")
        except:
            try:
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            except:
                print(f"[Tab {name}] HARD FAIL {page_num}")
                results[page_num] = []
                page_queue.task_done()
                continue

        await asyncio.sleep(0.3)

        ids = await extract_ids_js(page)

        # No products on page
        if not ids:
            print(f"[Tab {name}] No products on page {page_num}")
            results[page_num] = []
            page_queue.task_done()
            continue

        # ---- REPEAT DETECTED ----
        if ids[0] == first_sku and page_num != 1:
            print(f"[Tab {name}] REPEAT DETECTED on page {page_num} → GLOBAL STOP")

            results[page_num] = ["REPEAT"]

            # Trigger global stop
            stop_event.set()

            # Empty queue instantly
            while not page_queue.empty():
                try:
                    page_queue.get_nowait()
                    page_queue.task_done()
                except:
                    break

            return

        # Normal success
        print(f"[Tab {name}] Found {len(ids)} SKUs on page {page_num}")
        results[page_num] = ids

        save_partial_results(results)

        page_queue.task_done()


# ---------------------------------------------
# MAIN SCRAPER
# ---------------------------------------------
async def main():
    # Connect to existing Chrome
    ws_url = "http://localhost:9222/json/version"
    data = requests.get(ws_url).json()
    websocket = data["webSocketDebuggerUrl"]

    playwright = await async_playwright().start()
    browser = await playwright.chromium.connect_over_cdp(websocket)
    context = browser.contexts[0]

    # ---------------------------------------------
    # STEP 1 — Load Page 1 Sequentially
    # ---------------------------------------------
    print("Loading page 1 (single-thread)…")
    first_page = await context.new_page()

    try:
        await first_page.goto(BASE_URL.format(page=1), timeout=60000, wait_until="networkidle")
    except:
        await first_page.goto(BASE_URL.format(page=1), timeout=60000, wait_until="domcontentloaded")

    await asyncio.sleep(1)

    first_page_ids = await extract_ids_js(first_page)
    if not first_page_ids:
        print("ERROR: Could not extract SKUs from page 1.")
        await browser.close()
        await playwright.stop()
        return

    first_sku = first_page_ids[0]
    print(f"FIRST PAGE FIRST SKU = {first_sku}")

    results = {1: first_page_ids}

    save_partial_results(results)

    await first_page.close()

    # ---------------------------------------------
    # STEP 2 — Start Multi-Thread Scraping (12 tabs)
    # ---------------------------------------------
    tabs = []
    for i in range(NUM_TABS):
        p = await context.new_page()
        tabs.append(p)

    print(f"Created {NUM_TABS} tabs")

    page_queue = asyncio.Queue()

    # Load pages 2..999 into the queue
    for page_num in range(2, 1000):
        await page_queue.put(page_num)

    workers = []
    for i in range(NUM_TABS):
        w = asyncio.create_task(tab_worker(i + 1, tabs[i], page_queue, results, first_sku))
        workers.append(w)

    # Wait until stop_event OR queue exhaustion
    while not stop_event.is_set() and not page_queue.empty():
        await asyncio.sleep(0.05)

    # Trigger shutdown
    stop_event.set()

    # Empty queue entirely
    while not page_queue.empty():
        try:
            page_queue.get_nowait()
            page_queue.task_done()
        except:
            break

    # Send shutdown signals to all workers
    for _ in range(NUM_TABS):
        await page_queue.put(None)

    await asyncio.gather(*workers, return_exceptions=True)

    # Close all tabs
    for p in tabs:
        await p.close()

    # ---------------------------------------------
    # FINAL RESULT COMPILATION
    # ---------------------------------------------
    all_ids = []
    for page_num in sorted(results.keys()):
        ids = results[page_num]
        if ids == ["REPEAT"]:
            break
        all_ids.extend(ids)

    all_ids = sorted(set(all_ids))

    print("\n---------------------------------------")
    print("SCRAPING COMPLETE")
    print("---------------------------------------")
    print(f"Total unique SKUs: {len(all_ids)}")

    await browser.close()
    await playwright.stop()


if __name__ == "__main__":
    asyncio.run(main())

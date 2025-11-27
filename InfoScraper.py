import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import traceback
import sys

PRODUCT_URL = "https://www.razimports.com/{sku}"
QTY_URL = "https://www.razimports.com/catalog/product/getActualStatus/"
NUM_TABS = 15

RAZ_COOKIES = {
    "STUID": "6287ff8d-b62c-0b0b-3501-47b7c229a041",
    "STVID": "fb100d90-3914-ef2b-a2a7-f0ed2baabfc2",
    "form_key": "k4lI2yKMLx3sikXJ",
    "mage-banners-cache-storage": "{}",
    "mage-messages": "",
    "amcookie_policy_restriction": "allowed",
    "PHPSESSID": "640bf5dfea61986fae1a0b654152cad1",
    "X-Magento-Vary": "623a50248f9c3bf0d42166a2bdf3c087c5c72df9108fa79872386691473df46d",
    "mage-cache-sessid": "true",
    "mage-cache-storage": "{}",
    "mage-cache-storage-section-invalidation": "{}",
    "product_data_storage": "{}",
    "private_content_version": "2235ee4bc0ac734bba930f64c82b35a0",
    "section_data_ids": "{%22customer%22:1764137004%2C%22compare-products%22:1764137004%2C%22last-ordered-items%22:1764137004%2C%22requisition%22:1764137004%2C%22cart%22:1764137023%2C%22directory-data%22:1764137004%2C%22captcha%22:1764137004%2C%22wishlist%22:1764137004%2C%22company%22:1764137004%2C%22company_authorization%22:1764137004%2C%22instant-purchase%22:1764137004%2C%22loggedAsCustomer%22:1764137004%2C%22multiplewishlist%22:1764137004%2C%22persistent%22:1764137004%2C%22account-manager-portal-link%22:1764137004%2C%22recently_viewed_product%22:1764137004%2C%22recently_compared_product%22:1764137004%2C%22product_data_storage%22:1764137004}"
}


async def fetch_qty_available(sku, context):
    cookies = await context.cookies()
    raz_cookies = [c for c in cookies if "razimports" in c["domain"]]
    cookie_dict = {c["name"]: c["value"] for c in raz_cookies}
    form_key = cookie_dict.get("form_key", "")

    headers = {
        "accept": "*/*",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://www.razimports.com",
        "referer": f"https://www.razimports.com/{sku}",
        "x-requested-with": "XMLHttpRequest",
        "user-agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }

    payload = f"sku={sku}&form_key={form_key}"

    try:
        resp = await context.request.post(QTY_URL, data=payload, headers=headers)
        if resp.ok:
            data = await resp.json()
            return data.get("actualStatus")
    except:
        return None

    return None

def extract_prices(soup):
    minimum_order = soup.find("select", class_="mp-better-qty-input")
    qty_txt = None
    if(minimum_order):
        qty_text = minimum_order.get_text(strip=True)
        if("Buy " in qty_text):
            qty_txt = qty_text.split("Buy ")[1].split(" for")[0]
            print(qty_txt)

    price_box = soup.find("div", class_="price-box")
    if not price_box:
        return {
            "min_order_qty": None,
            "min_case_price": None,
            "min_case_qty": None,
            "case_price": None,
            "case_qty": None
        }

    # Get ALL price-container blocks IN ORDER
    blocks = price_box.find_all("span", class_="price-container")

    # Storage
    sale_price = sale_qty = None
    min_case_price = min_case_qty = None
    case_price = case_qty = None

    # Helper: extract price + qty from a block
    def parse_block(block):
        price_tag = (
            block.find("span", class_="price-wrapper") or
            block.find("span", class_="price")
        )
        qty_tag = (
            block.find("span", class_="price-label") or
            block.find("span", class_="origin-price-label")
        )
        price = price_tag.get_text(strip=True).replace("$", "") if price_tag else None
        qty = qty_tag.get_text(strip=True) if qty_tag else None
        return price, qty
    
    _, qty = parse_block(blocks[0])
    if ("Min" in qty):
        min_case_price, min_case_qty = parse_block(blocks[0])
        case_price, case_qty = parse_block(blocks[1])
    else:    
        case_price, case_qty = parse_block(blocks[0])
        min_case_price, min_case_qty = parse_block(blocks[1])
        
    # Return data
    return {
        "min_order_qty": qty_txt,
        "min_case_price": min_case_price,
        "min_case_qty": min_case_qty,
        "case_price": case_price,
        "case_qty": case_qty
    }


# ---------------------------------------------------------
# SCRAPE PRODUCT (with new bullet merging rules)
# ---------------------------------------------------------
async def scrape_product(page, sku):
    url = PRODUCT_URL.format(sku=sku)
    print(f"[SCRAPING] {sku} → {url}")

    try:
        await page.goto(url, timeout=60000, wait_until="networkidle")
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        title_tag = soup.find("title")
        name = title_tag.get_text(strip=True) if title_tag else None

        price_box = soup.find("div", attrs={"data-role": "priceBox"})
        product_id = price_box["data-product-id"] if price_box else None

        upc = None
        upc_tag = soup.find("div", class_="product attribute upc-code")
        if upc_tag:
            val = upc_tag.find("div", class_="value")
            if val:
                upc = val.get_text(strip=True)

        # ------------------------------------
        # CONTENT SECTION (REAL CONTENT VALUE)
        # ------------------------------------
        content_val = None
        content_block = soup.find("div", class_="product attribute content")
        if content_block:
            val = content_block.find("div", class_="value")
            if val:
                content_val = val.get_text(strip=True)

        # -------------------------------
        # MERGED BULLETS PROCESSING
        # -------------------------------
        bullets = []

        block1 = soup.find("div", "attribute raz-product-bullet-attr")
        if block1:
            bullets.extend([li.get_text(strip=True) for li in block1.find_all("li")])

        block2 = soup.find("ul", "raz-product-bullet-attr-extra")
        if block2:
            bullets.extend([li.get_text(strip=True) for li in block2.find_all("li")])

        seen = set()
        cleaned = []
        for b in bullets:
            if b not in seen:
                seen.add(b)
                cleaned.append(b)

        material = None
        dimensions = None
        remaining = []

        for b in cleaned:
            if b.startswith("Made of "):
                material = b
            elif b.startswith("Measures: "):
                dimensions = b
            else:
                remaining.append(b)

        color = None
        if remaining:
            color = remaining[0]
            remaining = remaining[1:]

        image_url = (
            f"https://www.razimports.com/media/catalog_product/ImagesFiles2/{sku}.jpg"
            "?quality=100&bg-color=255,255,255&fit=bounds&height=1000&width=1000&canvas=1000:1000"
        )

        qty_available = await fetch_qty_available(sku, page.context)

        price_data = extract_prices(soup)

        row = {
            "sku": sku,
            "name": name,
            "upc": upc,
            "product_id": product_id,
            "qty_available": qty_available,
            "image_url": image_url,
            "material": material,
            "dimensions": dimensions,
            "color": color,
        }

        row.update(price_data)

        for i, b in enumerate(remaining):
            row[f"bullet_{i+1}"] = b

        return row

    except:
        print(f"[ERROR] Failed scraping {sku}")
        return {
            "sku": sku,
            "name": "ERROR",
            "upc": None,
            "product_id": None,
            "qty_available": None,
            "image_url": None,
            "material": None,
            "dimensions": None,
            "color": None,
            "case_price": None,
            "case_qty": None,
            "original_price_min_each": None,
            "original_qty_min": None,
            "original_price_case": None,
            "original_qty_case": None
        }


# ---------------------------------------------------------
# WORKER
# ---------------------------------------------------------
async def worker(tab_id, page, queue, out_list):
    while True:
        sku = await queue.get()
        if sku is None:
            queue.task_done()
            return

        result = await scrape_product(page, sku)
        out_list.append(result)

        queue.task_done()


# ---------------------------------------------------------
# MAIN MULTITHREADED SCRAPER
# ---------------------------------------------------------
async def main():
    df = pd.read_excel("raz_skus.xlsx")
    skus = [str(s) for s in df["sku"].tolist()]

    print(f"Total SKUs: {len(skus)}")

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        context = browser.contexts[0]

        for name, value in RAZ_COOKIES.items():
            await context.add_cookies([{
                "name": name,
                "value": value,
                "domain": "www.razimports.com",
                "path": "/",
                "httpOnly": False,
                "secure": True,
                "sameSite": "Lax"
            }])

        pages = [await context.new_page() for _ in range(NUM_TABS)]

        queue = asyncio.Queue()
        out_list = []

        for sku in skus:
            await queue.put(sku)

        tasks = []
        for i, p in enumerate(pages):
            t = asyncio.create_task(worker(i + 1, p, queue, out_list))
            tasks.append(t)

        for _ in range(NUM_TABS):
            await queue.put(None)

        await queue.join()
        await asyncio.gather(*tasks)

        df_out = pd.DataFrame(out_list)
        df_out.to_excel("raz_product_info.xlsx", index=False)

        print("\n-------------------------------")
        print("SCRAPING COMPLETE")
        print("-------------------------------")
        print("Saved → raz_product_info.xlsx")


if __name__ == "__main__":
    asyncio.run(main())

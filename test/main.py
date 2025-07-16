import asyncio

from cloudscraper.interpreters import interpreters
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import cloudscraper
import requests

url = "https://www.fastpeoplesearch.com/"

async def main() -> None:
    async with async_playwright() as playwright:
        async with await playwright.chromium.launch(
            channel='chrome',
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
            ]
        ) as browser: # type : Browser
            context: BrowserContext = await browser.new_context()

            page: Page = await context.new_page()
            await page.goto(url)

            scraper = cloudscraper.create_scraper(
                interpreters="nodejs",
                delay=10,
                browser={
                    "browser": 'chrome',
                    "platform": 'windows',
                    "desktop": False,
                },

            )
            responses = scraper.get(url)
            print(responses.status_code)

            # done Capcha

            await page.wait_for_timeout(30000)

            await page.get_by_role('button', name='I Agree').click()

            with open('../final_results/xxx.txt', 'r', encoding='utf-8') as f:
                for item in f:
                    print(item)


            await page.locator('#search-name-name').fill('Peters')
            await page.locator('#search-name-address').fill('Peters')

            await page.get_by_role('button', name='Free Search').click()
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
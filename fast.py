from __future__ import annotations
import os, asyncio, time, logging, re
from typing import Any, Tuple
from dotenv import load_dotenv
from gologin import GoLogin
from urllib.parse import urlparse
import requests
from playwright.async_api import async_playwright, Page, Browser

# ---------- Config ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, 'results'); os.makedirs(RESULTS_DIR, exist_ok=True)
LOG_PATH = os.path.join(RESULTS_DIR, 'fastps_patchright_gologin.log')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler(LOG_PATH, encoding='utf-8'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

load_dotenv()
# Токен можна перезаписати змінною середовища GLOGIN_TOKEN, інакше береться цей за замовчуванням
DEFAULT_GLOGIN_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2ODcxYTdjNjdlY2Y3NmE1ODkwZTg1ZTIiLCJ0eXBlIjoiZGV2Iiwiand0aWQiOiI2ODcxYTg0MWVjOTQyZWYzMTY4MTRlZjQifQ.jKv0hx7Vcd9A4-9ausX2tuQpZCcTpRx5FMGZyf95I_w"
GLOGIN_TOKEN = os.getenv('GLOGIN_TOKEN', DEFAULT_GLOGIN_TOKEN)
if not GLOGIN_TOKEN:
    logger.error('GLOGIN_TOKEN is empty'); exit(1)

PROXY_FILE = os.path.join(BASE_DIR, 'proxies.txt')
GOLOGIN_TMP = os.path.join(BASE_DIR, 'gologin_tmp'); os.makedirs(GOLOGIN_TMP, exist_ok=True)
FINAL_RESULTS_DIR = os.path.join(BASE_DIR, 'final_results')

# ---------- Setup proxy ----------
def load_first_proxy() -> str | None:
    if not os.path.exists(PROXY_FILE):
        return None
    with open(PROXY_FILE, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '://' in line:
                return line
            parts = line.split(':')
            if len(parts) == 4:
                h, p, u, pwd = parts
                return f'http://{u}:{pwd}@{h}:{p}'
            if len(parts) == 2:
                h, p = parts
                return f'http://{h}:{p}'
    return None

# --------- Queue builder from final_results ----------
def build_queue() -> list[Tuple[str, str]]:
    tasks: list[Tuple[str, str]] = []
    if not os.path.isdir(FINAL_RESULTS_DIR):
        logger.warning('final_results directory not found');
        return tasks
    # 1) Спочатку переглядаємо вкладені каталоги (штати)
    for entry in os.listdir(FINAL_RESULTS_DIR):
        p = os.path.join(FINAL_RESULTS_DIR, entry)
        targets: list[str] = []
        if os.path.isdir(p):
            # усі .txt файли у підкаталозі
            targets.extend([os.path.join(p, fn) for fn in os.listdir(p) if fn.lower().endswith('.txt')])
        elif entry.lower().endswith('.txt'):
            # .txt файли безпосередньо в FINAL_RESULTS_DIR
            targets.append(p)

        for txt_path in targets:
            addr = ''
            try:
                with open(txt_path, encoding='utf-8') as f:
                    for line in f:
                        ls = line.strip()
                        if ls.startswith('Address:'):
                            addr = ls.split(':', 1)[1].strip()
                        elif ls.startswith(('Owner:', 'Name:')) and addr:
                            name = ls.split(':', 1)[1].strip()
                            if name and addr:
                                tasks.append((name, addr)); addr = ''
            except Exception as e:
                logger.warning(f'Cannot parse {txt_path}: {e}')
    logger.info(f'Queue built: {len(tasks)} records')
    return tasks

# ---------- GoLogin  + Patchright startup ----------
async def start_patchright_with_gologin(proxy_url: str | None) -> Tuple[any, Browser, Page]:
    gl_api = GoLogin({'token': GLOGIN_TOKEN, 'profile_path': GOLOGIN_TMP})
    payload: dict[str, Any] = {
        'name': f'patchright_profile_{int(time.time())}',
        'os': 'win',
        'navigator': {'language': 'en-US', 'userAgent': 'random', 'resolution': 'random'},
        'proxyEnabled': bool(proxy_url),
    }
    if proxy_url:
        pr = urlparse(proxy_url)
        payload['proxy'] = {
            'mode': pr.scheme,
            'host': pr.hostname,
            'port': pr.port,
            'username': pr.username or '',
            'password': pr.password or '',
        }
    profile_id = gl_api.create(payload)
    gl = GoLogin({'token': GLOGIN_TOKEN, 'profile_id': profile_id, 'port': 3500, 'tmpdir': GOLOGIN_TMP})
    ws_endpoint = gl.start()
    if not ws_endpoint:
        raise RuntimeError('GoLogin did not return debuggerAddress')

    # Patchright очікує http://host:port — перетворюємо, якщо потрібно
    if ws_endpoint.startswith('ws://') or ws_endpoint.startswith('wss://'):
        pr = urlparse(ws_endpoint)
        cdp_url = f'http://{pr.hostname}:{pr.port}'
    elif ws_endpoint.startswith('http://') or ws_endpoint.startswith('https://'):
        cdp_url = ws_endpoint
    else:
        cdp_url = f'http://{ws_endpoint}'

    patchright = await async_playwright().start()
    browser = await patchright.chromium.connect_over_cdp(cdp_url)

    # Використовуємо вже створений контекст/сторінку GoLogin
    if browser.contexts:
        ctx = browser.contexts[0]
    else:
        ctx = await browser.new_context()

    # Додаткові анти-детекційні скрипти для Patchright
    await ctx.add_init_script("""
        // Відключаємо webdriver detection
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        
        // Підміняємо chrome runtime
        window.chrome = window.chrome || {};
        window.chrome.runtime = window.chrome.runtime || {};
        
        // Видаляємо automation ознаки
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        
        // Підміняємо plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Підміняємо permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
    """)

    if ctx.pages:
        page = ctx.pages[0]
    else:
        page = await ctx.new_page()
    
    # Додаткові налаштування сторінки для анти-детекції
    await page.set_extra_http_headers({
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
    })
    
    return patchright, browser, page

# ---------- Turnstile helpers ----------
IFRAME_SEL = 'iframe[src*="challenges.cloudflare.com"], iframe[src*="turnstile"]'
SITEKEY_RE = re.compile(r'/(0x[\da-fA-F]{8,})/')

async def click_turnstile_checkbox(page: Page, timeout: int = 10000) -> bool:
    try:
        frame = await (await page.wait_for_selector(IFRAME_SEL, timeout=timeout)).content_frame()
        if not frame:
            return False
        cb = await frame.query_selector('input[type="checkbox"]') or \
             await frame.query_selector('label') or \
             await frame.query_selector('span')
        if cb:
            await cb.click()
            await page.wait_for_timeout(1500)
        token = await page.evaluate("document.querySelector('input[name=\'cf-turnstile-response\']')?.value||''")
        return bool(token and len(token) > 30)
    except Exception as e:
        logger.debug(f'click error: {e}')
        return False

# координатний клік у чекбокс (покращений алгоритм для Patchright)
async def brute_click_turnstile(page: Page, loops: int = 15) -> bool:
    """Повертає True, якщо за loops спроб клікнули чекбокс і зʼявився токен."""
    logger.info('🎯 Запускаємо координатний клік по Turnstile (Patchright)...')
    
    for attempt in range(loops):
        await asyncio.sleep(1)
        
        # Шукаємо всі Turnstile iframe'и
        turnstile_frames = []
        for frame in page.frames:
            if 'challenges.cloudflare.com' in frame.url and '/turnstile/' in frame.url:
                turnstile_frames.append(frame)
        
        if not turnstile_frames:
            logger.debug(f'Спроба {attempt + 1}: Turnstile iframe не знайдено')
            continue
            
        for frame in turnstile_frames:
            try:
                # Спочатку чекаємо чи завантажився чекбокс
                logger.debug(f'Перевіряємо готовність iframe {frame.url}')
                title = await frame.evaluate("document.title")
                has_checkbox = await frame.evaluate("""
                    document.querySelector('input[type="checkbox"], label, span[role="checkbox"], .cb-c') !== null
                """)
                
                if "Checking" in title or not has_checkbox:
                    logger.debug(f'iframe ще завантажується: title="{title}", has_checkbox={has_checkbox}')
                    continue
                
                elem = await frame.frame_element()
                bb = await elem.bounding_box()
                if not bb:
                    continue
                
                # Пробуємо різні позиції для кліку (оптимізовано для Turnstile)
                positions = [
                    (bb['x'] + 15, bb['y'] + bb['height'] * 0.5),                # Крайня ліва позиція
                    (bb['x'] + bb['width'] * 0.08, bb['y'] + bb['height'] * 0.5), # 8% ширини (чекбокс)
                    (bb['x'] + bb['width'] * 0.12, bb['y'] + bb['height'] * 0.5), # 12% ширини
                    (bb['x'] + bb['width'] * 0.15, bb['y'] + bb['height'] * 0.5), # 15% ширини
                    (bb['x'] + 25, bb['y'] + bb['height'] * 0.4),                # Трохи вище
                    (bb['x'] + 25, bb['y'] + bb['height'] * 0.6),                # Трохи нижче
                ]
                
                for x, y in positions:
                    logger.debug(f'Спроба {attempt + 1}: клік в ({x:.1f}, {y:.1f})')
                    # Використовуємо більш природний клік з рандомними затримками
                    await page.mouse.move(x, y)
                    await page.wait_for_timeout(100 + int(time.time() * 1000) % 200)  # рандомна затримка
                    await page.mouse.click(x=x, y=y)
                    await page.wait_for_timeout(2000)
                    
                    # Перевіряємо токен після кожного кліку
                    token = await page.evaluate("document.querySelector('input[name=\"cf-turnstile-response\"]')?.value||''")
                    logger.debug(f'Токен після кліку: {token[:50] if token else "пустий"}...')
                    if token and len(token) > 30:
                        logger.info('🎉 Токен отримано координатним кліком!')
                        return True
                        
            except Exception as e:
                logger.debug(f'Спроба {attempt + 1} помилка: {e}')
    
    logger.warning('❌ Координатний клік не спрацював')
    return False

# ---------- Cloudflare managed challenge wait helper ----------
async def wait_for_cf_challenge(page: Page, max_sec: int = 40) -> bool:
    """Чекає, поки сторінка з класом page-manage-challenge (Just a moment…) зникне.
    Повертає True, якщо за max_sec ми перейшли на сторінку без цього класу."""
    start = time.time()
    while time.time() - start < max_sec:
        try:
            cls = await page.evaluate("document.documentElement.className")
        except Exception:
            cls = ''
        
        # Перевіряємо чи challenge ще активний
        if cls and 'page-manage-challenge' in cls:
            logger.debug(f'⏳ Challenge активний, чекаємо... ({int(time.time() - start)}s)')
            
            # Діагностика: що є на сторінці  
            try:
                elements_info = await page.evaluate("""
                    (() => {
                        const info = {
                            hasTurnstileInput: !!document.querySelector('input[name="cf-turnstile-response"]'),
                            turnstileValue: document.querySelector('input[name="cf-turnstile-response"]')?.value || '',
                            hasMainWrapper: !!document.querySelector('.main-wrapper'),
                            iframes: Array.from(document.querySelectorAll('iframe')).map(iframe => iframe.src)
                        };
                        return info;
                    })()
                """)
                logger.debug(f'📋 Стан сторінки: {elements_info}')
                
                # Якщо токен вже з'явився - challenge пройдено
                if elements_info.get('turnstileValue') and len(elements_info['turnstileValue']) > 30:
                    logger.info('🎉 Challenge автоматично пройдено!')
                    return True
                
                # Шукаємо активні Turnstile iframe'и
                turnstile_found = False
                for frame in page.frames:
                    if 'challenges.cloudflare.com' in frame.url and '/turnstile/' in frame.url:
                        logger.info(f'🔍 Знайшли Turnstile iframe: {frame.url}')
                        turnstile_found = True
                        
                        try:
                            # Спочатку діагностуємо що є в iframe
                            iframe_content = await frame.evaluate("""
                                (() => {
                                    const info = {
                                        html: document.documentElement.outerHTML.substring(0, 1000),
                                        allElements: Array.from(document.querySelectorAll('*')).map(el => ({
                                            tag: el.tagName,
                                            id: el.id,
                                            className: el.className,
                                            text: el.textContent?.substring(0, 50)
                                        })).slice(0, 20),
                                        inputs: Array.from(document.querySelectorAll('input')).map(inp => ({
                                            type: inp.type,
                                            id: inp.id,
                                            className: inp.className
                                        })),
                                        clickableElements: Array.from(document.querySelectorAll('label, button, span, div')).filter(el => 
                                            el.textContent?.includes('human') || 
                                            el.textContent?.includes('verify') ||
                                            el.getAttribute('role') === 'checkbox' ||
                                            el.className?.includes('checkbox') ||
                                            el.className?.includes('cb-')
                                        ).map(el => ({
                                            tag: el.tagName,
                                            text: el.textContent?.substring(0, 50),
                                            className: el.className,
                                            role: el.getAttribute('role')
                                        }))
                                    };
                                    return info;
                                })()
                            """)
                            logger.info(f'🔍 Вміст iframe: {iframe_content}')
                            
                            # Чекаємо поки iframe завантажиться повністю
                            try:
                                # Чекаємо поки зникне "Checking your Browser…" і з'явиться чекбокс
                                logger.info('⏳ Чекаємо завантаження Turnstile чекбоксу...')
                                for wait_attempt in range(20):  # До 20 секунд
                                    title = await frame.evaluate("document.title")
                                    has_checkbox = await frame.evaluate("""
                                        document.querySelector('input[type="checkbox"], label, span[role="checkbox"], .cb-c') !== null
                                    """)
                                    
                                    logger.debug(f'Спроба {wait_attempt + 1}: title="{title}", has_checkbox={has_checkbox}')
                                    
                                    if has_checkbox and "Checking" not in title:
                                        logger.info('✅ Turnstile чекбокс завантажено!')
                                        break
                                    
                                    await page.wait_for_timeout(1000)
                                else:
                                    logger.warning('⚠️ Turnstile чекбокс не з\'явився за 20 секунд')
                                    
                            except Exception as e:
                                logger.warning(f'⚠️ Помилка очікування чекбоксу: {e}')
                            
                            # Спробуємо різні селектори для чекбоксу
                            selectors = [
                                'input[type="checkbox"]',
                                'label',
                                'span[role="checkbox"]',
                                'div[role="checkbox"]',
                                '.cb-c',
                                '[data-testid="checkbox"]',
                                'span:contains("human")',
                                'label:contains("human")', 
                                'div:contains("Verify")',
                                '*[class*="checkbox"]',
                                '*[class*="cb-"]'
                            ]
                            
                            checkbox_clicked = False
                            for selector in selectors:
                                try:
                                    checkbox = await frame.query_selector(selector)
                                    if checkbox:
                                        logger.info(f'🎯 Знайшли і клікаємо елемент ({selector}) в iframe...')
                                        # Більш природний клік з рухом миші
                                        box = await checkbox.bounding_box()
                                        if box:
                                            await page.mouse.move(box['x'] + box['width']/2, box['y'] + box['height']/2)
                                            await page.wait_for_timeout(50 + int(time.time() * 1000) % 100)
                                        await checkbox.click()
                                        await page.wait_for_timeout(3000)
                                        checkbox_clicked = True
                                        break
                                except Exception as e:
                                    logger.debug(f'Селектор {selector} не спрацював: {e}')
                            
                            # Якщо не знайшли чекбокс, пробуємо координатний клік
                            if not checkbox_clicked:
                                logger.info('🎯 Пробуємо координатний клік в iframe...')
                                iframe_element = await frame.frame_element()
                                if iframe_element:
                                    box = await iframe_element.bounding_box()
                                    if box:
                                        # Пробуємо різні позиції
                                        positions = [
                                            (box['x'] + 25, box['y'] + box['height'] * 0.5),  # Ліва частина
                                            (box['x'] + box['width'] * 0.15, box['y'] + box['height'] * 0.5),
                                            (box['x'] + box['width'] * 0.3, box['y'] + box['height'] * 0.5),
                                        ]
                                        
                                        for x, y in positions:
                                            logger.info(f'🎯 Клік в позицію ({x:.0f}, {y:.0f})')
                                            await page.mouse.move(x, y)
                                            await page.wait_for_timeout(50 + int(time.time() * 1000) % 100)
                                            await page.mouse.click(x, y)
                                            await page.wait_for_timeout(2000)
                                            
                                            # Перевіряємо токен після кожного кліку
                                            token = await page.evaluate("document.querySelector('input[name=\"cf-turnstile-response\"]')?.value || ''")
                                            if token and len(token) > 30:
                                                logger.info('🎉 Токен отримано координатним кліком!')
                                                return True
                                        
                                        checkbox_clicked = True
                            
                            if checkbox_clicked:
                                # Перевіряємо чи з'явився токен
                                for _ in range(10):  # Чекаємо до 10 секунд
                                    token = await page.evaluate("document.querySelector('input[name=\"cf-turnstile-response\"]')?.value || ''")
                                    if token and len(token) > 30:
                                        logger.info('🎉 Токен отримано після кліку!')
                                        return True
                                    await page.wait_for_timeout(1000)
                        
                        except Exception as e:
                            logger.debug(f'Помилка роботи з Turnstile iframe: {e}')
                
                # Якщо не знайшли Turnstile, але є input - можливо challenge автоматичний
                if not turnstile_found and elements_info.get('hasTurnstileInput'):
                    logger.info('🤔 Turnstile input знайдено, але iframe відсутній - можливо автоматичний challenge')
                    # Чекаємо ще трохи
                    await page.wait_for_timeout(3000)
                
            except Exception as e:
                logger.debug(f'Помилка діагностики: {e}')
        
        else:
            # Challenge завершено
            logger.info('✅ Challenge завершено, сторінка змінилась')
            return True
            
        await page.wait_for_timeout(1000)
    
    logger.error(f'❌ Challenge не завершився за {max_sec} секунд')
    return False

async def valid_capha(page: Page):
    # Перевіряємо чи є Turnstile (спочатку)
    await page.reload()
    has_turnstile = await page.query_selector('input[name="cf-turnstile-response"]') is not None
    if has_turnstile:
        logger.info('⏳ Виявлено Cloudflare Turnstile, обробляємо...')
        if not await wait_for_cf_challenge(page, max_sec=45):
            logger.warning('❌ Challenge не завершився')
            return False
        logger.info('✅ Turnstile пройдено!')
        # Дочекатися перезавантаження сторінки
        await page.wait_for_timeout(15000)

# ---------- Task runner ----------
async def run_single(page: Page, name: str, address: str, idx: int) -> bool:
    logger.info(f'🌐 Переходимо на FastPeopleSearch...')
    
    # Спробуємо з обробкою timeout
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f'🔄 Спроба {attempt + 1}/{max_retries} завантаження сторінки...')
            await page.goto('https://www.fastpeoplesearch.com/', wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(60000)
            logger.info('✅ Сторінка завантажена успішно!')
            break
        except Exception as e:
            logger.warning(f'⚠️ Спроба {attempt + 1} не вдалася: {e}')
            if attempt == max_retries - 1:
                logger.error('❌ Не вдалося завантажити сторінку після всіх спроб')

                # Спробуємо спростити завантаження
                try:
                    logger.info('🔄 Спробуємо завантажити без очікування load...')
                    await page.goto('https://www.fastpeoplesearch.com/', wait_until='domcontentloaded', timeout=30000)
                    logger.info('✅ Сторінка завантажена з domcontentloaded!')
                    break
                except Exception as e2:
                    logger.error(f'❌ Критична помилка завантаження: {e2}')
                    return False
            else:
                await page.wait_for_timeout(5000)  # Чекаємо перед повторною спробою


    # Додаткова затримка для завантаження
    await page.wait_for_timeout(60000)
    
    # Перевіряємо чи завантажилася сторінка
    try:
        await page.wait_for_load_state('networkidle', timeout=10000)
    except Exception as e:
        logger.warning(f'⚠️ NetworkIdle timeout: {e}')

    # Закриваємо cookie-banner, якщо є
    try:
        btn = await page.query_selector('button:text("I AGREE")')
        if btn:
            logger.info('🍪 Закриваємо cookie banner')
            await btn.click()
            await page.wait_for_timeout(1000)
    except Exception:
        pass

    # Діагностика: що є на сторінці
    try:
        title = await page.title()
        url = page.url
        logger.info(f'📋 Сторінка завантажена: {title} | {url}')

        await valid_capha(page)

        # Тепер шукаємо поля форми (після проходження Turnstile)
        search_input = None
        # З логів знаємо що поле називається "searchfaker-input"
        selectors = [
            'input[name="searchfaker-input"]',
            'input[id="searchfaker-input"]',
            'input[placeholder*="Search"]',
            'input[name="name"]',  # на всякий випадок
            'input[name="search"]'
        ]

        for selector in selectors:
            search_input = await page.query_selector(selector)
            if search_input:
                logger.info(f'✅ Знайдено поле пошуку: {selector}')
                break

        if not search_input:
            logger.error('❌ Поле пошуку не знайдено після Turnstile')
            await page.screenshot(path=f'error_no_search_{idx}.png')
            return False
        
        # Заповнюємо єдине поле пошуку
        logger.info(f'📝 Заповнюємо пошук')
        await page.locator('#search-name-name').fill(f'{name}')
        await page.locator('#search-name-address').fill(f'{address}')
        await page.keyboard.press('Enter')
        
    except Exception as e:
        logger.error(f'❌ Помилка при заповненні форми: {e}')
        await page.screenshot(path=f'error_form_fill_{idx}.png')
        return False

    #Valid capha after enter name and adress
    try:
        logger.info('Validation capha on the page...')
        await valid_capha(page)
    except Exception:
        logger.info('Capha is not found')

    # # Чекаємо результати пошуку
    # try:
    #     await page.wait_for_load_state('networkidle', timeout=15000)
    #     logger.info('📋 Очікування завершення пошуку...')
    # except Exception:
    #     logger.debug('NetworkIdle timeout після пошуку')
    #
    # await page.wait_for_timeout(3000)
    # await page.screenshot(path=f'patchright_gl_{idx}.png')
    # logger.info(f'Screenshot patchright_gl_{idx}.png saved')
    return True

# ---------- main ----------
async def main():
    tasks = build_queue()
    proxy_url = load_first_proxy()
    while True:
        patchright, browser, page = await start_patchright_with_gologin(proxy_url)
        try:
            for idx, (n, a) in enumerate(tasks, 1):
                if idx % 5 == 0:
                    break
                logger.info(f'▶️ {idx}/{len(tasks)} {n} | {a}')
                ok = await run_single(page, n, a, idx)
                if not ok:
                    break
        finally:
            await browser.close()
            await patchright.stop()

if __name__ == '__main__':
    asyncio.run(main())
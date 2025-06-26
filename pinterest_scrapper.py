import asyncio
import os
import random
import re
import sys
from datetime import datetime
import threading

import asyncpg
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do Banco de Dados
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pinterest_images")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configurações do Pinterest e Scraping
PINTEREST_EMAIL = os.getenv("PINTEREST_EMAIL")
PINTEREST_PASSWORD = os.getenv("PINTEREST_PASSWORD")
BOARD_URL = os.getenv("BOARD_URL", "https://br.pinterest.com/feed/")
MAX_IMAGES_TO_COLLECT = int(os.getenv("MAX_IMAGES_TO_COLLECT", 100))
IMAGE_SAVE_DIR = "IMAGENS"

# Configurações de Robustez
SCROLL_PAUSE_TIME = 2
RANDOM_DELAY_MIN = 1.5
RANDOM_DELAY_MAX = 3.5
VIEWPORT_WIDTH = 1280
VIEWPORT_HEIGHT = 900
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

# Variável global para armazenar o loop do banco de dados e a conexão
_db_loop: asyncio.BaseEventLoop = None
_db_conn: asyncpg.Connection = None


def _run_db_loop():
    """Função para ser executada em um thread separado para o loop do DB."""
    global _db_loop
    _db_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_db_loop)
    _db_loop.run_forever()


async def connect_to_db_in_thread():
    """Conecta ao banco de dados dentro do thread do loop do DB."""
    global _db_conn
    try:
        _db_conn = await asyncpg.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Conexão com o banco de dados PostgreSQL estabelecida com sucesso!")
        return _db_conn
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL no thread: {e}")
        return None


async def setup_database_connection_threaded():
    """Inicia um thread para o loop do DB e conecta ao banco de dados."""
    global _db_loop
    if _db_loop is None or not _db_loop.is_running():
        thread = threading.Thread(target=_run_db_loop, daemon=True)
        thread.start()
        await asyncio.sleep(0.1)  # Pequena pausa para o thread iniciar

    conn = await asyncio.get_running_loop().run_in_executor(
        None,
        lambda: asyncio.run_coroutine_threadsafe(connect_to_db_in_thread(), _db_loop).result()
    )
    return conn


async def close_database_connection_threaded():
    """Fecha a conexão do banco de dados e para o loop do DB."""
    global _db_conn, _db_loop
    if _db_conn:
        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: asyncio.run_coroutine_threadsafe(_db_conn.close(), _db_loop).result()
        )
        print("Conexão com o banco de dados fechada.")
    if _db_loop and _db_loop.is_running():
        _db_loop.call_soon_threadsafe(_db_loop.stop)


async def login_pinterest(page: Page, email: str, password: str):
    """Tenta fazer login no Pinterest."""
    print("Tentando fazer login no Pinterest...")
    try:
        await page.goto("https://br.pinterest.com/login/", wait_until="domcontentloaded")
        print(f"URL atual após goto login: {page.url}")
        await asyncio.sleep(random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX))

        try:
            await page.locator('button[data-test-id="cookies-accept-btn"], button[aria-label="Aceitar cookies"]').click(
                timeout=5000)
            print("Pop-up de cookies aceito.")
            await asyncio.sleep(random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX))
        except PlaywrightTimeoutError:
            print("Nenhum pop-up de cookies encontrado ou já fechado.")
        except Exception as e:
            print(f"Erro ao tentar fechar pop-up de cookies: {e}")

        await page.fill('input[name="id"]', email)
        await asyncio.sleep(random.uniform(0.5, 1.5))
        await page.fill('input[name="password"]', password)
        await asyncio.sleep(random.uniform(1.0, 2.0))
        print(f"URL atual após preencher credenciais: {page.url}")

        print("Tentando clicar no botão de login...")
        try:
            await page.locator('button[data-test-id="login-button"]').click(timeout=10000)
            print("Clicou no botão de login com o seletor data-test-id.")
        except PlaywrightTimeoutError:
            print("Seletor data-test-id não funcionou. Tentando por texto...")
            try:
                await page.get_by_role("button", name=re.compile(r"entrar", re.IGNORECASE)).click(timeout=10000)
                print("Clicou no botão de login por texto 'Entrar'.")
            except PlaywrightTimeoutError:
                print("Seletor por texto não funcionou. Tentando forçar o clique no seletor original...")
                try:
                    await page.locator('button[data-test-id="login-button"]').click(timeout=10000, force=True)
                    print("Forçou o clique no botão de login com data-test-id.")
                except PlaywrightTimeoutError:
                    print("Todas as tentativas de clicar no botão de login falharam.")
                    return False

        print(f"URL atual após tentativa de clique no login: {page.url}")

        try:
            await page.wait_for_selector(
                'div[aria-label="Feed de início"], '
                '[data-test-id="search-box"] input[type="search"], '
                'div[data-test-id="pin"]',
                state='visible',
                timeout=60000
            )
            print(f"Elemento de feed logado encontrado. URL atual: {page.url}")
        except PlaywrightTimeoutError:
            print("Timeout esperando o elemento de feed logado. Pode não ter sido autenticado ou a página é diferente.")
            print(f"URL no momento do timeout do seletor de feed: {page.url}")
            return False

        current_url = page.url
        if not (current_url.startswith("https://br.pinterest.com/") and not current_url.startswith(
                "https://br.pinterest.com/login/")):
            print(
                f"A URL final ({current_url}) não é a esperada para o feed logado. O login pode não ter sido completo.")
            return False

        print(f"Login no Pinterest realizado com sucesso e página de feed detectada. URL final: {page.url}")
        return True
    except PlaywrightTimeoutError as e:
        print(f"Erro de timeout durante o login: {e}")
        print(f"URL no momento do timeout: {page.url}")
        print(
            "Verifique as credenciais, a URL de login, ou se o Pinterest mudou sua interface, ou o redirecionamento pós-login.")
        return False
    except Exception as e:
        print(f"Erro inesperado durante o login: {e}")
        print(f"URL no momento do erro inesperado: {page.url}")
        return False


async def scrape_pin_data(pin_element):
    """Extrai os dados de um único elemento pin."""
    image_url = None
    pin_url = None
    title = None
    description = None
    pinterest_id = None

    try:
        img_element = await pin_element.query_selector('img')
        image_url = await img_element.get_attribute('src') if img_element else None

        # Validação da URL da imagem
        if image_url and not (image_url.startswith('https://i.pinimg.com/') or image_url.startswith('data:image')):
            # print(f"  Aviso: Image URL ({image_url}) não parece ser um pin real. Ignorando.")
            return None  # Retorna None se não for uma URL de imagem de pin válida

        pin_link_element = await pin_element.query_selector('a[href*="/pin/"]')
        if pin_link_element:
            pin_url = await pin_link_element.get_attribute('href')
            if pin_url and not pin_url.startswith('http'):
                pin_url = f"https://br.pinterest.com{pin_url}"

        title_element = await pin_element.query_selector(
            'h1, [data-test-id="pin-closeup-title"], [data-test-id="pin-card-title"], div[data-test-id="pin-title"], [data-test-id="card-title"]'
        )
        if title_element:
            title = await title_element.inner_text()
            if title:
                title = title.strip().replace('\n', ' ')

        description_element = await pin_element.query_selector(
            '[data-test-id="pin-closeup-description"], [data-test-id="pin-card-description"], div[data-test-id="pin-description"], [data-test-id="card-description"]'
        )
        if description_element:
            description = await description_element.inner_text()
            if description:
                description = description.strip().replace('\n', ' ')

        if pin_url:
            match = re.search(r'/pin/(\d+)/', pin_url)
            if match:
                pinterest_id = match.group(1)
        elif image_url:
            match = re.search(r'/(\d+)x/(\d+)\.\w+$', image_url)
            if match:
                pinterest_id = match.group(2)
            else:
                match = re.search(r'/(\d+)\.\w+$', image_url)
                if match:
                    pinterest_id = match.group(1)

        if image_url and pinterest_id:
            return {
                "pinterest_id": pinterest_id,
                "title": title,
                "description": description,
                "image_url": image_url,
                "board_url": BOARD_URL,
                "pin_url": pin_url,
                "collected_at": datetime.now()
            }
    except Exception as e:
        # print(f"Erro ao extrair dados de um pin: {e}") # Descomente para depurar erros de pin individual
        pass  # Ignora erros de pins individuais
    return None


async def scroll_and_collect_pinterest(page: Page, target_images: int):
    """
    Rola a página e coleta os dados dos pins incrementalmente,
    até atingir o número alvo de imagens ou o fim da rolagem.
    """
    collected_pins_data = {}  # Usaremos um dicionário para garantir unicidade pelo pinterest_id
    last_height = await page.evaluate("document.body.scrollHeight")
    scroll_count = 0
    max_scrolls = 200  # Limite de rolagens para evitar loop infinito

    # Adiciona um set para controlar os elementos de pin já processados nesta iteração de scroll
    processed_pin_elements_ids = set()

    print(f"Iniciando rolagem e coleta para {target_images} imagens...")

    while len(collected_pins_data) < target_images and scroll_count < max_scrolls:
        # Espera para garantir que os pins estejam carregados e visíveis
        await page.wait_for_selector('div[data-test-id="pin"]', state='attached', timeout=10000)
        await asyncio.sleep(
            random.uniform(RANDOM_DELAY_MIN / 2, RANDOM_DELAY_MAX / 2))  # Pequena pausa para elementos renderizarem

        pins = await page.query_selector_all('div[data-test-id="pin"]')

        current_batch_count = 0
        for pin_element in pins:
            # Playwright element handles have a unique ID that can be used for tracking
            pin_element_id = await pin_element.evaluate(
                "el => el.dataset.testId + '-' + el.getBoundingClientRect().top + '-' + el.getBoundingClientRect().left")

            if pin_element_id not in processed_pin_elements_ids:
                pin_data = await scrape_pin_data(pin_element)
                if pin_data and pin_data["pinterest_id"] not in collected_pins_data:
                    collected_pins_data[pin_data["pinterest_id"]] = pin_data
                    current_batch_count += 1
                    print(
                        f"  Coletado Pin {pin_data['pinterest_id']}. Total: {len(collected_pins_data)}/{target_images}")
                processed_pin_elements_ids.add(pin_element_id)

        print(f"Pins únicos coletados até agora: {len(collected_pins_data)}/{target_images}")

        if len(collected_pins_data) >= target_images:
            print("Número alvo de pins atingido durante a rolagem. Encerrando rolagem.")
            break

        # Rolar a página
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        await asyncio.sleep(random.uniform(SCROLL_PAUSE_TIME, SCROLL_PAUSE_TIME + 1))  # Pausa para o conteúdo carregar

        new_height = await page.evaluate("document.body.scrollHeight")
        scroll_count += 1
        print(f"Rolagem {scroll_count} de {max_scrolls} concluída.")

        if new_height == last_height and current_batch_count == 0:  # Nenhuma nova altura e nenhum novo pin coletado na última iteração
            print("Não há mais conteúdo para rolar ou nenhum novo pin foi encontrado. Fim da página ou limite.")
            break

        last_height = new_height

        if scroll_count >= max_scrolls:
            print(f"Limite máximo de rolagens ({max_scrolls}) atingido. Encerrando rolagem.")
            break

    print(f"Finalizado rolagem. Total de pins coletados: {len(collected_pins_data)}.")
    return list(collected_pins_data.values())  # Retorna a lista de dicionários de dados dos pins


async def save_image_locally(image_url: str, save_dir: str):
    """Baixa uma imagem para o diretório local."""
    if not image_url:
        return None

    os.makedirs(save_dir, exist_ok=True)
    pinterest_id_match = re.search(r'/(\w+)\.\w+$', image_url)
    if pinterest_id_match:
        filename = f"{pinterest_id_match.group(1)}.jpg"
    else:
        filename = image_url.split('/')[-1]
        filename = filename.split('?')[0]
        filename = re.sub(r'[^\w\-. ]', '', filename)[:100] + ".jpg"

    file_path = os.path.join(save_dir, filename)

    if os.path.exists(file_path):
        return file_path

    try:
        process = await asyncio.create_subprocess_shell(
            f'curl -o "{file_path}" "{image_url}"',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            return file_path
        else:
            print(f"Erro ao baixar {image_url}: {stderr.decode()}")
            return None
    except Exception as e:
        print(f"Exceção ao baixar {image_url}: {e}")
        return None


async def insert_image_data(image_data: dict):
    """Insere os dados de uma imagem no banco de dados, usando o loop do DB em um thread separado."""
    global _db_conn, _db_loop
    if not _db_conn or not _db_loop or not _db_loop.is_running():
        print("Conexão com o banco de dados não está ativa para inserção.")
        return False
    try:
        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: asyncio.run_coroutine_threadsafe(
                _db_conn.execute(
                    """
                    INSERT INTO images (pinterest_id, title, description, image_url, board_url, pin_url, collected_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (pinterest_id) DO NOTHING;
                    """,
                    image_data["pinterest_id"],
                    image_data["title"],
                    image_data["description"],
                    image_data["image_url"],
                    image_data["board_url"],
                    image_data["pin_url"],
                    image_data["collected_at"],
                ), _db_loop
            ).result()
        )
        return True
    except asyncpg.exceptions.PostgresError as e:
        print(f"Erro ao inserir dados no banco de dados para {image_data.get('pin_url')}: {e}")
        return False
    except Exception as e:
        print(f"Erro inesperado ao inserir dados: {e}")
        return False


async def main():
    """Função principal para orquestrar o scraping."""
    import nest_asyncio
    nest_asyncio.apply()

    conn = await setup_database_connection_threaded()
    if not conn:
        print("Não foi possível estabelecer conexão com o banco de dados. Encerrando.")
        await close_database_connection_threaded()
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-zygote',
                '--single-process'
            ]
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': VIEWPORT_WIDTH, 'height': VIEWPORT_HEIGHT},
            locale='pt-BR',
            timezone_id='America/Sao_Paulo'
        )

        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    { name: 'Chrome PDF Viewer', description: 'Portable Document Format' },
                    { name: 'Chrome PDF Viewer', description: 'Portable Document Format' }
                ]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['pt-BR', 'pt', 'en-US', 'en']
            });
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 4
            });
            window.chrome = { runtime: {}, csi: () => {}, loadTimes: () => {} };
            window.navigator.chrome = window.chrome;
            window.console.debug = () => {};
        """)

        page = await context.new_page()

        if PINTEREST_EMAIL and PINTEREST_PASSWORD:
            print("Credenciais de login fornecidas. Tentando login no Pinterest...")
            logged_in = await login_pinterest(page, PINTEREST_EMAIL, PINTEREST_PASSWORD)
            if not logged_in:
                print("Login falhou ou não pôde ser verificado. Encerrando o scraping.")
                await close_database_connection_threaded()
                await browser.close()
                return

        else:
            print("Nenhuma credencial de login do Pinterest fornecida. Prosseguindo sem login.")

        try:
            print(f"Iniciando rolagem e coleta na página atual ({page.url})...")
            await asyncio.sleep(random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX))

            # Agora, esta função vai rolar E coletar os dados
            images_data = await scroll_and_collect_pinterest(page, MAX_IMAGES_TO_COLLECT)

            print(f"Iniciando download e inserção de dados para {len(images_data)} imagens.")
            inserted_count = 0
            downloaded_count = 0

            # Como collected_pins_data já lida com unicidade por pinterest_id,
            # não precisamos mais do `processed_pin_ids` aqui.

            for img_data in images_data:
                if img_data.get("image_url"):
                    local_path = await save_image_locally(img_data["image_url"], IMAGE_SAVE_DIR)
                    if local_path:
                        downloaded_count += 1

                success = await insert_image_data(img_data)
                if success:
                    inserted_count += 1
                await asyncio.sleep(random.uniform(0.1, 0.5))

            print(f"--- Coleta Concluída! ---")
            print(f"Total de pins únicos coletados na página: {len(images_data)}")
            print(f"Imagens únicas inseridas no DB: {inserted_count}")
            print(f"Imagens baixadas localmente: {downloaded_count}")

        except PlaywrightTimeoutError as e:
            print(f"Erro de timeout durante o scraping: {e}")
            print(f"URL no momento do timeout: {page.url}")
            print("Verifique sua conexão ou se o Pinterest está bloqueando.")
        except Exception as e:
            print(f"Erro geral durante o scraping: {e}")
            print(f"URL no momento do erro geral: {page.url}")
        finally:
            await close_database_connection_threaded()
            await browser.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    os.makedirs(IMAGE_SAVE_DIR, exist_ok=True)
    asyncio.run(main())
# scraping.py

import re
import time
import json
import random
import requests
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor
import cloudscraper
from twocaptcha import TwoCaptcha
from anticaptchaofficial.recaptchav2proxyless import recaptchaV2Proxyless
from anticaptchaofficial.hcaptchaproxyless import hCaptchaProxyless
from config import HARDWARE_CONFIG, TIMEOUT_CONFIG

class CaptchaSolver:
    def __init__(self):
        self.two_captcha = TwoCaptcha('TU_API_KEY_2CAPTCHA')
        self.use_2captcha = True
        self.anticaptcha_key = 'TU_API_KEY_ANTICAPTCHA'

    def solve_recaptcha(self, site_key: str, url: str) -> str:
        try:
            if self.use_2captcha:
                result = self.two_captcha.recaptcha(
                    sitekey=site_key,
                    url=url,
                    invisible=1
                )
                return result['code']
            else:
                solver = recaptchaV2Proxyless()
                solver.set_verbose(1)
                solver.set_key(self.anticaptcha_key)
                solver.set_website_url(url)
                solver.set_website_key(site_key)
                return solver.solve_and_return_solution()
        except Exception as e:
            print(f"Error en ReCaptcha: {str(e)}")
            self.use_2captcha = not self.use_2captcha
            return self.solve_recaptcha(site_key, url)

    def solve_hcaptcha(self, site_key: str, url: str) -> str:
        try:
            if self.use_2captcha:
                result = self.two_captcha.hcaptcha(
                    sitekey=site_key,
                    url=url
                )
                return result['code']
            else:
                solver = hCaptchaProxyless()
                solver.set_verbose(1)
                solver.set_key(self.anticaptcha_key)
                solver.set_website_url(url)
                solver.set_website_key(site_key)
                return solver.solve_and_return_solution()
        except Exception as e:
            print(f"Error en HCaptcha: {str(e)}")
            self.use_2captcha = not self.use_2captcha
            return self.solve_hcaptcha(site_key, url)

class RegexPatterns:
    """Patrones avanzados de detección"""
    
    PHONES = [
        re.compile(r'(?:\+34|0034|34)?[ -]*(6|7|8|9)[ -]*([0-9][ -]*){8}'),
        re.compile(r'(?:\+34|0034|34)?[ -]*(91|93|95|96|98|99)[ -]*([0-9][ -]*){7}'),
        re.compile(r'Tel[eé]fono:?\s*([0-9\s]{9,})'),
        re.compile(r'(?:Contacta|Contacto|Llama):?\s*([0-9\s]{9,})')
    ]
    
    SOCIAL = {
        'facebook': [
            re.compile(r'https?://(?:www\.)?facebook\.com/[\w.-]+/?'),
            re.compile(r'https?://(?:www\.)?fb\.me/[\w.-]+/?'),
            re.compile(r'(?:Facebook|FB):\s*@?([\w.-]+)')
        ],
        'twitter': [
            re.compile(r'https?://(?:www\.)?twitter\.com/[\w_]+/?'),
            re.compile(r'https?://(?:www\.)?x\.com/[\w_]+/?'),
            re.compile(r'(?:Twitter|X):\s*@?([\w_]+)')
        ],
        'linkedin': [
            re.compile(r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[\w-]+/?'),
            re.compile(r'https?://(?:www\.)?linkedin\.com/(?:company|in)/([\w-]+)')
        ],
        'instagram': [
            re.compile(r'https?://(?:www\.)?instagram\.com/[\w._]+/?'),
            re.compile(r'(?:Instagram|IG):\s*@?([\w._]+)')
        ]
    }
    
    ECOMMERCE = [
        re.compile(r'\b(?:carrito|cesta|cart|basket)\b', re.IGNORECASE),
        re.compile(r'\b(?:comprar|buy|purchase)\b', re.IGNORECASE),
        re.compile(r'\b(?:checkout|pago|payment)\b', re.IGNORECASE),
        re.compile(r'\b(?:tienda|shop|store)\b', re.IGNORECASE),
        re.compile(r'\b(?:añadir|add)\s+(?:al|to)\s+(?:carrito|cart)\b', re.IGNORECASE)
    ]

class ProWebScraper:
    def __init__(self, use_proxies: bool = False):
        self.regex = RegexPatterns()
        self.user_agent = UserAgent()
        self.scraper = cloudscraper.create_scraper()
        self.chrome_options = self._setup_chrome_options()
        self.max_workers = HARDWARE_CONFIG["max_workers"]
        self.captcha_solver = CaptchaSolver()
        self._setup_parallel_processing()

    def _setup_chrome_options(self) -> Options:
        options = Options()
        for opt in HARDWARE_CONFIG["chrome_options"]:
            options.add_argument(opt)
        options.add_argument(f'user-agent={self.user_agent.random}')
        if HARDWARE_CONFIG["gpu_enabled"]:
            options.add_argument('--enable-gpu-rasterization')
            options.add_argument('--enable-zero-copy')
        return options

    def _setup_parallel_processing(self):
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

    def scrape_url(self, url: str, company_data: dict) -> dict:
        result = {
            'url': url,
            'url_exists': False,
            'url_limpia': self._clean_url(url),
            'url_status': None,
            'url_status_mensaje': None,
            'phones': [],
            'social_media': {},
            'is_ecommerce': False,
            'validation_score': 0
        }
        content = self._get_page_content(url)
        if not content:
            return self._handle_failed_scrape(result)
        soup = BeautifulSoup(content, 'html.parser')
        text_content = soup.get_text()
        result['validation_score'] = self._validate_content(text_content, company_data)
        if result['validation_score'] > 0:
            result['url_exists'] = True
            result.update(self._extract_all_data(soup, text_content))
        return result

    def _get_page_content(self, url: str) -> str:
        methods = [
            self._try_cloudscraper,
            self._try_selenium_undetected,
            self._try_selenium_stealth
        ]
        for method in methods:
            try:
                content = method(url)
                if content and len(content) > 100:
                    return content
            except Exception as e:
                print(f"Method failed: {str(e)}")
                continue
        return None

    def _try_cloudscraper(self, url: str) -> str:
        response = self.scraper.get(url, timeout=TIMEOUT_CONFIG["request_timeout"])
        if response.status_code == 200:
            return response.text
        raise Exception(f"CloudScraper failed: {response.status_code}")

    def _try_selenium_undetected(self, url: str) -> str:
        driver = uc.Chrome(options=self.chrome_options)
        try:
            driver.get(url)
            self._handle_cookies(driver)
            if not self._handle_captcha(driver):
                raise Exception("Captcha handling failed")
            return driver.page_source
        finally:
            driver.quit()

    def _try_selenium_stealth(self, url: str) -> str:
        driver = webdriver.Chrome(options=self.chrome_options)
        try:
            driver.get(url)
            self._handle_cookies(driver)
            return driver.page_source
        finally:
            driver.quit()

    def _handle_cookies(self, driver):
        cookie_buttons = [
            "//button[contains(translate(., 'ACEPT', 'acept'), 'accept')]",
            "//button[contains(translate(., 'ACEPT', 'acept'), 'aceptar')]",
            "//a[contains(@class, 'cookie') and contains(text(), 'Accept')]",
            "//div[contains(@class, 'cookie')]//button",
            "//*[contains(@id, 'cookie-law')]//*[contains(text(), 'Accept')]",
            "//*[contains(@class, 'cookie-banner')]//*[contains(text(), 'Aceptar')]"
        ]
        for xpath in cookie_buttons:
            try:
                button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, xpath)))
                button.click()
                time.sleep(0.5)
                return
            except:
                continue

    def _handle_captcha(self, driver) -> bool:
        try:
            if self._detect_recaptcha(driver):
                return self._solve_recaptcha(driver)
            elif self._detect_hcaptcha(driver):
                return self._solve_hcaptcha(driver)
            elif self._detect_cloudflare(driver):
                return self._handle_cloudflare(driver)
            return True
        except Exception as e:
            print(f"Error en manejo de CAPTCHA: {str(e)}")
            return False

    def _detect_recaptcha(self, driver) -> dict:
        try:
            iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='recaptcha']")
            site_key = iframe.get_attribute('src').split('k=')[1].split('&')[0]
            return {"present": True, "site_key": site_key}
        except:
            return {"present": False}

    def _detect_hcaptcha(self, driver) -> dict:
        try:
            iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='hcaptcha']")
            site_key = iframe.get_attribute('data-hcaptcha-widget-id')
            return {"present": True, "site_key": site_key}
        except:
            return {"present": False}

    def _detect_cloudflare(self, driver) -> bool:
        try:
            return bool(driver.find_element(By.ID, "challenge-form"))
        except:
            return False

    def _solve_recaptcha(self, driver) -> bool:
        try:
            recaptcha_info = self._detect_recaptcha(driver)
            if not recaptcha_info["present"]:
                return True
            solution = self.captcha_solver.solve_recaptcha(
                recaptcha_info["site_key"],
                driver.current_url
            )
            driver.execute_script(
                "document.getElementById('g-recaptcha-response').innerHTML = arguments[0]",
                solution
            )
            submit_button = driver.find_element(By.CSS_SELECTOR, 
                "button[type='submit'], input[type='submit']")
            submit_button.click()
            return True
        except Exception as e:
            print(f"Error resolviendo reCAPTCHA: {str(e)}")
            return False

    def _solve_hcaptcha(self, driver) -> bool:
        try:
            hcaptcha_info = self._detect_hcaptcha(driver)
            if not hcaptcha_info["present"]:
                return True
            solution = self.captcha_solver.solve_hcaptcha(
                hcaptcha_info["site_key"],
                driver.current_url
            )
            driver.execute_script(
                "document.getElementsByName('h-captcha-response')[0].innerHTML = arguments[0]",
                solution
            )
            submit_button = driver.find_element(By.CSS_SELECTOR, 
                "button[type='submit'], input[type='submit']")
            submit_button.click()
            return True
        except Exception as e:
            print(f"Error resolviendo hCAPTCHA: {str(e)}")
            return False

    def _handle_cloudflare(self, driver) -> bool:
        try:
            WebDriverWait(driver, 30).until(
                EC.invisibility_of_element_located((By.ID, "challenge-form"))
            )
            return True
        except:
            return False

    def _validate_content(self, text_content: str, company_data: dict) -> int:
        score = 0
        text_content = text_content.lower()
        if company_data.get('nif') and company_data['nif'].lower() in text_content:
            score += 3
        if company_data.get('razon_social'):
            razon_social = company_data['razon_social'].lower()
            if razon_social in text_content:
                score += 2
            else:
                keywords = razon_social.split()
                for keyword in keywords:
                    if len(keyword) > 3 and keyword in text_content:
                        score += 0.5
        if company_data.get('domicilio'):
            domicilio = company_data['domicilio'].lower()
            if domicilio in text_content:
                score += 2
            else:
                parts = domicilio.split()
                for part in parts:
                    if len(part) > 3 and part in text_content:
                        score += 0.5
        if company_data.get('cod_postal') and company_data['cod_postal'] in text_content:
            score += 1
        if company_data.get('nom_poblacion') and company_data['nom_poblacion'].lower() in text_content:
            score += 1
        return score

    def _extract_all_data(self, soup: BeautifulSoup, text_content: str) -> dict:
        return {
            'phones': self._extract_phones(text_content),
            'social_media': self._extract_social_media(soup, text_content),
            'is_ecommerce': self._detect_ecommerce(soup, text_content)
        }

    def _extract_phones(self, text_content: str) -> list:
        phones = set()
        for pattern in self.regex.PHONES:
            matches = pattern.finditer(text_content)
            for match in matches:
                phone = re.sub(r'\D', '', match.group())
                if 9 <= len(phone) <= 12:
                    phones.add(phone[-9:])
        return list(phones)[:3]

    def _extract_social_media(self, soup: BeautifulSoup, text_content: str) -> dict:
        social_media = {}
        for network, patterns in self.regex.SOCIAL.items():
            for pattern in patterns:
                for link in soup.find_all('a', href=True):
                    href = link['href'].lower()
                    if pattern.search(href):
                        clean_url = re.sub(r'[?#].*$', '', href)
                        social_media[network] = clean_url
                        break
                if network not in social_media:
                    matches = pattern.finditer(text_content)
                    for match in matches:
                        if match.groups():
                            social_media[network] = match.group(1)
                        else:
                            social_media[network] = match.group(0)
                        break
        return social_media

    def _detect_ecommerce(self, soup: BeautifulSoup, text_content: str) -> bool:
        for pattern in self.regex.ECOMMERCE:
            if pattern.search(text_content):
                return True
        ecommerce_elements = [
            "//form[contains(@action, 'cart')]",
            "//form[contains(@action, 'checkout')]",
            "//div[contains(@class, 'cart')]",
            "//div[contains(@class, 'shop')]",
            "//a[contains(@href, 'cart')]",
            "//button[contains(@class, 'add-to-cart')]",
            "//input[contains(@name, 'quantity')]",
            "//div[contains(@class, 'product-price')]",
            "//span[contains(@class, 'price')]"
        ]
        for selector in ecommerce_elements:
            try:
                elements = soup.select(selector)
                if elements:
                    return True
            except:
                continue
        return False

    def _clean_url(self, url: str) -> str:
        url = url.strip().lower()
        url = re.sub(r'^https?://', '', url)
        url = re.sub(r'^www\.', '', url)
        url = url.split('/')[0]
        return url

    def _handle_failed_scrape(self, result: dict) -> dict:
        result.update({
            'url_status': 404,
            'url_status_mensaje': "No se pudo acceder a la página",
            'phones': [],
            'social_media': {},
            'is_ecommerce': False,
            'validation_score': 0
        })
        return result

    def __del__(self):
        try:
            self.executor.shutdown(wait=True)
        except:
            pass

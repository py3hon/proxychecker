import asyncio
import aiohttp
import aiohttp_socks
import sys
import time
import ssl
import random
import json
import re
import socket
import threading
from urllib.parse import urlparse, unquote
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
from pathlib import Path

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    from rich import box
    from rich.prompt import Prompt, Confirm, IntPrompt
    from rich.layout import Layout
    from rich.text import Text
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "rich", "beautifulsoup4", "lxml", "aiohttp-socks"])
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    from rich import box
    from rich.prompt import Prompt, Confirm, IntPrompt
    from rich.layout import Layout
    from rich.text import Text
    from bs4 import BeautifulSoup

console = Console()
SETTINGS_FILE = "settings.json"
VALID_PROXIES_FILE = "valid_proxies.txt"
STOP_EVENT = threading.Event()

DEFAULT_SETTINGS = {
    "threads": 300,
    "timeout": 8,
    "retries": 1,
    "sources": {
        "proxy_list_download": True,
        "free_proxy_list": True,
        "sslproxies": True,
        "us_proxy": True,
        "socks_proxy": True,
        "proxyscrape": True,
        "geonode": True,
        "advanced_name": True,
        "openproxy_space": True,
        "proxyscan": True,
        "spys_me": True,
        "proxy_daily": True,
        "proxy_nova": True,
        "hideme": True,
        "proxydb": True,
        "proxylistplus": True,
        "freeproxycz": True,
        "coolproxy": True,
        "proxyranker": True,
        "multiproxy": True,
        "foxtools": True,
        "pubproxy": True,
        "proxy11": True,
        "proxy_sale": True,
        "github_the_speed_x": True,
        "github_clarketm": True,
        "github_monosans": True,
        "github_jetkai": True,
        "github_shiftytr": True,
        "github_mertguven": True,
        "github_sunny": True,
        "github_zevtyardt": True,
        "github_r00t_3xp10it": True,
        "github_saswat": True,
        "github_hookzof": True,
        "github_fate0": True
    },
    "auto_save": True,
    "export_format": "all"
}


@dataclass
class ProxyResult:
    proxy: str
    type: str
    status: str
    response_time: float
    anonymity: str
    country: str
    username: str = ""
    password: str = ""
    error: Optional[str] = None


class SettingsManager:
    def __init__(self):
        self.settings = self.load_settings()
    
    def load_settings(self) -> Dict:
        if Path(SETTINGS_FILE).exists():
            try:
                with open(SETTINGS_FILE, "r") as f:
                    loaded = json.load(f)
                    for key, value in DEFAULT_SETTINGS.items():
                        if key not in loaded:
                            loaded[key] = value
                    return loaded
            except Exception:
                return DEFAULT_SETTINGS.copy()
        return DEFAULT_SETTINGS.copy()
    
    def save_settings(self):
        with open(SETTINGS_FILE, "w") as f:
            json.dump(self.settings, f, indent=2)
    
    def get(self, key: str, default=None):
        return self.settings.get(key, default)
    
    def set(self, key: str, value):
        self.settings[key] = value
        self.save_settings()


class ProxyScraper:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    
    def __init__(self, settings: SettingsManager):
        self.settings = settings
        self.scraped_proxies: List[str] = []
        self.session: Optional[aiohttp.ClientSession] = None
    
    def get_headers(self):
        return {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive"
        }
    
    async def init_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=50, ssl=False)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    
    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def fetch(self, url: str, return_json: bool = False):
        try:
            await self.init_session()
            async with self.session.get(url, headers=self.get_headers(), ssl=False) as response:
                if response.status == 200:
                    if return_json:
                        return await response.json()
                    return await response.text()
        except Exception:
            pass
        return None

    async def scrape_proxy_list_download(self) -> List[str]:
        proxies = []
        for ptype in ["http", "https", "socks4", "socks5"]:
            url = f"https://www.proxy-list.download/api/v1/get?type={ptype}"
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
            await asyncio.sleep(0.2)
        return proxies
    
    async def scrape_free_proxy_list(self) -> List[str]:
        proxies = []
        urls = [
            "https://free-proxy-list.net/",
            "https://free-proxy-list.net/anonymous.html",
            "https://free-proxy-list.net/uk-proxy.html",
            "https://free-proxy-list.net/elite-proxy.html"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                    table = soup.find("table", {"id": "proxylisttable"})
                    if table:
                        for row in table.find_all("tr")[1:]:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                ip, port = cols[0].text.strip(), cols[1].text.strip()
                                if ip and port:
                                    proxies.append(f"{ip}:{port}")
                except Exception:
                    pass
            await asyncio.sleep(0.2)
        return proxies
    
    async def scrape_sslproxies(self) -> List[str]:
        proxies = []
        text = await self.fetch("https://www.sslproxies.org/")
        if text:
            try:
                soup = BeautifulSoup(text, "html.parser")
                table = soup.find("table", {"id": "proxylisttable"})
                if table:
                    for row in table.find_all("tr")[1:]:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            ip, port = cols[0].text.strip(), cols[1].text.strip()
                            if ip and port:
                                proxies.append(f"{ip}:{port}")
            except Exception:
                pass
        return proxies
    
    async def scrape_us_proxy(self) -> List[str]:
        proxies = []
        text = await self.fetch("https://www.us-proxy.org/")
        if text:
            try:
                soup = BeautifulSoup(text, "html.parser")
                table = soup.find("table", {"id": "proxylisttable"})
                if table:
                    for row in table.find_all("tr")[1:]:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            ip, port = cols[0].text.strip(), cols[1].text.strip()
                            if ip and port:
                                proxies.append(f"{ip}:{port}")
            except Exception:
                pass
        return proxies
    
    async def scrape_socks_proxy(self) -> List[str]:
        proxies = []
        text = await self.fetch("https://www.socks-proxy.net/")
        if text:
            try:
                soup = BeautifulSoup(text, "html.parser")
                table = soup.find("table", {"id": "proxylisttable"})
                if table:
                    for row in table.find_all("tr")[1:]:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            ip, port = cols[0].text.strip(), cols[1].text.strip()
                            if ip and port:
                                proxies.append(f"{ip}:{port}")
            except Exception:
                pass
        return proxies
    
    async def scrape_proxyscrape(self) -> List[str]:
        proxies = []
        urls = [
            "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
            "https://api.proxyscrape.com/v2/?request=get&protocol=socks4&timeout=10000&country=all",
            "https://api.proxyscrape.com/v2/?request=get&protocol=socks5&timeout=10000&country=all",
            "https://api.proxyscrape.com/v2/?request=get&protocol=https&timeout=10000&country=all&ssl=all&anonymity=all"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
            await asyncio.sleep(0.2)
        return proxies
    
    async def scrape_geonode(self) -> List[str]:
        proxies = []
        for page in range(1, 6):
            url = f"https://proxylist.geonode.com/api/proxy-list?limit=500&page={page}&sort_by=lastChecked&sort_type=desc"
            data = await self.fetch(url, return_json=True)
            if data and isinstance(data, dict):
                try:
                    for item in data.get("data", []):
                        ip, port = item.get("ip"), item.get("port")
                        if ip and port:
                            proxies.append(f"{ip}:{port}")
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_advanced_name(self) -> List[str]:
        proxies = []
        for page in range(1, 6):
            url = f"https://advanced.name/freeproxy?type=all&page={page}"
            text = await self.fetch(url)
            if text:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                    for row in soup.find_all("tr")[1:]:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            ip_port = cols[0].text.strip()
                            if ":" in ip_port:
                                proxies.append(ip_port)
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_openproxy_space(self) -> List[str]:
        proxies = []
        urls = [
            "https://openproxy.space/list/http",
            "https://openproxy.space/list/socks4",
            "https://openproxy.space/list/socks5"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                matches = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5})', text)
                proxies.extend(matches)
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_proxyscan(self) -> List[str]:
        proxies = []
        for limit in [500, 1000]:
            url = f"https://www.proxyscan.io/api/proxy?limit={limit}&format=json"
            data = await self.fetch(url, return_json=True)
            if data and isinstance(data, list):
                try:
                    for item in data:
                        ip, port = item.get("Ip"), item.get("Port")
                        if ip and port:
                            proxies.append(f"{ip}:{port}")
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_spys_me(self) -> List[str]:
        proxies = []
        for url in ["https://spys.me/proxy.txt", "https://spys.me/socks.txt"]:
            text = await self.fetch(url)
            if text:
                for line in text.splitlines():
                    if ":" in line and not line.startswith("#"):
                        proxy = line.split()[0] if " " in line else line.strip()
                        if ":" in proxy:
                            proxies.append(proxy)
        return proxies
    
    async def scrape_proxy_daily(self) -> List[str]:
        proxies = []
        text = await self.fetch("https://proxy-daily.com/")
        if text:
            try:
                soup = BeautifulSoup(text, "html.parser")
                divs = soup.find_all("div", {"class": "centeredProxyList"})
                for div in divs:
                    content = div.get_text()
                    lines = [l.strip() for l in content.splitlines() if ":" in l]
                    proxies.extend(lines)
            except Exception:
                pass
        return proxies
    
    async def scrape_proxy_nova(self) -> List[str]:
        proxies = []
        countries = ["us", "gb", "de", "fr", "ca", "au"]
        for country in countries:
            url = f"https://www.proxynova.com/proxy-server-list/country-{country}/"
            text = await self.fetch(url)
            if text:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                    table = soup.find("table", {"id": "tbl_proxy_list"})
                    if table:
                        for row in table.find_all("tr")[1:]:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                ip_script = cols[0].find("script")
                                if ip_script and ip_script.string:
                                    ip_match = re.search(r'"([^"]+)"', ip_script.string)
                                    ip = ip_match.group(1) if ip_match else cols[0].text.strip()
                                else:
                                    ip = cols[0].text.strip()
                                port = cols[1].text.strip()
                                if ip and port:
                                    proxies.append(f"{ip}:{port}")
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_hideme(self) -> List[str]:
        proxies = []
        for page in range(1, 6):
            url = f"https://hidemy.name/en/proxy-list/?start={page * 64}#list"
            text = await self.fetch(url)
            if text:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                    table = soup.find("table", {"class": "proxy__t"})
                    if table:
                        for row in table.find_all("tr")[1:]:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                ip = cols[0].text.strip()
                                port = cols[1].text.strip()
                                if ip and port:
                                    proxies.append(f"{ip}:{port}")
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_proxydb(self) -> List[str]:
        proxies = []
        for offset in [0, 50, 100, 150]:
            url = f"http://proxydb.net/?protocol=http&protocol=https&anonlvl=2&anonlvl=3&anonlvl=4&offset={offset}"
            text = await self.fetch(url)
            if text:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                    table = soup.find("table")
                    if table:
                        for row in table.find_all("tr")[1:]:
                            cols = row.find_all("td")
                            if len(cols) >= 1:
                                addr = cols[0].text.strip()
                                if ":" in addr:
                                    proxies.append(addr)
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_proxylistplus(self) -> List[str]:
        proxies = []
        urls = [
            "https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-1",
            "https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-2",
            "https://list.proxylistplus.com/Socks-List-1"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                    rows = soup.find_all("tr", {"class": "cells"})
                    for row in rows:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            ip = cols[0].text.strip()
                            port = cols[1].text.strip()
                            if ip and port:
                                proxies.append(f"{ip}:{port}")
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_freeproxycz(self) -> List[str]:
        proxies = []
        urls = [
            "http://free-proxy-list.net/",
            "https://www.us-proxy.org/",
            "https://free-proxy-list.net/uk-proxy.html",
            "https://free-proxy-list.net/anonymous.html"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                    table = soup.find("table", {"id": "proxylisttable"})
                    if table:
                        for row in table.find_all("tr")[1:]:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                ip = cols[0].text.strip()
                                port = cols[1].text.strip()
                                if ip and port:
                                    proxies.append(f"{ip}:{port}")
                except Exception:
                    pass
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_coolproxy(self) -> List[str]:
        proxies = []
        text = await self.fetch("https://cool-proxy.net/proxies.json")
        if text:
            try:
                data = json.loads(text)
                for item in data:
                    ip = item.get("ip", "")
                    port = item.get("port", "")
                    if ip and port:
                        proxies.append(f"{ip}:{port}")
            except Exception:
                pass
        return proxies
    
    async def scrape_proxyranker(self) -> List[str]:
        proxies = []
        text = await self.fetch("https://proxyranker.com/")
        if text:
            matches = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5})', text)
            proxies.extend(matches)
        return proxies
    
    async def scrape_multiproxy(self) -> List[str]:
        proxies = []
        urls = [
            "http://multiproxy.org/txt_all/proxy.txt",
            "http://multiproxy.org/txt_anon/proxy.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_foxtools(self) -> List[str]:
        proxies = []
        url = "http://api.foxtools.ru/v2/Proxy.txt"
        text = await self.fetch(url)
        if text:
            lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
            proxies.extend(lines)
        return proxies
    
    async def scrape_pubproxy(self) -> List[str]:
        proxies = []
        for _ in range(5):
            url = "http://pubproxy.com/api/proxy?limit=20&format=txt"
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_proxy11(self) -> List[str]:
        proxies = []
        for page in range(1, 10):
            url = f"https://proxy11.com/api/proxy.txt?country=US&speed=fast&page={page}"
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_proxy_sale(self) -> List[str]:
        proxies = []
        for page in range(1, 5):
            url = f"https://proxy-sale.com/api/proxy-list?page={page}"
            text = await self.fetch(url)
            if text:
                matches = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5})', text)
                proxies.extend(matches)
            await asyncio.sleep(0.3)
        return proxies
    
    async def scrape_github_the_speed_x(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_github_clarketm(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
            "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_github_monosans(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
            await asyncio.sleep(0.2)
        return proxies
    
    async def scrape_github_jetkai(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_github_shiftytr(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_github_mertguven(self) -> List[str]:
        proxies = []
        url = "https://raw.githubusercontent.com/MertGuven/Proxy-List/main/proxy-list.txt"
        text = await self.fetch(url)
        if text:
            lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
            proxies.extend(lines)
        return proxies
    
    async def scrape_github_sunny(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
            "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
            "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks4_proxies.txt",
            "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks5_proxies.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_github_zevtyardt(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/Zaeem20/PROXY-List/main/http.txt",
            "https://raw.githubusercontent.com/Zaeem20/PROXY-List/main/socks4.txt",
            "https://raw.githubusercontent.com/Zaeem20/PROXY-List/main/socks5.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_github_r00t_3xp10it(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/r00t-3xp10it/Proxies-Grabber/main/.github/workflows/proxies.txt",
            "https://raw.githubusercontent.com/r00t-3xp10it/Proxies-Grabber/main/proxies.txt"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                proxies.extend(lines)
        return proxies
    
    async def scrape_github_saswat(self) -> List[str]:
        proxies = []
        url = "https://raw.githubusercontent.com/saswatcodes/proxy-list/main/https.txt"
        text = await self.fetch(url)
        if text:
            lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
            proxies.extend(lines)
        return proxies
    
    async def scrape_github_hookzof(self) -> List[str]:
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
            "https://raw.githubusercontent.com/hookzof/socks5_list/master/tg/socks.json"
        ]
        for url in urls:
            text = await self.fetch(url)
            if text:
                if "json" in url:
                    try:
                        data = json.loads(text)
                        for item in data:
                            ip = item.get("ip", "")
                            port = item.get("port", "")
                            if ip and port:
                                proxies.append(f"{ip}:{port}")
                    except:
                        pass
                else:
                    lines = [l.strip() for l in text.splitlines() if ":" in l and l.strip()]
                    proxies.extend(lines)
        return proxies
    
    async def scrape_github_fate0(self) -> List[str]:
        proxies = []
        url = "https://raw.githubusercontent.com/fate0/proxylist/master/proxy.list"
        text = await self.fetch(url)
        if text:
            for line in text.splitlines():
                try:
                    data = json.loads(line)
                    host = data.get("host", "")
                    port = data.get("port", "")
                    if host and port:
                        proxies.append(f"{host}:{port}")
                except:
                    if ":" in line:
                        proxies.append(line.strip())
        return proxies
    
    async def scrape_selected(self, selected_sources: List[str], max_proxies: int = 0):
        self.scraped_proxies = []
        source_map = {
            "proxy_list_download": ("proxy-list.download", self.scrape_proxy_list_download),
            "free_proxy_list": ("free-proxy-list.net", self.scrape_free_proxy_list),
            "sslproxies": ("sslproxies.org", self.scrape_sslproxies),
            "us_proxy": ("us-proxy.org", self.scrape_us_proxy),
            "socks_proxy": ("socks-proxy.net", self.scrape_socks_proxy),
            "proxyscrape": ("proxyscrape.com", self.scrape_proxyscrape),
            "geonode": ("geonode.com", self.scrape_geonode),
            "advanced_name": ("advanced.name", self.scrape_advanced_name),
            "openproxy_space": ("openproxy.space", self.scrape_openproxy_space),
            "proxyscan": ("proxyscan.io", self.scrape_proxyscan),
            "spys_me": ("spys.me", self.scrape_spys_me),
            "proxy_daily": ("proxy-daily.com", self.scrape_proxy_daily),
            "proxy_nova": ("proxynova.com", self.scrape_proxy_nova),
            "hideme": ("hidemy.name", self.scrape_hideme),
            "proxydb": ("proxydb.net", self.scrape_proxydb),
            "proxylistplus": ("proxylistplus.com", self.scrape_proxylistplus),
            "freeproxycz": ("free-proxy-list variants", self.scrape_freeproxycz),
            "coolproxy": ("cool-proxy.net", self.scrape_coolproxy),
            "proxyranker": ("proxyranker.com", self.scrape_proxyranker),
            "multiproxy": ("multiproxy.org", self.scrape_multiproxy),
            "foxtools": ("foxtools.ru", self.scrape_foxtools),
            "pubproxy": ("pubproxy.com", self.scrape_pubproxy),
            "proxy11": ("proxy11.com", self.scrape_proxy11),
            "proxy_sale": ("proxy-sale.com", self.scrape_proxy_sale),
            "github_the_speed_x": ("GitHub: TheSpeedX", self.scrape_github_the_speed_x),
            "github_clarketm": ("GitHub: clarketm", self.scrape_github_clarketm),
            "github_monosans": ("GitHub: monosans", self.scrape_github_monosans),
            "github_jetkai": ("GitHub: jetkai", self.scrape_github_jetkai),
            "github_shiftytr": ("GitHub: ShiftyTR", self.scrape_github_shiftytr),
            "github_mertguven": ("GitHub: MertGuven", self.scrape_github_mertguven),
            "github_sunny": ("GitHub: sunny9577", self.scrape_github_sunny),
            "github_zevtyardt": ("GitHub: Zaeem20", self.scrape_github_zevtyardt),
            "github_r00t_3xp10it": ("GitHub: r00t-3xp10it", self.scrape_github_r00t_3xp10it),
            "github_saswat": ("GitHub: saswatcodes", self.scrape_github_saswat),
            "github_hookzof": ("GitHub: hookzof", self.scrape_github_hookzof),
            "github_fate0": ("GitHub: fate0", self.scrape_github_fate0)
        }
        
        await self.init_session()
        
        tasks = []
        names = []
        
        for source_key in selected_sources:
            if source_key in source_map:
                name, func = source_map[source_key]
                tasks.append(func())
                names.append(name)
        
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
            for name in names:
                progress.add_task(f"[cyan]Scraping {name}...", total=None)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, list):
                    self.scraped_proxies.extend(result)
                    console.print(f"[green]✓ {names[i]}: {len(result)} proxies[/green]")
                else:
                    console.print(f"[red]✗ {names[i]}: Failed[/red]")
        
        await self.close_session()
        
        seen = set()
        unique = []
        for proxy in self.scraped_proxies:
            p = proxy.strip()
            if p and p not in seen:
                seen.add(p)
                unique.append(p)
        
        self.scraped_proxies = unique
        
        if max_proxies > 0 and len(self.scraped_proxies) > max_proxies:
            self.scraped_proxies = self.scraped_proxies[:max_proxies]
            console.print(f"[yellow]Limited to {max_proxies} proxies[/yellow]")
        
        return self.scraped_proxies


class ProxyChecker:
    TEST_URLS = [
        "http://httpbin.org/ip",
        "http://icanhazip.com"
    ]
    
    def __init__(self, settings: SettingsManager):
        self.results: List[ProxyResult] = []
        self.working_count = 0
        self.failed_count = 0
        self.settings = settings
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.existing_proxies = self._load_existing()
        
    def _load_existing(self) -> set:
        existing = set()
        if Path(VALID_PROXIES_FILE).exists():
            try:
                with open(VALID_PROXIES_FILE, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            existing.add(self._normalize_proxy(line))
            except Exception:
                pass
        return existing
    
    def _normalize_proxy(self, proxy_str: str) -> str:
        proxy_str = proxy_str.strip()
        for prefix in ["http://", "https://", "socks4://", "socks5://"]:
            if proxy_str.startswith(prefix):
                proxy_str = proxy_str[len(prefix):]
        return proxy_str
    
    def parse_proxy(self, proxy_str: str):
        proxy_str = proxy_str.strip()
        if not proxy_str or proxy_str.startswith("#"):
            return None
        
        username = ""
        password = ""
        protocol = "HTTP"
        
        if "://" in proxy_str:
            parsed = urlparse(proxy_str)
            protocol = parsed.scheme.upper()
            if protocol == "HTTPS":
                protocol = "HTTP"
            if parsed.username:
                username = unquote(parsed.username)
            if parsed.password:
                password = unquote(parsed.password)
            host = parsed.hostname
            port = parsed.port
            if host and port:
                return (protocol, host, port, username, password)
        else:
            if "@" in proxy_str:
                auth_part, addr_part = proxy_str.rsplit("@", 1)
                if ":" in auth_part:
                    username, password = auth_part.split(":", 1)
                    username = unquote(username)
                    password = unquote(password)
                proxy_str = addr_part
            
            if ":" in proxy_str:
                try:
                    host, port_str = proxy_str.rsplit(":", 1)
                    port = int(port_str.strip())
                    host = host.strip()
                    return (protocol, host, port, username, password)
                except ValueError:
                    return None
        
        return None
    
    async def try_connect(self, host: str, port: int, protocol: str, username: str, password: str, timeout: int):
        start_time = time.time()
        
        auth_str = f"{username}:{password}@" if username and password else ""
        
        if protocol in ["HTTP", "HTTPS"]:
            try:
                connector = aiohttp.TCPConnector(ssl=False, limit=1)
                session = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=timeout))
                try:
                    proxy_url = f"http://{auth_str}{host}:{port}"
                    async with session.get("http://httpbin.org/ip", proxy=proxy_url, ssl=False, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                        if response.status == 200:
                            elapsed = round((time.time() - start_time) * 1000, 2)
                            return ProxyResult(
                                proxy=f"{host}:{port}",
                                type=protocol,
                                status="WORKING",
                                response_time=elapsed,
                                anonymity="Elite",
                                country="Unknown",
                                username=username,
                                password=password
                            )
                finally:
                    await session.close()
                    await connector.close()
            except Exception:
                pass
                
        elif protocol in ["SOCKS4", "SOCKS5"]:
            try:
                proxy_url = f"{protocol.lower()}://{auth_str}{host}:{port}"
                connector = aiohttp_socks.ProxyConnector.from_url(proxy_url, rdns=True, ssl=False)
                session = aiohttp.ClientSession(connector=connector)
                try:
                    async with session.get("http://httpbin.org/ip", ssl=False, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                        if response.status == 200:
                            elapsed = round((time.time() - start_time) * 1000, 2)
                            return ProxyResult(
                                proxy=f"{host}:{port}",
                                type=protocol,
                                status="WORKING",
                                response_time=elapsed,
                                anonymity="High",
                                country="Unknown",
                                username=username,
                                password=password
                            )
                finally:
                    await session.close()
                    await connector.close()
            except Exception:
                pass
        
        return None
    
    async def check_proxy(self, proxy_str: str):
        parsed = self.parse_proxy(proxy_str)
        if not parsed:
            return ProxyResult(
                proxy=proxy_str,
                type="Unknown",
                status="INVALID",
                response_time=0,
                anonymity="None",
                country="Unknown",
                error="Invalid format"
            )
        
        protocol, host, port, username, password = parsed
        timeout = self.settings.get("timeout")
        retries = self.settings.get("retries")
        
        best_result = None
        
        protocols_to_try = [protocol] if protocol != "HTTP" else ["HTTP", "HTTPS"]
        
        for proto in protocols_to_try:
            for attempt in range(retries):
                if STOP_EVENT.is_set():
                    break
                result = await self.try_connect(host, port, proto, username, password, timeout)
                if result:
                    best_result = result
                    break
            if best_result:
                break
        
        if best_result:
            return best_result
        
        return ProxyResult(
            proxy=f"{host}:{port}",
            type="Unknown",
            status="FAILED",
            response_time=0,
            anonymity="None",
            country="Unknown",
            username=username,
            password=password,
            error="No response"
        )
    
    async def check_multiple(self, proxies: List[str]):
        self.results = []
        self.working_count = 0
        self.failed_count = 0
        
        new_proxies = []
        for p in proxies:
            normalized = self._normalize_proxy(p)
            if normalized not in self.existing_proxies:
                new_proxies.append(p)
        
        if len(new_proxies) < len(proxies):
            console.print(f"[yellow]Skipping {len(proxies) - len(new_proxies)} already checked[/yellow]")
        
        if not new_proxies:
            console.print("[yellow]All proxies already in file[/yellow]")
            return
        
        threads = self.settings.get("threads")
        semaphore = asyncio.Semaphore(threads)
        checked = 0
        total = len(new_proxies)
        
        async def check_with_limit(proxy):
            nonlocal checked
            async with semaphore:
                if STOP_EVENT.is_set():
                    return None
                result = await self.check_proxy(proxy)
                if result:
                    self.results.append(result)
                    if result.status == "WORKING":
                        self.working_count += 1
                    else:
                        self.failed_count += 1
                checked += 1
                if checked % 100 == 0 or checked == total:
                    console.print(f"[dim]Checked {checked}/{total}... Working: {self.working_count}[/dim]")
                return result
        
        await asyncio.gather(*[check_with_limit(p) for p in new_proxies])
    
    def display_results(self):
        console.print()
        total = len(self.results)
        working = self.working_count
        failed = self.failed_count
        
        summary = Panel(
            f"[green]Working: {working}[/green] | [red]Failed: {failed}[/red] | [yellow]Total: {total}[/yellow] | "
            f"[cyan]Success: {(working/total*100) if total > 0 else 0:.1f}%[/cyan]",
            title="[bold white]RESULTS[/bold white]",
            border_style="bright_cyan",
            padding=(1, 2)
        )
        console.print(summary)
        
        if working == 0:
            console.print("[red]No working proxies found[/red]")
            return
        
        table = Table(
            title="[bold white]WORKING PROXIES[/bold white]",
            box=box.DOUBLE_EDGE,
            show_header=True,
            header_style="bold bright_cyan",
            border_style="bright_cyan",
            row_styles=["none", "dim"]
        )
        table.add_column("Proxy", style="bright_cyan", width=25)
        table.add_column("Type", style="bright_magenta", width=10)
        table.add_column("Speed(ms)", style="bright_yellow", width=12)
        table.add_column("Anonymity", style="bright_blue", width=12)
        
        sorted_results = sorted(
            [r for r in self.results if r.status == "WORKING"],
            key=lambda x: x.response_time
        )
        
        for r in sorted_results[:100]:
            table.add_row(r.proxy, r.type, str(r.response_time), r.anonymity)
        
        if len(sorted_results) > 100:
            table.add_row(f"... and {len(sorted_results) - 100} more", "", "", "")
        
        console.print(table)
        
        if self.settings.get("auto_save", True):
            self._save_working_proxies(sorted_results)
    
    def _save_working_proxies(self, working_results: List[ProxyResult]):
        export_format = self.settings.get("export_format", "all")
        
        lines = []
        for r in working_results:
            auth_str = f"{r.username}:{r.password}@" if r.username and r.password else ""
            
            if export_format == "ip_port":
                lines.append(f"{r.proxy}")
            elif export_format == "authenticated":
                if auth_str:
                    lines.append(f"{auth_str}{r.proxy}")
                else:
                    lines.append(f"{r.proxy}")
            elif export_format == "protocol_ip_port":
                protocol = r.type.lower().replace("https", "http")
                lines.append(f"{protocol}://{r.proxy}")
            else:
                protocol = r.type.lower().replace("https", "http")
                lines.append(f"{protocol}://{auth_str}{r.proxy}")
        
        mode = "a" if Path(VALID_PROXIES_FILE).exists() else "w"
        with open(VALID_PROXIES_FILE, mode) as f:
            if mode == "a" and f.tell() > 0:
                f.write("\n")
            f.write("\n".join(lines))
        
        console.print(f"\n[green]Saved {len(lines)} working proxies to {VALID_PROXIES_FILE}[/green]")


def print_banner():
    banner = """
[bold bright_cyan]
   ███████╗ ██████╗ █████╗ ██████╗ ███████╗
   ██╔════╝██╔════╝██╔══██╗██╔══██╗██╔════╝
   ███████╗██║     ███████║██████╔╝███████╗
   ╚════██║██║     ██╔══██║██╔══██╗╚════██║
   ███████║╚██████╗██║  ██║██║  ██║███████║
   ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝
[/bold bright_cyan]
[bold white]         PROXY SCRAPER & CHECKER[/bold white]
[dim]    High Performance | 30+ Sources | Smart Export[/dim]
"""
    console.print(banner)


def make_source_table(sources):
    table = Table(show_header=False, box=box.SIMPLE, padding=(0, 2))
    table.add_column("Source", style="bright_cyan")
    table.add_column("Status", justify="center")
    
    for name, enabled in sources.items():
        status = "[bright_green]●[/bright_green]" if enabled else "[bright_red]●[/bright_red]"
        display_name = name.replace("_", " ").title()
        table.add_row(display_name, status)
    
    return table


def select_sources(settings: SettingsManager) -> List[str]:
    sources = settings.get("sources")
    
    while True:
        console.print("\n[bold bright_cyan]╭─ SOURCE SELECTION ─╮[/bold bright_cyan]")
        
        items = list(sources.items())
        per_page = 15
        total_pages = (len(items) + per_page - 1) // per_page
        
        for page in range(total_pages):
            console.print(f"\n[dim]Page {page + 1}/{total_pages}[/dim]")
            start = page * per_page
            end = min(start + per_page, len(items))
            
            for i in range(start, end):
                name, enabled = items[i]
                status = "[bright_green]ON[/bright_green]" if enabled else "[bright_red]OFF[/bright_red]"
                display_name = name.replace("_", " ").title()
                console.print(f"  {i+1:2}. {display_name:30} {status}")
        
        console.print(f"\n[bright_yellow]a[/bright_yellow]. Toggle All")
        console.print(f"[bright_yellow]b[/bright_yellow]. Continue")
        console.print(f"[bright_yellow]q[/bright_yellow]. Back")
        
        choice = Prompt.ask("\n[bright_cyan]Select[/bright_cyan]", default="b")
        
        if choice.lower() == "a":
            new_state = not all(sources.values())
            for key in sources:
                sources[key] = new_state
            settings.set("sources", sources)
            console.print("[green]All sources toggled[/green]")
            
        elif choice.lower() == "b":
            selected = [k for k, v in sources.items() if v]
            return selected
            
        elif choice.lower() == "q":
            return []
            
        else:
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(items):
                    key = items[idx][0]
                    sources[key] = not sources[key]
                    settings.set("sources", sources)
                else:
                    console.print("[red]Invalid number[/red]")
            except ValueError:
                console.print("[red]Invalid input[/red]")


async def scrape_and_check(settings: SettingsManager):
    selected = select_sources(settings)
    
    if not selected:
        console.print("[red]No sources selected[/red]")
        return
    
    max_proxies = IntPrompt.ask("[bright_cyan]Max proxies to scrape (0 = unlimited)[/bright_cyan]", default=0)
    
    scraper = ProxyScraper(settings)
    checker = ProxyChecker(settings)
    
    console.print("\n[bright_yellow]Scraping...[/bright_yellow]")
    proxies = await scraper.scrape_selected(selected, max_proxies)
    
    if not proxies:
        console.print("[red]No proxies found[/red]")
        return
    
    console.print(f"\n[bright_green]Found {len(proxies)} unique proxies[/bright_green]\n")
    
    if Confirm.ask("[bright_cyan]Check proxies?[/bright_cyan]", default=True):
        await checker.check_multiple(proxies)
        checker.display_results()


def settings_menu(settings: SettingsManager):
    while True:
        console.print("\n[bold bright_cyan]╭─ SETTINGS ─╮[/bold bright_cyan]")
        console.print(f"[bright_white]1.[/bright_white] Threads: [bright_yellow]{settings.get('threads')}[/bright_yellow]")
        console.print(f"[bright_white]2.[/bright_white] Timeout: [bright_yellow]{settings.get('timeout')}s[/bright_yellow]")
        console.print(f"[bright_white]3.[/bright_white] Retries: [bright_yellow]{settings.get('retries')}[/bright_yellow]")
        console.print(f"[bright_white]4.[/bright_white] Export Format: [bright_yellow]{settings.get('export_format')}[/bright_yellow]")
        console.print(f"[bright_white]5.[/bright_white] Back")
        
        choice = Prompt.ask("[bright_cyan]Select[/bright_cyan]", choices=["1", "2", "3", "4", "5"], default="5")
        
        if choice == "1":
            val = IntPrompt.ask("Threads", default=settings.get("threads"))
            settings.set("threads", max(10, min(1000, val)))
        elif choice == "2":
            val = IntPrompt.ask("Timeout", default=settings.get("timeout"))
            settings.set("timeout", max(1, val))
        elif choice == "3":
            val = IntPrompt.ask("Retries", default=settings.get("retries"))
            settings.set("retries", max(1, min(5, val)))
        elif choice == "4":
            console.print("\n[dim]Formats:[/dim]")
            console.print("  ip_port           - Just IP:PORT")
            console.print("  authenticated     - USER:PASS@IP:PORT")
            console.print("  protocol_ip_port  - protocol://IP:PORT")
            console.print("  all               - protocol://USER:PASS@IP:PORT")
            val = Prompt.ask("Format", choices=["ip_port", "authenticated", "protocol_ip_port", "all"], 
                           default=settings.get("export_format"))
            settings.set("export_format", val)
        elif choice == "5":
            break


def main():
    print_banner()
    
    settings = SettingsManager()
    
    while True:
        console.print("\n[bold bright_cyan]╭─ MAIN MENU ─╮[/bold bright_cyan]")
        console.print("[bright_white]1.[/bright_white] Check single proxy")
        console.print("[bright_white]2.[/bright_white] Check proxies from file")
        console.print("[bright_white]3.[/bright_white] Scrape + Check")
        console.print("[bright_white]4.[/bright_white] Settings")
        console.print("[bright_white]5.[/bright_white] Exit")
        
        choice = Prompt.ask("[bright_cyan]Select[/bright_cyan]", choices=["1", "2", "3", "4", "5"], default="3")
        
        if choice == "1":
            proxy = Prompt.ask("[bright_cyan]Enter proxy (ip:port or user:pass@ip:port)[/bright_cyan]")
            if proxy:
                checker = ProxyChecker(settings)
                result = asyncio.run(checker.check_proxy(proxy))
                checker.results = [result]
                checker.working_count = 1 if result.status == "WORKING" else 0
                checker.failed_count = 0 if result.status == "WORKING" else 1
                checker.display_results()
                
        elif choice == "2":
            filename = Prompt.ask("[bright_cyan]Filename[/bright_cyan]", default="proxies.txt")
            filepath = Path(filename)
            
            if not filepath.exists():
                console.print(f"[red]File not found: {filename}[/red]")
                continue
            
            with open(filepath, "r") as f:
                proxies = [l.strip() for l in f if l.strip() and not l.startswith("#")]
            
            if not proxies:
                console.print("[red]No proxies found[/red]")
                continue
            
            console.print(f"\n[bright_green]Loaded {len(proxies)} proxies[/bright_green]\n")
            
            if Confirm.ask("[bright_cyan]Check?[/bright_cyan]", default=True):
                checker = ProxyChecker(settings)
                asyncio.run(checker.check_multiple(proxies))
                checker.display_results()
                
        elif choice == "3":
            asyncio.run(scrape_and_check(settings))
                
        elif choice == "4":
            settings_menu(settings)
                
        elif choice == "5":
            console.print("\n[bold bright_cyan]Goodbye![/bold bright_cyan]")
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[bold red]Exiting...[/bold red]")
        sys.exit(0)
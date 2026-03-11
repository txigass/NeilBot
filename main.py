"""
NEIL Dota 2 — Discord Bot
Melhorias v2:
  - Cache thread-safe com asyncio.Lock e TTLs diferenciados
  - get_season_timestamps() centralizado
  - get_season_rank() com cache de 10min
  - get_match_stats() com cache de 24h (resultado nunca muda)
  - asyncio.gather() em todos os comandos de grupo
  - Rate limiter para OpenDota (semaforo)
  - Deteccao de cookie expirado com log e aviso
  - Timeout em todas as requests HTTP
  - on_command_error global
  - SEASON_LABEL automatico
  - helpers reutilizaveis (base_embed, make_bar, ts_to_date, etc)
  - Ajuda sem campos duplicados
"""

import os
import asyncio
import discord
import aiohttp
import logging
from discord.ext import commands
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# ==============================================
# ENV
# ==============================================
load_dotenv()

DISCORD_TOKEN         = os.getenv("DISCORD_TOKEN")
FACEIT_API_KEY        = os.getenv("FACEIT_API_KEY")
FACEIT_SESSION_COOKIE = os.getenv("FACEIT_SESSION_COOKIE", "")

HUB_ID     = "87489b24-246e-40e4-b459-cad1113e94d4"
SEASON     = 3
GAME       = "dota2"
BANNER_URL = "https://i.imgur.com/UQWsV0T.jpeg"

SEASON_LABEL     = f"Season {SEASON} (Ongoing)"
DISCORD_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
WEBHOOK_SECRET   = os.getenv("FACEIT_WEBHOOK_SECRET", "")
WEBHOOK_PORT     = int(os.getenv("WEBHOOK_PORT", "8080"))


# ==============================================
# STORAGE — vinculacoes FACEIT <-> Discord
# Salvo em arquivo JSON para persistir entre reinicializacoes
# ==============================================
import json
import hmac
import hashlib
import sqlite3

# ==============================================
# CONFIG — Double Down (DD)
# ==============================================
DD_LOG_CHANNEL_ID = int(os.getenv("DD_LOG_CHANNEL_ID", "0"))  # canal de logs de admin
DD_PER_GAMES      = 10    # a cada N partidas, ganha 1 DD
DD_MAX            = 5     # teto de DDs por season
DD_DB_FILE        = "dd_season.db"

def load_vinculos() -> dict:
    """Carrega vínculos do SQLite para memória."""
    con = sqlite3.connect(DD_DB_FILE)
    rows = con.execute("SELECT discord_id, faceit_nick FROM vinculos").fetchall()
    con.close()
    return {row[0]: row[1] for row in rows}

def save_vinculos(data: dict):
    """Não utilizado — vínculos são salvos diretamente no SQLite."""
    pass

def vinculos_add(discord_id: str, faceit_nick: str):
    con = sqlite3.connect(DD_DB_FILE)
    con.execute("""
        INSERT INTO vinculos (discord_id, faceit_nick) VALUES (?,?)
        ON CONFLICT(discord_id) DO UPDATE SET faceit_nick=excluded.faceit_nick
    """, (discord_id, faceit_nick.lower()))
    con.commit()
    con.close()

def vinculos_remove(discord_id: str):
    con = sqlite3.connect(DD_DB_FILE)
    con.execute("DELETE FROM vinculos WHERE discord_id=?", (discord_id,))
    con.commit()
    con.close()

def vinculos_get_nick(discord_id: str) -> str | None:
    con = sqlite3.connect(DD_DB_FILE)
    row = con.execute("SELECT faceit_nick FROM vinculos WHERE discord_id=?", (discord_id,)).fetchone()
    con.close()
    return row[0] if row else None

def vinculos_get_discord_id(faceit_nick: str) -> str | None:
    con = sqlite3.connect(DD_DB_FILE)
    row = con.execute("SELECT discord_id FROM vinculos WHERE faceit_nick=?", (faceit_nick.lower(),)).fetchone()
    con.close()
    return row[0] if row else None

def vinculos_get_all() -> dict:
    con = sqlite3.connect(DD_DB_FILE)
    rows = con.execute("SELECT discord_id, faceit_nick FROM vinculos").fetchall()
    con.close()
    return {row[0]: row[1] for row in rows}

# {discord_id_str: faceit_nick_lower} — carregado após dd_init_db()
VINCULOS: dict = {}

# Partidas ativas: {match_id: {"category": category_id, "ch1": ch1_id, "ch2": ch2_id}}
ACTIVE_MATCHES: dict = {}

# ==============================================
# BANCO DE DADOS — Double Down (DD)
# Fluxo:
#   1. Jogador usa !dobrar ANTES da partida → cria aposta pendente
#   2. Admin usa !validar @user vitoria|derrota → fecha aposta, aplica pontos
#   3. !meusdds → jogador vê histórico do mês
#
# Pontos: vitória normal +20 / derrota normal -10
#         vitória com DD  +40 / derrota com DD  -20
# Saldo de DDs: a cada 10 partidas na season ganha 1 DD (máx 5)
# ==============================================
DD_PTS_WIN      = 15
DD_PTS_LOSS     = -10
DD_BONUS_WIN    = 15   # bônus extra por usar DD em vitória  (+30 total)
DD_BONUS_LOSS   = -10  # bônus extra negativo em derrota     (-20 total)

def dd_init_db():
    con = sqlite3.connect(DD_DB_FILE)
    # Saldo de DDs por season (quantos já foram usados)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dd_saldo (
            faceit_nick TEXT NOT NULL,
            season      INTEGER NOT NULL,
            usados      INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (faceit_nick, season)
        )
    """)
    # Apostas individuais (cada vez que o jogador usa !dobrar)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dd_apostas (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            faceit_nick  TEXT NOT NULL,
            discord_nick TEXT NOT NULL DEFAULT '',
            season       INTEGER NOT NULL,
            ts_aposta    TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'pendente',
            resultado    TEXT,
            pts_base     INTEGER,
            pts_bonus    INTEGER,
            pts_total    INTEGER,
            ts_validado  TEXT,
            admin_nick   TEXT
        )
    """)
    # Tabela de vínculos Discord ↔ Faceit
    con.execute("""
        CREATE TABLE IF NOT EXISTS vinculos (
            discord_id  TEXT PRIMARY KEY,
            faceit_nick TEXT NOT NULL
        )
    """)
    # Migração: adiciona coluna se não existir (compatibilidade com DB antigo)
    try:
        con.execute("ALTER TABLE dd_apostas ADD COLUMN discord_nick TEXT NOT NULL DEFAULT ''")
        con.commit()
    except Exception:
        pass
    con.commit()
    con.close()

    # Migração: importa vinculos.json para o SQLite (só na primeira vez)
    import os as _os
    if _os.path.exists("vinculos.json"):
        try:
            import json as _json
            with open("vinculos.json") as _f:
                _data = _json.load(_f)
            if _data:
                _con = sqlite3.connect(DD_DB_FILE)
                for _did, _nick in _data.items():
                    _con.execute("""
                        INSERT INTO vinculos (discord_id, faceit_nick) VALUES (?,?)
                        ON CONFLICT(discord_id) DO NOTHING
                    """, (_did, _nick.lower()))
                _con.commit()
                _con.close()
                log.info(f"[VINCULOS] Migrados {len(_data)} vínculos do JSON para o SQLite.")
                _os.rename("vinculos.json", "vinculos.json.bak")
        except Exception as _e:
            log.warning(f"[VINCULOS] Falha na migração do JSON: {_e}")

def dd_get_usados(nick: str, season: int) -> int:
    con = sqlite3.connect(DD_DB_FILE)
    row = con.execute(
        "SELECT usados FROM dd_saldo WHERE faceit_nick=? AND season=?",
        (nick.lower(), season)
    ).fetchone()
    con.close()
    return row[0] if row else 0

def dd_incrementar_usado(nick: str, season: int):
    con = sqlite3.connect(DD_DB_FILE)
    con.execute("""
        INSERT INTO dd_saldo (faceit_nick, season, usados) VALUES (?,?,1)
        ON CONFLICT(faceit_nick, season) DO UPDATE SET usados=usados+1
    """, (nick.lower(), season))
    con.commit()
    con.close()

def dd_criar_aposta(nick: str, discord_nick: str, season: int) -> int:
    from datetime import timedelta
    ts = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M (Brasília)")
    con = sqlite3.connect(DD_DB_FILE)
    cur = con.execute(
        "INSERT INTO dd_apostas (faceit_nick, discord_nick, season, ts_aposta) VALUES (?,?,?,?)",
        (nick.lower(), discord_nick, season, ts)
    )
    aposta_id = cur.lastrowid
    con.commit()
    con.close()
    return aposta_id

def dd_get_pendente(nick: str, season: int):
    con = sqlite3.connect(DD_DB_FILE)
    row = con.execute(
        "SELECT id, ts_aposta FROM dd_apostas WHERE faceit_nick=? AND season=? AND status='pendente' ORDER BY id DESC LIMIT 1",
        (nick.lower(), season)
    ).fetchone()
    con.close()
    return row  # (id, ts_aposta) ou None

def dd_validar_aposta(aposta_id: int, resultado: str, admin_nick: str):
    """resultado: 'vitoria' ou 'derrota'"""
    from datetime import timedelta
    ts = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M (Brasília)")
    if resultado == "vitoria":
        pts_base  = DD_PTS_WIN
        pts_bonus = DD_BONUS_WIN
    else:
        pts_base  = DD_PTS_LOSS
        pts_bonus = DD_BONUS_LOSS
    pts_total = pts_base + pts_bonus
    con = sqlite3.connect(DD_DB_FILE)
    con.execute("""
        UPDATE dd_apostas SET
            status='validado', resultado=?, pts_base=?, pts_bonus=?,
            pts_total=?, ts_validado=?, admin_nick=?
        WHERE id=?
    """, (resultado, pts_base, pts_bonus, pts_total, ts, admin_nick, aposta_id))
    con.commit()
    con.close()
    return pts_base, pts_bonus, pts_total

def dd_get_historico_mes(nick: str, season: int) -> list:
    """Retorna apostas validadas do mês atual."""
    con = sqlite3.connect(DD_DB_FILE)
    rows = con.execute("""
        SELECT resultado, pts_base, pts_bonus, pts_total, ts_validado, admin_nick
        FROM dd_apostas
        WHERE faceit_nick=? AND season=? AND status='validado'
        ORDER BY ts_validado DESC
    """, (nick.lower(), season)).fetchall()
    con.close()
    return rows

def dd_get_relatorio_mes(season: int) -> list:
    """Retorna todos os jogadores com apostas validadas na season, com totais."""
    con = sqlite3.connect(DD_DB_FILE)
    rows = con.execute("""
        SELECT
            faceit_nick,
            COUNT(*) as total_apostas,
            SUM(CASE WHEN resultado='vitoria' THEN 1 ELSE 0 END) as vitorias,
            SUM(CASE WHEN resultado='derrota' THEN 1 ELSE 0 END) as derrotas,
            SUM(pts_base) as pts_base_total,
            SUM(pts_bonus) as pts_bonus_total,
            SUM(pts_total) as pts_total
        FROM dd_apostas
        WHERE season=? AND status='validado'
        GROUP BY faceit_nick
        ORDER BY pts_total DESC
    """, (season,)).fetchall()
    con.close()
    return rows

def dd_get_pendentes_todos(season: int) -> list:
    """Retorna todos os jogadores com apostas pendentes na season."""
    con = sqlite3.connect(DD_DB_FILE)
    rows = con.execute("""
        SELECT faceit_nick, discord_nick, ts_aposta, id
        FROM dd_apostas
        WHERE season=? AND status='pendente'
        ORDER BY ts_aposta ASC
    """, (season,)).fetchall()
    con.close()
    return rows

def dd_get_pendentes_todos(season: int) -> list:
    """Retorna todos os jogadores com apostas pendentes na season."""
    con = sqlite3.connect(DD_DB_FILE)
    rows = con.execute("""
        SELECT faceit_nick, discord_nick, ts_aposta, id
        FROM dd_apostas
        WHERE season=? AND status='pendente'
        ORDER BY ts_aposta ASC
    """, (season,)).fetchall()
    con.close()
    return rows

async def dd_calc_saldo(faceit_nick: str) -> dict:
    """Calcula saldo disponível de DDs para usar."""
    player = await get_player(faceit_nick)
    if not player:
        return None
    pid      = player["player_id"]
    matches  = await get_history(pid, mode="season")
    partidas  = len(matches)
    ganhos    = min(partidas // DD_PER_GAMES, DD_MAX)
    usados    = dd_get_usados(faceit_nick, SEASON)
    pendente  = dd_get_pendente(faceit_nick, SEASON)
    disponivel = max(ganhos - usados, 0)
    return {
        "nick":       player.get("nickname", faceit_nick),
        "partidas":   partidas,
        "ganhos":     ganhos,
        "usados":     usados,
        "disponivel": disponivel,
        "pendente":   pendente,  # (id, ts) ou None
    }
# ==============================================
# LOG
# ==============================================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("neil-bot")

# ==============================================
# DISCORD
# ==============================================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

session = None

# ==============================================
# CACHE com TTLs diferenciados
# ==============================================
_cache      = {}
_cache_lock = asyncio.Lock()

TTL_PLAYER = timedelta(minutes=5)
TTL_RANK   = timedelta(minutes=10)
TTL_HEROES = timedelta(minutes=10)
TTL_LANES  = timedelta(minutes=10)
TTL_MATCH  = timedelta(hours=24)

async def cache_get(key):
    async with _cache_lock:
        entry = _cache.get(key)
        if not entry:
            return None
        if datetime.now() > entry["expire"]:
            del _cache[key]
            return None
        return entry["value"]

async def cache_set(key, value, ttl=TTL_PLAYER):
    async with _cache_lock:
        _cache[key] = {"value": value, "expire": datetime.now() + ttl}

# ==============================================
# UTILS
# ==============================================
def get_season_timestamps():
    """Retorna (ts_from, ts_to) do mes atual."""
    hoje   = datetime.now()
    inicio = datetime(hoje.year, hoje.month, 1)
    fim    = (
        datetime(hoje.year + 1, 1, 1)
        if hoje.month == 12
        else datetime(hoje.year, hoje.month + 1, 1)
    )
    return int(inicio.timestamp()), int(fim.timestamp()) - 1

def ts_to_date(ts):
    if not ts:
        return "?"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d/%m/%Y")

def top_dict(d, n):
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]

def steam64_to_account_id(steam_id_64):
    if not steam_id_64:
        return None
    s = str(steam_id_64).strip()
    if s.startswith("[U:1:"):
        try:
            return int(s[5:-1])
        except ValueError:
            return None
    try:
        val = int(s)
        return val - 76561197960265728 if val > 76561197960265728 else val
    except ValueError:
        return None

def hero_lines_str(heroes, n=3):
    if not heroes:
        return "Sem dados."
    return "\n".join(
        f"• {h['name']} — {h['matches']} partidas ({h['wr']}% WR)"
        for h in heroes[:n]
    )

def make_bar(pct, size=10, filled="🟠", empty="⚫"):
    n = round(pct / (100 / size))
    return filled * n + empty * (size - n)

def get_steam_id(player):
    return (
        player.get("games", {}).get("dota2", {}).get("game_player_id")
        or player.get("steam_id_64")
    )

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

# ==============================================
# RATE LIMITER — OpenDota
# ==============================================
_opendota_sem = asyncio.Semaphore(5)

# ==============================================
# MAPA DE HEROIS DOTA 2
# ==============================================
DOTA_HEROES = {
    "1":"Anti-Mage","2":"Axe","3":"Bane","4":"Bloodseeker","5":"Crystal Maiden",
    "6":"Drow Ranger","7":"Earthshaker","8":"Juggernaut","9":"Mirana","10":"Morphling",
    "11":"Shadow Fiend","12":"Phantom Lancer","13":"Puck","14":"Pudge","15":"Razor",
    "16":"Sand King","17":"Storm Spirit","18":"Sven","19":"Tiny","20":"Vengeful Spirit",
    "21":"Windranger","22":"Zeus","23":"Kunkka","25":"Lina","26":"Lion",
    "27":"Shadow Shaman","28":"Slardar","29":"Tidehunter","30":"Witch Doctor","31":"Lich",
    "32":"Riki","33":"Enigma","34":"Tinker","35":"Sniper","36":"Necrophos",
    "37":"Warlock","38":"Beastmaster","39":"Queen of Pain","40":"Venomancer",
    "41":"Faceless Void","42":"Wraith King","43":"Death Prophet","44":"Phantom Assassin",
    "45":"Pugna","46":"Templar Assassin","47":"Viper","48":"Luna","49":"Dragon Knight",
    "50":"Dazzle","51":"Clockwerk","52":"Leshrac","53":"Nature's Prophet","54":"Lifestealer",
    "55":"Dark Seer","56":"Clinkz","57":"Omniknight","58":"Enchantress","59":"Huskar",
    "60":"Night Stalker","61":"Broodmother","62":"Bounty Hunter","63":"Weaver",
    "64":"Jakiro","65":"Batrider","66":"Chen","67":"Spectre","68":"Ancient Apparition",
    "69":"Doom","70":"Ursa","71":"Spirit Breaker","72":"Gyrocopter","73":"Alchemist",
    "74":"Invoker","75":"Silencer","76":"Outworld Destroyer","77":"Lycan","78":"Brewmaster",
    "79":"Shadow Demon","80":"Lone Druid","81":"Chaos Knight","82":"Meepo","83":"Treant Protector",
    "84":"Ogre Magi","85":"Undying","86":"Rubick","87":"Disruptor","88":"Nyx Assassin",
    "89":"Naga Siren","90":"Keeper of the Light","91":"Io","92":"Visage","93":"Slark",
    "94":"Medusa","95":"Troll Warlord","96":"Centaur Warrunner","97":"Magnus","98":"Timbersaw",
    "99":"Bristleback","100":"Tusk","101":"Skywrath Mage","102":"Abaddon","103":"Elder Titan",
    "104":"Legion Commander","105":"Techies","106":"Ember Spirit","107":"Earth Spirit",
    "108":"Underlord","109":"Terrorblade","110":"Phoenix","111":"Oracle","112":"Winter Wyvern",
    "113":"Arc Warden","114":"Monkey King","119":"Dark Willow","120":"Pangolier",
    "121":"Grimstroke","123":"Hoodwink","126":"Void Spirit","128":"Snapfire","129":"Mars",
    "135":"Dawnbreaker","136":"Marci","137":"Primal Beast","138":"Muerta",
}

LANE_LABELS = {
    1: "Carry 🗡️",
    2: "Mid 🔮",
    3: "Offlane 🛡️",
    4: "Soft Supp 🌿",
    5: "Hard Supp 💚",
}

# ==============================================
# HTTP HELPERS
# ==============================================
TIMEOUT = aiohttp.ClientTimeout(total=15)

# Semaforo global: max 5 requests simultaneas para a API do FACEIT
_faceit_sem = asyncio.Semaphore(5)

async def faceit_get(url, params=None, _retries=3):
    """
    GET autenticado na API publica do FACEIT.
    - Semaforo limita a 5 requests simultaneas
    - Retry automatico com backoff em caso de 429
    """
    async with _faceit_sem:
        for attempt in range(_retries):
            try:
                async with session.get(
                    url,
                    headers={"Authorization": f"Bearer {FACEIT_API_KEY}"},
                    params=params,
                    timeout=TIMEOUT
                ) as r:
                    log.info(f"GET {r.url} -> {r.status}")
                    if r.status == 404:
                        return {}
                    if r.status == 429:
                        wait = 2 ** attempt  # 1s, 2s, 4s
                        log.warning(f"Rate limit FACEIT (429), aguardando {wait}s... (tentativa {attempt+1}/{_retries})")
                        await asyncio.sleep(wait)
                        continue
                    r.raise_for_status()
                    return await r.json()
            except asyncio.TimeoutError:
                log.warning(f"Timeout [{attempt+1}/{_retries}]: {url}")
                if attempt < _retries - 1:
                    await asyncio.sleep(1)
            except Exception as e:
                log.warning(f"Erro faceit_get [{attempt+1}/{_retries}]: {e}")
                if attempt < _retries - 1:
                    await asyncio.sleep(1)
        log.error(f"Desistindo apos {_retries} tentativas: {url}")
        return {}

async def internal_get(url, params=None):
    if not FACEIT_SESSION_COOKIE:
        log.error("FACEIT_SESSION_COOKIE nao configurado!")
        return {}
    headers = {
        "accept":          "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "faceit-referer":  "web-next",
        "referer":         f"https://www.faceit.com/en/club/be58bad1-052c-4fc9-94f3-cd7d6da8b6b5/leaderboards/{HUB_ID}/leaderboard?type=season&season={SEASON}",
        "user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "sec-fetch-dest":  "empty",
        "sec-fetch-mode":  "cors",
        "sec-fetch-site":  "same-origin",
        "cookie":          f"__Host-FaceitGatewayAuthorization={FACEIT_SESSION_COOKIE}",
    }
    try:
        async with session.get(url, headers=headers, params=params, timeout=TIMEOUT) as r:
            log.info(f"GET (internal) {r.url} -> {r.status}")
            if r.status in (401, 403):
                log.error("Cookie FACEIT expirado! Atualize FACEIT_SESSION_COOKIE no .env")
                return {"_cookie_expired": True}
            if r.status != 200:
                body = await r.text()
                log.warning(f"Erro {r.status}: {body[:300]}")
                return {}
            data = await r.json(content_type=None)
            return data.get("payload", data)
    except asyncio.TimeoutError:
        log.warning(f"Timeout internal: {url}")
        return {}
    except Exception as e:
        log.warning(f"Erro internal_get: {e}")
        return {}

async def opendota_get(url, params=None):
    async with _opendota_sem:
        try:
            async with session.get(
                url,
                headers={"Accept": "application/json"},
                params=params,
                timeout=TIMEOUT
            ) as r:
                log.info(f"GET (opendota) {r.url} -> {r.status}")
                if r.status == 429:
                    log.warning("OpenDota rate limit! Aguardando 5s...")
                    await asyncio.sleep(5)
                    return None
                if r.status != 200:
                    return None
                return await r.json(content_type=None)
        except asyncio.TimeoutError:
            log.warning(f"Timeout opendota: {url}")
            return None
        except Exception as e:
            log.warning(f"Erro opendota_get: {e}")
            return None

# ==============================================
# PLAYER
# ==============================================
async def get_player(nickname):
    key    = f"player:{nickname.lower()}"
    cached = await cache_get(key)
    if cached:
        return cached
    data = await faceit_get("https://open.faceit.com/data/v4/players", {"nickname": nickname})
    if "player_id" in data:
        await cache_set(key, data, TTL_PLAYER)
        return data
    return None

# ==============================================
# LEADERBOARD
# ==============================================
async def get_leaderboard_page(offset=0, limit=50):
    payload = await internal_get(
        f"https://www.faceit.com/api/leaderboard/v1/ranking/hub/{HUB_ID}",
        {"leaderboardType": "hub_season", "season": SEASON, "limit": limit, "offset": offset}
    )
    if payload.get("_cookie_expired"):
        return [{"_cookie_expired": True}]
    return payload.get("rankings", [])

async def get_season_rank(player_id):
    key    = f"rank:{player_id}"
    cached = await cache_get(key)
    if cached:
        return cached

    limit = 50
    for offset in range(0, 2000, limit):
        rankings = await get_leaderboard_page(offset, limit)
        if not rankings:
            break
        if rankings[0].get("_cookie_expired"):
            return 0, "N/A"
        for entry in rankings:
            pid = entry.get("placement", {}).get("entity_id", "")
            if str(pid) == str(player_id):
                result = (entry.get("points", 0), entry.get("position", "N/A"))
                await cache_set(key, result, TTL_RANK)
                return result
        if len(rankings) < limit:
            break

    return 0, "N/A"

async def get_leaderboard_players(n=20):
    rankings = await get_leaderboard_page(0, limit=min(n, 50))
    return [
        {"pid": e.get("placement", {}).get("entity_id"),
         "nickname": e.get("placement", {}).get("entity_name", "?")}
        for e in rankings
        if e.get("placement", {}).get("entity_id")
    ]

# ==============================================
# MATCH STATS com cache 24h
# ==============================================
async def get_match_stats(match_id):
    key    = f"match:{match_id}"
    cached = await cache_get(key)
    if cached:
        return cached
    data = await faceit_get(f"https://open.faceit.com/data/v4/matches/{match_id}/stats")
    if data:
        await cache_set(key, data, TTL_MATCH)
    return data or {}

def extract_hero_and_faction(rounds, player_id):
    if not rounds:
        return "", ""
    for idx, team in enumerate(rounds[0].get("teams", [])):
        for p in team.get("players", []):
            if p.get("player_id") == player_id:
                hero_id = str(p.get("player_stats", {}).get("Hero", ""))
                hero    = DOTA_HEROES.get(hero_id, "") if hero_id else ""
                return hero, f"faction{idx + 1}"
    return "", ""

# ==============================================
# HISTORY — com filtro de season opcional
# ==============================================
async def get_history(player_id, mode="season", limit=100):
    """
    mode="season" -> partidas do mes atual (uma pagina, rapido)
    mode="alltime" -> pagina todas as partidas sem limite
    """
    if mode == "season":
        ts_from, ts_to = get_season_timestamps()
        data = await faceit_get(
            f"https://open.faceit.com/data/v4/players/{player_id}/history",
            {"game": GAME, "limit": 100, "from": ts_from, "to": ts_to}
        )
        return data.get("items", [])

    # alltime: pagina ate buscar tudo, sem limite artificial
    # Cache de 30min para evitar repaginar historicos longos
    cache_key = f"history_alltime:{player_id}"
    cached = await cache_get(cache_key)
    if cached:
        log.info(f"History alltime CACHE HIT: {player_id} ({len(cached)} itens)")
        return cached

    all_matches = []
    page_size   = 100
    offset      = 0
    while True:
        data = await faceit_get(
            f"https://open.faceit.com/data/v4/players/{player_id}/history",
            {"game": GAME, "limit": page_size, "offset": offset}
        )
        items = data.get("items", [])
        all_matches.extend(items)
        log.info(f"History alltime offset={offset}: {len(items)} itens (total={len(all_matches)})")
        if len(items) < page_size:
            break
        offset += page_size

    await cache_set(cache_key, all_matches, timedelta(minutes=30))
    return all_matches

# ==============================================
# HEROES — season atual
# ==============================================
async def get_faceit_heroes(player_id, top_n=3):
    key    = f"faceit_heroes:{player_id}"
    cached = await cache_get(key)
    if cached:
        return cached

    matches = await get_history(player_id, mode="season", limit=50)
    log.info(f"FACEIT history: {len(matches)} partidas")

    hero_count = {}
    hero_wins  = {}

    async def process(match):
        mid    = match.get("match_id")
        winner = match.get("results", {}).get("winner")
        stats  = await get_match_stats(mid)
        rounds = stats.get("rounds", [])
        hero, faction = extract_hero_and_faction(rounds, player_id)
        if not hero:
            return
        hero_count[hero] = hero_count.get(hero, 0) + 1
        if winner and winner == faction:
            hero_wins[hero] = hero_wins.get(hero, 0) + 1

    await asyncio.gather(*[process(m) for m in matches])

    if not hero_count:
        return []

    result = [
        {"name": name, "matches": count, "wr": round(hero_wins.get(name, 0) / count * 100, 1)}
        for name, count in top_dict(hero_count, top_n)
    ]
    await cache_set(key, result, TTL_HEROES)
    return result

# ==============================================
# HEROES — carreira
# ==============================================
async def get_top_heroes(player_id, top_n=3):
    key    = f"top_heroes:{player_id}"
    cached = await cache_get(key)
    if cached:
        return cached

    data = await faceit_get(f"https://open.faceit.com/data/v4/players/{player_id}/stats/{GAME}")
    segs = [
        s for s in data.get("segments", [])
        if isinstance(s.get("stats"), dict) and "Matches" in s["stats"]
    ]
    if not segs:
        return []

    segs.sort(key=lambda x: int(x["stats"].get("Matches", 0)), reverse=True)
    result = [
        {"name": h.get("label", "?"), "matches": int(h["stats"].get("Matches", 0)),
         "wr": float(h["stats"].get("Win Rate %", 0))}
        for h in segs[:top_n]
    ]
    await cache_set(key, result, TTL_HEROES)
    return result

# ==============================================
# LANES — OpenDota
# ==============================================
async def get_top_lanes(steam_id_64, top_n=2):
    account_id = steam64_to_account_id(steam_id_64)
    if not account_id:
        return []

    key    = f"lanes:{account_id}"
    cached = await cache_get(key)
    if cached:
        return cached

    log.info(f"OpenDota account_id={account_id} (steam_id_64={steam_id_64})")
    # Testa se o perfil existe e esta publico
    profile = await opendota_get(f"https://api.opendota.com/api/players/{account_id}")
    if profile:
        private = profile.get("profile", {}) == {} or profile.get("leaderboard_rank") is None
        expose  = profile.get("solo_competitive_rank")
        log.info(f"OpenDota profile: name={profile.get('profile',{}).get('personaname')} | expose_public_matches={profile.get('profile',{}).get('fh_unavailable')}")

    matches = await opendota_get(
        f"https://api.opendota.com/api/players/{account_id}/matches",
        {"limit": 50, "significant": 0}
    )
    log.info(f"OpenDota matches retornados: {len(matches) if matches else 0}")
    if not matches:
        return []

    lane_count = {}
    for m in matches:
        role = m.get("lane_role")
        if role and role in LANE_LABELS:
            lane_count[role] = lane_count.get(role, 0) + 1

    if not lane_count:
        return []

    result = [
        {"name": LANE_LABELS[lane], "matches": count}
        for lane, count in top_dict(lane_count, top_n)
    ]
    await cache_set(key, result, TTL_LANES)
    return result

# ==============================================
# EMBED FACTORY
# ==============================================
def base_embed(title, color=0xFF4500, description=None, footer=None):
    e = discord.Embed(title=title, color=color, description=description)
    e.set_image(url=BANNER_URL)
    e.set_footer(text=footer or f"NEIL Dota 2 • {SEASON_LABEL}")
    return e

# ==============================================
# COMANDO !faceit
# ==============================================
@bot.command()
async def faceit(ctx, nickname=None):
    if not nickname:
        await ctx.send("❌ Use: !faceit [nickname]")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        pid = player["player_id"]
        (pontos, rank), heroes = await asyncio.gather(
            get_season_rank(pid),
            get_faceit_heroes(pid)
        )

        embed = base_embed(f"📋 Perfil NEIL — {nickname}")
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])
        embed.add_field(name="🏆 Rank NEIL", value=f"#{rank}",  inline=True)
        embed.add_field(name="🔥 Pontos",    value=f"{pontos}", inline=True)
        embed.add_field(name="🦸 Heróis (season atual)", value=hero_lines_str(heroes), inline=False)

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !heroes
# ==============================================
@bot.command()
async def heroes(ctx, nickname=None):
    if not nickname:
        await ctx.send("❌ Use: !heroes [nickname]")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        pid      = player["player_id"]
        heroes_list, lanes = await asyncio.gather(
            get_top_heroes(pid),
            get_top_lanes(get_steam_id(player))
        )

        embed = base_embed(f"🦸 Heroes & Lanes — {nickname}", footer="NEIL Dota 2 • OpenDota")
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])
        embed.add_field(name="🦸 Heróis mais jogados", value=hero_lines_str(heroes_list), inline=False)
        embed.add_field(
            name="🗺️ Lanes (ult. 50 partidas)",
            value="\n".join(f"• {l['name']} — {l['matches']} partidas" for l in lanes)
                  if lanes else "Perfil privado ou sem dados.",
            inline=False
        )

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !top10
# ==============================================
@bot.command()
async def top10(ctx):
    async with ctx.typing():
        rankings = await get_leaderboard_page(0, 10)
        if not rankings or rankings[0].get("_cookie_expired"):
            await ctx.send("❌ Nao foi possivel carregar o ranking. Cookie pode ter expirado.")
            return

        lines = [
            "{} **{}** — {} pts".format(
                MEDALS.get(e.get('position'), f"`#{e.get('position')}`"),
                e.get('placement', {}).get('entity_name', '?'),
                e.get('points', 0)
            )
            for e in rankings
        ]
        embed = base_embed("🏆 Top 10 — NEIL Dota 2", description="\n".join(lines))

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !vs  (uso: !nick1 vs nick2 [alltime])
# ==============================================
@bot.command(name="vs")
async def vs_cmd(ctx, nick1=None, nick2=None, mode="season"):
    if not nick1 or not nick2:
        await ctx.send("❌ Use: ![nick1] vs [nick2]  ou  ![nick1] vs [nick2] alltime")
        return

    async with ctx.typing():
        p1, p2 = await asyncio.gather(get_player(nick1), get_player(nick2))
        if not p1:
            await ctx.send(f"❌ **{nick1}** nao encontrado.")
            return
        if not p2:
            await ctx.send(f"❌ **{nick2}** nao encontrado.")
            return

        pid1, pid2 = p1["player_id"], p2["player_id"]

        # Busca historico COMPLETO dos dois jogadores em paralelo
        # alltime: pagina tudo ate o fim do historico
        # season: apenas o mes atual
        hist1, hist2 = await asyncio.gather(
            get_history(pid1, mode),
            get_history(pid2, mode)
        )

        ids1 = {m.get("match_id"): m for m in hist1}
        ids2 = {m.get("match_id"): m for m in hist2}

        log.info(f"VS mode={mode}: hist1={len(ids1)} partidas, hist2={len(ids2)} partidas")

        # Interseccao por match_id garante resultado identico
        # independente de qual jogador foi digitado primeiro
        match_ids_comuns = set(ids1.keys()) & set(ids2.keys())
        confrontos = [ids1[mid] for mid in match_ids_comuns]

        log.info(f"VS: {len(match_ids_comuns)} partidas em comum (interseccao)")

        if not confrontos:
            periodo = "na season atual" if mode == "season" else "no historico"
            await ctx.send(f"❌ Nenhum confronto entre **{nick1}** e **{nick2}** {periodo}.")
            return

        vitorias_p1 = 0
        derrotas_p1 = 0
        heroes_p1   = {}
        heroes_p2   = {}
        ultimo      = None

        # winner no /matches/stats vem como UUID do time vencedor
        # team_id em rounds[0].teams[].team_id tambem e UUID
        # Comparamos UUID com UUID — absoluto e simetrico
        async def analisar(match):
            nonlocal vitorias_p1, derrotas_p1, ultimo
            mid     = match.get("match_id")
            started = match.get("started_at")
            if not ultimo or (started and started > ultimo):
                ultimo = started

            stats  = await get_match_stats(mid)
            rounds = stats.get("rounds", [])
            if not rounds:
                return

            # winner = UUID do time vencedor (em round_stats.Winner)
            # IMPORTANTE: player_id dentro de players[] e o UUID do TIME, nao do jogador
            # O unico identificador real do jogador e o nickname
            rs = rounds[0].get("round_stats", {})
            winner_uuid = rs.get("Winner") or rs.get("winner") or ""
            team_id_p1  = None
            hero_p1     = None
            hero_p2     = None

            nick1_lower = nick1.lower()
            nick2_lower = nick2.lower()

            for team in rounds[0].get("teams", []):
                team_id = team.get("team_id", "")
                for p in team.get("players", []):
                    pnick = p.get("nickname", "").lower()
                    h_id  = str(p.get("player_stats", {}).get("Hero", ""))
                    h_name = DOTA_HEROES.get(h_id, "")
                    if pnick == nick1_lower:
                        team_id_p1 = team_id
                        hero_p1    = h_name
                    elif pnick == nick2_lower:
                        hero_p2    = h_name

            if hero_p1:
                heroes_p1[hero_p1] = heroes_p1.get(hero_p1, 0) + 1
            if hero_p2:
                heroes_p2[hero_p2] = heroes_p2.get(hero_p2, 0) + 1

            if team_id_p1 and winner_uuid:
                if winner_uuid == team_id_p1:
                    vitorias_p1 += 1
                else:
                    derrotas_p1 += 1

        await asyncio.gather(*[analisar(m) for m in confrontos])

        total  = vitorias_p1 + derrotas_p1
        pct_p1 = round(vitorias_p1 / total * 100, 1) if total else 0
        pct_p2 = round(100 - pct_p1, 1)
        top2   = lambda d: ", ".join(f"{h} ({c}x)" for h, c in top_dict(d, 2)) or "Sem dados"
        label  = f"Season {SEASON}" if mode == "season" else "All Time"

        embed = discord.Embed(
            title=f"⚔️ {nick1}  vs  {nick2}",
            description=f"**{total}** confrontos diretos encontrados",
            color=0xFF4500
        )
        if p1.get("avatar"):
            embed.set_thumbnail(url=p1["avatar"])
        embed.add_field(
            name="📊 Placar",
            value=(f"**{nick1}**: {vitorias_p1}V / {derrotas_p1}D — {pct_p1}%\n"
                   f"**{nick2}**: {derrotas_p1}V / {vitorias_p1}D — {pct_p2}%"),
            inline=False
        )
        embed.add_field(name="🎯 Aproveitamento",     value=make_bar(pct_p1),       inline=False)
        embed.add_field(name=f"🦸 Heróis de {nick1}", value=top2(heroes_p1),        inline=True)
        embed.add_field(name=f"🦸 Heróis de {nick2}", value=top2(heroes_p2),        inline=True)
        embed.add_field(name="📅 Último confronto",   value=ts_to_date(ultimo),     inline=False)
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • Head to Head — {label}")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !streaks
# ==============================================
@bot.command()
async def streaks(ctx):
    async with ctx.typing():
        players = await get_leaderboard_players(20)
        if not players:
            await ctx.send("❌ Nao foi possivel carregar o leaderboard.")
            return

        async def calc_streak(entry):
            pid     = entry["pid"]
            matches = await get_history(pid, mode="season", limit=50)
            matches.sort(key=lambda m: m.get("finished_at", 0), reverse=True)
            streak = 0
            for match in matches:
                winner   = match.get("results", {}).get("winner")
                team_key = next(
                    (tk for tk, team in match.get("teams", {}).items()
                     for p in team.get("players", []) if p.get("player_id") == pid),
                    None
                )
                if winner and team_key and winner == team_key:
                    streak += 1
                else:
                    break
            return entry["nickname"], streak

        results          = await asyncio.gather(*[calc_streak(e) for e in players])
        best_nick, best  = max(results, key=lambda x: x[1])

        if best == 0:
            await ctx.send("❌ Nenhum jogador com streak ativo na season.")
            return

        player = await get_player(best_nick) or {}
        embed  = base_embed(
            "🔥 Maior Streak da Season",
            description=f"**{best_nick}** está em chamas com **{best} vitórias seguidas!**"
        )
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !rivalidade [alltime]
# ==============================================
@bot.command()
async def rivalidade(ctx, mode="season"):
    async with ctx.typing():
        players    = await get_leaderboard_players(20)
        if not players:
            await ctx.send("❌ Nao foi possivel carregar o leaderboard.")
            return

        player_ids  = {e["pid"] for e in players}
        pid_to_nick = {e["pid"]: e["nickname"] for e in players}
        pair_count  = {}

        async def count_pairs(entry):
            pid     = entry["pid"]
            matches = await get_history(pid, mode, limit=100)
            for match in matches:
                for team in match.get("teams", {}).values():
                    for p in team.get("players", []):
                        other = p.get("player_id")
                        if other and other != pid and other in player_ids:
                            key = tuple(sorted([pid, other]))
                            pair_count[key] = pair_count.get(key, 0) + 1

        await asyncio.gather(*[count_pairs(e) for e in players])

        if not pair_count:
            await ctx.send("❌ Sem dados de rivalidade.")
            return

        lines = [
            f"⚔️ **{pid_to_nick.get(a, '?')}** vs **{pid_to_nick.get(b, '?')}** — {count // 2} confrontos"
            for (a, b), count in top_dict(pair_count, 5)
        ]
        label = f"Season {SEASON}" if mode == "season" else "All Time"
        embed = discord.Embed(
            title=f"🏹 Maiores Rivalidades — {label}",
            description="\n".join(lines),
            color=0xFF4500
        )
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • {label}")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !stats [nick]
# ==============================================
@bot.command()
async def stats(ctx, nickname=None):
    if not nickname:
        await ctx.send("❌ Use: !stats [nickname]")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        account_id = steam64_to_account_id(get_steam_id(player))
        if not account_id:
            await ctx.send("❌ Nao foi possivel obter o Steam ID.")
            return

        totals_data, wl_data = await asyncio.gather(
            opendota_get(f"https://api.opendota.com/api/players/{account_id}/totals", {"date": 30}),
            opendota_get(f"https://api.opendota.com/api/players/{account_id}/wl",     {"date": 30})
        )

        if not totals_data:
            await ctx.send("❌ Perfil privado ou sem dados no OpenDota.")
            return

        totals  = {item["field"]: item for item in totals_data}
        n       = max(totals.get("kills", {}).get("n", 1) or 1, 1)
        kills   = totals.get("kills",        {}).get("sum", 0) / n
        deaths  = totals.get("deaths",       {}).get("sum", 0) / n
        assists = totals.get("assists",      {}).get("sum", 0) / n
        gpm     = totals.get("gold_per_min", {}).get("sum", 0) / n
        xpm     = totals.get("xp_per_min",   {}).get("sum", 0) / n

        wins   = (wl_data or {}).get("win",  0)
        losses = (wl_data or {}).get("lose", 0)
        total  = wins + losses
        wr     = round(wins / total * 100, 1) if total else 0

        embed = base_embed(f"📈 Stats — {nickname} (ult. 30 dias)", footer="NEIL Dota 2 • OpenDota (ult. 30 dias)")
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])
        embed.add_field(name="⚔️ KDA Médio", value=f"{kills:.1f}/{deaths:.1f}/{assists:.1f}", inline=True)
        embed.add_field(name="💀 Ratio",      value=f"{(kills+assists)/max(deaths,1):.2f}",   inline=True)
        embed.add_field(name="💰 GPM",        value=f"{int(gpm)}",                            inline=True)
        embed.add_field(name="✨ XPM",        value=f"{int(xpm)}",                            inline=True)
        embed.add_field(name="🏆 V/D",        value=f"{wins}V / {losses}D",                  inline=True)
        embed.add_field(name="📊 WR",         value=f"{wr}%",                                 inline=True)

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !form [nick]
# ==============================================
@bot.command()
async def form(ctx, nickname=None):
    if not nickname:
        await ctx.send("❌ Use: !form [nickname]")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        pid     = player["player_id"]
        history = await faceit_get(
            f"https://open.faceit.com/data/v4/players/{pid}/history",
            {"game": GAME, "limit": 5}
        )
        matches = history.get("items", [])
        if not matches:
            await ctx.send("❌ Sem partidas encontradas.")
            return

        async def get_result(match):
            winner = match.get("results", {}).get("winner")
            stats  = await get_match_stats(match.get("match_id"))
            hero, faction = extract_hero_and_faction(stats.get("rounds", []), pid)
            won = (winner == faction) if winner and faction else None
            return won, hero or "?"

        results = await asyncio.gather(*[get_result(m) for m in matches])

        icons = []
        lines = []
        for won, hero in results:
            icon  = "🟢" if won else ("🔴" if won is False else "⚫")
            label = "Vitória" if won else ("Derrota" if won is False else "?")
            icons.append(icon)
            lines.append(f"{icon} {hero} — {label}")

        embed = base_embed(
            f"📋 Forma Recente — {nickname}",
            description=" ".join(icons) + "\n\n" + "\n".join(lines),
            footer="NEIL Dota 2 • Últimas 5 partidas"
        )
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !maisjogas
# ==============================================
@bot.command()
async def maisjogas(ctx):
    async with ctx.typing():
        players = await get_leaderboard_players(20)
        if not players:
            await ctx.send("❌ Nao foi possivel carregar o leaderboard.")
            return

        async def count_matches(entry):
            matches = await get_history(entry["pid"], mode="season", limit=100)
            return entry["nickname"], len(matches)

        results = await asyncio.gather(*[count_matches(e) for e in players])
        results = sorted(results, key=lambda x: x[1], reverse=True)[:10]

        lines = [
            f"{MEDALS.get(i+1, f'`#{i+1}`')} **{nick}** — {total} partidas"
            for i, (nick, total) in enumerate(results)
        ]
        embed = base_embed("🎮 Quem Mais Jogou — Season Atual", description="\n".join(lines))

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !nemesis [nick] [alltime]
# ==============================================
@bot.command()
async def nemesis(ctx, nickname=None, mode="season"):
    if not nickname:
        await ctx.send("❌ Use: !nemesis [nickname]  ou  !nemesis [nickname] alltime")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        pid     = player["player_id"]
        matches = await get_history(pid, mode, limit=100)

        # {enemy_pid: {nick, wins_against, total_against}}
        enemy_stats = {}

        async def analisar(match):
            stats  = await get_match_stats(match.get("match_id"))
            rounds = stats.get("rounds", [])
            if not rounds:
                return

            # player_id em /matches/stats e UUID do TIME, nao do jogador
            # Usar nickname para identificar o jogador principal
            rs = rounds[0].get("round_stats", {})
            winner_uuid = rs.get("Winner") or rs.get("winner") or ""
            my_team_id  = None
            nick_lower  = nick.lower()

            for team in rounds[0].get("teams", []):
                for p in team.get("players", []):
                    if p.get("nickname", "").lower() == nick_lower:
                        my_team_id = team.get("team_id", "")

            if not my_team_id:
                return

            i_lost = (winner_uuid and winner_uuid != my_team_id)

            for team in rounds[0].get("teams", []):
                if team.get("team_id") == my_team_id:
                    continue  # pula meu proprio time
                for p in team.get("players", []):
                    enick = p.get("nickname", "?")
                    if not enick or enick == "?":
                        continue
                    if enick not in enemy_stats:
                        enemy_stats[enick] = {"nick": enick, "wins_against": 0, "total": 0}
                    enemy_stats[enick]["total"] += 1
                    if i_lost:
                        enemy_stats[enick]["wins_against"] += 1

        await asyncio.gather(*[analisar(m) for m in matches])

        # Filtra quem enfrentou pelo menos 2x e ordena por vitorias contra
        filtered = {
            epid: s for epid, s in enemy_stats.items()
            if s["total"] >= (20 if mode == "alltime" else 1)
        }

        if not filtered:
            periodo = "season atual" if mode == "season" else "historico"
            min_txt = " (min. 20 confrontos)" if mode == "alltime" else ""
            await ctx.send(f"❌ Sem dados de nemesis para **{nickname}** no {periodo}.{min_txt}")
            return

        top3 = sorted(
            filtered.values(),
            key=lambda x: x["wins_against"] / x["total"],
            reverse=True
        )[:3]
        lines = []
        for i, e in enumerate(top3):
            wr_contra = round(e["wins_against"] / e["total"] * 100, 1)
            icon = "🩸" if i == 0 else "💀"
            lines.append(
                f"{icon} **{e['nick']}** — {wr_contra}% WR contra você ({e['wins_against']}V/{e['total']-e['wins_against']}D em {e['total']} jogos)"
            )

        label = f"Season {SEASON}" if mode == "season" else "All Time"
        embed = discord.Embed(
            title=f"👹 Nemesis de {nickname} — {label}",
            description="\n".join(lines),
            color=0xFF4500
        )
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • {label} — só confrontos diretos (times opostos)")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !duo [nick1] [nick2]
# ==============================================
@bot.command()
async def duo(ctx, nick1=None, nick2=None):
    if not nick1 or not nick2:
        await ctx.send("❌ Use: !duo [nick1] [nick2]")
        return

    async with ctx.typing():
        p1, p2 = await asyncio.gather(get_player(nick1), get_player(nick2))
        if not p1:
            await ctx.send(f"❌ **{nick1}** nao encontrado.")
            return
        if not p2:
            await ctx.send(f"❌ **{nick2}** nao encontrado.")
            return

        pid1, pid2 = p1["player_id"], p2["player_id"]
        matches    = await get_history(pid1, mode="alltime", limit=100)

        juntos   = 0
        vitorias = 0
        ultimo   = None

        for match in matches:
            winner  = match.get("results", {}).get("winner")
            started = match.get("started_at")
            team_p1 = team_p2 = None

            for tk, team in match.get("teams", {}).items():
                for p in team.get("players", []):
                    if p.get("player_id") == pid1:
                        team_p1 = tk
                    elif p.get("player_id") == pid2:
                        team_p2 = tk

            if team_p1 and team_p2 and team_p1 == team_p2:
                juntos += 1
                if winner == team_p1:
                    vitorias += 1
                if not ultimo or (started and started > ultimo):
                    ultimo = started

        if juntos == 0:
            await ctx.send(f"❌ **{nick1}** e **{nick2}** nunca jogaram juntos.")
            return

        derrotas = juntos - vitorias
        wr       = round(vitorias / juntos * 100, 1)

        embed = discord.Embed(
            title=f"🤝 Duo — {nick1} & {nick2}",
            description=f"**{juntos}** partidas juntos",
            color=0x00AA00
        )
        if p1.get("avatar"):
            embed.set_thumbnail(url=p1["avatar"])
        embed.add_field(name="📊 Resultado",      value=f"{vitorias}V / {derrotas}D", inline=True)
        embed.add_field(name="📈 WR em Dupla",    value=f"{wr}%",                     inline=True)
        embed.add_field(name="🎯 Aproveitamento", value=make_bar(wr, filled="🟢"),    inline=False)
        embed.add_field(name="📅 Último juntos",  value=ts_to_date(ultimo),           inline=False)
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text="NEIL Dota 2 • Histórico completo")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !carrasco [nick] [alltime]
# Quem esse jogador mais derrotou
# ==============================================
@bot.command()
async def carrasco(ctx, nickname=None, mode="season"):
    if not nickname:
        await ctx.send("❌ Use: !carrasco [nickname]  ou  !carrasco [nickname] alltime")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        pid     = player["player_id"]
        matches = await get_history(pid, mode, limit=100)

        # {enemy_pid: {nick, losses_against, total}}
        victim_stats = {}

        async def analisar_carrasco(match):
            stats  = await get_match_stats(match.get("match_id"))
            rounds = stats.get("rounds", [])
            if not rounds:
                return

            rs = rounds[0].get("round_stats", {})
            winner_uuid = rs.get("Winner") or rs.get("winner") or ""
            my_team_id  = None
            nick_lower  = nick.lower()

            for team in rounds[0].get("teams", []):
                for p in team.get("players", []):
                    if p.get("nickname", "").lower() == nick_lower:
                        my_team_id = team.get("team_id", "")

            if not my_team_id:
                return

            i_won = (winner_uuid and winner_uuid == my_team_id)

            for team in rounds[0].get("teams", []):
                if team.get("team_id") == my_team_id:
                    continue
                for p in team.get("players", []):
                    enick = p.get("nickname", "?")
                    if not enick or enick == "?":
                        continue
                    if enick not in victim_stats:
                        victim_stats[enick] = {"nick": enick, "losses_against": 0, "total": 0}
                    victim_stats[enick]["total"] += 1
                    if i_won:
                        victim_stats[enick]["losses_against"] += 1

        await asyncio.gather(*[analisar_carrasco(m) for m in matches])

        filtered = {
            epid: s for epid, s in victim_stats.items()
            if s["total"] >= (20 if mode == "alltime" else 1)
        }

        if not filtered:
            min_txt = " (min. 20 confrontos)" if mode == "alltime" else ""
            await ctx.send(f"❌ Sem dados para **{nickname}**.{min_txt}")
            return

        top3 = sorted(
            filtered.values(),
            key=lambda x: x["losses_against"] / x["total"],
            reverse=True
        )[:3]
        lines = []
        for i, e in enumerate(top3):
            meu_wr = round(e["losses_against"] / e["total"] * 100, 1)
            icon = "⚔️" if i == 0 else "🗡️"
            lines.append(
                f"{icon} **{e['nick']}** — {meu_wr}% WR seu contra ele ({e['losses_against']}V/{e['total']-e['losses_against']}D em {e['total']} jogos)"
            )
        label = f"Season {SEASON}" if mode == "season" else "All Time"
        embed = discord.Embed(
            title=f"⚔️ Carrasco — {nickname} ({label})",
            description="\n".join(lines),
            color=0xFF4500
        )
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • {label}")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !meta
# Herois mais jogados nas partidas do hub na season
# Busca partidas diretamente do hub (nao do historico individual)
# para garantir que so partidas da NEIL sejam contadas
# ==============================================
async def get_hub_matches(limit=200) -> list:
    """Busca partidas recentes do hub da NEIL via API publica."""
    key    = f"hub_matches:{limit}"
    cached = await cache_get(key)
    if cached:
        return cached

    ts_from, ts_to = get_season_timestamps()
    all_matches = []
    page_size   = 20  # endpoint de hub usa limit menor
    offset      = 0

    while True:
        data = await faceit_get(
            f"https://open.faceit.com/data/v4/hubs/{HUB_ID}/matches",
            {"type": "past", "offset": offset, "limit": page_size}
        )
        items = data.get("items", [])
        if not items:
            break

        # Filtra pela season (mes atual)
        for match in items:
            started = match.get("started_at", 0)
            if started and ts_from <= started <= ts_to:
                all_matches.append(match)
            elif started and started < ts_from:
                # Partidas mais antigas que o mes — para de paginar
                await cache_set(key, all_matches, TTL_RANK)
                return all_matches

        offset += page_size
        if offset >= limit:
            break

    await cache_set(key, all_matches, TTL_RANK)
    log.info(f"Hub matches da season: {len(all_matches)}")
    return all_matches

@bot.command()
async def meta(ctx):
    async with ctx.typing():
        await ctx.send("⏳ Buscando partidas do hub... pode demorar alguns segundos.")

        hub_matches = await get_hub_matches(limit=300)
        if not hub_matches:
            await ctx.send("❌ Sem partidas encontradas no hub para a season atual.")
            return

        hero_count: dict[str, int] = {}
        hero_wins:  dict[str, int] = {}

        async def process(match):
            mid    = match.get("match_id")
            winner = match.get("results", {}).get("winner")
            stats  = await get_match_stats(mid)
            rounds = stats.get("rounds", [])
            if not rounds:
                return
            for idx, team in enumerate(rounds[0].get("teams", [])):
                faction = f"faction{idx + 1}"
                for p in team.get("players", []):
                    h_id   = str(p.get("player_stats", {}).get("Hero", ""))
                    h_name = DOTA_HEROES.get(h_id, "")
                    if h_name:
                        hero_count[h_name] = hero_count.get(h_name, 0) + 1
                        if winner == faction:
                            hero_wins[h_name] = hero_wins.get(h_name, 0) + 1

        await asyncio.gather(*[process(m) for m in hub_matches])

        if not hero_count:
            await ctx.send("❌ Sem dados de heróis nas partidas do hub.")
            return

        top10 = top_dict(hero_count, 10)
        total_picks = sum(hero_count.values())
        lines = []
        for i, (hero, count) in enumerate(top10):
            wr       = round(hero_wins.get(hero, 0) / count * 100, 1)
            pick_pct = round(count / len(hub_matches) / 2 * 100, 1)  # pick rate por jogo
            icon     = MEDALS.get(i + 1, f"`#{i+1}`")
            lines.append(f"{icon} **{hero}** — {count}x ({pick_pct}% pick rate, {wr}% WR)")

        embed = discord.Embed(
            title=f"📊 Meta NEIL — Season {SEASON}",
            description="\n".join(lines),
            color=0xFF4500
        )
        embed.add_field(
            name="📋 Base de dados",
            value=f"{len(hub_matches)} partidas do hub analisadas",
            inline=False
        )
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • Season {SEASON} — apenas partidas da NEIL")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !compare [nick1] [nick2]
# Compara dois jogadores lado a lado
# ==============================================
@bot.command()
async def compare(ctx, nick1=None, nick2=None):
    if not nick1 or not nick2:
        await ctx.send("❌ Use: !compare [nick1] [nick2]")
        return

    async with ctx.typing():
        p1, p2 = await asyncio.gather(get_player(nick1), get_player(nick2))
        if not p1:
            await ctx.send(f"❌ **{nick1}** nao encontrado.")
            return
        if not p2:
            await ctx.send(f"❌ **{nick2}** nao encontrado.")
            return

        pid1, pid2 = p1["player_id"], p2["player_id"]

        # Busca tudo em paralelo
        (pts1, rank1), (pts2, rank2), heroes1, heroes2, hist1, hist2 = await asyncio.gather(
            get_season_rank(pid1),
            get_season_rank(pid2),
            get_faceit_heroes(pid1, top_n=1),
            get_faceit_heroes(pid2, top_n=1),
            get_history(pid1, mode="season", limit=100),
            get_history(pid2, mode="season", limit=100),
        )

        def calc_wr(matches, pid):
            wins = sum(
                1 for m in matches
                if m.get("results", {}).get("winner") == next(
                    (tk for tk, team in m.get("teams", {}).items()
                     for p in team.get("players", []) if p.get("player_id") == pid), None
                )
            )
            total = len(matches)
            return wins, total, round(wins / total * 100, 1) if total else 0

        w1, t1, wr1 = calc_wr(hist1, pid1)
        w2, t2, wr2 = calc_wr(hist2, pid2)

        fav1 = heroes1[0]["name"] if heroes1 else "?"
        fav2 = heroes2[0]["name"] if heroes2 else "?"

        def cmp(a, b, higher_is_better=True):
            if a == b:
                return "🟡", "🟡"
            winner = (a > b) == higher_is_better
            return ("🟢", "🔴") if winner else ("🔴", "🟢")

        r1_icon, r2_icon = cmp(rank1, rank2, higher_is_better=False) if rank1 != "N/A" and rank2 != "N/A" else ("—", "—")
        p1_icon, p2_icon = cmp(pts1, pts2)
        w1_icon, w2_icon = cmp(wr1, wr2)
        t1_icon, t2_icon = cmp(t1, t2)

        embed = discord.Embed(
            title=f"🔍 Compare — {nick1}  vs  {nick2}",
            color=0xFF4500
        )
        if p1.get("avatar"):
            embed.set_thumbnail(url=p1["avatar"])

        embed.add_field(name="​", value=f"**{nick1}**", inline=True)
        embed.add_field(name="📊 Stat",   value="Rank\nPontos\nWR Season\nPartidas\nHerói Fav.", inline=True)
        embed.add_field(name="​", value=f"**{nick2}**", inline=True)

        embed.add_field(name="​", value=f"{r1_icon} #{rank1}", inline=True)
        embed.add_field(name="​", value="​",                    inline=True)
        embed.add_field(name="​", value=f"{r2_icon} #{rank2}", inline=True)

        embed.add_field(name="​", value=f"{p1_icon} {pts1} pts", inline=True)
        embed.add_field(name="​", value="​",                      inline=True)
        embed.add_field(name="​", value=f"{p2_icon} {pts2} pts", inline=True)

        embed.add_field(name="​", value=f"{w1_icon} {wr1}%", inline=True)
        embed.add_field(name="​", value="​",                   inline=True)
        embed.add_field(name="​", value=f"{w2_icon} {wr2}%", inline=True)

        embed.add_field(name="​", value=f"{t1_icon} {t1} jogos", inline=True)
        embed.add_field(name="​", value="​",                      inline=True)
        embed.add_field(name="​", value=f"{t2_icon} {t2} jogos", inline=True)

        embed.add_field(name="​", value=f"🦸 {fav1}", inline=True)
        embed.add_field(name="​", value="​",           inline=True)
        embed.add_field(name="​", value=f"🦸 {fav2}", inline=True)

        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • Season {SEASON} — 🟢 Melhor  🔴 Pior")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !parceiro [nick]
# Com quem tem melhor WR em duo
# ==============================================
@bot.command()
async def parceiro(ctx, nickname=None):
    if not nickname:
        await ctx.send("❌ Use: !parceiro [nickname]")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        pid     = player["player_id"]
        matches = await get_history(pid, mode="season", limit=100)

        # {ally_pid: {nick, wins, total}}
        ally_stats = {}

        for match in matches:
            winner  = match.get("results", {}).get("winner")
            my_team = None
            allies  = []

            for tk, team in match.get("teams", {}).items():
                for p in team.get("players", []):
                    if p.get("player_id") == pid:
                        my_team = tk
                    else:
                        allies.append((tk, p.get("player_id", ""), p.get("nickname", "?")))

            if not my_team:
                continue

            won = (winner == my_team)
            for tk, apid, anick in allies:
                if tk == my_team and apid:  # mesmo time
                    if apid not in ally_stats:
                        ally_stats[apid] = {"nick": anick, "wins": 0, "total": 0}
                    ally_stats[apid]["total"] += 1
                    if won:
                        ally_stats[apid]["wins"] += 1

        # Filtra minimo 3 jogos juntos
        filtered = {
            apid: s for apid, s in ally_stats.items()
            if s["total"] >= 3
        }

        if not filtered:
            await ctx.send(f"❌ **{nickname}** nao tem duplas com pelo menos 3 jogos juntos na season.")
            return

        # Ordena por WR
        sorted_allies = sorted(
            filtered.values(),
            key=lambda x: x["wins"] / x["total"],
            reverse=True
        )[:5]

        lines = []
        for i, s in enumerate(sorted_allies):
            wr   = round(s["wins"] / s["total"] * 100, 1)
            icon = MEDALS.get(i + 1, f"`#{i+1}`")
            lines.append(f"{icon} **{s['nick']}** — {wr}% WR ({s['wins']}V/{s['total']-s['wins']}D em {s['total']} jogos)")

        embed = discord.Embed(
            title=f"🤝 Melhores Parceiros — {nickname}",
            description="\n".join(lines),
            color=0x00AA00
        )
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • Season {SEASON} — mín. 3 jogos juntos")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !ausente
# Jogadores do hub sem partidas na season
# ==============================================
@bot.command()
async def ausente(ctx):
    async with ctx.typing():
        # Pega todos os jogadores do leaderboard (ate 50)
        rankings = await get_leaderboard_page(0, limit=50)
        if not rankings:
            await ctx.send("❌ Nao foi possivel carregar o leaderboard.")
            return

        ts_from, ts_to = get_season_timestamps()

        async def check_ausente(entry):
            pid      = entry.get("placement", {}).get("entity_id")
            nickname = entry.get("placement", {}).get("entity_name", "?")
            pontos   = entry.get("points", 0)
            if not pid:
                return None
            # Jogadores com 0 pontos provavelmente nao jogaram
            if pontos > 0:
                return None
            history = await faceit_get(
                f"https://open.faceit.com/data/v4/players/{pid}/history",
                {"game": GAME, "limit": 1, "from": ts_from, "to": ts_to}
            )
            total = history.get("end", 0) or len(history.get("items", []))
            if total == 0:
                return nickname
            return None

        results  = await asyncio.gather(*[check_ausente(e) for e in rankings])
        ausentes = [n for n in results if n]

        if not ausentes:
            await ctx.send("✅ Todos os jogadores do hub já jogaram na season!")
            return

        lines = [f"😴 **{nick}**" for nick in ausentes]
        embed = discord.Embed(
            title=f"😴 Ausentes — Season {SEASON}",
            description=f"**{len(ausentes)}** jogadores sem partidas no mês:\n\n" + "\n".join(lines),
            color=0x888888
        )
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • Season {SEASON}")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !historico [nick]
# Evolucao de pontos semana a semana na season
# ==============================================
@bot.command()
async def historico(ctx, nickname=None):
    if not nickname:
        await ctx.send("❌ Use: !historico [nickname]")
        return

    async with ctx.typing():
        player = await get_player(nickname)
        if not player:
            await ctx.send(f"❌ Jogador **{nickname}** nao encontrado.")
            return

        pid  = player["player_id"]
        hoje = datetime.now()
        inicio_mes = datetime(hoje.year, hoje.month, 1)

        # Divide o mes em semanas
        semanas = []
        atual = inicio_mes
        while atual <= hoje:
            fim_semana = min(atual + timedelta(days=6), hoje)
            semanas.append((atual, fim_semana))
            atual = atual + timedelta(days=7)

        # Para cada semana, busca partidas e calcula V/D
        async def get_semana_stats(inicio, fim):
            history = await faceit_get(
                f"https://open.faceit.com/data/v4/players/{pid}/history",
                {
                    "game":  GAME,
                    "limit": 50,
                    "from":  int(inicio.timestamp()),
                    "to":    int(fim.timestamp()),
                }
            )
            matches = history.get("items", [])
            wins = sum(
                1 for m in matches
                if m.get("results", {}).get("winner") == next(
                    (tk for tk, team in m.get("teams", {}).items()
                     for p in team.get("players", []) if p.get("player_id") == pid), None
                )
            )
            return inicio.strftime("%d/%m"), len(matches), wins

        resultados = await asyncio.gather(*[get_semana_stats(s, e) for s, e in semanas])

        lines     = []
        total_pts = 0
        for label, total, wins in resultados:
            if total == 0:
                lines.append(f"📅 **Sem. {label}** — sem jogos")
                continue
            losses  = total - wins
            pts     = wins * 25 - losses * 10   # estimativa simples de pontos
            total_pts += pts
            bar     = "🟢" * wins + "🔴" * losses
            lines.append(f"📅 **Sem. {label}** — {bar}  ({wins}V/{losses}D ≈ +{pts}pts)")

        embed = discord.Embed(
            title=f"📈 Histórico da Season — {nickname}",
            description="\n".join(lines) if lines else "Sem dados.",
            color=0xFF4500
        )
        if player.get("avatar"):
            embed.set_thumbnail(url=player["avatar"])
        embed.set_image(url=BANNER_URL)
        embed.set_footer(text=f"NEIL Dota 2 • Season {SEASON} — pts estimados (25 por V, -10 por D)")

    await ctx.send(embed=embed)

# ==============================================
# COMANDO !ajuda
# ==============================================
@bot.command()
async def ajuda(ctx):
    cmds_gerais = [
        ("!faceit [nick]",              "Rank, pontos e heróis da season atual."),
        ("!heroes [nick]",              "Heróis de carreira e lanes (OpenDota)."),
        ("!top10",                      "Top 10 do leaderboard da season."),
        ("![nick1] vs [nick2]",         "Confronto direto na season atual."),
        ("![nick1] vs [nick2] alltime", "Confronto direto no histórico completo."),
        ("!streaks",                    "Jogador com maior sequência de vitórias."),
        ("!rivalidade",                 "Pares que mais se enfrentaram na season."),
        ("!rivalidade alltime",         "Pares que mais se enfrentaram (all time)."),
        ("!stats [nick]",               "KDA, GPM, XPM médios (ult. 30 dias)."),
        ("!form [nick]",                "Últimas 5 partidas com herói e resultado."),
        ("!maisjogas",                  "Ranking de quem mais jogou na season."),
        ("!nemesis [nick]",             "Quem mais te derrotou na season."),
        ("!nemesis [nick] alltime",     "Quem mais te derrotou (all time)."),
        ("!carrasco [nick]",            "Quem você mais derrotou na season."),
        ("!carrasco [nick] alltime",    "Quem você mais derrotou (all time)."),
        ("!duo [nick1] [nick2]",        "WR quando jogaram juntos."),
        ("!parceiro [nick]",            "Seus melhores parceiros de duo na season."),
        ("!compare [nick1] [nick2]",    "Compara dois jogadores lado a lado."),
        ("!meta",                       "Heróis mais jogados na season (hub inteiro)."),
        ("!ausente",                    "Jogadores sem partidas no mês."),
        ("!historico [nick]",           "Evolução semana a semana na season."),
    ]
    cmds_dd = [
        ("!dobrar [nick_faceit]",       "Ativa um DD antes da próxima partida."),
        ("!meusdds [nick_faceit]",      "Saldo e histórico de DDs do jogador."),
        ("!validar [nick] v/d",         "🔒 Admin: valida resultado de um DD."),
        ("!relatoriodds",               "🔒 Admin: relatório de DDs da season."),
        ("!vincular @user [nick]",      "🔒 Admin: vincula Discord ao Faceit."),
        ("!desvincular @user",          "🔒 Admin: remove vínculo."),
        ("!listvinculos",               "🔒 Admin: lista todos os vínculos."),
        ("!ajuda",                      "Mostra esta lista."),
    ]
    embed1 = discord.Embed(
        title="📖 Comandos NEIL Bot",
        description="Bem-vindo à Liga NEIL de Dota 2!",
        color=0xFF4500
    )
    for name, value in cmds_gerais:
        embed1.add_field(name=name, value=value, inline=False)
    embed1.set_footer(text=f"NEIL Dota 2 — {SEASON_LABEL}")

    embed2 = discord.Embed(
        title="🎲 Double Down (DD)",
        color=0xFF8C00
    )
    for name, value in cmds_dd:
        embed2.add_field(name=name, value=value, inline=False)
    embed2.set_image(url=BANNER_URL)
    embed2.set_footer(text=f"NEIL Dota 2 — {SEASON_LABEL}")

    await ctx.send(embed=embed1)
    await ctx.send(embed=embed2)

# ==============================================
# HELPER — log de ações DD para canal Discord
# ==============================================
def _ts_brasilia(ts_utc_str: str) -> str:
    """Converte timestamp UTC (string ISO) para horário de Brasília (UTC-3)."""
    try:
        from datetime import timezone, timedelta
        dt = datetime.fromisoformat(ts_utc_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        brt = dt.astimezone(timezone(timedelta(hours=-3)))
        return brt.strftime("%d/%m/%Y %H:%M (BRT)")
    except Exception:
        return ts_utc_str

async def _dd_log_canal(msg: str):
    log.info(f"[DD-LOG] {msg}")
    if DD_LOG_CHANNEL_ID:
        ch = bot.get_channel(DD_LOG_CHANNEL_ID)
        if ch:
            await ch.send(f"📋 **[DD-LOG]** {msg}")

# ==============================================
# COMANDO !dobrar  — ativa DD antes de uma partida
# ==============================================
@bot.command()
async def dobrar(ctx):
    """Jogador ativa um DD antes de jogar. Uso: !dobrar (requer vínculo)"""
    nick = VINCULOS.get(str(ctx.author.id))
    if not nick:
        await ctx.send(
            "❌ Você não tem conta Faceit vinculada.\n"
            "Peça a um Admin para usar `!vincular @você [nick_faceit]`."
        )
        return
    discord_nick = str(ctx.author)

    async with ctx.typing():
        saldo = await dd_calc_saldo(nick)
        if not saldo:
            await ctx.send(f"❌ Nick **{nick}** não encontrado na Faceit.")
            return

        # Verifica se já tem aposta pendente
        if saldo["pendente"]:
            aposta_id, ts_ap = saldo["pendente"]
            await ctx.send(
                f"⚠️ **{saldo['nick']}**, você já tem um DD pendente de validação!\n"
                f"_(Ativado em {_ts_brasilia(ts_ap)} • ID #{aposta_id})_\n"
                f"Aguarde o Admin validar com `!validar {saldo['nick']} vitoria/derrota`."
            )
            return

        # Verifica saldo disponível
        if saldo["disponivel"] <= 0:
            falta_partidas = DD_PER_GAMES - (saldo["partidas"] % DD_PER_GAMES)
            if saldo["ganhos"] >= DD_MAX:
                msg_falta = f"Você atingiu o limite de **{DD_MAX} DDs** na season."
            else:
                msg_falta = f"Faltam **{falta_partidas}** partidas para ganhar o próximo DD."
            await ctx.send(
                f"❌ **{saldo['nick']}**, você não tem DDs disponíveis.\n{msg_falta}\n_(Ganhos: {saldo['ganhos']}/{DD_MAX} • Usados: {saldo['usados']})_"
            )
            return

        # Cria a aposta
        aposta_id = dd_criar_aposta(nick, discord_nick, SEASON)
        dd_incrementar_usado(nick, SEASON)

        barra = "🟠" * saldo["ganhos"] + "⚫" * (DD_MAX - saldo["ganhos"])
        embed = discord.Embed(
            title="🎲 Double Down Ativado!",
            description=(
                f"**{saldo['nick']}** ativou um DD para a próxima partida!\nApós jogar, mostre o resultado para o Admin validar."
            ),
            color=0xFF8C00
        )
        embed.add_field(name="🎯 DDs disponíveis após uso",
                        value=f"{saldo['disponivel'] - 1}/{DD_MAX - saldo['usados']}",
                        inline=True)
        embed.add_field(name="📋 ID da aposta", value=f"#{aposta_id}", inline=True)
        embed.add_field(
            name="💰 Resultado possível",
            value=(
                f"🟢 Vitória → **+{DD_PTS_WIN + DD_BONUS_WIN} pts** "
                f"_(base {DD_PTS_WIN:+} + bônus DD {DD_BONUS_WIN:+})_\n🔴 Derrota → **{DD_PTS_LOSS + DD_BONUS_LOSS} pts** "
                f"_(base {DD_PTS_LOSS} + bônus DD {DD_BONUS_LOSS})_"
            ),
            inline=False
        )
        embed.add_field(name="​", value=barra, inline=False)
        embed.set_footer(text=f"Ativado por {discord_nick} • Admin: !validar @{saldo['nick']} vitoria/derrota • {SEASON_LABEL}")
        await ctx.send(embed=embed)

        await _dd_log_canal(
            f"🎲 **{saldo['nick']}** (`{discord_nick}`) ativou DD (aposta #{aposta_id}) — "
            f"saldo restante: {saldo['disponivel'] - 1}/{DD_MAX}"
        )


# ==============================================
# COMANDO !validar [nick_faceit] vitoria|derrota  — Admin
# ==============================================
@bot.command()
@commands.has_permissions(administrator=True)
async def validar(ctx, nick_faceit: str = None, resultado: str = None):
    """Admin valida o resultado de um DD pendente pelo nick Faceit."""
    if not nick_faceit or not resultado:
        await ctx.send("❌ Use: `!validar [nick_faceit] vitoria` ou `!validar [nick_faceit] derrota`")
        return

    resultado = resultado.lower().strip()
    if resultado not in ("vitoria", "vitória", "derrota"):
        await ctx.send("❌ Resultado deve ser `vitoria` ou `derrota`.")
        return
    resultado = "vitoria" if resultado in ("vitoria", "vitória") else "derrota"

    nick = nick_faceit

    pendente = dd_get_pendente(nick, SEASON)
    if not pendente:
        await ctx.send(f"❌ **{nick}** não tem DD pendente de validação.")
        return

    aposta_id, ts_ap = pendente
    admin_nick = str(ctx.author)

    # Busca pontos ANTES de aplicar
    player = await get_player(nick)
    pts_antes = 0
    if player:
        pts_antes, _ = await get_season_rank(player["player_id"])

    pts_base, pts_bonus, pts_total = dd_validar_aposta(aposta_id, resultado, admin_nick)

    pts_depois = pts_antes + pts_total

    emoji   = "🟢" if resultado == "vitoria" else "🔴"
    cor     = 0x00C851 if resultado == "vitoria" else 0xFF4444
    res_txt = "Vitória" if resultado == "vitoria" else "Derrota"

    embed = discord.Embed(
        title=f"{emoji} DD Validado — {res_txt}!",
        color=cor
    )
    embed.add_field(name="Jogador",         value=f"**{nick}**",            inline=True)
    embed.add_field(name="Resultado",       value=res_txt,                  inline=True)
    embed.add_field(name="\u200b",          value="\u200b",                 inline=True)
    embed.add_field(name="Pontos base",     value=f"{pts_base:+} pts",      inline=True)
    embed.add_field(name="Bônus DD",        value=f"{pts_bonus:+} pts",     inline=True)
    embed.add_field(name="**Total DD**",    value=f"**{pts_total:+} pts**", inline=True)
    embed.add_field(name="Pontos antes",    value=f"{pts_antes} pts",       inline=True)
    embed.add_field(name="Pontos depois",   value=f"**{pts_depois} pts**",  inline=True)
    embed.add_field(name="Aposta",          value=f"#{aposta_id}",          inline=True)
    embed.set_footer(text=f"Validado por {admin_nick} • {SEASON_LABEL}")

    # Menciona o jogador se tiver vínculo
    discord_id = vinculos_get_discord_id(nick)
    mention_txt = f"<@{discord_id}> " if discord_id else ""
    await ctx.send(f"{mention_txt}", embed=embed)

    await _dd_log_canal(
        f"{emoji} Admin `{admin_nick}` validou DD #{aposta_id} de **{nick}** "
        f"→ {res_txt} → **{pts_total:+} pts** — Season {SEASON}"
    )


# ==============================================
# COMANDO !meusdds  — jogador vê seu histórico de DDs
# ==============================================
@bot.command()
async def meusdds(ctx, nick_faceit: str = None):
    """Mostra saldo e histórico de DDs. Uso: !meusdds ou !meusdds [nick] (admin)"""
    if nick_faceit:
        # Admin consultando qualquer jogador
        nick = nick_faceit.lower()
    else:
        nick = VINCULOS.get(str(ctx.author.id))
        if not nick:
            await ctx.send(
                "❌ Você não tem conta Faceit vinculada.\n"
                "Peça a um Admin para usar `!vincular @você [nick_faceit]`."
            )
            return

    async with ctx.typing():
        saldo = await dd_calc_saldo(nick)
        if not saldo:
            await ctx.send(f"❌ Nick **{nick}** não encontrado na Faceit.")
            return

        historico = dd_get_historico_mes(nick, SEASON)
        barra = "🟠" * saldo["ganhos"] + "⚫" * (DD_MAX - saldo["ganhos"])

        embed = discord.Embed(
            title=f"🎲 Meus DDs — {saldo['nick']}",
            color=0xFF8C00
        )
        embed.add_field(name="🎮 Partidas na season", value=str(saldo["partidas"]), inline=True)
        embed.add_field(name="🎯 DDs ganhos",         value=f"{saldo['ganhos']}/{DD_MAX}\n{barra}", inline=True)
        embed.add_field(name="✅ Disponível",          value=str(saldo["disponivel"]), inline=True)

        if saldo["pendente"]:
            _, ts_ap = saldo["pendente"]
            embed.add_field(
                name="⏳ Aposta pendente",
                value=f"Aguardando validação desde {_ts_brasilia(ts_ap)}",
                inline=False
            )

        if historico:
            pts_total_geral = sum(r[3] for r in historico)
            linhas = []
            for resultado, pts_base, pts_bonus, pts_total, ts_val, admin in historico:
                emoji = "🟢" if resultado == "vitoria" else "🔴"
                linhas.append(
                    f"{emoji} {pts_total:+} pts "
                    f"_(base {pts_base:+} + DD {pts_bonus:+})_"
                )
            embed.add_field(
                name=f"📊 Histórico de DDs usados ({len(historico)}x)",
                value="\n".join(linhas),
                inline=False
            )
            cor_saldo = 0x00C851 if pts_total_geral >= 0 else 0xFF4444
            embed.add_field(
                name="💰 Saldo total dos DDs",
                value=f"**{pts_total_geral:+} pts**",
                inline=False
            )
            embed.color = cor_saldo
        else:
            embed.add_field(name="📊 Histórico", value="Nenhum DD usado ainda.", inline=False)

        embed.set_footer(text=f"NEIL Dota 2 • {SEASON_LABEL} • Double Down System")
        await ctx.send(embed=embed)


# ==============================================
# COMANDO !relatoriodds  — Admin vê todos os DDs do mês
# ==============================================
@bot.command()
@commands.has_permissions(administrator=True)
async def relatoriodds(ctx):
    """Admin vê relatório de todos os DDs usados na season."""
    async with ctx.typing():
        relatorio  = dd_get_relatorio_mes(SEASON)
        pendentes  = dd_get_pendentes_todos(SEASON)

        embed = discord.Embed(
            title=f"📋 Relatório de DDs — {SEASON_LABEL}",
            color=0xFF8C00
        )

        # Seção pendentes
        if pendentes:
            linhas_pend = []
            for nick, discord_nick, ts_ap, aposta_id in pendentes:
                linhas_pend.append(f"⏳ **{nick}** — ativado em {_ts_brasilia(ts_ap)} _(#{aposta_id})_")
            embed.add_field(
                name=f"⚠️ Aguardando validação ({len(pendentes)})",
                value="\n".join(linhas_pend),
                inline=False
            )

        # Seção validados
        if relatorio:
            linhas = []
            for nick, total, vitorias, derrotas, pts_base, pts_bonus, pts_total in relatorio:
                emoji = "🟢" if pts_total >= 0 else "🔴"
                v_txt = f"{vitorias}V" if vitorias else ""
                d_txt = f"{derrotas}D" if derrotas else ""
                placar = " / ".join(filter(None, [v_txt, d_txt])) or "—"
                linhas.append(
                    f"{emoji} **{nick}** — {placar} → **{pts_total:+} pts** "
                    f"_(base {pts_base:+} + bônus {pts_bonus:+})_"
                )
            embed.add_field(
                name=f"✅ Validados ({len(relatorio)})",
                value="\n".join(linhas),
                inline=False
            )

        if not relatorio and not pendentes:
            await ctx.send("📋 Nenhum DD foi usado ainda nesta season.")
            return

        embed.set_footer(text=f"Pendentes: {len(pendentes)} • Validados: {len(relatorio)} • {SEASON_LABEL}")
        await ctx.send(embed=embed)
# ==============================================
# COMANDO !vincular @usuario [nick_faceit]  — Admin
# ==============================================
@bot.command()
@commands.has_permissions(administrator=True)
async def vincular(ctx, membro: discord.Member = None, nick_faceit: str = None):
    """Admin vincula um usuário Discord ao nick Faceit."""
    if not membro or not nick_faceit:
        await ctx.send("❌ Use: `!vincular @usuario [nick_faceit]`")
        return

    async with ctx.typing():
        player = await get_player(nick_faceit)
        if not player:
            await ctx.send(f"❌ Nick **{nick_faceit}** não encontrado na Faceit.")
            return

        nick_real = player.get("nickname", nick_faceit)
        vinculos_add(str(membro.id), nick_real)
        VINCULOS[str(membro.id)] = nick_real.lower()

        embed = discord.Embed(
            title="✅ Vínculo criado!",
            color=0x00C851
        )
        embed.add_field(name="Discord", value=membro.mention,    inline=True)
        embed.add_field(name="Faceit",  value=f"**{nick_real}**", inline=True)
        embed.set_footer(text=f"Vinculado por {ctx.author} • {SEASON_LABEL}")
        await ctx.send(embed=embed)
        log.info(f"[VINCULAR] {ctx.author} vinculou {membro} → {nick_real}")

@bot.command()
@commands.has_permissions(administrator=True)
async def desvincular(ctx, membro: discord.Member = None):
    """Admin remove o vínculo de um usuário."""
    if not membro:
        await ctx.send("❌ Use: `!desvincular @usuario`")
        return

    discord_id = str(membro.id)
    if discord_id not in VINCULOS:
        await ctx.send(f"❌ {membro.mention} não tem vínculo registrado.")
        return

    nick = VINCULOS.pop(discord_id)
    vinculos_remove(discord_id)
    await ctx.send(f"✅ Vínculo de {membro.mention} com **{nick}** removido.")

@bot.command()
@commands.has_permissions(administrator=True)
async def listvinculos(ctx):
    """Admin lista todos os vínculos registrados."""
    if not VINCULOS:
        await ctx.send("📋 Nenhum vínculo registrado.")
        return

    linhas = []
    for discord_id, nick in VINCULOS.items():
        membro = ctx.guild.get_member(int(discord_id))
        discord_txt = membro.mention if membro else f"ID:{discord_id}"
        linhas.append(f"{discord_txt} → **{nick}**")

    embed = discord.Embed(
        title="📋 Vínculos Discord ↔ Faceit",
        description="\n".join(linhas),
        color=0xFF4500
    )
    embed.set_footer(text=f"{len(VINCULOS)} vínculos • {SEASON_LABEL}")
    await ctx.send(embed=embed)


# ==============================================
@bot.command()
@commands.has_permissions(administrator=True)
async def clearcache(ctx):
    async with _cache_lock:
        total = len(_cache)
        _cache.clear()
    await ctx.send(f"✅ Cache limpo! {total} entradas removidas.")

# ==============================================
# ERROR HANDLER GLOBAL
# ==============================================
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    log.error(f"Erro no comando '{ctx.command}': {error}", exc_info=error)
    await ctx.send("❌ Erro inesperado. Tente novamente em instantes.")

# ==============================================
# ON_MESSAGE — detecta padroes customizados
# ==============================================
@bot.event
async def on_message(message):
    if message.author.bot:
        return

    content_msg = message.content.strip()

    # Padrao: !nick1 vs nick2 [alltime]
    if content_msg.startswith("!") and " vs " in content_msg.lower():
        parts = content_msg[1:].split(" vs ", 1)
        if len(parts) == 2:
            nick1 = parts[0].strip()
            rest  = parts[1].strip()
            if rest.lower().endswith(" alltime"):
                nick2, mode = rest[:-8].strip(), "alltime"
            else:
                nick2, mode = rest, "season"
            if nick1 and nick2 and " " not in nick1:
                ctx = await bot.get_context(message)
                await vs_cmd(ctx, nick1, nick2, mode)
                return

    # Padrao: !rivalidade alltime
    if content_msg.lower() == "!rivalidade alltime":
        ctx = await bot.get_context(message)
        await rivalidade(ctx, mode="alltime")
        return

    await bot.process_commands(message)

# ==============================================
# READY
# ==============================================
@bot.event
async def on_ready():
    global session, VINCULOS
    session = aiohttp.ClientSession()
    dd_init_db()
    VINCULOS = load_vinculos()
    log.info(f"✅ Bot conectado como {bot.user} — {len(VINCULOS)} vínculos carregados")
    if not FACEIT_SESSION_COOKIE:
        log.warning("⚠️ FACEIT_SESSION_COOKIE nao configurado! Leaderboard nao funcionara.")

bot.run(DISCORD_TOKEN)

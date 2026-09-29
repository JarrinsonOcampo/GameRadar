"""
scraper.py - Chaquopy (Android) - Escaneo de ofertas Steam / Epic / GOG.

Este archivo va en: app/src/main/python/scraper.py

Se ejecuta dentro del proceso de la app vía Chaquopy. Debe llamarse SIEMPRE
desde un hilo secundario (Dispatchers.IO en Kotlin), nunca desde el hilo
principal, porque hace peticiones de red bloqueantes.

Dependencias pip que debes declarar en app/build.gradle.kts, dentro de
chaquopy { defaultConfig { pip { ... } } }:
    - requests
    - beautifulsoup4
"""

import json
import re
import time
import threading
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# Ajuste de límites:
# Se incrementó GOG_MAX_PAGINAS a 150 (hasta 7200 productos) para garantizar
# que la consulta ordenada por "desc:discount" llegue hasta los rangos de
# descuento más bajos (50-59%, 40-49%, 30-39%, 20-29%, 10-19%, 1-9%) sin
# quedarse corta en rebajas grandes.
STEAM_MAX_ITEMS = 5000
EPIC_MAX_PAGINAS = 20     # 100 productos por página
GOG_MAX_PAGINAS = 150     # 48 productos por página (Suficiente para cubrir todo el catálogo)

# Steam limita las peticiones por IP (sobre todo desde GitHub Actions, que usa IPs
# de datacenter compartidas). Estas dos constantes controlan cuánto se le insiste.
STEAM_PAUSA_ENTRE_PAGINAS = 1.0   # segundos entre página y página (bájalo a 0 si solo corre en el celular)
STEAM_REINTENTOS = 4              # intentos por petición ante 403/429/503 o error de red

GOG_MONEDAS_VALIDAS = {
    "USD", "EUR", "GBP", "AUD", "CAD", "CHF", "PLN",
    "RUB", "NOK", "SEK", "DKK", "JPY", "CNY", "BRL", "ZAR",
}

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

# --------------------------------------------------------------------
# Idioma de la ficha de detalle: la UI (DetalleOfertaScreen.kt) manda
# siempre "es" / "en" / "pt" (ver codigoIdioma en ese archivo), acá los
# traducimos al código/locale que espera cada tienda.
# --------------------------------------------------------------------
IDIOMA_STEAM = {"es": "spanish", "en": "english", "pt": "brazilian"}
IDIOMA_EPIC_LOCALE = {"es": "es-ES", "en": "en-US", "pt": "pt-BR"}
IDIOMA_GOG_LOCALE = {"es": "es-ES", "en": "en-US", "pt": "pt-BR"}


def _log(etiqueta, error):
    """
    Los print() de Python en Chaquopy salen en Logcat (filtra por "python.stdout").
    """
    print(f"[scraper] {etiqueta}: {type(error).__name__}: {error}")


_MESES = {
    "es": ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"],
    "en": ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
    "pt": ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"],
}


def _fmt_fecha(texto, idioma):
    """'2016-11-29T00:00:00Z' / '2016.11.29' -> '29 NOV 2016' (mismo estilo que Steam)."""
    m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", texto or "")
    if not m:
        return texto or None
    anio, mes, dia = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not 1 <= mes <= 12:
        return texto
    return f"{dia} {_MESES.get(idioma, _MESES['en'])[mes - 1].upper()} {anio}"


def _lista_texto(valor):
    """Acepta str, lista de str o lista de {name/title: ...} y devuelve list[str]."""
    if not valor:
        return []
    if isinstance(valor, str):
        return [valor.strip()] if valor.strip() else []
    salida = []
    if isinstance(valor, list):
        for x in valor:
            if isinstance(x, dict):
                x = x.get("name") or x.get("title") or ""
            x = str(x).strip()
            if x:
                salida.append(x)
    return salida


def _a_texto(html, max_len=None):
    """HTML -> texto plano; si max_len, corta en límite de palabra."""
    if not html:
        return ""
    texto = BeautifulSoup(str(html), "html.parser").get_text(separator=" ")
    texto = re.sub(r"\s+", " ", texto).strip()
    if max_len and len(texto) > max_len:
        texto = texto[:max_len].rsplit(" ", 1)[0].rstrip(".,;:") + "…"
    return texto


def _fmt(valor, symbol, no_decimals):
    if no_decimals:
        return f"{symbol} {int(round(valor)):,}".replace(",", ".")
    return f"{symbol} {valor:,.2f}"


def _extraer_appid_steam(link):
    try:
        partes = link.rstrip("/").split("/")
        for clave in ("app", "sub", "bundle"):
            if clave in partes:
                return partes[partes.index(clave) + 1]
    except Exception:
        pass
    return None


def _steam_img_url(link):
    app_id = _extraer_appid_steam(link)
    if not app_id:
        return ""
    return (f"https://shared.akamai.steamstatic.com/store_item_assets/"
            f"steam/apps/{app_id}/capsule_sm_120.jpg")


# ---------------------------------------------------------------------
# STEAM
# ---------------------------------------------------------------------
def _steam_json(url, headers, etiqueta):
    """
    GET a Steam que devuelve el JSON o None. Ante 403/429/503 o errores de red
    espera y reintenta (espera 20s, 40s, 80s...). Todo fallo queda en el log
    (antes se ignoraban en silencio y la lista de Steam salía vacía).
    """
    espera = 20
    for intento in range(1, STEAM_REINTENTOS + 1):
        try:
            res = requests.get(url, headers=headers, timeout=20)
        except Exception as e:
            _log(f"steam {etiqueta} (red, intento {intento})", e)
            time.sleep(5)
            continue

        if res.status_code in (403, 429, 503):
            print(f"[scraper] steam {etiqueta}: HTTP {res.status_code} "
                  f"(intento {intento}/{STEAM_REINTENTOS}), reintento en {espera}s")
            time.sleep(espera)
            espera *= 2
            continue

        if res.status_code != 200:
            print(f"[scraper] steam {etiqueta}: HTTP {res.status_code} url={url}")
            return None

        try:
            return res.json()
        except ValueError:
            print(f"[scraper] steam {etiqueta}: la respuesta no es JSON "
                  f"({res.headers.get('Content-Type')}): {res.text[:200]!r}")
            return None

    print(f"[scraper] steam {etiqueta}: se agotaron los reintentos")
    return None


def _escanear_steam(cc_code, symbol, no_decimals, free_badge):
    cc_code = (cc_code or "").strip().lower()
    print(f"[scraper] steam: cc_code={cc_code!r}")
    resultados = []
    vistos = set()
    headers = dict(HEADERS_BASE, **{"Accept-Language": "es-ES,es;q=0.9"})

    urls_gratis = [
        f"https://store.steampowered.com/search/results/?query&maxprice=free&specials=1&cc={cc_code}&l=spanish&infinite=1",
        f"https://store.steampowered.com/search/results/?term=100%25&specials=1&cc={cc_code}&l=spanish&infinite=1",
    ]
    for url in urls_gratis:
        try:
            datos = _steam_json(url, headers, "gratis")
            if datos is None:
                continue
            soup = BeautifulSoup(datos.get("results_html", ""), "html.parser")
            for juego in soup.find_all("a", class_="search_result_row"):
                link = juego.get("href", "").split("?")[0]
                if not link or link in vistos:
                    continue

                nombre_el = juego.find("span", class_="title")
                nombre = nombre_el.text.strip() if nombre_el else "Desconocido"

                # IMPORTANTE: el % de descuento se lee del badge "discount_pct"
                # (el mismo elemento que usa el bucle de abajo para todo lo
                # demás), NUNCA buscando el texto "100%" en toda la tarjeta.
                # Buscarlo en el texto completo choca con juegos cuyo propio
                # NOMBRE contiene un número seguido de "%", como
                # "100% Orange Juice": esa cadena aparecía en el título y
                # el juego se marcaba como gratis aunque solo tuviera -10%.
                desc_el = juego.find("div", class_="discount_pct")
                p_orig_el = juego.find("div", class_="discount_original_price")
                p_fin_el = juego.find("div", class_="discount_final_price")
                p_orig = p_orig_el.text.strip() if p_orig_el else ""
                p_final = p_fin_el.text.strip() if p_fin_el else ""

                es_gratis = False
                if desc_el:
                    try:
                        pct = int(desc_el.text.strip().replace("-", "").replace("%", ""))
                        es_gratis = pct >= 100
                    except ValueError:
                        es_gratis = False

                # Respaldo solo para el caso raro de que Steam no ponga el
                # badge de % pero sí muestre "Gratis"/"Free" como precio
                # final. OJO: NO se usa "$0" in p_final, porque eso también
                # sería cierto para un juego que cueste $0.99.
                if not es_gratis and p_final:
                    p_final_low = p_final.lower()
                    es_gratis = "gratis" in p_final_low or "free" in p_final_low

                if es_gratis:
                    resultados.append({
                        "nombre": nombre, "desc": 100,
                        "p_orig": p_orig or "De Pago",
                        "p_final": free_badge,
                        "tipo": "Juego", "link": link,
                        "tienda": "Steam", "img_url": _steam_img_url(link),
                        "ref_id": _extraer_appid_steam(link) or "",
                    })
                    vistos.add(link)
        except Exception as e:
            _log("steam gratis", e)

    start = 0
    while start < STEAM_MAX_ITEMS:
        url = (f"https://store.steampowered.com/search/results/?query&start={start}"
               f"&count=50&specials=1&cc={cc_code}&l=spanish&infinite=1")
        try:
            datos = _steam_json(url, headers, f"página start={start}")
            if datos is None:
                break
            html = datos.get("results_html", "")
            if not html.strip():
                print(f"[scraper] steam: results_html vacío en start={start} (fin de resultados)")
                break

            soup = BeautifulSoup(html, "html.parser")
            juegos = soup.find_all("a", class_="search_result_row")
            if not juegos:
                break

            for juego in juegos:
                link = juego.get("href", "").split("?")[0]
                if not link or link in vistos:
                    continue

                nombre_el = juego.find("span", class_="title")
                nombre = nombre_el.text.strip() if nombre_el else "Desconocido"

                desc_el = juego.find("div", class_="discount_pct")
                if not desc_el:
                    continue
                try:
                    desc = int(desc_el.text.strip().replace("-", "").replace("%", ""))
                except ValueError:
                    continue

                p_orig_el = juego.find("div", class_="discount_original_price")
                p_fin_el = juego.find("div", class_="discount_final_price")

                resultados.append({
                    "nombre": nombre,
                    "desc": desc,
                    "p_orig": p_orig_el.text.strip() if p_orig_el else "",
                    "p_final": free_badge if desc == 100 else (p_fin_el.text.strip() if p_fin_el else ""),
                    "tipo": "DLC" if "/sub/" in link else "Juego",
                    "link": link,
                    "tienda": "Steam",
                    "img_url": _steam_img_url(link),
                    "ref_id": _extraer_appid_steam(link) or "",
                })
                vistos.add(link)

            start += 50
            time.sleep(STEAM_PAUSA_ENTRE_PAGINAS)
        except Exception as e:
            _log(f"steam página start={start}", e)
            break

    print(f"[scraper] steam: {len(resultados)} resultados para cc={cc_code}")
    return resultados


def _steam_review_text(review_score, idioma):
    tabla = {
        1: {"es": "Muy negativa", "en": "Overwhelmingly Negative", "pt": "Extremamente negativa"},
        2: {"es": "Muy negativa", "en": "Very Negative", "pt": "Muito negativa"},
        3: {"es": "Negativa", "en": "Negative", "pt": "Negativa"},
        4: {"es": "Mayormente negativa", "en": "Mostly Negative", "pt": "Majoritariamente negativa"},
        5: {"es": "Variada", "en": "Mixed", "pt": "Mista"},
        6: {"es": "Mayormente positiva", "en": "Mostly Positive", "pt": "Majoritariamente positiva"},
        7: {"es": "Positiva", "en": "Positive", "pt": "Positiva"},
        8: {"es": "Muy positiva", "en": "Very Positive", "pt": "Muito positiva"},
        9: {"es": "Abrumadoramente positiva", "en": "Overwhelmingly Positive", "pt": "Extremamente positiva"},
    }
    return tabla.get(review_score, {}).get(idioma, "")


def _steam_idiomas(idiomas_html):
    if not idiomas_html:
        return []
    cuerpo = idiomas_html.split("<br>")[0]
    resultado = []
    for parte in cuerpo.split(","):
        con_audio = "*" in parte
        nombre = BeautifulSoup(parte, "html.parser").get_text().replace("*", "").strip()
        if nombre:
            resultado.append({"idioma": nombre, "audio": con_audio})
    return resultado


def _detalle_steam(appid, idioma, cc_code):
    l = IDIOMA_STEAM.get(idioma, "english")
    resultado = {
        "tienda": "Steam", "nombre": None, "descripcion": "",
        "resena_texto": None, "resena_pct": None, "resena_total": None,
        "fecha_lanzamiento": None, "desarrollador": None, "editor": None,
        "generos": [], "categorias": [], "idiomas": [],
        "metacritic_score": None, "metacritic_url": None,
        "imagen": None, "controles": None,
    }
    try:
        res = requests.get(
            "https://store.steampowered.com/api/appdetails",
            params={"appids": appid, "l": l, "cc": cc_code},
            headers=HEADERS_BASE, timeout=10,
        )
        res.raise_for_status()
        entrada = res.json().get(str(appid), {})
        if entrada.get("success"):
            d = entrada["data"]
            resultado["nombre"] = d.get("name")
            resultado["descripcion"] = d.get("short_description", "")
            resultado["fecha_lanzamiento"] = (d.get("release_date") or {}).get("date")
            resultado["generos"] = [g["description"] for g in (d.get("genres") or []) if g.get("description")]
            resultado["desarrollador"] = ", ".join(d.get("developers", []) or []) or None
            resultado["editor"] = ", ".join(d.get("publishers", []) or []) or None
            resultado["categorias"] = [c["description"] for c in (d.get("categories") or []) if c.get("description")]
            metacritic = d.get("metacritic")
            if metacritic:
                resultado["metacritic_score"] = metacritic.get("score")
                resultado["metacritic_url"] = metacritic.get("url")
            resultado["imagen"] = d.get("header_image")
            resultado["idiomas"] = _steam_idiomas(d.get("supported_languages", ""))
            if d.get("controller_support") == "full":
                textos = {"es": "Compatible total con mando", "en": "Full controller support", "pt": "Suporte total a controle"}
                resultado["controles"] = textos.get(idioma, textos["en"])
    except Exception:
        pass

    try:
        res = requests.get(
            f"https://store.steampowered.com/appreviews/{appid}",
            params={"json": 1, "num_per_page": 0, "language": "all", "purchase_type": "all"},
            headers=HEADERS_BASE, timeout=10,
        )
        res.raise_for_status()
        resumen = res.json().get("query_summary", {})
        score = resumen.get("review_score", 0)
        total = resumen.get("total_reviews", 0)
        positivas = resumen.get("total_positive", 0)
        if total > 0:
            resultado["resena_texto"] = _steam_review_text(score, idioma)
            resultado["resena_total"] = total
            resultado["resena_pct"] = int(round((positivas / total) * 100))
    except Exception:
        pass

    return resultado


# ---------------------------------------------------------------------
# EPIC GAMES
# ---------------------------------------------------------------------
def _epic_link(item):
    slug = None
    catalog_ns = item.get("catalogNs") or {}
    for m in (catalog_ns.get("mappings") or []):
        if m.get("pageSlug"):
            slug = m["pageSlug"]
            break
    if not slug:
        for m in (item.get("offerMappings") or []):
            if m.get("pageSlug"):
                slug = m["pageSlug"]
                break
    if not slug:
        slug = item.get("productSlug") or item.get("urlSlug")
    if not slug:
        return "https://store.epicgames.com", ""

    slug = slug.split("/")[0]
    if item.get("offerType") == "BUNDLE":
        return f"https://store.epicgames.com/bundles/{slug}", slug
    return f"https://store.epicgames.com/p/{slug}", slug


def _epic_img(item):
    prioridad = ["OfferImageWide", "DieselStoreFrontWide", "Thumbnail",
                 "OfferImageTall", "VaultClosed"]
    imagenes = {img.get("type"): img.get("url") for img in (item.get("keyImages") or [])}
    for tipo in prioridad:
        if imagenes.get(tipo):
            return imagenes[tipo]
    return next(iter(imagenes.values()), "") if imagenes else ""


def _epic_tipo(item):
    return "DLC" if item.get("offerType") in ("ADD_ON", "DLC", "Dlc") else "Juego"


def _epic_es_gratis(item):
    promos = item.get("promotions") or {}
    ahora = datetime.now(timezone.utc)
    for grupo in (promos.get("promotionalOffers") or []):
        for oferta in (grupo.get("promotionalOffers") or []):
            ajuste = oferta.get("discountSetting") or {}
            if ajuste.get("discountPercentage") != 0:
                continue
            try:
                inicio = datetime.fromisoformat((oferta.get("startDate") or "").replace("Z", "+00:00"))
            except ValueError:
                inicio = None
            try:
                fin = datetime.fromisoformat((oferta.get("endDate") or "").replace("Z", "+00:00"))
            except ValueError:
                fin = None
            if inicio and ahora < inicio:
                continue
            if fin and ahora > fin:
                continue
            return True
    return False


_EPIC_CACHE = {}


def _epic_guardar_cache(slug, item):
    if not slug:
        return
    attrs = {a.get("key"): a.get("value")
             for a in (item.get("customAttributes") or []) if isinstance(a, dict)}
    _EPIC_CACHE[slug] = {
        "descripcion": item.get("description") or "",
        "desarrollador": attrs.get("developerName") or None,
        "editor": attrs.get("publisherName") or (item.get("seller") or {}).get("name") or None,
    }


def _escanear_epic(cc_code, symbol, no_decimals, free_badge):
    resultados = []
    slugs = set()
    pais = cc_code.upper()
    headers = dict(HEADERS_BASE, **{
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    })

    try:
        res = requests.get(
            "https://store-site-backend-static.ak.epicgames.com/freeGamesPromotions",
            params={"locale": "es-ES", "country": pais, "allowCountries": pais},
            headers=headers, timeout=15,
        )
        res.raise_for_status()
        elementos = (res.json().get("data", {}).get("Catalog", {})
                     .get("searchStore", {}).get("elements", []))

        for item in elementos:
            precio = ((item.get("price") or {}).get("totalPrice") or {})
            base = (precio.get("originalPrice") or 0) / 100
            final = (precio.get("discountPrice") or 0) / 100

            gratis = _epic_es_gratis(item) or (final == 0 and base > 0)
            if not gratis or base <= 0:
                continue

            link, slug = _epic_link(item)
            clave = slug or item.get("title", "")
            if clave in slugs:
                continue
            slugs.add(clave)
            _epic_guardar_cache(slug, item)

            resultados.append({
                "nombre": item.get("title", "Juego Epic"),
                "desc": 100,
                "p_orig": _fmt(base, symbol, no_decimals),
                "p_final": free_badge,
                "tipo": _epic_tipo(item),
                "link": link,
                "tienda": "Epic Games",
                "img_url": _epic_img(item),
                "ref_id": slug,
            })
    except Exception as e:
        _log("epic gratis", e)

    query = """
    query searchStoreQuery($count: Int, $start: Int, $country: String!, $locale: String!) {
      Catalog {
        searchStore(count: $count, start: $start, country: $country, locale: $locale, onSale: true) {
          paging { total }
          elements {
            title
            offerType
            productSlug
            urlSlug
            keyImages { type url }
            catalogNs { mappings(pageType: "productHome") { pageSlug } }
            offerMappings { pageSlug }
            price(country: $country) { totalPrice { originalPrice discountPrice } }
          }
        }
      }
    }
    """
    por_pagina = 100
    total = None

    for pagina in range(EPIC_MAX_PAGINAS):
        start = pagina * por_pagina
        if total is not None and start >= total:
            break

        payload = {
            "query": query,
            "variables": {"count": por_pagina, "start": start,
                          "country": pais, "locale": "es-ES"},
        }
        try:
            res = requests.post("https://store.epicgames.com/graphql",
                                json=payload, headers=headers, timeout=15)
            res.raise_for_status()
            tienda = res.json().get("data", {}).get("Catalog", {}).get("searchStore", {})
        except Exception as e:
            _log("epic catálogo", e)
            break

        elementos = tienda.get("elements", [])
        if not elementos:
            break
        total = (tienda.get("paging") or {}).get("total", total)

        for item in elementos:
            link, slug = _epic_link(item)
            clave = slug or item.get("title", "")
            if clave in slugs:
                continue

            precio = ((item.get("price") or {}).get("totalPrice") or {})
            base = (precio.get("originalPrice") or 0) / 100
            final = (precio.get("discountPrice") or 0) / 100
            if base <= 0 or final >= base:
                continue

            pct = int(round((1 - (final / base)) * 100))
            if pct <= 0 or pct >= 100:
                continue

            slugs.add(clave)
            _epic_guardar_cache(slug, item)
            resultados.append({
                "nombre": item.get("title", "Juego Epic"),
                "desc": pct,
                "p_orig": _fmt(base, symbol, no_decimals),
                "p_final": _fmt(final, symbol, no_decimals),
                "tipo": _epic_tipo(item),
                "link": link,
                "tienda": "Epic Games",
                "img_url": _epic_img(item),
                "ref_id": slug,
            })

        time.sleep(0.15)

    return resultados


_IDIOMAS_EPIC_RE = re.compile(
    r"(?:English|French|German|Spanish|Italian|Portuguese|Russian|Japanese|Korean|"
    r"Chinese|Polish|Turkish|Arabic|Czech|Dutch|Hungarian|Thai|Ukrainian|Swedish|"
    r"Finnish|Norwegian|Danish|Greek|Indonesian|Vietnamese|Romanian|Hindi|Hebrew|"
    r"Bulgarian|Catalan|Croatian|Slovak|Serbian|Lithuanian|Latvian|Estonian)"
    r"(?: \([^)]{2,30}\))?"
)


def _epic_fecha(texto, idioma):
    t = (texto or "").strip()
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", t)
    if m:
        mes, dia, anio = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if anio < 100:
            anio += 2000 if anio < 70 else 1900
        if 1 <= mes <= 12 and 1 <= dia <= 31:
            return f"{dia} {_MESES.get(idioma, _MESES['en'])[mes - 1].upper()} {anio}"
    return _fmt_fecha(t, idioma)


def _detalle_epic_html(slug, idioma, resultado):
    html = None
    for ruta in ("p", "bundles"):
        res = requests.get(
            f"https://store.epicgames.com/en-US/{ruta}/{slug}",
            headers=dict(HEADERS_BASE, **{"Accept": "text/html,application/xhtml+xml",
                                           "Accept-Language": "en-US,en;q=0.9"}),
            timeout=20,
        )
        if res.status_code == 200 and res.text:
            html = res.text
            break
    if not html:
        raise ValueError("la página de la tienda no respondió 200")

    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript", "svg"]):
        t.decompose()
    lineas = [re.sub(r"\s+", " ", l.replace("\u200b", "")).strip()
              for l in soup.get_text("\n").split("\n")]
    lineas = [l for l in lineas if l]
    plano = " ".join(lineas)
    encontrado = False

    m = re.search(r"Developer (.{1,100}?) Publisher (.{1,100}?) Release Date (\S+)", plano)
    if m:
        dev, pub, fecha = m.group(1), m.group(2), m.group(3)
    else:
        md = re.search(r"Developer (.{1,80}?) (?:Publisher|Release Date|Platform)\b", plano)
        mp = re.search(r"Publisher (.{1,80}?) (?:Release Date|Platform)\b", plano)
        mf = re.search(r"Release Date (\d{1,2}/\d{1,2}/\d{2,4})", plano)
        dev = md.group(1) if md else None
        pub = mp.group(1) if mp else None
        fecha = mf.group(1) if mf else None
    if dev:
        resultado["desarrollador"] = dev.strip()
        encontrado = True
    if pub:
        resultado["editor"] = pub.strip()
        encontrado = True
    if fecha:
        resultado["fecha_lanzamiento"] = _epic_fecha(fecha, idioma)
        encontrado = True

    def _indice(etiqueta):
        return next((i for i, l in enumerate(lineas) if l.lower() == etiqueta), -1)

    def _recoger(i, paradas):
        salida = []
        for l in lineas[i + 1:i + 12]:
            if l.lower() in paradas or len(l) > 40:
                break
            salida.append(l)
        return salida

    i_gen, i_feat = _indice("genres"), _indice("features")
    fin_bloque = 0
    if i_gen >= 0:
        generos = _recoger(i_gen, {"features"})
        if generos:
            resultado["generos"] = generos
            encontrado = True
        fin_bloque = i_gen + 1 + len(generos)
    if i_feat >= 0:
        feats = _recoger(i_feat, set())
        if feats:
            resultado["categorias"] = feats
            encontrado = True
        fin_bloque = max(fin_bloque, i_feat + 1 + len(feats))

    if fin_bloque and not resultado["descripcion"]:
        for l in lineas[fin_bloque:fin_bloque + 10]:
            if len(l) >= 60:
                resultado["descripcion"] = _a_texto(l, 700)
                encontrado = True
                break

    i = plano.find("Languages Supported")
    if i >= 0:
        seg = plano[i:i + 900].split("©")[0]
        ia, it = seg.find("Audio:"), seg.find("Text:")
        audio = _IDIOMAS_EPIC_RE.findall(seg[ia:it if it > ia else None]) if ia >= 0 else []
        texto = _IDIOMAS_EPIC_RE.findall(seg[it:]) if it >= 0 else []
        vistos = {n: True for n in audio}
        for n in texto:
            vistos.setdefault(n, False)
        if vistos:
            resultado["idiomas"] = [{"idioma": n, "audio": a} for n, a in vistos.items()]
            encontrado = True

    if not encontrado:
        raise ValueError("no se reconoció ningún dato en el HTML (¿cambió el formato?)")


def _epic_idiomas(lista):
    vistos = {}
    for entrada in _lista_texto(lista):
        if ":" not in entrada:
            continue
        etiqueta, resto = entrada.split(":", 1)
        etiqueta = etiqueta.strip().lower()
        es_audio = etiqueta.startswith(("audio", "áudio", "voice", "voz"))
        es_texto = etiqueta.startswith(("text", "texto", "subtit", "interface"))
        if not (es_audio or es_texto):
            continue
        for nombre in resto.replace(";", ",").split(","):
            nombre = nombre.strip()
            if nombre:
                vistos[nombre] = vistos.get(nombre, False) or es_audio
    return [{"idioma": n, "audio": a} for n, a in vistos.items()]


def _detalle_epic_contenido(slug, locale, idioma, resultado):
    res = requests.get(
        f"https://store-content.ak.epicgames.com/api/{locale}/content/products/{slug}",
        headers=dict(HEADERS_BASE, **{"Accept": "application/json"}), timeout=15,
    )
    res.raise_for_status()
    datos = res.json()
    resultado["nombre"] = datos.get("productName") or datos.get("_title") or resultado["nombre"]

    for pagina in (datos.get("pages") or []):
        d = pagina.get("data") or {}
        about = d.get("about") or {}
        meta = d.get("meta") or {}
        req = d.get("requirements") or {}

        if not resultado["descripcion"]:
            resultado["descripcion"] = _a_texto(
                about.get("shortDescription") or about.get("description"), 700
            )
        if not resultado["desarrollador"]:
            resultado["desarrollador"] = (
                ", ".join(_lista_texto(meta.get("developer")))
                or about.get("developerAttribution") or None
            )
        if not resultado["editor"]:
            resultado["editor"] = (
                ", ".join(_lista_texto(meta.get("publisher")))
                or about.get("publisherAttribution") or None
            )
        if not resultado["fecha_lanzamiento"] and meta.get("releaseDate"):
            resultado["fecha_lanzamiento"] = _fmt_fecha(str(meta["releaseDate"]), idioma)
        if not resultado["categorias"]:
            resultado["categorias"] = _lista_texto(meta.get("tags"))
        if not resultado["idiomas"]:
            resultado["idiomas"] = _epic_idiomas(req.get("languages"))


def _detalle_epic_graphql(slug, locale, idioma, cc_code, resultado):
    query = """
    query searchStoreQuery($country: String!, $locale: String!, $keywords: String) {
      Catalog {
        searchStore(count: 10, country: $country, locale: $locale, keywords: $keywords) {
          elements {
            title
            description
            effectiveDate
            productSlug
            urlSlug
            seller { name }
            customAttributes { key value }
            catalogNs { mappings(pageType: "productHome") { pageSlug } }
            offerMappings { pageSlug }
          }
        }
      }
    }
    """
    payload = {
        "query": query,
        "variables": {"country": cc_code.upper(), "locale": locale,
                      "keywords": re.sub(r"-[0-9a-f]{6}$", "", slug).replace("-", " ")},
    }
    res = requests.post(
        "https://store.epicgames.com/graphql", json=payload,
        headers=dict(HEADERS_BASE, **{"Accept": "application/json",
                                       "Content-Type": "application/json"}),
        timeout=15,
    )
    res.raise_for_status()
    cuerpo = res.json()
    if cuerpo.get("errors"):
        _log("epic graphql", (cuerpo["errors"][0] or {}).get("message"))
    elementos = (((cuerpo.get("data") or {}).get("Catalog") or {})
                 .get("searchStore") or {}).get("elements") or []
    item = next((e for e in elementos if _epic_link(e)[1] == slug), None)
    if not item:
        return

    resultado["nombre"] = resultado["nombre"] or item.get("title")
    if not resultado["descripcion"]:
        resultado["descripcion"] = _a_texto(item.get("description"), 700)
    atributos = {a.get("key"): a.get("value") for a in (item.get("customAttributes") or [])}
    if not resultado["desarrollador"]:
        resultado["desarrollador"] = atributos.get("developerName") or None
    if not resultado["editor"]:
        resultado["editor"] = (atributos.get("publisherName")
                               or (item.get("seller") or {}).get("name") or None)
    fecha = item.get("effectiveDate") or ""
    m = re.match(r"(\d{4})", fecha)
    if not resultado["fecha_lanzamiento"] and m and int(m.group(1)) <= datetime.now().year + 1:
        resultado["fecha_lanzamiento"] = _fmt_fecha(fecha, idioma)


def _detalle_epic(slug, idioma, cc_code):
    locale = IDIOMA_EPIC_LOCALE.get(idioma, "en-US")
    resultado = {
        "tienda": "Epic Games", "nombre": None, "descripcion": "",
        "resena_texto": None, "resena_pct": None, "resena_total": None,
        "fecha_lanzamiento": None, "desarrollador": None, "editor": None,
        "generos": [], "categorias": [], "idiomas": [],
        "metacritic_score": None, "metacritic_url": None,
        "imagen": None, "controles": None,
    }
    if not slug:
        return resultado

    c = _EPIC_CACHE.get(slug) or {}
    resultado["descripcion"] = _a_texto(c.get("descripcion"), 700)
    resultado["desarrollador"] = c.get("desarrollador")
    resultado["editor"] = c.get("editor")

    def _completo():
        return bool(resultado["descripcion"] and resultado["desarrollador"]
                    and resultado["fecha_lanzamiento"] and resultado["idiomas"])

    fuentes = (
        ("html", lambda: _detalle_epic_html(slug, idioma, resultado)),
        ("contenido", lambda: _detalle_epic_contenido(slug, locale, idioma, resultado)),
        ("graphql", lambda: _detalle_epic_graphql(slug, locale, idioma, cc_code, resultado)),
    )
    for nombre, fuente in fuentes:
        if _completo():
            break
        try:
            fuente()
        except Exception as e:
            _log(f"detalle_epic {nombre} ({slug})", e)

    return resultado


# ---------------------------------------------------------------------
# GOG
# ---------------------------------------------------------------------
def _gog_num(valor):
    try:
        return float(valor)
    except (TypeError, ValueError):
        return 0.0


def _gog_precios(producto):
    precio = producto.get("price") or {}
    base = _gog_num((precio.get("baseMoney") or {}).get("amount"))
    final = _gog_num((precio.get("finalMoney") or {}).get("amount"))
    if base <= 0:
        base = _gog_num(precio.get("base"))
        final = _gog_num(precio.get("final"))
    pct = int(round((1 - (final / base)) * 100)) if base > 0 else 0
    return base, final, pct


def _gog_link(producto):
    enlace = producto.get("storeLink")
    if enlace:
        return enlace
    slug = producto.get("slug")
    return f"https://www.gog.com/es/game/{slug}" if slug else "https://www.gog.com"


_GOG_CACHE = {}
_GOG_CAMPOS_CACHE = ("developers", "publishers", "genres", "features",
                     "releaseDate", "storeReleaseDate", "reviewsRating")


def _gog_guardar_cache(producto):
    pid = str(producto.get("id") or "")
    if pid:
        _GOG_CACHE[pid] = {k: producto.get(k) for k in _GOG_CAMPOS_CACHE}


def _escanear_gog(cc_code, moneda_gog, free_badge):
    resultados = []
    ids = set()
    moneda = moneda_gog if moneda_gog in GOG_MONEDAS_VALIDAS else "USD"
    session = requests.Session()
    session.headers.update(dict(HEADERS_BASE, **{
        "Accept": "application/json",
        "Accept-Language": "es-ES,es;q=0.9",
    }))

    def _pedir(pagina, extra):
        params = {
            "limit": 48, "page": pagina,
            "countryCode": cc_code.upper(), "locale": "es-ES",
            "currencyCode": moneda,
            "productType": "in:game,pack,dlc,extras",
            "order": "desc:discount",
        }
        params.update(extra)
        r = session.get("https://catalog.gog.com/v1/catalog", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    def _fmt_gog(valor):
        return f"{moneda} {valor:,.2f}"

    def _tipo(producto):
        return "DLC" if producto.get("productType") in ("dlc", "extras", "DLC") else "Juego"

    # 1) Gratis por promoción
    pagina, total_paginas = 1, 1
    while pagina <= min(total_paginas, 10):
        try:
            datos = _pedir(pagina, {"discounted": "eq:true", "price": "between:0,0"})
        except Exception as e:
            _log("gog gratis", e)
            break
        total_paginas = datos.get("pages", 1) or 1

        for p in datos.get("products", []):
            base, final, _ = _gog_precios(p)
            if base <= 0 or final > 0:
                continue
            pid = p.get("id")
            if pid in ids:
                continue
            _gog_guardar_cache(p)
            ids.add(pid)
            resultados.append({
                "nombre": p.get("title", "Juego GOG"),
                "desc": 100,
                "p_orig": _fmt_gog(base),
                "p_final": free_badge,
                "tipo": _tipo(p),
                "link": _gog_link(p),
                "tienda": "GOG",
                "img_url": p.get("coverHorizontal") or p.get("image") or "",
                "ref_id": str(pid) if pid else "",
            })
        pagina += 1
        time.sleep(0.15)

    # 2) Giveaway de portada
    try:
        r = session.get("https://www.gog.com/giveaway/api/status", timeout=10)
        if r.status_code == 200 and r.text.strip():
            datos = r.json()
            if isinstance(datos, dict):
                titulo = datos.get("title") or (datos.get("giveaway") or {}).get("name")
                if titulo:
                    resultados.append({
                        "nombre": f"{titulo} (Giveaway)",
                        "desc": 100,
                        "p_orig": "",
                        "p_final": free_badge,
                        "tipo": "Juego",
                        "link": "https://www.gog.com/giveaway",
                        "tienda": "GOG",
                        "img_url": "",
                        "ref_id": "",
                    })
    except Exception as e:
        _log("gog giveaway", e)

    # 3) Catálogo completo de descuentos, ordenado por mayor % primero.
    # Se amplía GOG_MAX_PAGINAS a 150 para garantizar que alcance a recuperar
    # todos los elementos con menor porcentaje de descuento (hasta 1% OFF).
    pagina, total_paginas = 1, 1
    while pagina <= min(total_paginas, GOG_MAX_PAGINAS):
        try:
            datos = _pedir(pagina, {"discounted": "eq:true"})
        except Exception as e:
            _log("gog catálogo", e)
            break
        total_paginas = datos.get("pages", 1) or 1
        productos = datos.get("products", [])
        if not productos:
            break

        for p in productos:
            pid = p.get("id")
            if pid in ids:
                continue
            base, final, pct = _gog_precios(p)
            if base <= 0 or final >= base or pct <= 0 or pct >= 100:
                continue
            _gog_guardar_cache(p)
            ids.add(pid)
            resultados.append({
                "nombre": p.get("title", "Juego GOG"),
                "desc": pct,
                "p_orig": _fmt_gog(base),
                "p_final": _fmt_gog(final),
                "tipo": _tipo(p),
                "link": _gog_link(p),
                "tienda": "GOG",
                "img_url": p.get("coverHorizontal") or p.get("image") or "",
                "ref_id": str(pid) if pid else "",
            })

        pagina += 1
        time.sleep(0.15)

    return resultados


def _gog_idiomas(pid, locale):
    try:
        res = requests.get(
            f"https://api.gog.com/v2/games/{pid}", params={"locale": locale},
            headers=dict(HEADERS_BASE, **{"Accept": "application/json"}), timeout=10,
        )
        res.raise_for_status()
        locs = ((res.json().get("_embedded") or {}).get("localizations")) or []
        vistos = {}
        for loc in locs:
            emb = loc.get("_embedded") or {}
            nombre = (emb.get("language") or {}).get("name")
            tipo = (emb.get("localizationScope") or {}).get("type")
            if nombre:
                vistos[nombre] = vistos.get(nombre, False) or tipo == "audio"
        return [{"idioma": n, "audio": a} for n, a in vistos.items()]
    except Exception as e:
        _log(f"gog idiomas ({pid})", e)
        return []


def _gog_valoracion(valor):
    valor = _gog_num(valor)
    if valor <= 0:
        return None, None
    escala = 5 if valor <= 5 else (50 if valor <= 50 else 100)
    return valor / (escala / 5), int(round(valor / escala * 100))


def _detalle_gog(pid, idioma, moneda_gog):
    locale = IDIOMA_GOG_LOCALE.get(idioma, "en-US")
    resultado = {
        "tienda": "GOG", "nombre": None, "descripcion": "",
        "resena_texto": None, "resena_pct": None, "resena_total": None,
        "fecha_lanzamiento": None, "desarrollador": None, "editor": None,
        "generos": [], "categorias": [], "idiomas": [],
        "metacritic_score": None, "metacritic_url": None,
        "imagen": None, "controles": None,
    }
    if not pid:
        return resultado
    pid = str(pid)

    c = _GOG_CACHE.get(pid) or {}
    if c:
        resultado["desarrollador"] = ", ".join(_lista_texto(c.get("developers"))) or None
        resultado["editor"] = ", ".join(_lista_texto(c.get("publishers"))) or None
        resultado["generos"] = _lista_texto(c.get("genres"))
        resultado["categorias"] = _lista_texto(c.get("features"))
        fecha = c.get("releaseDate") or c.get("storeReleaseDate")
        if fecha:
            resultado["fecha_lanzamiento"] = _fmt_fecha(str(fecha), idioma)
        sobre5, pct = _gog_valoracion(c.get("reviewsRating"))
        if pct is not None:
            etiquetas = {"es": "Valoración", "en": "Rating", "pt": "Avaliação"}
            resultado["resena_pct"] = pct
            resultado["resena_texto"] = f"{etiquetas.get(idioma, etiquetas['en'])} {sobre5:.1f}/5"

    try:
        res = requests.get(
            f"https://api.gog.com/products/{pid}",
            params={"expand": "description", "locale": locale},
            headers=dict(HEADERS_BASE, **{"Accept": "application/json"}), timeout=10,
        )
        res.raise_for_status()
        d = res.json()
        resultado["nombre"] = d.get("title")
        desc = d.get("description") or {}
        resultado["descripcion"] = _a_texto(desc.get("lead") or desc.get("full"), 700)
        if not resultado["fecha_lanzamiento"] and d.get("release_date"):
            resultado["fecha_lanzamiento"] = _fmt_fecha(str(d["release_date"]), idioma)
    except Exception as e:
        _log(f"detalle_gog producto ({pid})", e)

    resultado["idiomas"] = _gog_idiomas(pid, locale)

    return resultado


# ---------------------------------------------------------------------
# Punto de entrada llamado desde Kotlin: escaneo general
# ---------------------------------------------------------------------
def get_game_deals(cc_code="co", symbol="COL$", no_decimals=True,
                    gog_currency="USD", free_badge="¡GRATIS!",
                    tiendas="steam,epic,gog"):
    resultados = {}

    # "tiendas" viene de Kotlin como texto separado por comas.
    # Si llega vacío o con algo desconocido, se escanean todas.
    seleccion = {t.strip().lower() for t in (tiendas or "").split(",") if t.strip()}
    seleccion &= {"steam", "epic", "gog"}
    if not seleccion:
        seleccion = {"steam", "epic", "gog"}

    def _correr(nombre, fn, args):
        try:
            resultados[nombre] = fn(*args)
        except Exception:
            resultados[nombre] = []

    tareas = {
        "steam": (_escanear_steam, (cc_code, symbol, no_decimals, free_badge)),
        "epic": (_escanear_epic, (cc_code, symbol, no_decimals, free_badge)),
        "gog": (_escanear_gog, (cc_code, gog_currency, free_badge)),
    }
    hilos = [
        threading.Thread(target=_correr, args=(nombre, fn, args))
        for nombre, (fn, args) in tareas.items()
        if nombre in seleccion
    ]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()

    todos = []
    for lista in resultados.values():
        todos.extend(lista)

    todos.sort(key=lambda x: x["desc"], reverse=True)

    return json.dumps(todos, ensure_ascii=False)


# ---------------------------------------------------------------------
# Punto de entrada llamado desde Kotlin: ficha de un juego puntual
# ---------------------------------------------------------------------
def get_game_detail(tienda, ref_id, idioma="es", cc_code="co", gog_currency="USD"):
    try:
        if tienda == "Steam":
            detalle = _detalle_steam(ref_id, idioma, cc_code) if ref_id else {}
        elif tienda == "Epic Games":
            detalle = _detalle_epic(ref_id, idioma, cc_code) if ref_id else {}
        elif tienda == "GOG":
            detalle = _detalle_gog(ref_id, idioma, gog_currency) if ref_id else {}
        else:
            detalle = {}
    except Exception as e:
        _log(f"get_game_detail {tienda} ({ref_id})", e)
        detalle = {}
    if not ref_id:
        _log("get_game_detail", ValueError(f"ref_id vacío para {tienda}"))

    return json.dumps(detalle, ensure_ascii=False)
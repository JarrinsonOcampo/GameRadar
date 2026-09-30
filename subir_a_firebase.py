"""
subir_a_firebase.py — corre en GitHub Actions, NO en el celular del usuario.

Reutiliza scraper.py (el mismo que usa la app con Chaquopy, sin cambiarle
nada) para escanear Steam/Epic/GOG y sube el resultado a Firebase
Realtime Database. Por cada moneda escribe TRES nodos, todos en una sola
escritura atómica (o se guardan los tres o ninguno):

  /ofertas/{cc}   La lista completa que muestra la pantalla principal de la app
                  (actualizado, total, juegos). Pesa varios MB. Cada juego lleva
                  además "desde": el momento en que se detectó su descuento actual.

  /resumen/{cc}   Versión liviana que lee el Worker de notificaciones en segundo
                  plano, para no bajar varios MB en cada revisión:
                    meta     {firma, total, actualizado, version}
                    control  unos pocos {link, id} para que la app compruebe, con
                             datos reales, que calcula el MISMO id que este script
                    gratis   solo los juegos con 100 % de descuento
                    d        {id: {d: descuento, o: precio_orig, p: precio_final}}
                             para que el Worker lea solo SUS favoritos
                  "firma" cambia solo si cambia algún descuento; si es igual a la
                  de la revisión anterior, el Worker ni siquiera descarga el resto.

  /estado/{cc}    {id: "descuento|desde"}: memoria compacta de este script para
                  calcular "desde" sin volver a bajar los MB de /ofertas.
                  No la lee la app.

IDENTIFICADOR DE JUEGO (id_juego): primeros 16 caracteres hexadecimales del
SHA-1 del link en UTF-8. La app calcula lo mismo en IdJuego.kt. Si algún día se
cambia aquí, hay que cambiarlo allá (y en los vectores de prueba).

Variables de entorno que este script espera (las pone el workflow de
GitHub Actions, ver escanear.yml):
    GOOGLE_APPLICATION_CREDENTIALS  -> ruta al JSON de la cuenta de servicio de Firebase
    FIREBASE_DATABASE_URL           -> ej: https://gameradar-xxxxx-default-rtdb.firebaseio.com

Dependencias (ver requirements.txt):
    requests, beautifulsoup4, firebase-admin
"""

import copy
import hashlib
import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, db

import scraper  # el mismo scraper.py de siempre, copiado en esta carpeta

# --------------------------------------------------------------------
# Las mismas 10 monedas que ya soporta la app (ver MONEDAS en
# MainActivity.kt).
# Formato: (cc_code, symbol, no_decimals, gog_currency)
# --------------------------------------------------------------------
FREE_BADGE = "¡GRATIS!"

MONEDAS = [
    ("co", "COL$", True, "USD"),    # Colombia
    ("us", "$", False, "USD"),      # Estados Unidos
    ("es", "€", False, "EUR"),      # España / Eurozona
    ("mx", "MEX$", False, "USD"),   # México
    ("ar", "US$", False, "USD"),    # Argentina (Steam, Epic y GOG ya cobran en USD ahí)
    ("cl", "CLP$", True, "USD"),    # Chile
    ("pe", "S/", False, "USD"),     # Perú
    ("br", "R$", False, "BRL"),     # Brasil
    ("gb", "£", False, "GBP"),      # Reino Unido
    ("ca", "CDN$", False, "CAD"),   # Canadá
    # Steam los agrupa en la región "Latinoamérica USD" (precios en dólares, distintos a los de EE. UU.)
    ("bo", "US$", False, "USD"),    # Bolivia
    ("ec", "US$", False, "USD"),    # Ecuador
    ("py", "US$", False, "USD"),    # Paraguay
    ("ve", "US$", False, "USD"),    # Venezuela
    ("sv", "US$", False, "USD"),    # El Salvador
    ("pa", "US$", False, "USD"),    # Panamá
    ("ni", "US$", False, "USD"),    # Nicaragua
    # Steam tiene moneda propia en estos dos
    ("uy", "$U", True, "USD"),      # Uruguay
    ("cr", "₡", True, "USD"),       # Costa Rica
]

# Cuántas monedas se escanean AL MISMO TIEMPO. Subir este número acelera
# el job, pero golpea Steam/Epic/GOG con más peticiones simultáneas y
# aumenta el riesgo de que te empiecen a bloquear (rate limiting).
# Con 1 se escanea una moneda a la vez: es lo más seguro contra el bloqueo de
# Steam (que es lo que dejaba vacías las ofertas de Steam en varias monedas).
# Si ves que los logs ya no muestran HTTP 429, puedes probar con 2.
MONEDAS_EN_PARALELO = 1

# Si el scraper devuelve menos de esto, casi seguro una tienda falló o nos
# bloquearon: NO se sobrescribe lo que ya hay en Firebase.
MINIMO_OFERTAS_VALIDAS = 1

VERSION_RESUMEN = 2

# Pausa (segundos) entre una moneda y la siguiente, para que Steam "enfríe" la IP
# del runner. Sin esto, cuando Steam bloquea una moneda, la siguiente arranca ya
# bloqueada y falla o sube incompleta.
PAUSA_ENTRE_MONEDAS = 60

# Pausa entre páginas de Steam SOLO en el backend (la app sigue usando la de scraper.py).
# Steam empezaba a dar 429 cada ~30 páginas y, si se insiste, pasa a bloqueo suave (HTML).
# Ir un poco más lento evita llegar a ese punto.
PAUSA_STEAM_ENTRE_PAGINAS = 2.0
scraper.STEAM_PAUSA_ENTRE_PAGINAS = PAUSA_STEAM_ENTRE_PAGINAS

# Si una moneda falla (bloqueo), se espera esto y se reintenta UNA vez al final del job.
PAUSA_ANTES_DE_REINTENTAR = 300

# Steam da el MISMO precio a todos los países de su región "Latinoamérica USD"
# (Argentina, Bolivia, Ecuador, Paraguay, Venezuela, El Salvador, Panamá, Nicaragua, ...): el precio
# lo fija el desarrollador por región, no por país. En vez de escanear Steam 7 veces
# (700 peticiones, y Steam es la tienda que bloquea), se escanea UNA vez con "pa" y esa
# lista de STEAM se reutiliza en los demás países.
# Epic y GOG NO se comparten: son livianas y sí se consultan con el código de cada país,
# por si alguna de las dos tiene precios distintos por país.
# Cada país conserva su propio "desde" y su propio resumen.
# Formato: {país que se escanea: [países que reciben la lista de Steam]}
COPIAS = {
    "pa": ["ar", "bo", "ec", "py", "ve", "sv", "ni"],
}
_DESTINOS_DE_COPIA = {d for destinos in COPIAS.values() for d in destinos}
_CONFIG_MONEDA = {m[0]: m for m in MONEDAS}

# Un juego con este % de descuento o más cuenta para "grandes_total" en el resumen
# diario (p. ej. "45 juegos con más de 50% de descuento hoy").
DESCUENTO_GRANDE = 50

# Cuántos nombres de ejemplo van en meta.novedades_top (para el texto de la
# notificación, tipo "Portal 2, Elden Ring y 12 más entraron en descuento").
MAX_EJEMPLOS_NOVEDADES = 3


# --------------------------------------------------------------------
# Identificador de juego (DEBE coincidir con IdJuego.kt en la app)
# --------------------------------------------------------------------
def id_juego(link):
    """16 primeros caracteres hex (minúsculas) del SHA-1 del link codificado en UTF-8."""
    return hashlib.sha1(link.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------
# Estado previo, para calcular "desde"
# --------------------------------------------------------------------
def _cargar_previos(cc_code):
    """Devuelve {id: (descuento, desde_ms)} de la corrida anterior.
    {} significa "primera corrida de esta moneda".

    Si Firebase falla, la excepción se propaga a propósito: es mejor no subir nada
    que subir con fechas 'desde' perdidas."""
    estado = db.reference(f"/estado/{cc_code}").get()
    if estado is not None:
        if not isinstance(estado, dict):
            raise RuntimeError(f"/estado/{cc_code} no tiene el formato esperado")
        previos = {}
        for jid, valor in estado.items():
            try:
                desc, desde = str(valor).split("|")
                previos[jid] = (int(desc), int(desde))
            except ValueError:
                continue  # entrada corrupta: se ignora, no tumba la corrida
        return previos

    # Migración: todavía no existe /estado (primera corrida con este formato), pero
    # puede haber una lista de la versión anterior. La aprovechamos una sola vez
    # para no perder las fechas "desde" ya calculadas.
    anteriores = db.reference(f"/ofertas/{cc_code}/juegos").get()
    if isinstance(anteriores, dict):
        anteriores = list(anteriores.values())
    previos = {}
    for j in anteriores or []:
        if isinstance(j, dict) and j.get("link"):
            previos[id_juego(j["link"])] = (int(j.get("desc") or 0), int(j.get("desde") or 0))
    return previos


def _asignar_desde(juegos, previos):
    """Además de poner 'desde' en cada juego (ver docstring del módulo), cuenta cuántos
    son 'novedades' de ESTA corrida: juegos nuevos o que cambiaron de % (no cuenta la
    primera corrida de una moneda, para no marcar de una vez ~8.000 juegos como
    novedad). Ese conteo alimenta el aviso de "hay juegos nuevos en descuento".
    Devuelve la lista de esas novedades (para sacar ejemplos), ordenada de mayor a
    menor descuento."""
    ahora_ms = int(time.time() * 1000)
    novedades = []
    for j in juegos:
        anterior = previos.get(id_juego(j.get("link", "")))
        if anterior is not None and anterior[0] == int(j.get("desc") or 0):
            j["desde"] = anterior[1]          # sin cambios: conserva la fecha
        elif not previos:
            j["desde"] = 0                    # primera corrida: "ya estaba antes"
        else:
            j["desde"] = ahora_ms             # juego nuevo o cambió el porcentaje
            novedades.append(j)
    novedades.sort(key=lambda j: int(j.get("desc") or 0), reverse=True)
    return novedades


# --------------------------------------------------------------------
# Resumen liviano + firma + estado
# --------------------------------------------------------------------
def _elegir_control(links):
    """Elige unos pocos links de ejemplo para que la app verifique su cálculo de id
    con datos reales: el primero, el más largo, y el que más caracteres no-ASCII tenga
    (los que más fácil se calculan distinto si hubiera un problema de codificación)."""
    if not links:
        return []
    elegidos = [links[0], max(links, key=len)]
    no_ascii = [l for l in links if any(ord(c) > 127 for c in l)]
    if no_ascii:
        elegidos.append(max(no_ascii, key=lambda l: sum(ord(c) > 127 for c in l)))
    return [{"link": l, "id": id_juego(l)} for l in dict.fromkeys(elegidos)]


def construir_nodos(juegos, actualizado_iso, novedades):
    """A partir de la lista final (con 'desde') arma los tres nodos de Firebase.
    [novedades]: lo que devolvió _asignar_desde (juegos nuevos o con % cambiado en
    esta corrida, ya ordenados de mayor a menor descuento)."""
    d = {}
    estado = {}
    gratis = {}
    ids = {}
    links = []
    grandes_total = 0

    for j in juegos:
        link = j.get("link") or ""
        if not link:
            continue
        jid = id_juego(link)
        if jid in ids and ids[jid] != link:
            # Dos links distintos con el mismo id de 64 bits: prácticamente imposible,
            # pero si pasara, es mejor detener la corrida que confundir favoritos.
            raise RuntimeError(f"colisión de id_juego entre {ids[jid]!r} y {link!r}")
        if jid not in ids:
            links.append(link)
        ids[jid] = link

        desc = int(j.get("desc") or 0)
        d[jid] = {"d": desc, "o": j.get("p_orig") or "", "p": j.get("p_final") or ""}
        estado[jid] = f"{desc}|{int(j.get('desde') or 0)}"
        if desc >= DESCUENTO_GRANDE:
            grandes_total += 1

        # Mismo criterio que el Worker anterior: desc >= 100 y sin repetir link
        # (se queda con la primera aparición).
        if desc >= 100 and link not in gratis:
            gratis[link] = {
                "nombre": j.get("nombre") or "Desconocido",
                "link": link,
                "tienda": j.get("tienda") or "Steam",
                "tipo": j.get("tipo") or "Juego",
                "desc": desc,
                "p_orig": j.get("p_orig") or "",
                "p_final": j.get("p_final") or "",
            }

    # La firma depende SOLO de lo que decide si hay que avisar: qué juegos hay y con
    # qué descuento (los gratis son los de desc >= 100). Cambios de precio sin cambio
    # de porcentaje no la alteran.
    firma = hashlib.sha1(
        "\n".join(sorted(f"{k}:{v['d']}" for k, v in d.items())).encode("utf-8")
    ).hexdigest()[:16]

    nombres_novedades = [n.get("nombre") or "Desconocido" for n in novedades[:MAX_EJEMPLOS_NOVEDADES]]

    return {
        "ofertas": {
            "actualizado": actualizado_iso,
            "total": len(juegos),
            "juegos": juegos,
        },
        "resumen": {
            "meta": {
                "firma": firma,
                "total": len(d),
                "actualizado": actualizado_iso,
                "version": VERSION_RESUMEN,
                # Para el aviso de "hay juegos nuevos en descuento" (por revisión):
                "novedades": len(novedades),
                "novedades_top": nombres_novedades,
                # Para el resumen diario (lectura ultraliviana, solo estos 2 números):
                "gratis_total": len(gratis),
                "grandes_total": grandes_total,
            },
            "control": _elegir_control(links),
            "gratis": list(gratis.values()),
            "d": d,
        },
        "estado": estado,
    }


# --------------------------------------------------------------------
# Una moneda
# --------------------------------------------------------------------
def _publicar(cc_code, juegos, steam_incompleto):
    """Calcula 'desde' y los tres nodos de ESTE país y los sube a Firebase."""
    previos = _cargar_previos(cc_code)
    if steam_incompleto:
        if previos:
            # Steam nos cortó a mitad de camino: subir esto dejaría la moneda con muchos
            # menos juegos que antes. Se conserva lo que ya hay.
            raise RuntimeError("la lista de Steam quedó incompleta (bloqueo); "
                               "se conserva lo anterior en Firebase")
        print(f"[{cc_code}] AVISO: Steam quedó incompleto, pero es la primera vez de esta "
              f"moneda: se sube igual y se completa en la próxima corrida")

    novedades = _asignar_desde(juegos, previos)
    nodos = construir_nodos(juegos, datetime.now(timezone.utc).isoformat(), novedades)

    print(f"[{cc_code}] {len(juegos)} ofertas, subiendo a Firebase...")
    # Una sola escritura atómica en tres rutas: o se guardan las tres o ninguna, así
    # /resumen y /estado nunca quedan desfasados respecto a /ofertas.
    db.reference("/").update({
        f"ofertas/{cc_code}": nodos["ofertas"],
        f"resumen/{cc_code}": nodos["resumen"],
        f"estado/{cc_code}": nodos["estado"],
    })
    print(f"[{cc_code}] listo.")


def _escanear_y_subir(cc_code, symbol, no_decimals, gog_currency):
    print(f"[{cc_code}] escaneando Steam + Epic + GOG...")
    json_texto = scraper.get_game_deals(
        cc_code=cc_code,
        symbol=symbol,
        no_decimals=no_decimals,
        gog_currency=gog_currency,
        free_badge=FREE_BADGE,
        tiendas="steam,epic,gog",
    )
    juegos = json.loads(json_texto)
    steam_incompleto = scraper.STEAM_INCOMPLETO

    if len(juegos) < MINIMO_OFERTAS_VALIDAS:
        raise RuntimeError("el scraper devolvió 0 ofertas; se conserva lo anterior en Firebase")

    # Epic y GOG pueden responder bien aunque Steam nos haya bloqueado. Sin esta
    # comprobación se subiría la moneda SIN Steam y se pisarían los datos buenos.
    if not any(j.get("tienda") == "Steam" for j in juegos):
        raise RuntimeError("Steam devolvió 0 ofertas (¿límite de peticiones?); "
                           "se conserva lo anterior en Firebase")

    print(f"[{cc_code}] por tienda: {dict(Counter(j.get('tienda') for j in juegos))}")

    _publicar(cc_code, juegos, steam_incompleto)

    # Países que comparten la lista de Steam de este (ver COPIAS). De Steam se reutiliza lo
    # ya escaneado; Epic y GOG se consultan con el código de cada país. Cada país recibe su
    # propia copia porque _asignar_desde modifica los juegos.
    fallaron = []
    for destino in COPIAS.get(cc_code, []):
        try:
            _, sym_d, no_dec_d, gog_d = _CONFIG_MONEDA[destino]
            print(f"[{destino}] Steam: misma lista de [{cc_code}]; escaneando Epic + GOG propios...")
            steam_d = [j for j in juegos if j.get("tienda") == "Steam"]
            epic_gog_d = json.loads(scraper.get_game_deals(
                cc_code=destino, symbol=sym_d, no_decimals=no_dec_d,
                gog_currency=gog_d, free_badge=FREE_BADGE, tiendas="epic,gog",
            ))
            if not epic_gog_d:
                # Epic y GOG fallaron para este país: mejor usar los del país base que dejarlo sin ellas.
                print(f"[{destino}] AVISO: Epic/GOG no respondieron; se usan los de [{cc_code}]")
                epic_gog_d = [j for j in juegos if j.get("tienda") != "Steam"]
            lista_d = copy.deepcopy(steam_d) + copy.deepcopy(epic_gog_d)
            lista_d.sort(key=lambda j: int(j.get("desc") or 0), reverse=True)
            print(f"[{destino}] por tienda: {dict(Counter(j.get('tienda') for j in lista_d))}")
            _publicar(destino, lista_d, steam_incompleto)
        except Exception as e:
            print(f"[{destino}] ERROR: {type(e).__name__}: {e}")
            fallaron.append(destino)
    if fallaron:
        raise RuntimeError(f"no se pudo publicar la copia para: {', '.join(fallaron)}")


def escanear_y_subir(cc_code, symbol, no_decimals, gog_currency):
    """Escanea una moneda y, pase lo que pase, espera antes de la siguiente."""
    try:
        _escanear_y_subir(cc_code, symbol, no_decimals, gog_currency)
    finally:
        time.sleep(PAUSA_ENTRE_MONEDAS)


def _monedas_a_escanear():
    """Si el workflow define MONEDAS_A_ESCANEAR (ej. "co,us,es"), solo se escanean esas.
    Así cada job del matrix de GitHub Actions se encarga de un grupo y corren en paralelo,
    cada uno con su propia IP (Steam bloquea mucho menos). Sin la variable: todas."""
    filtro = os.environ.get("MONEDAS_A_ESCANEAR", "").replace(" ", "").lower()
    if not filtro:
        return [m for m in MONEDAS if m[0] not in _DESTINOS_DE_COPIA]
    pedidas = [c for c in filtro.split(",") if c]
    conocidas = {m[0] for m in MONEDAS}
    desconocidas = [c for c in pedidas if c not in conocidas]
    if desconocidas:
        raise SystemExit(f"MONEDAS_A_ESCANEAR trae códigos que no existen en MONEDAS: {desconocidas}")
    return [m for m in MONEDAS if m[0] in pedidas]


def _correr(monedas):
    """Escanea y sube cada moneda de la lista. Devuelve los códigos que fallaron."""
    fallidas = []
    with ThreadPoolExecutor(max_workers=MONEDAS_EN_PARALELO) as pool:
        futuros = {
            pool.submit(escanear_y_subir, cc, symbol, no_dec, gog): cc
            for cc, symbol, no_dec, gog in monedas
        }
        for futuro in as_completed(futuros):
            cc_code = futuros[futuro]
            try:
                futuro.result()
            except Exception as e:
                # Si una moneda falla, las demás igual suben su parte y NO se borra
                # lo que ya había en Firebase para esa moneda.
                print(f"[{cc_code}] ERROR: {type(e).__name__}: {e}")
                fallidas.append(cc_code)
    return fallidas


def main():
    cred = credentials.Certificate(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    firebase_admin.initialize_app(cred, {
        "databaseURL": os.environ["FIREBASE_DATABASE_URL"],
    })

    monedas = _monedas_a_escanear()
    fallidas = _correr(monedas)

    if fallidas:
        # Casi siempre es un bloqueo temporal de Steam: se espera un rato y se prueba
        # otra vez solo con las que fallaron.
        print(f"Falló: {', '.join(sorted(fallidas))}. Reintento en {PAUSA_ANTES_DE_REINTENTAR}s...")
        time.sleep(PAUSA_ANTES_DE_REINTENTAR)
        fallidas = _correr([m for m in monedas if m[0] in fallidas])

    if fallidas:
        # Salir con error hace que GitHub marque la corrida en rojo y te mande un correo,
        # en vez de fallar en silencio.
        print(f"Monedas con error: {', '.join(sorted(fallidas))}")
        sys.exit(1)


if __name__ == "__main__":
    main()

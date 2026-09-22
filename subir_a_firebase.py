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

import hashlib
import json
import os
import sys
import time
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
    ("ar", "ARS$", True, "USD"),    # Argentina
    ("cl", "CLP$", True, "USD"),    # Chile
    ("pe", "S/", False, "USD"),     # Perú
    ("br", "R$", False, "BRL"),     # Brasil
    ("gb", "£", False, "GBP"),      # Reino Unido
    ("ca", "CDN$", False, "CAD"),   # Canadá
]

# Cuántas monedas se escanean AL MISMO TIEMPO. Subir este número acelera
# el job, pero golpea Steam/Epic/GOG con más peticiones simultáneas y
# aumenta el riesgo de que te empiecen a bloquear (rate limiting).
MONEDAS_EN_PARALELO = 3

# Si el scraper devuelve menos de esto, casi seguro una tienda falló o nos
# bloquearon: NO se sobrescribe lo que ya hay en Firebase.
MINIMO_OFERTAS_VALIDAS = 1

VERSION_RESUMEN = 2

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
def escanear_y_subir(cc_code, symbol, no_decimals, gog_currency):
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

    if len(juegos) < MINIMO_OFERTAS_VALIDAS:
        raise RuntimeError("el scraper devolvió 0 ofertas; se conserva lo anterior en Firebase")

    _asignar_desde_resultado = _asignar_desde(juegos, _cargar_previos(cc_code))
    nodos = construir_nodos(juegos, datetime.now(timezone.utc).isoformat(), _asignar_desde_resultado)

    print(f"[{cc_code}] {len(juegos)} ofertas encontradas, subiendo a Firebase...")
    # Una sola escritura atómica en tres rutas: o se guardan las tres o ninguna, así
    # /resumen y /estado nunca quedan desfasados respecto a /ofertas.
    db.reference("/").update({
        f"ofertas/{cc_code}": nodos["ofertas"],
        f"resumen/{cc_code}": nodos["resumen"],
        f"estado/{cc_code}": nodos["estado"],
    })
    print(f"[{cc_code}] listo.")


def main():
    cred = credentials.Certificate(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    firebase_admin.initialize_app(cred, {
        "databaseURL": os.environ["FIREBASE_DATABASE_URL"],
    })

    fallidas = []
    with ThreadPoolExecutor(max_workers=MONEDAS_EN_PARALELO) as pool:
        futuros = {
            pool.submit(escanear_y_subir, cc, symbol, no_dec, gog): cc
            for cc, symbol, no_dec, gog in MONEDAS
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

    if fallidas:
        # Salir con error hace que GitHub marque la corrida en rojo y te mande un correo,
        # en vez de fallar en silencio.
        print(f"Monedas con error: {', '.join(sorted(fallidas))}")
        sys.exit(1)


if __name__ == "__main__":
    main()

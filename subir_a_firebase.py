"""
subir_a_firebase.py — corre en GitHub Actions, NO en el celular del usuario.

Reutiliza scraper.py (el mismo que ya tenías en Chaquopy, sin cambiarle
nada) para escanear Steam/Epic/GOG y sube el resultado a Firebase
Realtime Database, en /ofertas/{cc_code}. La app en Kotlin solo lee ese
JSON ya armado (ver FirebaseOfertasRepo.kt), así que la carga es casi
instantánea para el usuario.

NOVEDAD: cada juego ahora lleva un campo "desde" (milisegundos desde 1970,
UTC) con el momento en que este script detectó su descuento actual:
    - juego que ya estaba con el MISMO descuento  -> conserva su "desde"
    - juego nuevo, o que cambió de porcentaje      -> "desde" = ahora
    - juegos que ya existían antes de esta mejora  -> "desde" = 0
      (0 significa "no se sabe cuándo entró", y la app no los muestra
      en la sección de novedades)

Variables de entorno que este script espera (las pone el workflow de
GitHub Actions, ver escanear.yml):
    GOOGLE_APPLICATION_CREDENTIALS  -> ruta al JSON de la cuenta de servicio de Firebase
    FIREBASE_DATABASE_URL           -> ej: https://gameradar-xxxxx-default-rtdb.firebaseio.com

Dependencias (ver requirements.txt):
    requests, beautifulsoup4, firebase-admin
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, db

import scraper  # el mismo scraper.py de siempre, copiado en esta carpeta

# --------------------------------------------------------------------
# Las mismas 10 monedas que ya soporta la app (ver MONEDAS en
# MainActivity.kt). El "free_badge" no varía por moneda: el texto que
# ya usaba PythonBridge.escanearOfertas() siempre era "¡GRATIS!" por
# defecto (nunca se pasaba otro valor desde Kotlin), así que se deja
# igual acá para no cambiar el comportamiento.
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
# aumenta el riesgo de que te empiecen a bloquear (rate limiting), lo
# que dañaría los datos de TODAS las monedas a la vez. 3 es un punto
# medio razonable entre velocidad y no abusar de esos sitios.
MONEDAS_EN_PARALELO = 3


def cargar_previos(cc_code):
    """
    Lee de Firebase la lista del escaneo anterior y la devuelve como
    {link: juego}. Devuelve None si nunca se había subido nada para esta
    moneda (primer escaneo).
    """
    previo = db.reference(f"/ofertas/{cc_code}/juegos").get()
    if not previo:
        return None
    # Firebase devuelve una lista, o un dict si la lista quedó con huecos.
    if isinstance(previo, dict):
        previo = previo.values()
    return {
        j["link"]: j
        for j in previo
        if isinstance(j, dict) and j.get("link")
    }


def marcar_fechas(juegos, previos, ahora_ms):
    """Agrega el campo "desde" a cada juego según el escaneo anterior."""
    for juego in juegos:
        if previos is None:
            # Primer escaneo de esta moneda: no se sabe cuándo entró nada.
            juego["desde"] = 0
            continue

        anterior = previos.get(juego.get("link"))
        if anterior is not None and anterior.get("desc") == juego.get("desc"):
            # Mismo juego, mismo descuento: conserva la fecha original.
            juego["desde"] = anterior.get("desde", 0)
        else:
            # Juego nuevo en la lista, o cambió su porcentaje de descuento.
            juego["desde"] = ahora_ms


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

    if not juegos:
        # Un escaneo vacío casi siempre es un bloqueo o un fallo temporal.
        # Si se subiera, borraría la lista buena que ya está en Firebase
        # y el próximo escaneo marcaría TODO como "nuevo".
        print(f"[{cc_code}] 0 ofertas encontradas; no se sube nada para no borrar lo anterior.")
        return

    previos = cargar_previos(cc_code)
    ahora_ms = int(time.time() * 1000)
    marcar_fechas(juegos, previos, ahora_ms)
    nuevos = sum(1 for j in juegos if j["desde"] == ahora_ms)
    print(f"[{cc_code}] {len(juegos)} ofertas ({nuevos} nuevas o con descuento distinto), subiendo a Firebase...")

    db.reference(f"/ofertas/{cc_code}").set({
        "actualizado": datetime.now(timezone.utc).isoformat(),
        "total": len(juegos),
        "juegos": juegos,
    })
    print(f"[{cc_code}] listo.")


def main():
    cred = credentials.Certificate(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    firebase_admin.initialize_app(cred, {
        "databaseURL": os.environ["FIREBASE_DATABASE_URL"],
    })

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
                # Si una moneda falla, las demás igual deben subir su parte,
                # y NO se debe borrar lo que ya había en Firebase para esa moneda.
                print(f"[{cc_code}] ERROR: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()

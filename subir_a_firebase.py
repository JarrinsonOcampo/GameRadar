"""
subir_a_firebase.py — corre en GitHub Actions, NO en el celular del usuario.

Reutiliza scraper.py (el mismo que usa la app con Chaquopy, sin cambiarle
nada) para escanear Steam/Epic/GOG y sube el resultado a Firebase
Realtime Database, en /ofertas/{cc_code}. La app en Kotlin solo lee ese
JSON ya armado (ver FirebaseOfertasRepo.kt), así que la carga es casi
instantánea para el usuario.

Además de lo que devuelve el scraper, este script agrega a cada juego el
campo "desde" (milisegundos): el momento en que se detectó su descuento
actual. La pantalla de "Últimos descuentos" de la app se basa en él.
    - juego nuevo en la lista, o cambió su %  -> desde = ahora
    - mismo juego con el mismo %              -> se conserva el "desde" anterior
    - primera corrida de una moneda (sin datos previos) -> desde = 0
      ("ya estaba antes"), para no marcar cientos de juegos como novedad.

Variables de entorno que este script espera (las pone el workflow de
GitHub Actions, ver escanear.yml):
    GOOGLE_APPLICATION_CREDENTIALS  -> ruta al JSON de la cuenta de servicio de Firebase
    FIREBASE_DATABASE_URL           -> ej: https://gameradar-xxxxx-default-rtdb.firebaseio.com

Dependencias (ver requirements.txt):
    requests, beautifulsoup4, firebase-admin
"""

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


def _cargar_previos(ref):
    """Devuelve {link: juego} con lo que ya está en Firebase para esa moneda.
    Si Firebase falla, la excepción se propaga a propósito: es mejor no subir nada
    que subir con fechas 'desde' perdidas."""
    previos = ref.child("juegos").get()
    if isinstance(previos, dict):        # Firebase a veces devuelve listas como dict
        previos = list(previos.values())
    return {
        j.get("link"): j
        for j in (previos or [])
        if isinstance(j, dict) and j.get("link")
    }


def _asignar_desde(juegos, previos):
    ahora_ms = int(time.time() * 1000)
    for j in juegos:
        anterior = previos.get(j.get("link"))
        if anterior is not None and anterior.get("desc") == j.get("desc"):
            j["desde"] = anterior.get("desde", 0)      # sin cambios: conserva la fecha
        elif not previos:
            j["desde"] = 0                             # primera corrida: "ya estaba antes"
        else:
            j["desde"] = ahora_ms                      # juego nuevo o cambió el porcentaje


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

    ref = db.reference(f"/ofertas/{cc_code}")
    _asignar_desde(juegos, _cargar_previos(ref))

    print(f"[{cc_code}] {len(juegos)} ofertas encontradas, subiendo a Firebase...")
    ref.set({
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

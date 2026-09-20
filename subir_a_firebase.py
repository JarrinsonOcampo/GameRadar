"""
subir_a_firebase.py — corre en GitHub Actions, NO en el celular del usuario.

Reutiliza scraper.py (el mismo que ya tenías en Chaquopy, sin cambiarle
nada) para escanear Steam/Epic/GOG y sube el resultado a Firebase
Realtime Database, en /ofertas/{cc_code}. La app en Kotlin solo lee ese
JSON ya armado (ver FirebaseOfertasRepo.kt), así que la carga es casi
instantánea para el usuario.

Variables de entorno que este script espera (las pone el workflow de
GitHub Actions, ver escanear.yml):
    GOOGLE_APPLICATION_CREDENTIALS  -> ruta al JSON de la cuenta de servicio de Firebase
    FIREBASE_DATABASE_URL           -> ej: https://gameradar-xxxxx-default-rtdb.firebaseio.com

Dependencias (ver requirements.txt):
    requests, beautifulsoup4, firebase-admin
"""

import json
import os
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, db

import scraper  # el mismo scraper.py de siempre, copiado en esta carpeta

# --------------------------------------------------------------------
# Monedas que el backend deja listas en Firebase. Empieza con las que
# más usan tus usuarios y agrega más líneas cuando quieras cubrir otra
# moneda (cada una agrega tiempo de escaneo, pero no bloquea al usuario
# porque esto corre en la nube, no en su celular).
# Formato: (cc_code, symbol, no_decimals, gog_currency, free_badge)
# --------------------------------------------------------------------
MONEDAS = [
    ("co", "COL$", True, "USD", "¡GRATIS!"),   # Colombia
    ("us", "$", False, "USD", "FREE"),         # Estados Unidos / genérico USD
]


def escanear_y_subir(cc_code, symbol, no_decimals, gog_currency, free_badge):
    print(f"[{cc_code}] escaneando Steam + Epic + GOG...")
    json_texto = scraper.get_game_deals(
        cc_code=cc_code,
        symbol=symbol,
        no_decimals=no_decimals,
        gog_currency=gog_currency,
        free_badge=free_badge,
        tiendas="steam,epic,gog",
    )
    juegos = json.loads(json_texto)
    print(f"[{cc_code}] {len(juegos)} ofertas encontradas, subiendo a Firebase...")

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

    for cc_code, symbol, no_decimals, gog_currency, free_badge in MONEDAS:
        try:
            escanear_y_subir(cc_code, symbol, no_decimals, gog_currency, free_badge)
        except Exception as e:
            # Si una moneda falla, las demás igual deben subir su parte,
            # y NO se debe borrar lo que ya había en Firebase para esa moneda.
            print(f"[{cc_code}] ERROR: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()

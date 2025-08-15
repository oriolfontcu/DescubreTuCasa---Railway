import os, json, time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Query
from supabase import create_client, Client

app = FastAPI()
load_dotenv()

# ========= Config =========
N8N_URL = os.getenv("N8N_URL", "https://primary-production-eebd.up.railway.app/webhook/ddf4ff06-3b31-4bdb-a349-c2884a5402d3")

CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY")
CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Usa Service Role Key
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE = "https://open.tiktokapis.com/v2"

# Campos seguros para /video/list/
VIDEO_FIELDS: List[str] = [
    "id",
    "title",
    "create_time",
    "cover_image_url",
    "share_url",
    "view_count",
    "like_count",
    "comment_count",
    "share_count",
]

# ========= Acceso a tokens en Supabase =========
def get_tokens_row() -> Optional[Dict[str, Any]]:
    """
    Lee (si existe) la fila de tokens del provider 'tiktok'.
    """
    res = supabase.table("tokens") \
        .select("provider, account_open_id, access_token, refresh_token, scope, expires_at, updated_at") \
        .eq("provider", "tiktok") \
        .limit(1) \
        .execute()
    rows = res.data or []
    return rows[0] if rows else None

def upsert_tokens(
    access_token: Optional[str] = None,
    refresh_token: Optional[str] = None,
    scope: Optional[str] = None,
    expires_in: Optional[int] = None,
    account_open_id: Optional[str] = None,
) -> None:
    """
    Inserta/actualiza tokens en la fila provider='tiktok'.
    expires_in: segundos desde ahora (si viene de TikTok).
    """
    payload: Dict[str, Any] = {"provider": "tiktok"}

    if access_token is not None:
        payload["access_token"] = access_token
    if refresh_token is not None:
        payload["refresh_token"] = refresh_token
    if scope is not None:
        payload["scope"] = scope

    if expires_in is not None:
        expires_at = int(time.time()) + int(expires_in) - 60  # margen de 60s
        payload["expires_at"] = expires_at

    if account_open_id is not None:
        payload["account_open_id"] = account_open_id

    payload["updated_at"] = datetime.now(timezone.utc).isoformat()

    supabase.table("tokens").upsert(payload, on_conflict="provider").execute()

def get_valid_access_token() -> str:
    """
    Devuelve un access_token válido. Si está caducado o no existe, intenta refrescar.
    """
    row = get_tokens_row()
    if not row:
        raise RuntimeError("No hay tokens guardados en Supabase (provider='tiktok'). Realiza primero el intercambio de authorization_code.")

    now = int(time.time())
    access_token = row.get("access_token")
    expires_at = row.get("expires_at") or 0

    if access_token and now < int(expires_at):
        return access_token

    # Necesitamos refrescar
    refresh_token = row.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("No hay refresh_token guardado. Reautoriza la app.")

    return refresh_access_token(refresh_token)

def refresh_access_token(refresh_token: str) -> str:
    """
    Pide un nuevo access_token usando refresh_token y guarda los nuevos datos en Supabase.
    """
    resp = requests.post(
        f"{BASE}/oauth/token/",
        data={
            "client_key": CLIENT_KEY,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        try:
            body = resp.json()
        except Exception:
            body = {"text": resp.text}
        raise HTTPException(status_code=resp.status_code, detail={"oauth_refresh_error": body})

    data = resp.json()
    # TikTok suele devolver: access_token, refresh_token (puede rotar), expires_in, scope, ...
    access_token = data["access_token"]
    new_refresh = data.get("refresh_token", refresh_token)
    scope = data.get("scope")
    expires_in = int(data.get("expires_in", 3600))

    upsert_tokens(
        access_token=access_token,
        refresh_token=new_refresh,
        scope=scope,
        expires_in=expires_in,
    )
    return access_token

# ========= Llamadas API TikTok =========
def api_post(path: str, access_token: str, body: dict):
    """POST helper con logging de errores detallado."""
    url = f"{BASE}{path}?fields={','.join(VIDEO_FIELDS)}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, data=json.dumps(body))
    if resp.status_code >= 400:
        # Muestra el texto que devuelve TikTok para entender el 400
        try:
            print("TikTok error body:", json.dumps(resp.json(), ensure_ascii=False, indent=2))
        except Exception:
            print("TikTok error text:", resp.text)
        resp.raise_for_status()
    return resp.json()

def fetch_all_videos(max_count: int = 20) -> List[dict]:
    """
    Pagina todos los videos del usuario autenticado (owner).
    """
    token = get_valid_access_token()
    all_items: List[dict] = []
    cursor: Optional[str] = None
    page = 1

    while True:
        body = {
            "max_count": max_count,
            "fields": VIDEO_FIELDS,
        }
        if cursor:
            body["cursor"] = cursor

        print(f"[video.list] page={page} body={body}")
        data = api_post("/video/list/", token, body)

        # Estructura esperada: { "data": { "videos": [...], "cursor": "...", "has_more": true/false } }
        videos = ((data or {}).get("data") or {}).get("videos") or []
        all_items.extend(videos)

        meta = (data or {}).get("data") or {}
        has_more = meta.get("has_more", False)
        cursor = meta.get("cursor")
        print(f"[video.list] got={len(videos)} has_more={has_more} next_cursor={cursor}")

        if not has_more or not cursor:
            break

        page += 1

    print(f"[video.list] total videos: {len(all_items)}")
    return all_items

# ========= n8n =========
def call_n8n() -> dict:
    videos = fetch_all_videos(max_count=20)
    resp = requests.post(
        N8N_URL,
        json={"items": videos},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    return {"n8n_status": resp.status_code, "n8n_text": resp.text[:500], "count": len(videos)}

# ========= Endpoints =========
@app.get("/")
def run_now():
    """
    Dispara el flujo: asegura token válido, obtiene todos los vídeos y los manda a n8n.
    """
    try:
        result = call_n8n()
        return {"ok": True, **result}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/oauth/tiktok/callback")
def oauth_callback(code: str = Query(...), state: Optional[str] = Query(None)):
    """
    Intercambia el authorization_code por tokens y los guarda en Supabase.
    Luego obtiene el open_id con /user/info y lo guarda en la fila.
    """
    # 1) Intercambiar code -> tokens
    token_resp = requests.post(
        f"{BASE}/oauth/token/",
        data={
            "client_key": CLIENT_KEY,
            "client_secret": CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            # El redirect_uri debe EXACTAMENTE coincidir con el registrado en TikTok
            "redirect_uri": os.getenv("TIKTOK_REDIRECT_URI"),
        },
        timeout=30,
    )
    if token_resp.status_code >= 400:
        try:
            body = token_resp.json()
        except Exception:
            body = {"text": token_resp.text}
        raise HTTPException(status_code=token_resp.status_code, detail={"oauth_code_error": body})

    tok = token_resp.json()
    access_token = tok["access_token"]
    refresh_token = tok.get("refresh_token")
    scope = tok.get("scope")
    expires_in = int(tok.get("expires_in", 3600))

    # 2) Guardar tokens
    upsert_tokens(
        access_token=access_token,
        refresh_token=refresh_token,
        scope=scope,
        expires_in=expires_in,
    )

    # 3) Obtener open_id del usuario y guardarlo
    headers = {"Authorization": f"Bearer {access_token}"}
    info = requests.get(
        f"{BASE}/user/info/?fields=open_id,display_name,avatar_url",
        headers=headers,
        timeout=30,
    )
    if info.status_code < 400:
        user = (info.json().get("data") or {}).get("user") or {}
        open_id = user.get("open_id")
        if open_id:
            upsert_tokens(account_open_id=open_id)

    return {"ok": True, "saved": True, "state": state}

@app.get("/refresh")
def force_refresh():
    """
    Fuerza un refresh con el refresh_token guardado.
    """
    row = get_tokens_row()
    if not row or not row.get("refresh_token"):
        raise HTTPException(status_code=400, detail="No hay refresh_token guardado.")
    new_access = refresh_access_token(row["refresh_token"])
    return {"ok": True, "access_token_prefix": new_access[:12], "expires_at": get_tokens_row().get("expires_at")}

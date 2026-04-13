# Deploy — Pasos finales

## 1. Push inicial a GitHub (una sola vez, desde tu terminal)

```bash
cd ~/ruta/a/arch_monitor   # la carpeta donde está app.py

git init
git branch -M main
git remote add origin https://github.com/arch-protocol/dashboard_outflows.git
git add -A
git commit -m "feat: initial deploy"
git push -u origin main
```

> Si pide usuario/contraseña, usa tu usuario de GitHub y un
> **Personal Access Token** (no tu contraseña).
> Generalo en: https://github.com/settings/tokens → New token → repo

---

## 2. Agregar la Dune API key como Secret en GitHub

1. Ve a https://github.com/arch-protocol/dashboard_outflows/settings/secrets/actions
2. Click **New repository secret**
3. Name: `DUNE_API_KEY`
4. Value: tu API key de Dune
5. Click **Add secret**

> Esto permite que el workflow corra el pipeline sin exponer la clave.

---

## 3. Deploy en Streamlit Cloud (gratis, privado)

1. Ve a https://share.streamlit.io
2. Login con GitHub
3. Click **New app**
4. Repository: `arch-protocol/dashboard_outflows`
5. Branch: `main`
6. Main file path: `app.py`
7. Click **Deploy**

En "Advanced settings" podés agregar también la variable:
- `DUNE_API_KEY` = tu API key (por si la app la necesita en el futuro)

---

## 4. Cómo funciona la automatización

```
Cada día a las 9am Santiago
    ↓
GitHub Actions corre pipeline.py con la Dune API key
    ↓
Actualiza data/processed/ en el repo
    ↓
Streamlit Cloud detecta el nuevo commit y refresca los datos
    ↓
Abrís el dashboard y ves datos del día
```

También podés triggerearlo manualmente en cualquier momento:
https://github.com/arch-protocol/dashboard_outflows/actions → "Daily Pipeline Update" → "Run workflow"

---

## 5. Correr localmente

```bash
# Solo el dashboard (con datos ya procesados):
streamlit run app.py

# Actualizar datos desde Dune:
python pipeline.py --wallets wallets/clientes.xlsx --pref-csv wallets/Preferentes.csv --retail-csv wallets/Retail.csv
```

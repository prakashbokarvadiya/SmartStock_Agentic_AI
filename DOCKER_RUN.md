# SmartStock — Docker se Run Kaise Kare

## Step 1: Docker Desktop Start Karo

1. **Start Menu** mein search karo: `Docker Desktop`
2. Open karo — system tray mein whale icon aayega 🐳
3. **"Docker Desktop is running"** message aane tak wait karo (~30-60 seconds)

---

## Step 2: Project Folder Mein Jao (PowerShell/CMD)

```powershell
cd "E:\tops\Data science\Project\19_SmartStock_Agentic_AI"
```

---

## Step 3: Pehli Baar — Image Build Karo + Run Karo

```powershell
docker compose up --build
```

> ⏳ **Pehli baar 3-5 minute lagta hai** (Python packages download hote hain)
> Baad mein sirf `docker compose up` se seconds mein start hoga

---

## Step 4: App Access Karo

Browser mein open karo:

| Page | URL |
|------|-----|
| Home (Chat) | http://localhost:8000 |
| Products | http://localhost:8000/product |
| Orders | http://localhost:8000/order |
| Health Check | http://localhost:8000/health |

---

## Useful Commands

```powershell
# Background mein run karo (terminal free rahega)
docker compose up --build -d

# Live logs dekhne ke liye
docker compose logs -f app

# Band karne ke liye
docker compose down

# Dobara start karne ke liye (rebuild nahi, fast)
docker compose up

# Container ke andar jaane ke liye (debugging)
docker compose exec app bash

# Purana image delete karke fresh build
docker compose down && docker compose up --build
```

---

## Agar Error Aaye

### ❌ "port 8000 already in use"
```powershell
# Pehle purana uvicorn band karo, phir:
docker compose up
```

### ❌ "cannot find credentials.json"
```powershell
# credentials.json project folder mein hona chahiye
# Gmail setup ke liye: python email_automate.py
```

### ❌ Build fail ho
```powershell
# Logs dekho
docker compose build --no-cache
```

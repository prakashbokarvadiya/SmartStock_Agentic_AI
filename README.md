# SmartStock Agentic AI

> **Full-stack Inventory & Order Management System** powered by LangGraph Agentic AI, FastAPI, MCP Tools, and Supabase PostgreSQL.

---

## 🚀 Features

- 🤖 **Agentic AI Chatbot** — LangGraph + Gemini LLM with tool-calling (ask anything about products/orders in Hindi, English, or Hinglish)
- 🛠️ **MCP Tools** — Model Context Protocol server with 10+ database tools (get_products, get_orders, execute_sql_query, etc.)
- 📦 **Product Management** — Add, update, delete, search products
- 📋 **Order Management** — Add, update, delete, filter orders by status/customer
- 🔔 **Low Stock Email Alerts** — Gmail API + Gemini-generated emails, auto-monitors every 5 minutes
- 🧠 **RAG Memory** — ChromaDB vector store for semantic chat history retrieval
- 🐳 **Docker Ready** — Multi-stage Dockerfile + docker-compose

---

## 🏗️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI + Uvicorn |
| AI Agent | LangGraph + Google Gemini |
| Tools | MCP (Model Context Protocol) |
| Database | Supabase PostgreSQL + SQLAlchemy |
| Vector Store | ChromaDB |
| Email | Gmail API + OAuth2 |
| Frontend | HTML + Jinja2 Templates |
| Container | Docker + Docker Compose |

---

## 📁 Project Structure

```
SmartStock_Agentic_AI/
├── app.py               # FastAPI app + LangGraph agent
├── mcp_server.py        # MCP tool server (DB tools)
├── email_automate.py    # Stock monitor + Gmail alerts
├── schema.sql           # PostgreSQL schema
├── templates/           # Jinja2 HTML templates
│   ├── agentic_ai.html  # AI Chat UI
│   ├── product.html     # Product CRUD
│   └── order.html       # Order CRUD
├── Dockerfile           # Multi-stage Docker build
├── docker-compose.yml   # Docker Compose config
├── requirements.txt     # Python dependencies
├── .env.example         # Environment template
└── DOCKER_RUN.md        # Docker run instructions
```

---

## ⚙️ Setup

### 1. Environment Variables

```bash
cp .env.example .env
# .env mein apni keys fill karo
```

`.env` mein required variables:
```env
GEMINI_API_KEY=your_gemini_api_key
DATABASE_URL=postgresql://user:pass@host:5432/db
ADMIN_EMAIL=admin@example.com
LOW_STOCK_THRESHOLD=3
```

### 2. Run with Docker (Recommended)

```bash
# Pehli baar — build + run
docker compose up --build

# Background mein
docker compose up --build -d

# Logs
docker compose logs -f app

# Band karo
docker compose down
```

### 3. Run Locally (Development)

```bash
pip install -r requirements.txt
python -m uvicorn app:app --reload
```

---

## 🌐 Pages

| URL | Description |
|-----|------------|
| `http://localhost:8000` | AI Chat Interface |
| `http://localhost:8000/product` | Product Management |
| `http://localhost:8000/order` | Order Management |
| `http://localhost:8000/health` | Health Check API |

---

## 📧 Gmail Email Setup (One-time)

```bash
# credentials.json (Google Cloud Console se download karo)
python email_automate.py
# Browser mein OAuth consent dena hoga
```

---

## 📄 License

MIT License — free to use and modify.
